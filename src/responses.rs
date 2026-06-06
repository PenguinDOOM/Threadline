use std::mem;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::response::IntoResponse;
use futures_util::future::BoxFuture;
use futures_util::stream;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use tracing::debug;

use crate::auth::LoadedUpstreamAuth;
use crate::codex_ws::UpstreamSessionDescriptor;
use crate::errors::ThreadlineError;
use crate::registry::{RegistryAcquireError, RetainedSessionLease, RetainedSessionRegistry};
use crate::tools::{
    InternalToolCall, PendingInternalToolOutput, build_followup_input,
    event_contains_internal_tool_name, inject_internal_tools,
};
use crate::ws_pump::LiveUpstreamWebSocket;

pub const TURN_STATE_HEADER: &str = "x-codex-turn-state";

pub trait UpstreamAuthProvider: Send + Sync {
    fn load(&self) -> Result<LoadedUpstreamAuth, ThreadlineError>;
}

pub trait UpstreamConnector: Send + Sync {
    fn connect(
        &self,
        auth: LoadedUpstreamAuth,
        session: Option<UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>>;
}

#[derive(Clone)]
pub struct ThreadlineServices {
    auth_provider: Arc<dyn UpstreamAuthProvider>,
    connector: Arc<dyn UpstreamConnector>,
}

pub struct ConnectedUpstream {
    pub websocket: Arc<LiveUpstreamWebSocket>,
    pub session: UpstreamSessionDescriptor,
    pub turn_state: Option<String>,
}

#[derive(Clone)]
pub struct ResponsesRouteState {
    pub registry: Arc<RetainedSessionRegistry>,
    pub services: ThreadlineServices,
}

#[derive(Debug, Deserialize)]
struct DownstreamResponsesRequest {
    #[serde(default)]
    previous_response_id: Option<String>,
    #[serde(flatten)]
    payload: serde_json::Map<String, Value>,
}

struct ResponseStreamState {
    upstream: Arc<LiveUpstreamWebSocket>,
    lease: RetainedSessionLease,
    base_request: serde_json::Map<String, Value>,
    pending_internal_outputs: Vec<PendingInternalToolOutput>,
    done: bool,
}

impl ThreadlineServices {
    pub fn new(
        auth_provider: Arc<dyn UpstreamAuthProvider>,
        connector: Arc<dyn UpstreamConnector>,
    ) -> Self {
        Self {
            auth_provider,
            connector,
        }
    }

    pub fn auth_provider(&self) -> &Arc<dyn UpstreamAuthProvider> {
        &self.auth_provider
    }

    pub fn connector(&self) -> &Arc<dyn UpstreamConnector> {
        &self.connector
    }
}

pub async fn responses_handler(
    State(state): State<ResponsesRouteState>,
    axum::Json(payload): axum::Json<Value>,
) -> Result<impl IntoResponse, ThreadlineError> {
    let request = serde_json::from_value::<DownstreamResponsesRequest>(payload)
        .map_err(|_| ThreadlineError::InvalidResponsesRequest)?;
    let mut lease = acquire_lease(&state.registry, request.previous_response_id.as_deref()).await?;
    let auth = state.services.auth_provider().load()?;
    let upstream = ensure_upstream(&state.services, &mut lease, auth).await?;

    let mut upstream_request = request.payload;
    if let Some(previous_response_id) = &request.previous_response_id {
        upstream_request.insert(
            "previous_response_id".to_string(),
            Value::String(previous_response_id.clone()),
        );
    }
    inject_internal_tools(&mut upstream_request);
    send_response_create(&upstream, &upstream_request).await?;

    let stream = stream::unfold(
        ResponseStreamState {
            upstream,
            lease,
            base_request: upstream_request,
            pending_internal_outputs: Vec::new(),
            done: false,
        },
        |mut state| async move {
            loop {
                if state.done {
                    return None;
                }

                let next = match state.upstream.recv_text().await {
                    Ok(Some(text)) => text,
                    Ok(None) => {
                        state.lease.mark_upstream_recoverable().await;
                        state.done = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(
                                &ThreadlineError::UpstreamWebSocketClosed,
                            )),
                            state,
                        ));
                    }
                    Err(_) => {
                        state.lease.mark_upstream_recoverable().await;
                        state.done = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(
                                &ThreadlineError::UpstreamWebSocketClosed,
                            )),
                            state,
                        ));
                    }
                };

                let parsed = match serde_json::from_str::<Value>(&next) {
                    Ok(parsed) => parsed,
                    Err(_) => {
                        state.lease.mark_upstream_terminal().await;
                        state.done = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(
                                &ThreadlineError::UpstreamInvalidJson,
                            )),
                            state,
                        ));
                    }
                };

                let internal_tool_call = match InternalToolCall::from_event(&parsed) {
                    Ok(call) => call,
                    Err(error) => {
                        state.lease.mark_upstream_terminal().await;
                        state.done = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(&error)),
                            state,
                        ));
                    }
                };

                if let Some(call) = internal_tool_call {
                    match call.execute() {
                        Ok(output) => {
                            state.pending_internal_outputs.push(output);
                            continue;
                        }
                        Err(error) => {
                            state.lease.mark_upstream_terminal().await;
                            state.done = true;
                            return Some((
                                Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(&error)),
                                state,
                            ));
                        }
                    }
                }

                let event_type = parsed
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("message")
                    .to_string();

                if event_contains_internal_tool_name(&parsed) {
                    continue;
                }

                match event_type.as_str() {
                    "response.completed" => {
                        let response_id = parsed
                            .get("response")
                            .and_then(|response| response.get("id"))
                            .and_then(Value::as_str)
                            .map(ToString::to_string);

                        if let Some(response_id) = response_id.as_deref() {
                            state.lease.record_completed_marker(response_id).await;
                        }

                        if !state.pending_internal_outputs.is_empty() {
                            let Some(response_id) = response_id.as_deref() else {
                                let error = ThreadlineError::InternalToolFailed;
                                state.lease.mark_upstream_terminal().await;
                                state.done = true;
                                return Some((
                                    Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(&error)),
                                    state,
                                ));
                            };

                            let outputs = mem::take(&mut state.pending_internal_outputs);
                            if let Err(error) = send_followup_tool_outputs(
                                &state.upstream,
                                &state.base_request,
                                response_id,
                                outputs,
                            )
                            .await
                            {
                                state.lease.mark_upstream_terminal().await;
                                state.done = true;
                                return Some((
                                    Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(&error)),
                                    state,
                                ));
                            }
                            continue;
                        }

                        state.done = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_data_chunk(
                                &event_type,
                                &next,
                            )),
                            state,
                        ));
                    }
                    "response.failed" => {
                        state.lease.mark_upstream_terminal().await;
                        state.done = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(
                                &ThreadlineError::UpstreamResponseFailed,
                            )),
                            state,
                        ));
                    }
                    "error" => {
                        state.lease.mark_upstream_terminal().await;
                        state.done = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(
                                &ThreadlineError::UpstreamErrorEvent,
                            )),
                            state,
                        ));
                    }
                    _ => {
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_data_chunk(
                                &event_type,
                                &next,
                            )),
                            state,
                        ));
                    }
                }
            }
        },
    );

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        )
        .header(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"))
        .body(Body::from_stream(stream))
        .expect("build sse response");
    Ok(response)
}

async fn acquire_lease(
    registry: &RetainedSessionRegistry,
    previous_response_id: Option<&str>,
) -> Result<RetainedSessionLease, ThreadlineError> {
    match previous_response_id {
        Some(previous_response_id) => registry
            .acquire_previous(previous_response_id)
            .await
            .map_err(map_registry_error),
        None => registry.acquire_new().await.map_err(map_registry_error),
    }
}

async fn ensure_upstream(
    services: &ThreadlineServices,
    lease: &mut RetainedSessionLease,
    auth: LoadedUpstreamAuth,
) -> Result<Arc<LiveUpstreamWebSocket>, ThreadlineError> {
    if let Some(upstream) = lease.upstream() {
        if !upstream.is_closed() {
            return Ok(upstream);
        }

        lease.mark_upstream_recoverable().await;
    }

    let connected = services
        .connector()
        .connect(auth, Some(lease.session().clone()))
        .await?;
    lease.update_turn_state(connected.turn_state.clone()).await;
    lease
        .replace_upstream(Some(Arc::clone(&connected.websocket)))
        .await;
    Ok(connected.websocket)
}

async fn send_response_create(
    upstream: &LiveUpstreamWebSocket,
    response_payload: &serde_json::Map<String, Value>,
) -> Result<(), ThreadlineError> {
    let outbound = json!({
        "type": "response.create",
        "response": Value::Object(response_payload.clone()),
    });
    upstream
        .send_text(outbound.to_string())
        .await
        .map_err(|_| ThreadlineError::UpstreamWebSocketClosed)
}

async fn send_followup_tool_outputs(
    upstream: &LiveUpstreamWebSocket,
    base_request: &serde_json::Map<String, Value>,
    previous_response_id: &str,
    outputs: Vec<PendingInternalToolOutput>,
) -> Result<(), ThreadlineError> {
    let output_count = outputs.len();
    let mut response_payload = base_request.clone();
    response_payload.insert(
        "previous_response_id".to_string(),
        Value::String(previous_response_id.to_string()),
    );
    response_payload.insert("input".to_string(), build_followup_input(outputs));
    send_response_create(upstream, &response_payload).await?;
    debug!(
        previous_response_id = %previous_response_id,
        output_count,
        "internal_tool_followup_sent"
    );
    Ok(())
}

fn map_registry_error(error: RegistryAcquireError) -> ThreadlineError {
    match error {
        RegistryAcquireError::PreviousResponseNotFound => ThreadlineError::PreviousResponseNotFound,
        RegistryAcquireError::RetainedSessionConflict => ThreadlineError::RetainedSessionConflict,
        RegistryAcquireError::RetainedSessionCapacityExceeded => {
            ThreadlineError::RetainedSessionCapacityExceeded
        }
    }
}

fn sse_data_chunk(event: &str, payload: &str) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {payload}\n\n"))
}

fn sse_error_chunk(error: &ThreadlineError) -> Bytes {
    let payload = serde_json::to_string(&error.public_error_document())
        .expect("serialize threadline error payload");
    sse_data_chunk("error", &payload)
}
