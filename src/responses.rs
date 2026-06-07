use std::mem;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::response::IntoResponse;
use futures_util::future::BoxFuture;
use futures_util::stream;
use serde::Deserialize;
use serde_json::{Map, Value};
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
    services: ThreadlineServices,
    upstream: Arc<LiveUpstreamWebSocket>,
    lease: RetainedSessionLease,
    base_request: serde_json::Map<String, Value>,
    pending_internal_outputs: Vec<PendingInternalToolOutput>,
    previous_response_id: Option<String>,
    upstream_event_seen: bool,
    reconnect_attempted: bool,
    final_done_pending: bool,
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
    let mut upstream = ensure_upstream(&state.services, &mut lease, auth).await?;

    let mut upstream_request = request.payload;
    if let Some(previous_response_id) = &request.previous_response_id {
        upstream_request.insert(
            "previous_response_id".to_string(),
            Value::String(previous_response_id.clone()),
        );
    }
    inject_internal_tools(&mut upstream_request);
    let mut reconnect_attempted = false;
    if let Err(error) = send_response_create(&upstream, &upstream_request).await {
        if let Some(reconnected) = attempt_pre_first_event_reconnect(
            &state.services,
            &mut lease,
            &upstream_request,
            request.previous_response_id.as_deref(),
            false,
            &mut reconnect_attempted,
        )
        .await?
        {
            upstream = reconnected;
        } else {
            return Err(error);
        }
    }

    let stream = stream::unfold(
        ResponseStreamState {
            services: state.services.clone(),
            upstream,
            lease,
            base_request: upstream_request,
            pending_internal_outputs: Vec::new(),
            previous_response_id: request.previous_response_id,
            upstream_event_seen: false,
            reconnect_attempted,
            final_done_pending: false,
            done: false,
        },
        |mut state| async move {
            loop {
                if state.final_done_pending {
                    state.final_done_pending = false;
                    state.done = true;
                    debug!("downstream_sse_done_sent");
                    return Some((
                        Ok::<Bytes, std::convert::Infallible>(sse_done_chunk()),
                        state,
                    ));
                }

                if state.done {
                    debug!("downstream_sse_stream_finished");
                    return None;
                }

                let next = match state.upstream.recv_text().await {
                    Ok(Some(text)) => text,
                    Ok(None) => {
                        match attempt_pre_first_event_reconnect(
                            &state.services,
                            &mut state.lease,
                            &state.base_request,
                            state.previous_response_id.as_deref(),
                            state.upstream_event_seen,
                            &mut state.reconnect_attempted,
                        )
                        .await
                        {
                            Ok(Some(reconnected)) => {
                                state.upstream = reconnected;
                                continue;
                            }
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
                            Err(error) => {
                                state.done = true;
                                return Some((
                                    Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(&error)),
                                    state,
                                ));
                            }
                        }
                    }
                    Err(_) => {
                        match attempt_pre_first_event_reconnect(
                            &state.services,
                            &mut state.lease,
                            &state.base_request,
                            state.previous_response_id.as_deref(),
                            state.upstream_event_seen,
                            &mut state.reconnect_attempted,
                        )
                        .await
                        {
                            Ok(Some(reconnected)) => {
                                state.upstream = reconnected;
                                continue;
                            }
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
                            Err(error) => {
                                state.done = true;
                                return Some((
                                    Ok::<Bytes, std::convert::Infallible>(sse_error_chunk(&error)),
                                    state,
                                ));
                            }
                        }
                    }
                };

                state.upstream_event_seen = true;

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

                debug!(event_type, "upstream_event_received");

                if event_type.starts_with("response.output_item.")
                    && event_contains_internal_tool_name(&parsed)
                {
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

                        debug!(response_id, "final_response_completed");
                        state.final_done_pending = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(sse_json_chunk(
                                &event_type,
                                &parsed,
                            )),
                            state,
                        ));
                    }
                    "response.failed" => {
                        state.lease.mark_upstream_recoverable().await;
                        state.final_done_pending = true;
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(
                                sse_terminal_response_failed_chunk(&parsed),
                            ),
                            state,
                        ));
                    }
                    "error" => {
                        let error = parsed.get("error");
                        let error_code = error
                            .and_then(|value| value.get("code"))
                            .and_then(safe_scalar_field);
                        let error_message = error
                            .and_then(|value| value.get("message"))
                            .and_then(safe_scalar_field);
                        let status = parsed
                            .get("status")
                            .or_else(|| parsed.get("status_code"))
                            .and_then(safe_scalar_field);

                        debug!(
                            event_type,
                            error_code, error_message, status, "upstream_error_event"
                        );
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
                            Ok::<Bytes, std::convert::Infallible>(sse_json_chunk(
                                &event_type,
                                &parsed,
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

async fn attempt_pre_first_event_reconnect(
    services: &ThreadlineServices,
    lease: &mut RetainedSessionLease,
    request_payload: &serde_json::Map<String, Value>,
    previous_response_id: Option<&str>,
    upstream_event_seen: bool,
    reconnect_attempted: &mut bool,
) -> Result<Option<Arc<LiveUpstreamWebSocket>>, ThreadlineError> {
    let Some(previous_response_id) = previous_response_id else {
        return Ok(None);
    };

    if upstream_event_seen || *reconnect_attempted {
        return Ok(None);
    }

    *reconnect_attempted = true;
    lease.mark_upstream_recoverable().await;
    debug!(
        previous_response_id,
        session_id = %lease.session().session_id,
        thread_id = %lease.session().thread_id,
        window_id = %lease.session().window_id,
        "reconnect_continuation_attempt"
    );

    let auth = services.auth_provider().load()?;
    let upstream = match ensure_upstream(services, lease, auth).await {
        Ok(upstream) => upstream,
        Err(error) => {
            debug!(
                previous_response_id,
                session_id = %lease.session().session_id,
                thread_id = %lease.session().thread_id,
                "reconnect_continuation_failed"
            );
            return Err(error);
        }
    };

    if let Err(error) = send_response_create(&upstream, request_payload).await {
        debug!(
            previous_response_id,
            session_id = %lease.session().session_id,
            thread_id = %lease.session().thread_id,
            "reconnect_continuation_failed"
        );
        return Err(error);
    }

    Ok(Some(upstream))
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
    let turn_state = connected
        .turn_state
        .clone()
        .or_else(|| lease.session().turn_state.clone());
    lease.update_turn_state(turn_state).await;
    lease
        .replace_upstream(Some(Arc::clone(&connected.websocket)))
        .await;
    Ok(connected.websocket)
}

async fn send_response_create(
    upstream: &LiveUpstreamWebSocket,
    response_payload: &serde_json::Map<String, Value>,
) -> Result<(), ThreadlineError> {
    let mut outbound = response_payload.clone();
    remove_codex_unsupported_response_fields(&mut outbound);
    outbound.insert("store".to_string(), Value::Bool(false));
    match outbound.get("instructions") {
        Some(Value::Null) | None => {
            outbound.insert("instructions".to_string(), Value::String(String::new()));
        }
        Some(_) => {}
    }
    outbound.insert(
        "type".to_string(),
        Value::String("response.create".to_string()),
    );
    upstream
        .send_text(Value::Object(outbound).to_string())
        .await
        .map_err(|_| ThreadlineError::UpstreamWebSocketClosed)
}

const CODEX_UNSUPPORTED_RESPONSE_FIELDS: [&str; 4] = [
    "max_output_tokens",
    "max_tokens",
    "max_completion_tokens",
    "truncation",
];

fn remove_codex_unsupported_response_fields(payload: &mut serde_json::Map<String, Value>) {
    for field_name in CODEX_UNSUPPORTED_RESPONSE_FIELDS {
        payload.remove(field_name);
    }
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

fn sse_payload_chunk(event: &str, payload: &str) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {payload}\n\n"))
}

fn sse_json_chunk(event: &str, payload: &Value) -> Bytes {
    let payload = serde_json::to_string(payload).expect("serialize downstream sse payload");
    sse_payload_chunk(event, &payload)
}

fn sse_done_chunk() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}

fn sse_terminal_response_failed_chunk(payload: &Value) -> Bytes {
    let fallback = ThreadlineError::UpstreamResponseFailed.public_error();
    let error = payload.get("error");
    let mut response = Map::new();

    if let Some(response_id) = payload
        .get("response")
        .and_then(|value| value.get("id"))
        .and_then(safe_scalar_field)
    {
        response.insert("id".to_string(), Value::String(response_id));
    }

    response.insert("status".to_string(), Value::String("failed".to_string()));
    response.insert(
        "error".to_string(),
        Value::Object(Map::from_iter([
            (
                "code".to_string(),
                Value::String(
                    error
                        .and_then(|value| value.get("code"))
                        .and_then(safe_scalar_field)
                        .unwrap_or_else(|| fallback.code.into_owned()),
                ),
            ),
            (
                "message".to_string(),
                Value::String(
                    error
                        .and_then(|value| value.get("message"))
                        .and_then(safe_scalar_field)
                        .unwrap_or_else(|| fallback.message.into_owned()),
                ),
            ),
        ])),
    );

    sse_json_chunk(
        "response.failed",
        &Value::Object(Map::from_iter([
            (
                "type".to_string(),
                Value::String("response.failed".to_string()),
            ),
            ("response".to_string(), Value::Object(response)),
        ])),
    )
}

fn safe_scalar_field(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn sse_error_chunk(error: &ThreadlineError) -> Bytes {
    let payload = serde_json::to_value(error.public_error_document())
        .expect("convert threadline error payload to json value");
    sse_json_chunk("error", &payload)
}
