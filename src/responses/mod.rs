use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::response::IntoResponse;
use serde_json::Value;
use tracing::debug;

use crate::auth::LoadedUpstreamAuth;
use crate::errors::ThreadlineError;
use crate::registry::{RegistryAcquireError, RetainedSessionLease, RetainedSessionRegistry};
use crate::tools::inject_internal_tools;
use crate::ws_pump::LiveUpstreamWebSocket;

mod downstream;
mod translation;
mod upstream;

use self::downstream::parse_downstream_request;
use self::translation::{ResponseStreamState, response_stream};
use self::upstream::send_response_create;

pub use self::upstream::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};

pub const TURN_STATE_HEADER: &str = "x-codex-turn-state";

#[derive(Clone)]
pub struct ResponsesRouteState {
    pub registry: Arc<RetainedSessionRegistry>,
    pub services: ThreadlineServices,
}

pub async fn responses_handler(
    State(state): State<ResponsesRouteState>,
    axum::Json(payload): axum::Json<Value>,
) -> Result<impl IntoResponse, ThreadlineError> {
    let request = parse_downstream_request(payload)?;
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

    let stream = response_stream(
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

fn map_registry_error(error: RegistryAcquireError) -> ThreadlineError {
    match error {
        RegistryAcquireError::PreviousResponseNotFound => ThreadlineError::PreviousResponseNotFound,
        RegistryAcquireError::RetainedSessionConflict => ThreadlineError::RetainedSessionConflict,
        RegistryAcquireError::RetainedSessionCapacityExceeded => {
            ThreadlineError::RetainedSessionCapacityExceeded
        }
    }
}
