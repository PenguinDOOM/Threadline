use std::sync::Arc;

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde_json::Value;
use tokio_tungstenite::connect_async;

use crate::auth::{AuthDiscoveryOptions, load_upstream_auth};
use crate::codex_ws::build_handshake_request;
use crate::config::ThreadlineConfig;
use crate::errors::ThreadlineError;
use crate::registry::RetainedSessionRegistry;
use crate::responses::{
    ConnectedUpstream, ResponsesRouteState, ThreadlineServices, responses_handler,
};
use crate::ws_pump::LiveUpstreamWebSocket;

const MODEL_CREATED_UNSPECIFIED: u64 = 0;

#[derive(Clone)]
struct AppState {
    config: ThreadlineConfig,
    responses: ResponsesRouteState,
}

#[derive(Serialize)]
struct HealthPayload {
    status: &'static str,
    service: &'static str,
}

#[derive(Serialize)]
struct ModelListPayload {
    object: &'static str,
    data: Vec<ModelEntry>,
}

#[derive(Serialize)]
struct ModelEntry {
    id: String,
    object: &'static str,
    created: u64,
    owned_by: &'static str,
}

pub fn build_router(config: ThreadlineConfig) -> Router {
    build_router_with_services(
        config,
        ThreadlineServices::new(
            Arc::new(DefaultAuthProvider),
            Arc::new(DefaultUpstreamConnector),
        ),
    )
}

pub fn build_router_with_services(
    config: ThreadlineConfig,
    services: ThreadlineServices,
) -> Router {
    let responses = ResponsesRouteState {
        registry: Arc::new(RetainedSessionRegistry::new(
            config.retained_session_capacity,
        )),
        services,
    };
    let state = AppState { config, responses };

    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(models))
        .route("/v1/responses", post(responses_route))
        .with_state(state)
}

async fn health() -> Json<HealthPayload> {
    Json(HealthPayload {
        status: "ok",
        service: "threadline",
    })
}

async fn models(State(state): State<AppState>) -> Json<ModelListPayload> {
    Json(ModelListPayload {
        object: "list",
        data: vec![ModelEntry {
            id: state.config.model_id,
            object: "model",
            created: MODEL_CREATED_UNSPECIFIED,
            owned_by: "threadline",
        }],
    })
}

async fn responses_route(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<impl axum::response::IntoResponse, ThreadlineError> {
    responses_handler(State(state.responses), Json(payload)).await
}

#[derive(Clone)]
struct DefaultAuthProvider;

impl crate::responses::UpstreamAuthProvider for DefaultAuthProvider {
    fn load(&self) -> Result<crate::auth::LoadedUpstreamAuth, ThreadlineError> {
        load_upstream_auth(&AuthDiscoveryOptions::from_env(None))
            .map_err(|_| ThreadlineError::UpstreamCredentialsUnavailable)
    }
}

#[derive(Clone)]
struct DefaultUpstreamConnector;

impl crate::responses::UpstreamConnector for DefaultUpstreamConnector {
    fn connect(
        &self,
        auth: crate::auth::LoadedUpstreamAuth,
        session: Option<crate::codex_ws::UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
        Box::pin(async move {
            let upstream_url = std::env::var("THREADLINE_UPSTREAM_URL")
                .map_err(|_| ThreadlineError::UpstreamUrlMissing)?;
            let handshake = build_handshake_request(&upstream_url, &auth, session)
                .map_err(|_| ThreadlineError::UpstreamWebSocketConnectFailed)?;
            let (stream, response) = connect_async(handshake.request)
                .await
                .map_err(|_| ThreadlineError::UpstreamWebSocketConnectFailed)?;
            let turn_state = response
                .headers()
                .get(crate::responses::TURN_STATE_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string);

            Ok(ConnectedUpstream {
                websocket: Arc::new(LiveUpstreamWebSocket::from_stream(stream)),
                session: handshake.session,
                turn_state,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::http::Response;
    use tokio_tungstenite::tungstenite::Error as TungsteniteError;

    use super::*;

    #[test]
    fn upstream_http_connect_error_maps_to_status_error() {
        let error = TungsteniteError::Http(
            Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(None)
                .unwrap(),
        );

        let mapped = map_upstream_connect_error(error);

        assert!(matches!(
            mapped,
            ThreadlineError::UpstreamWebSocketHandshakeRejected { status }
                if status == StatusCode::UNAUTHORIZED
        ));
    }

    #[test]
    fn upstream_non_http_connect_error_remains_bad_gateway_failure() {
        let error = TungsteniteError::Io(std::io::Error::other("dial failed"));

        let mapped = map_upstream_connect_error(error);

        assert!(matches!(
            mapped,
            ThreadlineError::UpstreamWebSocketConnectFailed
        ));
        assert_eq!(mapped.status_code(), StatusCode::BAD_GATEWAY);
    }
}
