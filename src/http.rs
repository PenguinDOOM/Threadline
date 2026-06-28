use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tracing::warn;

use crate::auth::{AuthDiscoveryOptions, load_upstream_auth};
use crate::codex_ws::build_handshake_request;
use crate::config::ThreadlineConfig;
use crate::errors::ThreadlineError;
use crate::models::{RouteProfile, advertised_model_ids_for_profile};
use crate::registry::RetainedSessionRegistry;
use crate::responses::{
    ConnectedUpstream, DownstreamRequestMetadata, ResponsesRouteState, ThreadlineServices,
    responses_handler,
};
use crate::ws_pump::LiveUpstreamWebSocket;

const MODEL_CREATED_UNSPECIFIED: u64 = 0;
const DEFAULT_UPSTREAM_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";
const INTERACTION_TYPE_HEADER: &str = "x-interaction-type";

#[derive(Clone)]
struct AppState {
    profile: RouteProfile,
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
    let connector = DefaultUpstreamConnector {
        codex_client_version: config.codex_client_version.clone(),
    };

    build_router_with_services(
        config,
        ThreadlineServices::new(Arc::new(DefaultAuthProvider), Arc::new(connector)),
    )
}

pub fn build_router_with_services(
    config: ThreadlineConfig,
    services: ThreadlineServices,
) -> Router {
    let responses = ResponsesRouteState {
        profile: config.profile,
        registry: Arc::new(RetainedSessionRegistry::new(
            config.retained_session_capacity,
        )),
        services,
    };
    let state = AppState {
        profile: config.profile,
        responses,
    };

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
        data: advertised_model_ids_for_profile(state.profile)
            .iter()
            .map(|model_id| ModelEntry {
                id: (*model_id).to_string(),
                object: "model",
                created: MODEL_CREATED_UNSPECIFIED,
                owned_by: "threadline",
            })
            .collect(),
    })
}

async fn responses_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<impl axum::response::IntoResponse, ThreadlineError> {
    let request_metadata = extract_downstream_request_metadata(&headers);
    responses_handler(State(state.responses), Json(payload), request_metadata).await
}

fn extract_downstream_request_metadata(headers: &HeaderMap) -> DownstreamRequestMetadata {
    let interaction_type = headers
        .get_all(INTERACTION_TYPE_HEADER)
        .iter()
        .next()
        .map(|value| value.as_bytes());

    DownstreamRequestMetadata::from_interaction_type_header_bytes(interaction_type)
}

#[derive(Clone)]
struct DefaultAuthProvider;

impl crate::responses::UpstreamAuthProvider for DefaultAuthProvider {
    fn load(&self) -> Result<crate::auth::LoadedUpstreamAuth, ThreadlineError> {
        load_upstream_auth(&AuthDiscoveryOptions::from_env())
            .map_err(|_| ThreadlineError::UpstreamCredentialsUnavailable)
    }
}

#[derive(Clone)]
struct DefaultUpstreamConnector {
    codex_client_version: String,
}

impl DefaultUpstreamConnector {
    fn upstream_url() -> String {
        std::env::var("THREADLINE_UPSTREAM_URL")
            .unwrap_or_else(|_| DEFAULT_UPSTREAM_URL.to_string())
    }
}

fn upstream_connect_error_kind(error: &TungsteniteError) -> &'static str {
    match error {
        TungsteniteError::ConnectionClosed => "connection_closed",
        TungsteniteError::AlreadyClosed => "already_closed",
        TungsteniteError::Io(_) => "io",
        TungsteniteError::Tls(_) => "tls",
        TungsteniteError::Capacity(_) => "capacity",
        TungsteniteError::Protocol(_) => "protocol",
        TungsteniteError::WriteBufferFull(_) => "write_buffer_full",
        TungsteniteError::Utf8 => "utf8",
        TungsteniteError::AttackAttempt => "attack_attempt",
        TungsteniteError::Url(_) => "url",
        TungsteniteError::HttpFormat(_) => "http_format",
        _ => unreachable!("http errors are handled before upstream_connect_error_kind"),
    }
}

fn map_upstream_connect_error(error: TungsteniteError) -> ThreadlineError {
    match error {
        TungsteniteError::Http(response) => {
            let status = response.status();
            warn!(
                upstream_status = status.as_u16(),
                upstream_status_reason = status.canonical_reason().unwrap_or("unknown"),
                "upstream_websocket_handshake_rejected"
            );
            ThreadlineError::UpstreamWebSocketHandshakeRejected { status }
        }
        other => {
            warn!(
                error_kind = upstream_connect_error_kind(&other),
                error = %other,
                "upstream_websocket_connect_failed"
            );
            ThreadlineError::UpstreamWebSocketConnectFailed
        }
    }
}

impl crate::responses::UpstreamConnector for DefaultUpstreamConnector {
    fn connect(
        &self,
        auth: crate::auth::LoadedUpstreamAuth,
        session: Option<crate::codex_ws::UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
        let codex_client_version = self.codex_client_version.clone();

        Box::pin(async move {
            let upstream_url = Self::upstream_url();
            let handshake =
                build_handshake_request(&upstream_url, &auth, &codex_client_version, session)
                    .map_err(|_| ThreadlineError::UpstreamWebSocketConnectFailed)?;
            let (stream, response) = connect_async(handshake.request)
                .await
                .map_err(map_upstream_connect_error)?;
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
    use axum::http::{HeaderValue, Response, StatusCode};
    use std::ffi::OsString;
    use std::sync::Mutex;
    use tokio_tungstenite::tungstenite::Error as TungsteniteError;

    use super::*;
    use crate::responses::DownstreamInteractionType;

    static UPSTREAM_URL_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct UpstreamUrlEnvGuard {
        original: Option<OsString>,
    }

    impl UpstreamUrlEnvGuard {
        fn acquire() -> Self {
            let original = std::env::var_os("THREADLINE_UPSTREAM_URL");
            Self { original }
        }
    }

    impl Drop for UpstreamUrlEnvGuard {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => unsafe { std::env::set_var("THREADLINE_UPSTREAM_URL", value) },
                None => unsafe { std::env::remove_var("THREADLINE_UPSTREAM_URL") },
            }
        }
    }

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

    #[test]
    fn upstream_connect_error_kind_uses_coarse_io_bucket() {
        let error = TungsteniteError::Io(std::io::Error::other("dial failed"));

        assert_eq!(upstream_connect_error_kind(&error), "io");
    }

    #[test]
    fn upstream_connect_error_kind_distinguishes_closed_connections() {
        assert_eq!(
            upstream_connect_error_kind(&TungsteniteError::ConnectionClosed),
            "connection_closed"
        );
    }

    #[test]
    fn upstream_url_uses_default_when_env_is_unset() {
        let _lock = UPSTREAM_URL_ENV_LOCK.lock().unwrap();
        let _guard = UpstreamUrlEnvGuard::acquire();
        unsafe { std::env::remove_var("THREADLINE_UPSTREAM_URL") };

        assert_eq!(
            DefaultUpstreamConnector::upstream_url(),
            DEFAULT_UPSTREAM_URL
        );
    }

    #[test]
    fn upstream_url_prefers_env_override_when_present() {
        let _lock = UPSTREAM_URL_ENV_LOCK.lock().unwrap();
        let _guard = UpstreamUrlEnvGuard::acquire();
        unsafe {
            std::env::set_var(
                "THREADLINE_UPSTREAM_URL",
                "wss://example.invalid/backend-api/codex/responses",
            )
        };

        assert_eq!(
            DefaultUpstreamConnector::upstream_url(),
            "wss://example.invalid/backend-api/codex/responses"
        );
    }

    #[test]
    fn interaction_type_header_uses_first_duplicate_value() {
        let mut headers = HeaderMap::new();
        headers.append(
            INTERACTION_TYPE_HEADER,
            HeaderValue::from_static("conversation-start"),
        );
        headers.append(
            INTERACTION_TYPE_HEADER,
            HeaderValue::from_static("conversation-compaction"),
        );

        let metadata = extract_downstream_request_metadata(&headers);

        assert_eq!(
            metadata.interaction_type(),
            DownstreamInteractionType::Other
        );

        let mut headers = HeaderMap::new();
        headers.append(
            INTERACTION_TYPE_HEADER,
            HeaderValue::from_static(" conversation-compaction "),
        );
        headers.append(
            INTERACTION_TYPE_HEADER,
            HeaderValue::from_static("conversation-start"),
        );

        let metadata = extract_downstream_request_metadata(&headers);

        assert_eq!(
            metadata.interaction_type(),
            DownstreamInteractionType::ConversationCompaction
        );
    }
}
