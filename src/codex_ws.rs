use axum::http::Request;
use thiserror::Error;
use uuid::Uuid;

use crate::auth::LoadedUpstreamAuth;

pub const RESPONSES_WEBSOCKETS_BETA_HEADER: &str = "responses_websockets=2026-02-06";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamSessionDescriptor {
    pub session_id: String,
    pub thread_id: String,
    pub window_id: String,
    pub turn_state: Option<String>,
}

impl UpstreamSessionDescriptor {
    pub fn refresh_window(&mut self) {
        self.window_id = new_request_id();
    }
}

#[derive(Debug)]
pub struct CodexHandshake {
    pub request: Request<()>,
    pub session: UpstreamSessionDescriptor,
    pub client_request_id: String,
}

#[derive(Debug, Error)]
pub enum HandshakeBuildError {
    #[error("Threadline could not build the upstream websocket request.")]
    RequestBuildFailed,
}

pub fn build_handshake_request(
    url: &str,
    auth: &LoadedUpstreamAuth,
    session: Option<UpstreamSessionDescriptor>,
) -> Result<CodexHandshake, HandshakeBuildError> {
    let session = session.unwrap_or_else(|| UpstreamSessionDescriptor {
        session_id: new_request_id(),
        thread_id: new_request_id(),
        window_id: new_request_id(),
        turn_state: None,
    });
    let client_request_id = new_request_id();

    let mut builder = Request::builder()
        .method("GET")
        .uri(url)
        .header("authorization", format!("Bearer {}", auth.bearer_token))
        .header("openai-beta", RESPONSES_WEBSOCKETS_BETA_HEADER)
        .header("session-id", &session.session_id)
        .header("thread-id", &session.thread_id)
        .header("x-codex-window-id", &session.window_id)
        .header("x-client-request-id", &client_request_id);

    if let Some(turn_state) = &session.turn_state {
        builder = builder.header("x-codex-turn-state", turn_state);
    }

    let request = builder
        .body(())
        .map_err(|_| HandshakeBuildError::RequestBuildFailed)?;

    Ok(CodexHandshake {
        request,
        session,
        client_request_id,
    })
}

fn new_request_id() -> String {
    Uuid::now_v7().to_string()
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::auth::{AuthSource, LoadedUpstreamAuth, RefreshBoundary};

    use super::{
        RESPONSES_WEBSOCKETS_BETA_HEADER, UpstreamSessionDescriptor, build_handshake_request,
    };

    fn test_auth() -> LoadedUpstreamAuth {
        LoadedUpstreamAuth {
            bearer_token: "top-secret-token".to_string(),
            source: AuthSource::ExplicitOverride,
            refresh_boundary: RefreshBoundary::NotAvailable,
        }
    }

    #[test]
    fn handshake_generates_required_headers_and_identifiers() {
        let handshake = build_handshake_request("ws://localhost:9001/codex", &test_auth(), None)
            .expect("handshake should build");
        let headers = handshake.request.headers();

        assert_eq!(
            handshake.request.uri().to_string(),
            "ws://localhost:9001/codex"
        );
        assert_eq!(headers["authorization"], "Bearer top-secret-token");
        assert_eq!(headers["openai-beta"], RESPONSES_WEBSOCKETS_BETA_HEADER);
        Uuid::parse_str(headers["session-id"].to_str().unwrap()).expect("session id uuid");
        Uuid::parse_str(headers["thread-id"].to_str().unwrap()).expect("thread id uuid");
        Uuid::parse_str(headers["x-codex-window-id"].to_str().unwrap()).expect("window id uuid");
        Uuid::parse_str(headers["x-client-request-id"].to_str().unwrap()).expect("request id uuid");
        assert!(headers.get("x-codex-turn-state").is_none());
    }

    #[test]
    fn handshake_reuses_supplied_session_context_and_turn_state() {
        let session = UpstreamSessionDescriptor {
            session_id: "session-123".to_string(),
            thread_id: "thread-456".to_string(),
            window_id: "window-789".to_string(),
            turn_state: Some("turn-state-abc".to_string()),
        };

        let handshake = build_handshake_request(
            "wss://example.invalid/upstream",
            &test_auth(),
            Some(session.clone()),
        )
        .expect("handshake should build");
        let headers = handshake.request.headers();

        assert_eq!(headers["session-id"], session.session_id);
        assert_eq!(headers["thread-id"], session.thread_id);
        assert_eq!(headers["x-codex-window-id"], session.window_id);
        assert_eq!(headers["x-codex-turn-state"], "turn-state-abc");
        assert_ne!(headers["x-client-request-id"], "turn-state-abc");
    }
}
