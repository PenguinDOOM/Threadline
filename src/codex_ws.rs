use thiserror::Error;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::{HeaderValue, Request},
};
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

    let mut request = url
        .into_client_request()
        .map_err(|_| HandshakeBuildError::RequestBuildFailed)?;
    let headers = request.headers_mut();

    headers.insert(
        "authorization",
        header_value(&format!("Bearer {}", auth.bearer_token))?,
    );
    headers.insert(
        "OpenAI-Beta",
        header_value(RESPONSES_WEBSOCKETS_BETA_HEADER)?,
    );
    headers.insert("originator", header_value("codex_vscode")?);
    headers.insert(
        "user-agent",
        header_value(&format!(
            "codex_vscode/0.1.0 Threadline/{}",
            env!("CARGO_PKG_VERSION")
        ))?,
    );
    headers.insert("version", header_value(env!("CARGO_PKG_VERSION"))?);
    headers.insert("session-id", header_value(&session.session_id)?);
    headers.insert("thread-id", header_value(&session.thread_id)?);
    headers.insert("x-codex-window-id", header_value(&session.window_id)?);
    headers.insert(
        "x-client-request-id",
        header_value(&client_request_id)?,
    );

    if let Some(turn_state) = &session.turn_state {
        headers.insert("x-codex-turn-state", header_value(turn_state)?);
    }

    Ok(CodexHandshake {
        request,
        session,
        client_request_id,
    })
}

fn header_value(value: &str) -> Result<HeaderValue, HandshakeBuildError> {
    HeaderValue::from_str(value).map_err(|_| HandshakeBuildError::RequestBuildFailed)
}

fn new_request_id() -> String {
    Uuid::now_v7().to_string()
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::auth::{AuthSource, LoadedUpstreamAuth, RefreshBoundary};

    use super::{
        HandshakeBuildError, RESPONSES_WEBSOCKETS_BETA_HEADER, UpstreamSessionDescriptor,
        build_handshake_request,
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
        assert_eq!(headers["connection"], "Upgrade");
        assert_eq!(headers["upgrade"], "websocket");
        assert!(headers.get("sec-websocket-key").is_some());
        assert_eq!(headers["sec-websocket-version"], "13");
        assert_eq!(headers["authorization"], "Bearer top-secret-token");
        assert_eq!(headers["openai-beta"], RESPONSES_WEBSOCKETS_BETA_HEADER);
        assert_eq!(headers["originator"], "codex_vscode");
        assert_eq!(
            headers["user-agent"],
            format!("codex_vscode/0.1.0 Threadline/{}", env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(headers["version"], env!("CARGO_PKG_VERSION"));
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

    #[test]
    fn handshake_rejects_invalid_upstream_url() {
        let error = build_handshake_request("not a websocket url", &test_auth(), None)
            .expect_err("invalid url should fail");

        assert!(matches!(error, HandshakeBuildError::RequestBuildFailed));
    }
}
