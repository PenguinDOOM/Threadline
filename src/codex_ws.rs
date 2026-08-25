use serde_json::{Map, Value};
use thiserror::Error;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::{HeaderValue, Request},
};
use uuid::Uuid;

use crate::auth::LoadedUpstreamAuth;
use crate::identity::installation_id;

pub const RESPONSES_WEBSOCKETS_BETA_HEADER: &str = "responses_websockets=2026-02-06";
const INSTALLATION_ID_HEADER: &str = "x-codex-installation-id";
const SESSION_ID_METADATA_KEY: &str = "session_id";
const THREAD_ID_METADATA_KEY: &str = "thread_id";
const WINDOW_ID_METADATA_KEY: &str = "x-codex-window-id";
#[cfg(test)]
const EXPECTED_CODEX_CLIENT_VERSION: &str = "0.136.0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamSessionDescriptor {
    pub session_id: String,
    pub thread_id: String,
    pub window_id: String,
    pub turn_state: Option<String>,
}

impl UpstreamSessionDescriptor {
    pub fn new(thread_id: Option<String>) -> Self {
        let thread_id = thread_id
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(new_request_id);
        let window_id = window_id_for_generation(&thread_id, 0);

        Self {
            session_id: new_request_id(),
            thread_id,
            window_id,
            turn_state: None,
        }
    }

    pub fn set_window_generation(&mut self, generation: u64) {
        self.window_id = window_id_for_generation(&self.thread_id, generation);
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
    codex_client_version: &str,
    session: Option<UpstreamSessionDescriptor>,
) -> Result<CodexHandshake, HandshakeBuildError> {
    build_handshake_request_with_installation_id(
        url,
        auth,
        codex_client_version,
        installation_id(),
        session,
    )
}

pub fn apply_session_metadata(
    payload: &mut Map<String, Value>,
    session: &UpstreamSessionDescriptor,
) {
    apply_session_metadata_with_installation_id(payload, session, installation_id());
}

fn apply_session_metadata_with_installation_id(
    payload: &mut Map<String, Value>,
    session: &UpstreamSessionDescriptor,
    installation_id: &str,
) {
    let metadata = payload
        .entry("client_metadata".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !metadata.is_object() {
        *metadata = Value::Object(Map::new());
    }
    let metadata = metadata
        .as_object_mut()
        .expect("client_metadata was normalized to an object");

    metadata.insert(
        INSTALLATION_ID_HEADER.to_string(),
        Value::String(installation_id.to_string()),
    );
    metadata.insert(
        SESSION_ID_METADATA_KEY.to_string(),
        Value::String(session.session_id.clone()),
    );
    metadata.insert(
        THREAD_ID_METADATA_KEY.to_string(),
        Value::String(session.thread_id.clone()),
    );
    metadata.insert(
        WINDOW_ID_METADATA_KEY.to_string(),
        Value::String(session.window_id.clone()),
    );
}

fn build_handshake_request_with_installation_id(
    url: &str,
    auth: &LoadedUpstreamAuth,
    codex_client_version: &str,
    installation_id: &str,
    session: Option<UpstreamSessionDescriptor>,
) -> Result<CodexHandshake, HandshakeBuildError> {
    let session = session.unwrap_or_else(|| UpstreamSessionDescriptor::new(None));
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
            "codex_vscode/{codex_client_version} Threadline/{}",
            env!("CARGO_PKG_VERSION")
        ))?,
    );
    headers.insert("version", header_value(codex_client_version)?);
    headers.insert(INSTALLATION_ID_HEADER, header_value(installation_id)?);
    headers.insert("session-id", header_value(&session.session_id)?);
    headers.insert("thread-id", header_value(&session.thread_id)?);
    headers.insert("x-codex-window-id", header_value(&session.window_id)?);
    headers.insert("x-client-request-id", header_value(&client_request_id)?);

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

fn window_id_for_generation(thread_id: &str, generation: u64) -> String {
    format!("{thread_id}:{generation}")
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use uuid::Uuid;

    use crate::auth::{AuthSource, LoadedUpstreamAuth, RefreshBoundary};

    use super::{
        EXPECTED_CODEX_CLIENT_VERSION, HandshakeBuildError, RESPONSES_WEBSOCKETS_BETA_HEADER,
        UpstreamSessionDescriptor, apply_session_metadata_with_installation_id,
        build_handshake_request_with_installation_id,
    };

    const TEST_INSTALLATION_ID: &str = "11111111-2222-4333-8444-555555555555";

    fn test_auth() -> LoadedUpstreamAuth {
        LoadedUpstreamAuth {
            bearer_token: "top-secret-token".to_string(),
            source: AuthSource::CodexKeyring,
            refresh_boundary: RefreshBoundary::NotAvailable,
        }
    }

    #[test]
    fn handshake_generates_required_headers_and_identifiers() {
        let handshake = build_handshake_request_with_installation_id(
            "ws://localhost:9001/codex",
            &test_auth(),
            EXPECTED_CODEX_CLIENT_VERSION,
            TEST_INSTALLATION_ID,
            None,
        )
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
            format!(
                "codex_vscode/{EXPECTED_CODEX_CLIENT_VERSION} Threadline/{}",
                env!("CARGO_PKG_VERSION")
            )
        );
        assert_eq!(headers["version"], EXPECTED_CODEX_CLIENT_VERSION);
        assert_eq!(headers["x-codex-installation-id"], TEST_INSTALLATION_ID);
        Uuid::parse_str(headers["session-id"].to_str().unwrap()).expect("session id uuid");
        let thread_id = headers["thread-id"].to_str().expect("thread id header");
        Uuid::parse_str(thread_id).expect("thread id uuid");
        assert_eq!(
            headers["x-codex-window-id"],
            format!("{thread_id}:0")
        );
        Uuid::parse_str(headers["x-client-request-id"].to_str().unwrap()).expect("request id uuid");
        assert!(headers.get("x-codex-turn-state").is_none());
    }

    #[test]
    fn handshake_reuses_supplied_session_context_and_turn_state() {
        let session = UpstreamSessionDescriptor {
            session_id: "session-123".to_string(),
            thread_id: "thread-456".to_string(),
            window_id: "thread-456:3".to_string(),
            turn_state: Some("turn-state-abc".to_string()),
        };

        let handshake = build_handshake_request_with_installation_id(
            "wss://example.invalid/upstream",
            &test_auth(),
            EXPECTED_CODEX_CLIENT_VERSION,
            TEST_INSTALLATION_ID,
            Some(session.clone()),
        )
        .expect("handshake should build");
        let headers = handshake.request.headers();

        assert_eq!(headers["x-codex-installation-id"], TEST_INSTALLATION_ID);
        assert_eq!(headers["session-id"], session.session_id);
        assert_eq!(headers["thread-id"], session.thread_id);
        assert_eq!(headers["x-codex-window-id"], session.window_id);
        assert_eq!(headers["x-codex-turn-state"], "turn-state-abc");
        assert_ne!(headers["x-client-request-id"], "turn-state-abc");
    }

    #[test]
    fn session_descriptor_uses_supplied_thread_and_window_generation() {
        let thread_id = "48a65359-981b-47c2-9612-e1c64ae07e22".to_string();
        let mut session = UpstreamSessionDescriptor::new(Some(thread_id.clone()));

        assert_eq!(session.thread_id, thread_id);
        assert_eq!(session.window_id, format!("{}:0", session.thread_id));
        Uuid::parse_str(&session.session_id).expect("session id uuid");

        session.set_window_generation(2);
        assert_eq!(session.window_id, format!("{}:2", session.thread_id));
    }

    #[test]
    fn session_metadata_projects_identifiers_without_removing_other_metadata() {
        let session = UpstreamSessionDescriptor {
            session_id: "session-123".to_string(),
            thread_id: "48a65359-981b-47c2-9612-e1c64ae07e22".to_string(),
            window_id: "48a65359-981b-47c2-9612-e1c64ae07e22:0".to_string(),
            turn_state: None,
        };
        let mut payload = json!({
            "client_metadata": {
                "downstream_key": "preserve"
            }
        });

        apply_session_metadata_with_installation_id(
            payload.as_object_mut().expect("payload object"),
            &session,
            TEST_INSTALLATION_ID,
        );

        assert_eq!(payload["client_metadata"]["downstream_key"], "preserve");
        assert_eq!(
            payload["client_metadata"]["x-codex-installation-id"],
            TEST_INSTALLATION_ID
        );
        assert_eq!(payload["client_metadata"]["session_id"], session.session_id);
        assert_eq!(payload["client_metadata"]["thread_id"], session.thread_id);
        assert_eq!(
            payload["client_metadata"]["x-codex-window-id"],
            session.window_id
        );
    }

    #[test]
    fn session_metadata_replaces_non_object_client_metadata() {
        let session = UpstreamSessionDescriptor::new(None);
        let mut payload = json!({ "client_metadata": "invalid" });

        apply_session_metadata_with_installation_id(
            payload.as_object_mut().expect("payload object"),
            &session,
            TEST_INSTALLATION_ID,
        );

        assert!(payload["client_metadata"].is_object());
        assert_eq!(
            payload["client_metadata"]["x-codex-installation-id"],
            Value::String(TEST_INSTALLATION_ID.to_string())
        );
    }

    #[test]
    fn handshake_rejects_invalid_upstream_url() {
        let error = build_handshake_request_with_installation_id(
            "not a websocket url",
            &test_auth(),
            EXPECTED_CODEX_CLIENT_VERSION,
            TEST_INSTALLATION_ID,
            None,
        )
        .expect_err("invalid url should fail");

        assert!(matches!(error, HandshakeBuildError::RequestBuildFailed));
    }
}