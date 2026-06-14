use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::Duration;

use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures_util::{StreamExt, future::BoxFuture, stream};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;
use uuid::Uuid;

#[path = "support/scripted_ws.rs"]
mod scripted_ws;

use scripted_ws::ScriptedWebSocketServer;
use threadline::auth::{AuthSource, LoadedUpstreamAuth, RefreshBoundary};
use threadline::codex_ws::UpstreamSessionDescriptor;
use threadline::config::ThreadlineConfig;
use threadline::errors::ThreadlineError;
use threadline::http::build_router_with_services;
use threadline::responses::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};
use threadline::ws_pump::LiveUpstreamWebSocket;

#[derive(Clone)]
struct StaticAuthProvider;

impl UpstreamAuthProvider for StaticAuthProvider {
    fn load(&self) -> Result<LoadedUpstreamAuth, ThreadlineError> {
        Ok(LoadedUpstreamAuth {
            bearer_token: "test-token".to_string(),
            source: AuthSource::CodexKeyring,
            refresh_boundary: RefreshBoundary::NotAvailable,
        })
    }
}

struct PlannedConnection {
    server: Arc<ScriptedWebSocketServer>,
    turn_state: Option<String>,
}

#[derive(Clone)]
struct RecordingConnector {
    plans: Arc<Mutex<VecDeque<PlannedConnection>>>,
    sessions: Arc<Mutex<Vec<UpstreamSessionDescriptor>>>,
    websockets: Arc<Mutex<Vec<Weak<LiveUpstreamWebSocket>>>>,
}

impl RecordingConnector {
    fn new(plans: Vec<PlannedConnection>) -> Self {
        Self {
            plans: Arc::new(Mutex::new(plans.into())),
            sessions: Arc::new(Mutex::new(Vec::new())),
            websockets: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn recorded_sessions(&self) -> Vec<UpstreamSessionDescriptor> {
        self.sessions.lock().await.clone()
    }

    async fn recorded_websockets(&self) -> Vec<Weak<LiveUpstreamWebSocket>> {
        self.websockets.lock().await.clone()
    }
}

impl UpstreamConnector for RecordingConnector {
    fn connect(
        &self,
        _auth: LoadedUpstreamAuth,
        session: Option<UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
        let plans = Arc::clone(&self.plans);
        let sessions = Arc::clone(&self.sessions);
        let websockets = Arc::clone(&self.websockets);
        Box::pin(async move {
            let session = session.unwrap_or_else(new_session_descriptor);
            let plan = plans
                .lock()
                .await
                .pop_front()
                .expect("planned websocket connection");
            sessions.lock().await.push(session.clone());

            let (stream, _) = connect_async(plan.server.url())
                .await
                .map_err(|_| ThreadlineError::UpstreamWebSocketConnectFailed)?;

            let websocket = Arc::new(LiveUpstreamWebSocket::from_stream(stream));
            websockets.lock().await.push(Arc::downgrade(&websocket));

            Ok(ConnectedUpstream {
                websocket,
                session,
                turn_state: plan.turn_state,
            })
        })
    }
}

#[derive(Clone)]
struct FailingConnector;

impl UpstreamConnector for FailingConnector {
    fn connect(
        &self,
        _auth: LoadedUpstreamAuth,
        _session: Option<UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
        Box::pin(async { Err(ThreadlineError::UpstreamWebSocketConnectFailed) })
    }
}

fn build_test_router(
    config: ThreadlineConfig,
    connector: Arc<dyn UpstreamConnector>,
) -> axum::Router {
    build_router_with_services(
        config,
        ThreadlineServices::new(Arc::new(StaticAuthProvider), connector),
    )
}

async fn post_responses(app: axum::Router, payload: Value) -> Response<Body> {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .expect("request"),
    )
    .await
    .expect("response")
}

fn message_text(message: Message) -> String {
    match message {
        Message::Text(text) => text.to_string(),
        other => panic!("expected text message, got {other:?}"),
    }
}

fn new_session_descriptor() -> UpstreamSessionDescriptor {
    UpstreamSessionDescriptor {
        session_id: Uuid::now_v7().to_string(),
        thread_id: Uuid::now_v7().to_string(),
        window_id: Uuid::now_v7().to_string(),
        turn_state: None,
    }
}

fn assert_codex_unsupported_response_fields_are_absent(payload: &Value) {
    for field_name in [
        "max_output_tokens",
        "max_tokens",
        "max_completion_tokens",
        "truncation",
    ] {
        assert!(
            payload.get(field_name).is_none(),
            "expected upstream response.create payload to omit {field_name}, got {payload:?}"
        );
    }
}

fn split_sse_frames(body: &str) -> Vec<&str> {
    body.split("\n\n")
        .filter(|frame| !frame.trim().is_empty())
        .collect()
}

fn sse_event_and_data(frame: &str) -> (&str, &str) {
    let mut event = None;
    let mut data = None;
    let mut unexpected_lines = Vec::new();

    for (index, line) in frame.lines().enumerate() {
        if let Some(value) = line.strip_prefix("event: ") {
            assert!(
                event.replace(value).is_none(),
                "expected exactly one event line in SSE frame, found duplicate at line {}: {frame}",
                index + 1
            );
            continue;
        }

        if let Some(value) = line.strip_prefix("data: ") {
            assert!(
                data.replace(value).is_none(),
                "expected compact single-line SSE data payload, found duplicate data line at line {}: {frame}",
                index + 1
            );
            continue;
        }

        unexpected_lines.push(format!("line {}: {line}", index + 1));
    }

    assert!(
        unexpected_lines.is_empty(),
        "expected exactly one event line and one compact data line in SSE frame; unexpected lines: {}. Frame: {frame}",
        unexpected_lines.join(" | ")
    );

    (
        event.unwrap_or_else(|| panic!("missing event line in SSE frame: {frame}")),
        data.unwrap_or_else(|| panic!("missing data line in SSE frame: {frame}")),
    )
}

fn assert_done_frame(frame: &str) {
    assert_eq!(
        frame, "data: [DONE]",
        "expected a bare downstream DONE frame without an event line"
    );
}

fn auxiliary_summary_text() -> &'static str {
    concat!(
        "The conversation has grown too large for the context window and must be compacted now",
        "\n\n",
        "Your ONLY task right now is to produce a comprehensive summary",
        "\n",
        "Output your summary wrapped in <summary> and </summary> tags"
    )
}

fn auxiliary_summary_input_item() -> Value {
    json!({
        "type": "message",
        "role": "system",
        "content": [
            {
                "type": "input_text",
                "text": auxiliary_summary_text()
            }
        ]
    })
}

fn auxiliary_summary_request(previous_response_id: Option<&str>) -> Value {
    let mut payload = json!({
        "model": "gpt-5.4",
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Continue from the earlier answer."
                    }
                ]
            },
            auxiliary_summary_input_item()
        ]
    });

    if let Some(previous_response_id) = previous_response_id {
        payload["previous_response_id"] = json!(previous_response_id);
    }

    payload
}

async fn next_body_chunk(
    body_stream: &mut (impl futures_util::Stream<Item = Result<Bytes, axum::Error>> + Unpin),
) -> Bytes {
    body_stream
        .next()
        .await
        .expect("expected body chunk before EOF")
        .expect("body chunk")
}

#[tokio::test]
async fn response_marker_continuity_reconnects_with_saved_turn_state() {
    let first_server = Arc::new(ScriptedWebSocketServer::start().await);
    let second_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&first_server),
            turn_state: Some("turn-state-1".to_string()),
        },
        PlannedConnection {
            server: Arc::clone(&second_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector.clone()));

    let first_response =
        post_responses(app.clone(), json!({"model":"gpt-5.4","input":"first"})).await;
    assert_eq!(first_response.status(), StatusCode::OK);

    let first_payload: Value = serde_json::from_str(&message_text(
        first_server
            .recv_client_message()
            .await
            .expect("first request message"),
    ))
    .expect("first request json");
    assert_eq!(first_payload["type"], "response.create");

    first_server
        .send_text(r#"{"type":"response.created","response":{"id":"response-1"}}"#)
        .await;
    first_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let first_body = to_bytes(first_response.into_body(), usize::MAX)
        .await
        .expect("first body");
    let first_body_text = String::from_utf8(first_body.to_vec()).expect("utf8 body");
    assert!(first_body_text.contains("event: response.created"));
    assert!(first_body_text.contains("event: response.completed"));

    first_server.send_close(1000, "done").await;
    sleep(Duration::from_millis(50)).await;

    let second_response = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"second",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(second_response.status(), StatusCode::OK);

    let second_payload: Value = serde_json::from_str(&message_text(
        second_server
            .recv_client_message()
            .await
            .expect("second request message"),
    ))
    .expect("second request json");
    assert_eq!(second_payload["type"], "response.create");
    assert!(second_payload.get("response").is_none());
    assert_eq!(second_payload["previous_response_id"], "response-1");

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0].session_id, sessions[1].session_id);
    assert_eq!(sessions[0].thread_id, sessions[1].thread_id);
    assert_eq!(sessions[1].turn_state.as_deref(), Some("turn-state-1"));

    second_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-2"}}"#)
        .await;
    let _ = to_bytes(second_response.into_body(), usize::MAX)
        .await
        .expect("second body");
}

#[tokio::test]
async fn context_management_compaction_is_forwarded_without_changing_marker_semantics() {
    let first_server = Arc::new(ScriptedWebSocketServer::start().await);
    let second_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&first_server),
            turn_state: Some("turn-state-1".to_string()),
        },
        PlannedConnection {
            server: Arc::clone(&second_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let first_response =
        post_responses(app.clone(), json!({"model":"gpt-5.4","input":"first"})).await;
    assert_eq!(first_response.status(), StatusCode::OK);

    let first_payload: Value = serde_json::from_str(&message_text(
        first_server
            .recv_client_message()
            .await
            .expect("first request message"),
    ))
    .expect("first request json");
    assert_eq!(first_payload["type"], "response.create");

    first_server
        .send_text(r#"{"type":"response.created","response":{"id":"response-1"}}"#)
        .await;
    first_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(first_response.into_body(), usize::MAX)
        .await
        .expect("first body");

    first_server.send_close(1000, "done").await;
    sleep(Duration::from_millis(50)).await;

    let second_response = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"second",
            "previous_response_id":"response-1",
            "context_management": {
                "type":"compaction",
                "compact_threshold": 12345
            },
            "reasoning":{"effort":"high","summary":"auto"},
            "include":["reasoning.encrypted_content"],
            "truncation":"auto"
        }),
    )
    .await;
    assert_eq!(second_response.status(), StatusCode::OK);

    let second_payload: Value = serde_json::from_str(&message_text(
        second_server
            .recv_client_message()
            .await
            .expect("second request message"),
    ))
    .expect("second request json");
    assert_eq!(second_payload["type"], "response.create");
    assert_eq!(second_payload["previous_response_id"], "response-1");
    assert_eq!(
        second_payload["context_management"],
        json!({
            "type":"compaction",
            "compact_threshold": 12345
        })
    );
    assert_eq!(
        second_payload["reasoning"],
        json!({"effort":"high","summary":"auto"})
    );
    assert_eq!(
        second_payload["include"],
        json!(["reasoning.encrypted_content"])
    );
    assert!(second_payload.get("response").is_none());
    assert_codex_unsupported_response_fields_are_absent(&second_payload);

    second_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-2"}}"#)
        .await;
    let _ = to_bytes(second_response.into_body(), usize::MAX)
        .await
        .expect("second body");
}

#[tokio::test]
async fn missing_previous_response_id_returns_stable_not_found() {
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(FailingConnector));

    let response = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"missing",
            "previous_response_id":"response-missing"
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");
}

#[tokio::test]
async fn summary_request_with_active_previous_response_id_uses_auxiliary_session() {
    let retained_server = Arc::new(ScriptedWebSocketServer::start().await);
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&retained_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&summary_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        Arc::new(connector),
    );

    let initial = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(initial.status(), StatusCode::OK);
    let _ = retained_server
        .recv_client_message()
        .await
        .expect("seed request");
    retained_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(initial.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let active = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    let _ = retained_server
        .recv_client_message()
        .await
        .expect("active followup request");

    let summary = post_responses(app, auxiliary_summary_request(Some("response-1"))).await;
    assert_eq!(summary.status(), StatusCode::OK);
}

#[tokio::test]
async fn summary_request_with_unknown_previous_response_id_uses_auxiliary_session() {
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&summary_server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, auxiliary_summary_request(Some("response-missing"))).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn summary_request_does_not_forward_previous_response_id_upstream() {
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&summary_server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, auxiliary_summary_request(Some("response-1"))).await;
    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = serde_json::from_str(&message_text(
        summary_server
            .recv_client_message()
            .await
            .expect("summary request"),
    ))
    .expect("summary request json");
    assert_eq!(payload["type"], "response.create");
    assert!(payload.get("previous_response_id").is_none());
}

#[tokio::test]
async fn summary_request_with_context_management_keeps_context_management_but_omits_previous_response_id()
 {
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&summary_server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let mut payload = auxiliary_summary_request(Some("response-1"));
    payload["context_management"] = json!({
        "type": "compaction",
        "compact_threshold": 12345
    });

    let response = post_responses(app, payload).await;
    assert_eq!(response.status(), StatusCode::OK);

    let forwarded: Value = serde_json::from_str(&message_text(
        summary_server
            .recv_client_message()
            .await
            .expect("summary request"),
    ))
    .expect("summary request json");
    assert!(forwarded.get("previous_response_id").is_none());
    assert_eq!(
        forwarded["context_management"],
        json!({
            "type": "compaction",
            "compact_threshold": 12345
        })
    );
}

#[tokio::test]
async fn summary_request_without_previous_response_id_uses_auxiliary_session() {
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let ordinary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&summary_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&ordinary_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        Arc::new(connector),
    );

    let summary = post_responses(app.clone(), auxiliary_summary_request(None)).await;
    assert_eq!(summary.status(), StatusCode::OK);
    let summary_payload: Value = serde_json::from_str(&message_text(
        summary_server
            .recv_client_message()
            .await
            .expect("summary request"),
    ))
    .expect("summary request json");
    assert!(summary_payload.get("previous_response_id").is_none());

    let ordinary = post_responses(app, json!({"model":"gpt-5.4","input":"ordinary"})).await;
    assert_eq!(ordinary.status(), StatusCode::OK);
}

#[tokio::test]
async fn summary_response_id_is_not_registered_as_continuation_marker() {
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&summary_server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let summary = post_responses(app.clone(), auxiliary_summary_request(Some("response-1"))).await;
    assert_eq!(summary.status(), StatusCode::OK);
    let _ = summary_server
        .recv_client_message()
        .await
        .expect("summary request");
    summary_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-summary"}}"#)
        .await;
    let _ = to_bytes(summary.into_body(), usize::MAX)
        .await
        .expect("summary body");

    let rejected = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"resume",
            "previous_response_id":"response-summary"
        }),
    )
    .await;
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(rejected.into_body(), usize::MAX)
        .await
        .expect("rejected body");
    let payload: Value = serde_json::from_slice(&body).expect("rejected json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");
}

#[tokio::test]
async fn transient_summary_request_does_not_evict_existing_retained_marker() {
    let retained_server = Arc::new(ScriptedWebSocketServer::start().await);
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let resumed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&retained_server),
            turn_state: Some("turn-state-1".to_string()),
        },
        PlannedConnection {
            server: Arc::clone(&summary_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&resumed_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        Arc::new(connector),
    );

    let seed = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(seed.status(), StatusCode::OK);
    let _ = retained_server
        .recv_client_message()
        .await
        .expect("seed request");
    retained_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(seed.into_body(), usize::MAX)
        .await
        .expect("seed body");
    retained_server.send_close(1000, "seed complete").await;
    sleep(Duration::from_millis(50)).await;

    let summary = post_responses(app.clone(), auxiliary_summary_request(Some("response-1"))).await;
    assert_eq!(summary.status(), StatusCode::OK);
    let _ = summary_server
        .recv_client_message()
        .await
        .expect("summary request");
    summary_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-summary"}}"#)
        .await;
    let _ = to_bytes(summary.into_body(), usize::MAX)
        .await
        .expect("summary body");

    let resumed = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"resume",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed_payload: Value = serde_json::from_str(&message_text(
        resumed_server
            .recv_client_message()
            .await
            .expect("resumed request"),
    ))
    .expect("resumed request json");
    assert_eq!(resumed_payload["previous_response_id"], "response-1");
}

#[tokio::test]
async fn transient_summary_request_uses_no_retained_capacity_after_completion() {
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let ordinary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&summary_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&ordinary_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        Arc::new(connector),
    );

    let summary = post_responses(app.clone(), auxiliary_summary_request(None)).await;
    assert_eq!(summary.status(), StatusCode::OK);
    let _ = summary_server
        .recv_client_message()
        .await
        .expect("summary request");
    summary_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-summary"}}"#)
        .await;
    let _ = to_bytes(summary.into_body(), usize::MAX)
        .await
        .expect("summary body");

    let ordinary = post_responses(app, json!({"model":"gpt-5.4","input":"ordinary"})).await;
    assert_eq!(ordinary.status(), StatusCode::OK);
}

#[tokio::test]
async fn transient_summary_request_can_run_while_previous_marker_is_active_at_capacity() {
    let retained_server = Arc::new(ScriptedWebSocketServer::start().await);
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&retained_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&summary_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        Arc::new(connector),
    );

    let seed = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(seed.status(), StatusCode::OK);
    let _ = retained_server
        .recv_client_message()
        .await
        .expect("seed request");
    retained_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(seed.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let active = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    let _ = retained_server
        .recv_client_message()
        .await
        .expect("active followup request");

    let summary = post_responses(app, auxiliary_summary_request(Some("response-1"))).await;
    assert_eq!(summary.status(), StatusCode::OK);
}

#[tokio::test]
async fn transient_summary_request_failure_or_drop_does_not_leave_capacity_blocked() {
    let failed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let ordinary_after_failed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let failure_connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&failed_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&ordinary_after_failed_server),
            turn_state: None,
        },
    ]);
    let failure_app = build_test_router(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        Arc::new(failure_connector),
    );

    let failed_summary = post_responses(failure_app.clone(), auxiliary_summary_request(None)).await;
    assert_eq!(failed_summary.status(), StatusCode::OK);
    let _ = failed_server
        .recv_client_message()
        .await
        .expect("failed summary request");
    failed_server
        .send_text(r#"{"type":"response.failed","response":{"id":"response-summary"},"error":{"code":"upstream_response_failed","message":"failed"}}"#)
        .await;
    let _ = to_bytes(failed_summary.into_body(), usize::MAX)
        .await
        .expect("failed summary body");

    let ordinary_after_failed = post_responses(
        failure_app,
        json!({"model":"gpt-5.4","input":"ordinary-after-failed"}),
    )
    .await;
    assert_eq!(ordinary_after_failed.status(), StatusCode::OK);

    let dropped_server = Arc::new(ScriptedWebSocketServer::start().await);
    let ordinary_after_drop_server = Arc::new(ScriptedWebSocketServer::start().await);
    let drop_connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&dropped_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&ordinary_after_drop_server),
            turn_state: None,
        },
    ]);
    let drop_app = build_test_router(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        Arc::new(drop_connector),
    );

    let dropped_summary = post_responses(drop_app.clone(), auxiliary_summary_request(None)).await;
    assert_eq!(dropped_summary.status(), StatusCode::OK);
    let _ = dropped_server
        .recv_client_message()
        .await
        .expect("dropped summary request");
    drop(dropped_summary);
    sleep(Duration::from_millis(50)).await;

    let ordinary_after_drop = post_responses(
        drop_app,
        json!({"model":"gpt-5.4","input":"ordinary-after-drop"}),
    )
    .await;
    assert_eq!(ordinary_after_drop.status(), StatusCode::OK);
}

#[tokio::test]
async fn transient_summary_request_terminal_paths_close_pump_or_upstream_handle() {
    let completion_server = Arc::new(ScriptedWebSocketServer::start().await);
    let completion_connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&completion_server),
        turn_state: None,
    }]);
    let completion_app = build_test_router(
        ThreadlineConfig::default(),
        Arc::new(completion_connector.clone()),
    );

    let completion_response = post_responses(completion_app, auxiliary_summary_request(None)).await;
    assert_eq!(completion_response.status(), StatusCode::OK);
    let _ = completion_server
        .recv_client_message()
        .await
        .expect("completion summary request");
    completion_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-summary"}}"#)
        .await;
    let _ = to_bytes(completion_response.into_body(), usize::MAX)
        .await
        .expect("completion summary body");
    sleep(Duration::from_millis(50)).await;
    let completion_sockets = completion_connector.recorded_websockets().await;
    assert!(completion_sockets[0].upgrade().is_none());

    let failure_server = Arc::new(ScriptedWebSocketServer::start().await);
    let failure_connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&failure_server),
        turn_state: None,
    }]);
    let failure_app = build_test_router(
        ThreadlineConfig::default(),
        Arc::new(failure_connector.clone()),
    );

    let failure_response = post_responses(failure_app, auxiliary_summary_request(None)).await;
    assert_eq!(failure_response.status(), StatusCode::OK);
    let _ = failure_server
        .recv_client_message()
        .await
        .expect("failure summary request");
    failure_server
        .send_text(r#"{"type":"response.failed","response":{"id":"response-summary"},"error":{"code":"upstream_response_failed","message":"failed"}}"#)
        .await;
    let _ = to_bytes(failure_response.into_body(), usize::MAX)
        .await
        .expect("failure summary body");
    sleep(Duration::from_millis(50)).await;
    let failure_sockets = failure_connector.recorded_websockets().await;
    assert!(failure_sockets[0].upgrade().is_none());

    let drop_server = Arc::new(ScriptedWebSocketServer::start().await);
    let drop_connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&drop_server),
        turn_state: None,
    }]);
    let drop_app = build_test_router(
        ThreadlineConfig::default(),
        Arc::new(drop_connector.clone()),
    );

    let drop_response = post_responses(drop_app, auxiliary_summary_request(None)).await;
    assert_eq!(drop_response.status(), StatusCode::OK);
    let _ = drop_server
        .recv_client_message()
        .await
        .expect("drop summary request");
    drop(drop_response);
    sleep(Duration::from_millis(50)).await;
    let drop_sockets = drop_connector.recorded_websockets().await;
    assert!(drop_sockets[0].upgrade().is_none());
}

#[tokio::test]
async fn concurrent_marker_reuse_returns_conflict_and_client_drop_releases_the_lease() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let initial = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(initial.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let active = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    let _ = server
        .recv_client_message()
        .await
        .expect("active followup request");

    let conflict = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"conflict",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    drop(active);
    sleep(Duration::from_millis(50)).await;

    let retried = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"retry",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(retried.status(), StatusCode::OK);
}

#[tokio::test]
async fn retained_session_capacity_exhaustion_returns_503() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server,
        turn_state: None,
    }]);
    let app = build_test_router(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        Arc::new(connector),
    );

    let active = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"first"})).await;
    assert_eq!(active.status(), StatusCode::OK);

    let exhausted = post_responses(app, json!({"model":"gpt-5.4","input":"second"})).await;
    assert_eq!(exhausted.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(exhausted.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(
        payload["error"]["code"],
        "retained_session_capacity_exceeded"
    );

    drop(active);
}

#[tokio::test]
async fn upstream_connect_failure_returns_502() {
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(FailingConnector));

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"connect"})).await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(
        payload["error"]["code"],
        "upstream_websocket_connect_failed"
    );
}

#[tokio::test]
async fn upstream_pretty_json_is_compacted_before_downstream_sse() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"pretty-delta"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server
        .recv_client_message()
        .await
        .expect("pretty delta request");
    server
        .send_text("{\n  \"type\": \"response.output_text.delta\",\n  \"delta\": \"hello\"\n}")
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    assert_eq!(
        frames.len(),
        3,
        "expected delta, completed, and bare DONE SSE frames, got body: {body_text}"
    );

    let (event, data) = sse_event_and_data(frames[0]);
    let payload: Value = serde_json::from_str(data).expect("delta json");
    let (completed_event, completed_data) = sse_event_and_data(frames[1]);
    let completed_payload: Value = serde_json::from_str(completed_data).expect("completed json");

    assert_eq!(event, "response.output_text.delta");
    assert_eq!(
        payload,
        json!({"type":"response.output_text.delta","delta":"hello"})
    );
    assert_eq!(completed_event, "response.completed");
    assert_eq!(
        completed_payload,
        json!({"type":"response.completed","response":{"id":"response-1"}})
    );

    assert_done_frame(frames[2]);
}

#[tokio::test]
async fn downstream_body_stream_exposes_route_chunk_boundaries() {
    let app = axum::Router::new().route(
        "/chunks",
        axum::routing::get(|| async {
            Body::from_stream(stream::iter([
                Ok::<_, std::convert::Infallible>(Bytes::from_static(b"first")),
                Ok::<_, std::convert::Infallible>(Bytes::from_static(b"second")),
            ]))
        }),
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/chunks")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);

    let mut body_stream = response.into_body().into_data_stream();
    let first = next_body_chunk(&mut body_stream).await;
    let second = next_body_chunk(&mut body_stream).await;
    let third = body_stream.next().await;

    assert_eq!(first, Bytes::from_static(b"first"));
    assert_eq!(second, Bytes::from_static(b"second"));
    assert!(third.is_none(), "expected EOF after the second body chunk");
}

#[tokio::test]
async fn upstream_pretty_response_completed_is_compacted_before_downstream_sse() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"pretty-completed"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server
        .recv_client_message()
        .await
        .expect("pretty completed request");
    server
        .send_text(
            "{\n  \"type\": \"response.completed\",\n  \"response\": {\n    \"id\": \"response-1\"\n  }\n}",
        )
        .await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    assert_eq!(
        frames.len(),
        2,
        "expected completed SSE plus bare DONE frame, got body: {body_text}"
    );

    let (event, data) = sse_event_and_data(frames[0]);
    let payload: Value = serde_json::from_str(data).expect("completed json");

    assert_eq!(event, "response.completed");
    assert_eq!(
        payload,
        json!({"type":"response.completed","response":{"id":"response-1"}})
    );

    assert_done_frame(frames[1]);
}

#[tokio::test]
async fn downstream_completed_and_done_are_separate_body_chunks_before_eof() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"chunk-boundary"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server
        .recv_client_message()
        .await
        .expect("chunk-boundary request");
    server
        .send_text(
            "{\n  \"type\": \"response.completed\",\n  \"response\": {\n    \"id\": \"response-1\"\n  }\n}",
        )
        .await;

    let mut body_stream = response.into_body().into_data_stream();
    let first = next_body_chunk(&mut body_stream).await;
    let first_text = String::from_utf8(first.to_vec()).expect("utf8 first chunk");
    assert!(
        !first_text.contains("data: [DONE]"),
        "expected the completed chunk to exclude the bare DONE sentinel"
    );
    let (event, data) = sse_event_and_data(first_text.trim_end());
    let payload: Value = serde_json::from_str(data).expect("completed json");
    assert_eq!(event, "response.completed");
    assert_eq!(
        payload,
        json!({"type":"response.completed","response":{"id":"response-1"}}),
        "expected the first chunk to contain only the compact response.completed SSE frame"
    );

    let second = match body_stream.next().await {
        Some(Ok(chunk)) => chunk,
        Some(Err(error)) => panic!("expected a bare DONE chunk, got body error: {error}"),
        None => panic!(
            "expected a separate bare DONE chunk after the completed chunk, but reached EOF after first chunk: {first_text:?}"
        ),
    };
    let third = body_stream.next().await;

    assert_eq!(
        second,
        Bytes::from_static(b"data: [DONE]\n\n"),
        "expected the second chunk to be exactly the bare downstream DONE sentinel"
    );
    assert!(third.is_none(), "expected EOF after the bare DONE chunk");
}

#[tokio::test]
async fn completed_marker_can_be_reused_after_terminal_chunk_before_done_or_eof() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let seed = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(seed.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(seed.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let active = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    let active_payload: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("active request"),
    ))
    .expect("active request json");
    assert_eq!(active_payload["previous_response_id"], "response-1");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-2"}}"#)
        .await;

    let mut active_body = active.into_body().into_data_stream();
    let first_chunk = next_body_chunk(&mut active_body).await;
    let first_text = String::from_utf8(first_chunk.to_vec()).expect("utf8 first chunk");
    let (event, data) = sse_event_and_data(first_text.trim_end());
    let payload: Value = serde_json::from_str(data).expect("completed json");
    assert_eq!(event, "response.completed");
    assert_eq!(payload["response"]["id"], "response-2");

    let resumed = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"resume-before-done",
            "previous_response_id":"response-2"
        }),
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed_payload: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("resumed request"),
    ))
    .expect("resumed request json");
    assert_eq!(resumed_payload["previous_response_id"], "response-2");

    let done_chunk = next_body_chunk(&mut active_body).await;
    assert_eq!(done_chunk, Bytes::from_static(b"data: [DONE]\n\n"));
    assert!(
        active_body.next().await.is_none(),
        "expected EOF after DONE"
    );

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-3"}}"#)
        .await;
    let _ = to_bytes(resumed.into_body(), usize::MAX)
        .await
        .expect("resumed body");
}

#[tokio::test]
async fn recoverable_upstream_close_releases_prior_marker_before_body_drop() {
    let first_server = Arc::new(ScriptedWebSocketServer::start().await);
    let reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&first_server),
            turn_state: Some("turn-state-1".to_string()),
        },
        PlannedConnection {
            server: Arc::clone(&reconnect_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let initial = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(initial.status(), StatusCode::OK);
    let _ = first_server
        .recv_client_message()
        .await
        .expect("seed request");
    first_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;

    let mut initial_body = initial.into_body().into_data_stream();
    let first_chunk = next_body_chunk(&mut initial_body).await;
    let first_text = String::from_utf8(first_chunk.to_vec()).expect("utf8 first chunk");
    let (event, data) = sse_event_and_data(first_text.trim_end());
    let payload: Value = serde_json::from_str(data).expect("completed json");
    assert_eq!(event, "response.completed");
    assert_eq!(payload["response"]["id"], "response-1");

    first_server.send_close(1000, "done").await;
    sleep(Duration::from_millis(50)).await;

    let resumed = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"resume-after-close",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed_payload: Value = serde_json::from_str(&message_text(
        reconnect_server
            .recv_client_message()
            .await
            .expect("resumed request"),
    ))
    .expect("resumed request json");
    assert_eq!(resumed_payload["previous_response_id"], "response-1");

    let done_chunk = next_body_chunk(&mut initial_body).await;
    assert_eq!(done_chunk, Bytes::from_static(b"data: [DONE]\n\n"));
    assert!(
        initial_body.next().await.is_none(),
        "expected EOF after DONE"
    );

    reconnect_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-2"}}"#)
        .await;
    let _ = to_bytes(resumed.into_body(), usize::MAX)
        .await
        .expect("resumed body");
}

#[tokio::test]
async fn live_shaped_response_completed_with_internal_tool_name_still_reaches_done_and_eof() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(
        app,
        json!({"model":"gpt-5.4","input":"live-shaped-completed"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server
        .recv_client_message()
        .await
        .expect("live-shaped-completed request");
    server
        .send_text(
            r#"{"type":"response.completed","response":{"id":"response-1","output":[{"type":"function_call","name":"threadline_echo","call_id":"call-1"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}}"#,
        )
        .await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(
        frames.len(),
        2,
        "expected completed SSE plus bare DONE frame, got body: {body_text}"
    );

    let (event, data) = sse_event_and_data(frames[0]);
    let payload: Value = serde_json::from_str(data).expect("completed json");
    assert_eq!(event, "response.completed");
    assert_eq!(payload["response"]["id"], "response-1");
    assert_eq!(
        payload["response"]["output"][0]["name"], "threadline_echo",
        "expected payload normalization to stay unchanged for response.completed"
    );
    assert_done_frame(frames[1]);
}

#[tokio::test]
async fn completed_with_internal_function_call_and_assistant_text_synthesizes_delta() {
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-internal-visible-text",
            "output": [
                {
                    "type": "function_call",
                    "name": "threadline_echo",
                    "call_id": "call-internal"
                },
                {
                    "id": "assistant-item-internal-visible",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "visible completed text"
                        }
                    ]
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![completed_event.clone()]).await;

    assert_eq!(capture.downstream_events.len(), 2);
    assert_eq!(
        capture.downstream_events[0].event,
        "response.output_text.delta"
    );
    assert_eq!(
        capture.downstream_events[0].payload,
        json!({
            "type": "response.output_text.delta",
            "delta": "visible completed text",
            "item_id": "assistant-item-internal-visible",
            "output_index": 1,
            "content_index": 0
        })
    );
    assert_eq!(capture.downstream_events[1].event, "response.completed");
    assert_eq!(capture.downstream_events[1].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn upstream_response_failed_emits_response_failed_terminal_event() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"failure"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("failure request");
    server
        .send_text(r#"{"type":"response.failed","response":{"id":"response-1"},"error":{"code":"upstream_response_failed","message":"failed"}}"#)
        .await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("failed frame"));
    let payload: Value = serde_json::from_str(data).expect("failed json");

    assert_eq!(frames.len(), 2);
    assert_eq!(event, "response.failed");
    assert_eq!(payload["type"], "response.failed");
    assert_eq!(payload["response"]["id"], "response-1");
    assert_eq!(payload["response"]["status"], "failed");
    assert_eq!(
        payload["response"]["error"]["code"],
        "upstream_response_failed"
    );
    assert_eq!(payload["response"]["error"]["message"], "failed");
    assert_done_frame(frames[1]);
}

#[tokio::test]
async fn response_failed_preserves_prior_completed_marker_for_resume() {
    let first_server = Arc::new(ScriptedWebSocketServer::start().await);
    let reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&first_server),
            turn_state: Some("turn-state-1".to_string()),
        },
        PlannedConnection {
            server: Arc::clone(&reconnect_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let initial = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(initial.status(), StatusCode::OK);
    let _ = first_server
        .recv_client_message()
        .await
        .expect("seed request");
    first_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(initial.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let failed = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"failure",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(failed.status(), StatusCode::OK);
    let failed_payload: Value = serde_json::from_str(&message_text(
        first_server
            .recv_client_message()
            .await
            .expect("failed request message"),
    ))
    .expect("failed request json");
    assert!(failed_payload.get("response").is_none());
    assert_eq!(failed_payload["previous_response_id"], "response-1");
    first_server
        .send_text(r#"{"type":"response.failed","response":{"id":"response-failed"},"error":{"code":"upstream_response_failed","message":"failed"}}"#)
        .await;
    let _ = to_bytes(failed.into_body(), usize::MAX)
        .await
        .expect("failed body");

    first_server.send_close(1000, "failed turn complete").await;
    sleep(Duration::from_millis(50)).await;

    let resumed = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"resume",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed_payload: Value = serde_json::from_str(&message_text(
        reconnect_server
            .recv_client_message()
            .await
            .expect("resumed request message"),
    ))
    .expect("resumed request json");
    assert!(resumed_payload.get("response").is_none());
    assert_eq!(resumed_payload["previous_response_id"], "response-1");
    reconnect_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-2"}}"#)
        .await;
    let _ = to_bytes(resumed.into_body(), usize::MAX)
        .await
        .expect("resumed body");
}

#[tokio::test]
async fn failed_turn_releases_prior_marker_before_body_drop() {
    let first_server = Arc::new(ScriptedWebSocketServer::start().await);
    let reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&first_server),
            turn_state: Some("turn-state-1".to_string()),
        },
        PlannedConnection {
            server: Arc::clone(&reconnect_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let initial = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(initial.status(), StatusCode::OK);
    let _ = first_server
        .recv_client_message()
        .await
        .expect("seed request");
    first_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(initial.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let failed = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"failure",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(failed.status(), StatusCode::OK);
    let failed_payload: Value = serde_json::from_str(&message_text(
        first_server
            .recv_client_message()
            .await
            .expect("failed request message"),
    ))
    .expect("failed request json");
    assert_eq!(failed_payload["previous_response_id"], "response-1");
    first_server
        .send_text(r#"{"type":"response.failed","response":{"id":"response-failed"},"error":{"code":"upstream_response_failed","message":"failed"}}"#)
        .await;

    let mut failed_body = failed.into_body().into_data_stream();
    let failed_chunk = next_body_chunk(&mut failed_body).await;
    let failed_text = String::from_utf8(failed_chunk.to_vec()).expect("utf8 failed chunk");
    let (event, data) = sse_event_and_data(failed_text.trim_end());
    let failed_event: Value = serde_json::from_str(data).expect("failed event json");
    assert_eq!(event, "response.failed");
    assert_eq!(failed_event["response"]["id"], "response-failed");

    let resumed = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"resume-before-failed-body-drop",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed_payload: Value = serde_json::from_str(&message_text(
        reconnect_server
            .recv_client_message()
            .await
            .expect("resumed request message"),
    ))
    .expect("resumed request json");
    assert_eq!(resumed_payload["previous_response_id"], "response-1");

    let done_chunk = next_body_chunk(&mut failed_body).await;
    assert_eq!(done_chunk, Bytes::from_static(b"data: [DONE]\n\n"));
    assert!(
        failed_body.next().await.is_none(),
        "expected EOF after DONE"
    );

    reconnect_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-2"}}"#)
        .await;
    let _ = to_bytes(resumed.into_body(), usize::MAX)
        .await
        .expect("resumed body");
}

#[tokio::test]
async fn response_failed_id_is_not_a_continuation_marker() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let initial = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(initial.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(initial.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let failed = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"failure",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(failed.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("failed request");
    server
        .send_text(r#"{"type":"response.failed","response":{"id":"response-failed"},"error":{"code":"upstream_response_failed","message":"failed"}}"#)
        .await;
    let _ = to_bytes(failed.into_body(), usize::MAX)
        .await
        .expect("failed body");

    let rejected = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"invalid-resume",
            "previous_response_id":"response-failed"
        }),
    )
    .await;

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(rejected.into_body(), usize::MAX)
        .await
        .expect("rejected body");
    let payload: Value = serde_json::from_slice(&body).expect("rejected json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");
}

#[tokio::test]
async fn upstream_error_event_emits_a_single_compact_sse_error() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"error"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("error request");
    server
        .send_text(
            r#"{"type":"error","error":{"code":"upstream_boom","message":"boom"},"status":502}"#,
        )
        .await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("error frame"));
    let payload: Value = serde_json::from_str(data).expect("error json");

    assert_eq!(
        frames.len(),
        1,
        "raw upstream error must not emit terminal response.failed plus DONE frames: {body_text}"
    );
    assert_eq!(event, "error");
    assert_eq!(payload["error"]["code"], "upstream_error_event");
    assert!(
        payload.get("response").is_none(),
        "raw upstream error must not be rewritten into a response.failed payload: {payload:?}"
    );
}

#[tokio::test]
async fn done_sentinel_is_not_forwarded_as_downstream_data() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"done"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("done request");
    server.send_text("[DONE]").await;
    server.send_close(1000, "done").await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("error frame"));
    let payload: Value = serde_json::from_str(data).expect("error json");

    assert_eq!(event, "error");
    assert_eq!(payload["error"]["code"], "upstream_invalid_json");
    assert!(!body_text.contains("data: [DONE]"));
}

#[tokio::test]
async fn malformed_upstream_json_emits_a_stable_sse_error_and_releases_the_marker() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let initial = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(initial.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let response = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"malformed",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server
        .recv_client_message()
        .await
        .expect("malformed request");
    server.send_text("not-json").await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("error frame"));
    let payload: Value = serde_json::from_str(data).expect("error json");

    assert_eq!(event, "error");
    assert_eq!(payload["error"]["code"], "upstream_invalid_json");

    sleep(Duration::from_millis(50)).await;
    let retried = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"retry",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(retried.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(retried.into_body(), usize::MAX)
        .await
        .expect("retry body");
    let payload: Value = serde_json::from_slice(&body).expect("retry json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");
}

#[tokio::test]
async fn nested_response_markers_remain_reusable_without_main_agent_assumptions() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector.clone()));

    let first = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"first"})).await;
    let _ = server.recv_client_message().await.expect("first request");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-parent"}}"#)
        .await;
    let _ = to_bytes(first.into_body(), usize::MAX)
        .await
        .expect("first body");

    let second = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"second",
            "previous_response_id":"response-parent"
        }),
    )
    .await;
    let second_payload: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("second request"),
    ))
    .expect("second request json");
    assert!(second_payload.get("response").is_none());
    assert_eq!(second_payload["previous_response_id"], "response-parent");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-child"}}"#)
        .await;
    let _ = to_bytes(second.into_body(), usize::MAX)
        .await
        .expect("second body");

    let third = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"third",
            "previous_response_id":"response-parent"
        }),
    )
    .await;
    let third_payload: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("third request"),
    ))
    .expect("third request json");
    assert!(third_payload.get("response").is_none());
    assert_eq!(third_payload["previous_response_id"], "response-parent");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-third"}}"#)
        .await;
    let _ = to_bytes(third.into_body(), usize::MAX)
        .await
        .expect("third body");

    let fourth = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"fourth",
            "previous_response_id":"response-child"
        }),
    )
    .await;
    let fourth_payload: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("fourth request"),
    ))
    .expect("fourth request json");
    assert!(fourth_payload.get("response").is_none());
    assert_eq!(fourth_payload["previous_response_id"], "response-child");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-fourth"}}"#)
        .await;
    let _ = to_bytes(fourth.into_body(), usize::MAX)
        .await
        .expect("fourth body");

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn supported_request_fields_are_preserved_while_codex_unsupported_fields_are_omitted() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "type":"wrong.type",
            "model":"gpt-5.4",
            "input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]}],
            "tools":[{
                "type":"function",
                "name":"user_tool",
                "description":"User-defined tool",
                "parameters":{"type":"object","properties":{},"additionalProperties":false}
            }],
            "tool_choice":{"type":"function","name":"user_tool"},
            "parallel_tool_calls":false,
            "reasoning":{"effort":"high","summary":"auto"},
            "include":["reasoning.encrypted_content"],
            "store":true,
            "prompt_cache_key":"cache-key-1",
            "max_output_tokens":321,
            "max_tokens":654,
            "max_completion_tokens":987,
            "truncation":"auto"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let request_payload: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("request message"),
    ))
    .expect("request json");
    assert_eq!(request_payload["type"], "response.create");
    assert!(request_payload.get("response").is_none());
    let response_payload = &request_payload;
    let tools = response_payload["tools"].as_array().expect("tools array");

    assert!(tools.iter().any(|tool| tool["name"] == "user_tool"));
    assert_eq!(
        response_payload["tool_choice"],
        json!({"type":"function","name":"user_tool"})
    );
    assert_eq!(response_payload["parallel_tool_calls"], Value::Bool(false));
    assert_eq!(
        response_payload["reasoning"],
        json!({"effort":"high","summary":"auto"})
    );
    assert_eq!(
        response_payload["include"],
        json!(["reasoning.encrypted_content"])
    );
    assert_eq!(response_payload["store"], Value::Bool(false));
    assert_eq!(
        response_payload["prompt_cache_key"],
        Value::String("cache-key-1".to_string())
    );
    assert_codex_unsupported_response_fields_are_absent(response_payload);

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
}

#[tokio::test]
async fn missing_or_null_instructions_are_normalized_for_upstream_response_create() {
    let missing_server = Arc::new(ScriptedWebSocketServer::start().await);
    let null_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&missing_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&null_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let missing_response = post_responses(
        app.clone(),
        json!({
            "type":"wrong.type",
            "model":"gpt-5.4",
            "input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]}],
            "max_output_tokens":321,
            "max_tokens":654,
            "max_completion_tokens":987,
            "truncation":"auto"
        }),
    )
    .await;
    assert_eq!(missing_response.status(), StatusCode::OK);

    let missing_payload: Value = serde_json::from_str(&message_text(
        missing_server
            .recv_client_message()
            .await
            .expect("missing request message"),
    ))
    .expect("missing request json");
    assert_eq!(missing_payload["type"], "response.create");
    assert_eq!(
        missing_payload["instructions"],
        Value::String(String::new())
    );
    assert_eq!(missing_payload["store"], Value::Bool(false));
    assert_codex_unsupported_response_fields_are_absent(&missing_payload);

    missing_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(missing_response.into_body(), usize::MAX)
        .await
        .expect("missing body");

    let null_response = post_responses(
        app,
        json!({
            "type":"wrong.type",
            "model":"gpt-5.4",
            "input":[{"role":"user","content":[{"type":"input_text","text":"hello again"}]}],
            "instructions":null,
            "max_output_tokens":654,
            "max_tokens":321,
            "max_completion_tokens":111,
            "truncation":"disabled"
        }),
    )
    .await;
    assert_eq!(null_response.status(), StatusCode::OK);

    let null_payload: Value = serde_json::from_str(&message_text(
        null_server
            .recv_client_message()
            .await
            .expect("null request message"),
    ))
    .expect("null request json");
    assert_eq!(null_payload["type"], "response.create");
    assert_eq!(null_payload["instructions"], Value::String(String::new()));
    assert_eq!(null_payload["store"], Value::Bool(false));
    assert_codex_unsupported_response_fields_are_absent(&null_payload);

    null_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-2"}}"#)
        .await;
    let _ = to_bytes(null_response.into_body(), usize::MAX)
        .await
        .expect("null body");
}

#[tokio::test]
async fn explicit_instructions_are_preserved_in_upstream_response_create() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "type":"wrong.type",
            "model":"gpt-5.4",
            "input":[{"role":"user","content":[{"type":"input_text","text":"preserve me"}]}],
            "instructions":"explicit downstream instructions",
            "max_output_tokens":987,
            "max_tokens":654,
            "max_completion_tokens":321,
            "truncation":"auto"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = serde_json::from_str(&message_text(
        server
            .recv_client_message()
            .await
            .expect("explicit instructions request message"),
    ))
    .expect("explicit instructions request json");
    assert_eq!(payload["type"], "response.create");
    assert_eq!(
        payload["instructions"],
        Value::String("explicit downstream instructions".to_string())
    );
    assert_codex_unsupported_response_fields_are_absent(&payload);

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-3"}}"#)
        .await;
    let _ = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("explicit instructions body");
}

struct DownstreamSseEvent {
    event: String,
    payload: Value,
}

struct ApplyPatchStreamCapture {
    upstream_events: Vec<Value>,
    downstream_events: Vec<DownstreamSseEvent>,
    done_frame: String,
}

struct CompactionStreamCapture {
    upstream_events: Vec<Value>,
    downstream_events: Vec<DownstreamSseEvent>,
    done_frame: String,
}

struct CompletedOutputStreamCapture {
    downstream_events: Vec<DownstreamSseEvent>,
    done_frame: String,
}

async fn capture_visible_apply_patch_stream() -> ApplyPatchStreamCapture {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response =
        post_responses(app, json!({"model":"gpt-5.4","input":"apply-patch-stream"})).await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = server
        .recv_client_message()
        .await
        .expect("apply patch stream request");

    let mut body_stream = response.into_body().into_data_stream();

    let added_event = json!({
        "type": "response.output_item.added",
        "output_index": 0,
        "item": {
            "id": "fc_apply_patch_1",
            "type": "function_call",
            "call_id": "call-apply-patch",
            "name": "apply_patch",
            "arguments": ""
        }
    });
    server.send_text(&added_event.to_string()).await;

    let added_chunk = next_body_chunk(&mut body_stream).await;
    let added_text = String::from_utf8(added_chunk.to_vec()).expect("utf8 added chunk");
    let (added_sse_event, added_sse_data) = sse_event_and_data(added_text.trim_end());
    let added_payload: Value = serde_json::from_str(added_sse_data).expect("added payload json");

    let first_delta_event = json!({
        "type": "response.function_call_arguments.delta",
        "output_index": 0,
        "item_id": "fc_apply_patch_1",
        "delta": "{\"input\":\"*** Begin Patch"
    });
    server.send_text(&first_delta_event.to_string()).await;

    let first_delta_chunk = next_body_chunk(&mut body_stream).await;
    let first_delta_text =
        String::from_utf8(first_delta_chunk.to_vec()).expect("utf8 first delta chunk");
    let (first_delta_sse_event, first_delta_sse_data) =
        sse_event_and_data(first_delta_text.trim_end());
    let first_delta_payload: Value =
        serde_json::from_str(first_delta_sse_data).expect("first delta payload json");

    let second_delta_event = json!({
        "type": "response.function_call_arguments.delta",
        "output_index": 0,
        "item_id": "fc_apply_patch_1",
        "delta": "\n*** End Patch\"}"
    });
    server.send_text(&second_delta_event.to_string()).await;

    let second_delta_chunk = next_body_chunk(&mut body_stream).await;
    let second_delta_text =
        String::from_utf8(second_delta_chunk.to_vec()).expect("utf8 second delta chunk");
    let (second_delta_sse_event, second_delta_sse_data) =
        sse_event_and_data(second_delta_text.trim_end());
    let second_delta_payload: Value =
        serde_json::from_str(second_delta_sse_data).expect("second delta payload json");

    let arguments_done_event = json!({
        "type": "response.function_call_arguments.done",
        "output_index": 0,
        "item_id": "fc_apply_patch_1",
        "arguments": "{\"input\":\"*** Begin Patch\n*** End Patch\"}"
    });
    server.send_text(&arguments_done_event.to_string()).await;

    let arguments_done_chunk = next_body_chunk(&mut body_stream).await;
    let arguments_done_text =
        String::from_utf8(arguments_done_chunk.to_vec()).expect("utf8 arguments done chunk");
    let (arguments_done_sse_event, arguments_done_sse_data) =
        sse_event_and_data(arguments_done_text.trim_end());
    let arguments_done_payload: Value =
        serde_json::from_str(arguments_done_sse_data).expect("arguments done payload json");

    let done_event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "id": "fc_apply_patch_1",
            "type": "function_call",
            "call_id": "call-apply-patch",
            "name": "apply_patch",
            "arguments": "{\"input\":\"*** Begin Patch\n*** End Patch\"}"
        }
    });
    server.send_text(&done_event.to_string()).await;

    let done_chunk = next_body_chunk(&mut body_stream).await;
    let done_text = String::from_utf8(done_chunk.to_vec()).expect("utf8 done chunk");
    let (done_sse_event, done_sse_data) = sse_event_and_data(done_text.trim_end());
    let done_payload: Value = serde_json::from_str(done_sse_data).expect("done payload json");

    let completed_event =
        json!({"type": "response.completed", "response": {"id": "response-apply-patch"}});
    server.send_text(&completed_event.to_string()).await;

    let completed_chunk = next_body_chunk(&mut body_stream).await;
    let completed_text = String::from_utf8(completed_chunk.to_vec()).expect("utf8 completed chunk");
    let (completed_sse_event, completed_sse_data) = sse_event_and_data(completed_text.trim_end());
    let completed_payload: Value =
        serde_json::from_str(completed_sse_data).expect("completed payload json");

    let done_sentinel_chunk = next_body_chunk(&mut body_stream).await;
    let done_sentinel_text =
        String::from_utf8(done_sentinel_chunk.to_vec()).expect("utf8 done sentinel chunk");
    assert_done_frame(done_sentinel_text.trim_end());
    assert!(
        body_stream.next().await.is_none(),
        "expected EOF after downstream DONE sentinel"
    );

    ApplyPatchStreamCapture {
        upstream_events: vec![
            added_event,
            first_delta_event,
            second_delta_event,
            arguments_done_event,
            done_event,
            completed_event,
        ],
        downstream_events: vec![
            DownstreamSseEvent {
                event: added_sse_event.to_string(),
                payload: added_payload,
            },
            DownstreamSseEvent {
                event: first_delta_sse_event.to_string(),
                payload: first_delta_payload,
            },
            DownstreamSseEvent {
                event: second_delta_sse_event.to_string(),
                payload: second_delta_payload,
            },
            DownstreamSseEvent {
                event: arguments_done_sse_event.to_string(),
                payload: arguments_done_payload,
            },
            DownstreamSseEvent {
                event: done_sse_event.to_string(),
                payload: done_payload,
            },
            DownstreamSseEvent {
                event: completed_sse_event.to_string(),
                payload: completed_payload,
            },
        ],
        done_frame: done_sentinel_text.trim_end().to_string(),
    }
}

async fn capture_compaction_stream(compaction_name_field: &str) -> CompactionStreamCapture {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response =
        post_responses(app, json!({"model":"gpt-5.4","input":"compaction-stream"})).await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = server
        .recv_client_message()
        .await
        .expect("compaction stream request");

    let mut body_stream = response.into_body().into_data_stream();

    let added_event = json!({
        "type": "response.output_item.added",
        "output_index": 0,
        "item": {
            "id": "cmp_1",
            "type": "compaction",
            compaction_name_field: "threadline_echo",
            "encrypted_content": "opaque-added"
        }
    });
    server.send_text(&added_event.to_string()).await;

    let added_chunk = next_body_chunk(&mut body_stream).await;
    let added_text = String::from_utf8(added_chunk.to_vec()).expect("utf8 added chunk");
    let (added_sse_event, added_sse_data) = sse_event_and_data(added_text.trim_end());
    let added_payload: Value = serde_json::from_str(added_sse_data).expect("added payload json");

    let done_event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "id": "cmp_1",
            "type": "compaction",
            compaction_name_field: "threadline_echo",
            "encrypted_content": "opaque-done"
        }
    });
    server.send_text(&done_event.to_string()).await;

    let done_chunk = next_body_chunk(&mut body_stream).await;
    let done_text = String::from_utf8(done_chunk.to_vec()).expect("utf8 done chunk");
    let (done_sse_event, done_sse_data) = sse_event_and_data(done_text.trim_end());
    let done_payload: Value = serde_json::from_str(done_sse_data).expect("done payload json");

    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-compaction",
            "output": [
                {
                    "id": "cmp_1",
                    "type": "compaction",
                    compaction_name_field: "threadline_echo",
                    "encrypted_content": "opaque-completed"
                },
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "done"
                        }
                    ]
                }
            ]
        }
    });
    server.send_text(&completed_event.to_string()).await;

    let completed_chunk = next_body_chunk(&mut body_stream).await;
    let completed_text = String::from_utf8(completed_chunk.to_vec()).expect("utf8 completed chunk");
    let (completed_sse_event, completed_sse_data) = sse_event_and_data(completed_text.trim_end());
    let completed_payload: Value =
        serde_json::from_str(completed_sse_data).expect("completed payload json");

    let done_sentinel_chunk = next_body_chunk(&mut body_stream).await;
    let done_sentinel_text =
        String::from_utf8(done_sentinel_chunk.to_vec()).expect("utf8 done sentinel chunk");
    assert_done_frame(done_sentinel_text.trim_end());
    assert!(
        body_stream.next().await.is_none(),
        "expected EOF after downstream DONE sentinel"
    );

    CompactionStreamCapture {
        upstream_events: vec![added_event, done_event, completed_event],
        downstream_events: vec![
            DownstreamSseEvent {
                event: added_sse_event.to_string(),
                payload: added_payload,
            },
            DownstreamSseEvent {
                event: done_sse_event.to_string(),
                payload: done_payload,
            },
            DownstreamSseEvent {
                event: completed_sse_event.to_string(),
                payload: completed_payload,
            },
        ],
        done_frame: done_sentinel_text.trim_end().to_string(),
    }
}

async fn capture_completed_output_stream(
    upstream_events: Vec<Value>,
) -> CompletedOutputStreamCapture {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(
        app,
        json!({"model":"gpt-5.4","input":"completed-output-stream"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = server
        .recv_client_message()
        .await
        .expect("completed output stream request");

    let mut body_stream = response.into_body().into_data_stream();
    for upstream_event in upstream_events {
        server.send_text(&upstream_event.to_string()).await;
    }

    let mut downstream_events = Vec::new();
    let done_frame = loop {
        let chunk = match body_stream.next().await {
            Some(Ok(chunk)) => chunk,
            Some(Err(error)) => panic!("expected SSE chunk, got body error: {error}"),
            None => panic!("expected downstream DONE sentinel before EOF"),
        };

        let chunk_text = String::from_utf8(chunk.to_vec()).expect("utf8 SSE chunk");
        let frame = chunk_text.trim_end();
        if frame == "data: [DONE]" {
            break frame.to_string();
        }

        let (event, data) = sse_event_and_data(frame);
        let payload: Value = serde_json::from_str(data).expect("SSE payload json");
        downstream_events.push(DownstreamSseEvent {
            event: event.to_string(),
            payload,
        });
    };

    assert!(
        body_stream.next().await.is_none(),
        "expected EOF after downstream DONE sentinel"
    );

    CompletedOutputStreamCapture {
        downstream_events,
        done_frame,
    }
}

#[tokio::test]
async fn responses_bridge_apply_patch_added_precedes_delta_with_vs_code_required_metadata() {
    let capture = capture_visible_apply_patch_stream().await;

    let added_index = capture
        .downstream_events
        .iter()
        .position(|event| event.event == "response.output_item.added")
        .expect("added event");
    let first_delta_index = capture
        .downstream_events
        .iter()
        .position(|event| event.event == "response.function_call_arguments.delta")
        .expect("first delta event");

    assert!(
        added_index < first_delta_index,
        "expected visible function call added event before argument deltas"
    );

    let added_payload = &capture.downstream_events[added_index].payload;
    assert_eq!(added_payload["type"], "response.output_item.added");
    assert_eq!(added_payload["output_index"], 0);
    assert_eq!(added_payload["item"]["type"], "function_call");
    assert_eq!(added_payload["item"]["name"], "apply_patch");
    assert_eq!(added_payload["item"]["call_id"], "call-apply-patch");
    assert_eq!(added_payload["item"]["id"], "fc_apply_patch_1");
}

#[tokio::test]
async fn responses_bridge_apply_patch_delta_matches_added_output_index() {
    let capture = capture_visible_apply_patch_stream().await;

    let added_payload = &capture.downstream_events[0].payload;
    let added_output_index = added_payload["output_index"].clone();
    let delta_events: Vec<&DownstreamSseEvent> = capture
        .downstream_events
        .iter()
        .filter(|event| event.event == "response.function_call_arguments.delta")
        .collect();

    assert_eq!(
        delta_events.len(),
        2,
        "expected two visible argument deltas"
    );
    for delta_event in delta_events {
        assert_eq!(
            delta_event.payload["output_index"], added_output_index,
            "expected visible argument delta to preserve added output_index"
        );
        assert_eq!(delta_event.payload["item_id"], "fc_apply_patch_1");
    }
}

#[tokio::test]
async fn responses_bridge_apply_patch_done_preserves_complete_arguments() {
    let capture = capture_visible_apply_patch_stream().await;

    let arguments_done_payload = &capture.downstream_events[3].payload;
    assert_eq!(
        arguments_done_payload["type"],
        "response.function_call_arguments.done"
    );
    assert_eq!(arguments_done_payload["output_index"], 0);
    assert_eq!(arguments_done_payload["item_id"], "fc_apply_patch_1");
    assert_eq!(
        arguments_done_payload["arguments"],
        "{\"input\":\"*** Begin Patch\n*** End Patch\"}"
    );

    let done_payload = &capture.downstream_events[4].payload;
    assert_eq!(done_payload["type"], "response.output_item.done");
    assert_eq!(done_payload["output_index"], 0);
    assert_eq!(done_payload["item"]["id"], "fc_apply_patch_1");
    assert_eq!(done_payload["item"]["call_id"], "call-apply-patch");
    assert_eq!(done_payload["item"]["name"], "apply_patch");
    assert_eq!(
        done_payload["item"]["arguments"],
        "{\"input\":\"*** Begin Patch\n*** End Patch\"}"
    );
}

#[tokio::test]
async fn responses_bridge_visible_function_call_payloads_are_forwarded_without_mutation() {
    let capture = capture_visible_apply_patch_stream().await;

    for (index, upstream_event) in capture.upstream_events.iter().enumerate() {
        assert_eq!(
            capture.downstream_events[index].payload, *upstream_event,
            "expected downstream SSE payload to match upstream event for index {index}"
        );
        assert_eq!(
            capture.downstream_events[index].event,
            upstream_event["type"]
                .as_str()
                .expect("upstream event type"),
            "expected downstream SSE event name to match upstream event type for index {index}"
        );
    }
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn compaction_output_item_added_is_forwarded_downstream() {
    let capture = capture_compaction_stream("name").await;

    assert_eq!(
        capture.downstream_events[0].event,
        "response.output_item.added"
    );
    assert_eq!(
        capture.downstream_events[0].payload,
        capture.upstream_events[0]
    );
    assert_eq!(
        capture.downstream_events[0].payload["item"]["type"],
        "compaction"
    );
    assert_eq!(
        capture.downstream_events[0].payload["item"]["encrypted_content"],
        "opaque-added"
    );
}

#[tokio::test]
async fn compaction_output_item_done_is_forwarded_downstream() {
    let capture = capture_compaction_stream("tool_name").await;

    assert_eq!(
        capture.downstream_events[1].event,
        "response.output_item.done"
    );
    assert_eq!(
        capture.downstream_events[1].payload,
        capture.upstream_events[1]
    );
    assert_eq!(
        capture.downstream_events[1].payload["item"]["type"],
        "compaction"
    );
    assert_eq!(
        capture.downstream_events[1].payload["item"]["encrypted_content"],
        "opaque-done"
    );
}

#[tokio::test]
async fn completed_response_preserves_compaction_output() {
    let capture = capture_compaction_stream("name").await;

    assert_eq!(capture.downstream_events[2].event, "response.completed");
    assert_eq!(
        capture.downstream_events[2].payload,
        capture.upstream_events[2]
    );
    assert_eq!(
        capture.downstream_events[2].payload["response"]["output"][0]["type"],
        "compaction"
    );
    assert_eq!(
        capture.downstream_events[2].payload["response"]["output"][0]["encrypted_content"],
        "opaque-completed"
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn completed_only_assistant_output_text_is_synthesized_as_delta() {
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-completed-only",
            "output": [
                {
                    "id": "assistant-item-1",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "hello from completed"
                        }
                    ]
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![completed_event.clone()]).await;

    assert_eq!(capture.downstream_events.len(), 2);
    assert_eq!(
        capture.downstream_events[0].event,
        "response.output_text.delta"
    );
    assert_eq!(
        capture.downstream_events[0].payload,
        json!({
            "type": "response.output_text.delta",
            "delta": "hello from completed",
            "item_id": "assistant-item-1",
            "output_index": 0,
            "content_index": 0
        })
    );
    assert_eq!(capture.downstream_events[1].event, "response.completed");
    assert_eq!(capture.downstream_events[1].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn output_text_done_only_text_is_synthesized_as_delta() {
    let output_text_done_event = json!({
        "type": "response.output_text.done",
        "item_id": "assistant-item-done-only",
        "output_index": 0,
        "content_index": 0,
        "text": "hello from output_text.done"
    });
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-output-text-done-only"
        }
    });

    let capture = capture_completed_output_stream(vec![
        output_text_done_event.clone(),
        completed_event.clone(),
    ])
    .await;

    assert_eq!(capture.downstream_events.len(), 3);
    assert_eq!(
        capture.downstream_events[0].event,
        "response.output_text.delta"
    );
    assert_eq!(
        capture.downstream_events[0].payload,
        json!({
            "type": "response.output_text.delta",
            "delta": "hello from output_text.done",
            "item_id": "assistant-item-done-only",
            "output_index": 0,
            "content_index": 0
        })
    );
    assert_eq!(
        capture.downstream_events[1].event,
        "response.output_text.done"
    );
    assert_eq!(capture.downstream_events[1].payload, output_text_done_event);
    assert_eq!(capture.downstream_events[2].event, "response.completed");
    assert_eq!(capture.downstream_events[2].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn output_item_done_message_text_is_synthesized_as_delta() {
    let output_item_done_event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "id": "assistant-item-done-message",
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": "hello from output_item.done"
                }
            ]
        }
    });
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-output-item-done-message"
        }
    });

    let capture = capture_completed_output_stream(vec![
        output_item_done_event.clone(),
        completed_event.clone(),
    ])
    .await;

    assert_eq!(capture.downstream_events.len(), 3);
    assert_eq!(
        capture.downstream_events[0].event,
        "response.output_text.delta"
    );
    assert_eq!(
        capture.downstream_events[0].payload,
        json!({
            "type": "response.output_text.delta",
            "delta": "hello from output_item.done",
            "item_id": "assistant-item-done-message",
            "output_index": 0,
            "content_index": 0
        })
    );
    assert_eq!(
        capture.downstream_events[1].event,
        "response.output_item.done"
    );
    assert_eq!(capture.downstream_events[1].payload, output_item_done_event);
    assert_eq!(capture.downstream_events[2].event, "response.completed");
    assert_eq!(capture.downstream_events[2].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn streamed_output_text_delta_is_not_duplicated_from_completed_output() {
    let delta_event = json!({
        "type": "response.output_text.delta",
        "delta": "hello from stream",
        "item_id": "assistant-item-2",
        "output_index": 0,
        "content_index": 0
    });
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-prior-delta",
            "output": [
                {
                    "id": "assistant-item-2",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "hello from stream"
                        }
                    ]
                }
            ]
        }
    });

    let capture =
        capture_completed_output_stream(vec![delta_event.clone(), completed_event.clone()]).await;

    assert_eq!(capture.downstream_events.len(), 2);
    assert_eq!(
        capture
            .downstream_events
            .iter()
            .filter(|event| event.event == "response.output_text.delta")
            .count(),
        1,
        "expected the existing streamed delta to remain unique"
    );
    assert_eq!(
        capture.downstream_events[0].event,
        "response.output_text.delta"
    );
    assert_eq!(capture.downstream_events[0].payload, delta_event);
    assert_eq!(capture.downstream_events[1].event, "response.completed");
    assert_eq!(capture.downstream_events[1].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn multiple_done_only_visible_text_sources_are_not_dropped() {
    let output_text_done_event = json!({
        "type": "response.output_text.done",
        "item_id": "assistant-item-first",
        "output_index": 0,
        "content_index": 0,
        "text": "first visible text"
    });
    let output_item_done_event = json!({
        "type": "response.output_item.done",
        "output_index": 1,
        "item": {
            "id": "assistant-item-second",
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": "second visible text"
                }
            ]
        }
    });
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-multiple-visible-sources",
            "output": [
                {
                    "id": "assistant-item-third",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "third visible text"
                        }
                    ]
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![
        output_text_done_event.clone(),
        output_item_done_event.clone(),
        completed_event.clone(),
    ])
    .await;

    let delta_payloads: Vec<Value> = capture
        .downstream_events
        .iter()
        .filter(|event| event.event == "response.output_text.delta")
        .map(|event| event.payload.clone())
        .collect();

    assert_eq!(
        delta_payloads,
        vec![
            json!({
                "type": "response.output_text.delta",
                "delta": "first visible text",
                "item_id": "assistant-item-first",
                "output_index": 0,
                "content_index": 0
            }),
            json!({
                "type": "response.output_text.delta",
                "delta": "second visible text",
                "item_id": "assistant-item-second",
                "output_index": 1,
                "content_index": 0
            }),
            json!({
                "type": "response.output_text.delta",
                "delta": "third visible text",
                "item_id": "assistant-item-third",
                "output_index": 0,
                "content_index": 0
            })
        ]
    );
    assert_eq!(capture.downstream_events.len(), 6);
    assert_eq!(
        capture.downstream_events[1].event,
        "response.output_text.done"
    );
    assert_eq!(capture.downstream_events[1].payload, output_text_done_event);
    assert_eq!(
        capture.downstream_events[3].event,
        "response.output_item.done"
    );
    assert_eq!(capture.downstream_events[3].payload, output_item_done_event);
    assert_eq!(capture.downstream_events[5].event, "response.completed");
    assert_eq!(capture.downstream_events[5].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn completed_without_assistant_output_text_does_not_synthesize_delta() {
    let completed_cases = vec![
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-function-call-only",
                "output": [
                    {
                        "type": "function_call",
                        "name": "apply_patch",
                        "call_id": "call-1"
                    }
                ]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-non-output-text",
                "output": [
                    {
                        "id": "assistant-item-3",
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "refusal",
                                "refusal": "declined"
                            }
                        ]
                    }
                ]
            }
        }),
    ];

    for completed_event in completed_cases {
        let capture = capture_completed_output_stream(vec![completed_event.clone()]).await;
        assert_eq!(
            capture.downstream_events.len(),
            1,
            "expected only response.completed when no assistant output_text is present"
        );
        assert_eq!(capture.downstream_events[0].event, "response.completed");
        assert_eq!(capture.downstream_events[0].payload, completed_event);
        assert_eq!(capture.done_frame, "data: [DONE]");
    }
}

#[tokio::test]
async fn completed_only_synthetic_delta_precedes_completed_and_done_chunks() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(
        app,
        json!({"model":"gpt-5.4","input":"completed-output-order"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = server
        .recv_client_message()
        .await
        .expect("completed output ordering request");

    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-ordering",
            "output": [
                {
                    "id": "assistant-item-4",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "ordered text"
                        }
                    ]
                }
            ]
        }
    });
    server.send_text(&completed_event.to_string()).await;

    let mut body_stream = response.into_body().into_data_stream();

    let first_chunk = next_body_chunk(&mut body_stream).await;
    let first_text = String::from_utf8(first_chunk.to_vec()).expect("utf8 first chunk");
    let (first_event, first_data) = sse_event_and_data(first_text.trim_end());
    let first_payload: Value = serde_json::from_str(first_data).expect("first payload json");
    assert_eq!(first_event, "response.output_text.delta");
    assert_eq!(first_payload["delta"], "ordered text");

    let second_chunk = next_body_chunk(&mut body_stream).await;
    let second_text = String::from_utf8(second_chunk.to_vec()).expect("utf8 second chunk");
    let (second_event, second_data) = sse_event_and_data(second_text.trim_end());
    let second_payload: Value = serde_json::from_str(second_data).expect("second payload json");
    assert_eq!(second_event, "response.completed");
    assert_eq!(second_payload, completed_event);

    let third_chunk = next_body_chunk(&mut body_stream).await;
    assert_eq!(third_chunk, Bytes::from_static(b"data: [DONE]\n\n"));
    assert!(
        body_stream.next().await.is_none(),
        "expected EOF after downstream DONE sentinel"
    );
}

#[tokio::test]
async fn completed_only_synthetic_delta_releases_marker_before_queued_completed() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let seed = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(seed.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-1"}}"#)
        .await;
    let _ = to_bytes(seed.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let active = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"completed-output-order",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    let active_payload: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("active request"),
    ))
    .expect("active request json");
    assert_eq!(active_payload["previous_response_id"], "response-1");

    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-ordering",
            "output": [
                {
                    "id": "assistant-item-4",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "ordered text"
                        }
                    ]
                }
            ]
        }
    });
    server.send_text(&completed_event.to_string()).await;

    let mut active_body = active.into_body().into_data_stream();
    let first_chunk = next_body_chunk(&mut active_body).await;
    let first_text = String::from_utf8(first_chunk.to_vec()).expect("utf8 first chunk");
    let (event, data) = sse_event_and_data(first_text.trim_end());
    let payload: Value = serde_json::from_str(data).expect("synthetic delta json");
    assert_eq!(event, "response.output_text.delta");
    assert_eq!(payload["delta"], "ordered text");

    let resumed = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"resume-before-queued-completed",
            "previous_response_id":"response-ordering"
        }),
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed_payload: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("resumed request"),
    ))
    .expect("resumed request json");
    assert_eq!(resumed_payload["previous_response_id"], "response-ordering");

    let second_chunk = next_body_chunk(&mut active_body).await;
    let second_text = String::from_utf8(second_chunk.to_vec()).expect("utf8 second chunk");
    let (second_event, second_data) = sse_event_and_data(second_text.trim_end());
    let second_payload: Value = serde_json::from_str(second_data).expect("completed json");
    assert_eq!(second_event, "response.completed");
    assert_eq!(second_payload, completed_event);

    let third_chunk = next_body_chunk(&mut active_body).await;
    assert_eq!(third_chunk, Bytes::from_static(b"data: [DONE]\n\n"));
    assert!(
        active_body.next().await.is_none(),
        "expected EOF after DONE"
    );

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-3"}}"#)
        .await;
    let _ = to_bytes(resumed.into_body(), usize::MAX)
        .await
        .expect("resumed body");
}

#[tokio::test]
async fn malformed_completed_output_does_not_panic_or_synthesize_delta() {
    let completed_cases = vec![
        (
            "missing-output",
            json!({
                "type": "response.completed",
                "response": {
                    "id": "response-missing-output"
                }
            }),
        ),
        (
            "output-not-array",
            json!({
                "type": "response.completed",
                "response": {
                    "id": "response-output-not-array",
                    "output": {}
                }
            }),
        ),
        (
            "content-missing",
            json!({
                "type": "response.completed",
                "response": {
                    "id": "response-content-missing",
                    "output": [
                        {
                            "type": "message",
                            "role": "assistant"
                        }
                    ]
                }
            }),
        ),
        (
            "content-not-array",
            json!({
                "type": "response.completed",
                "response": {
                    "id": "response-content-not-array",
                    "output": [
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": {}
                        }
                    ]
                }
            }),
        ),
        (
            "non-string-text",
            json!({
                "type": "response.completed",
                "response": {
                    "id": "response-non-string-text",
                    "output": [
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": 42
                                }
                            ]
                        }
                    ]
                }
            }),
        ),
    ];

    for (case_name, completed_event) in completed_cases {
        let capture = capture_completed_output_stream(vec![completed_event.clone()]).await;
        assert_eq!(
            capture.downstream_events.len(),
            1,
            "expected malformed case {case_name} to forward response.completed without a synthetic delta"
        );
        assert_eq!(
            capture.downstream_events[0].event, "response.completed",
            "expected malformed case {case_name} to preserve the completed event"
        );
        assert_eq!(
            capture.downstream_events[0].payload, completed_event,
            "expected malformed case {case_name} to remain unchanged downstream"
        );
        assert_eq!(capture.done_frame, "data: [DONE]");
    }
}

#[tokio::test]
async fn multi_part_assistant_output_text_is_synthesized_as_single_delta_from_first_contributing_part_metadata()
 {
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-multi-part",
            "output": [
                {
                    "type": "function_call",
                    "name": "apply_patch",
                    "call_id": "call-metadata-anchor"
                },
                {
                    "id": "assistant-item-5",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "   "
                        },
                        {
                            "type": "output_text",
                            "text": "Hello"
                        },
                        {
                            "type": "output_text",
                            "text": ""
                        },
                        {
                            "type": "output_text",
                            "text": "\n"
                        },
                        {
                            "type": "output_text",
                            "text": " world"
                        }
                    ]
                },
                {
                    "id": "assistant-item-6",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "!"
                        }
                    ]
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![completed_event.clone()]).await;

    assert_eq!(capture.downstream_events.len(), 2);
    assert_eq!(
        capture.downstream_events[0].payload,
        json!({
            "type": "response.output_text.delta",
            "delta": "Hello world!",
            "item_id": "assistant-item-5",
            "output_index": 1,
            "content_index": 1
        })
    );
    assert_eq!(capture.downstream_events[1].event, "response.completed");
    assert_eq!(capture.downstream_events[1].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}
