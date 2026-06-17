use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::{Arc, Weak};
use std::time::Duration;

use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures_util::{StreamExt, future::BoxFuture, stream};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;
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

fn assistant_text_completed_event(response_id: &str, text: &str) -> Value {
    json!({
        "type": "response.completed",
        "response": {
            "id": response_id,
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": text
                        }
                    ]
                }
            ]
        }
    })
}

fn no_observable_output_failed_event(response_id: &str) -> Value {
    json!({
        "type": "response.failed",
        "response": {
            "id": response_id,
            "status": "failed",
            "error": {
                "code": "threadline_no_observable_output",
                "message": "Response contained no observable output."
            }
        }
    })
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
        "context_management": {
            "type": "compaction",
            "compact_threshold": 12345
        },
        "tools": [
            {
                "type": "function",
                "name": "user_tool",
                "description": "User-defined tool",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            },
            {
                "type": "function",
                "name": "threadline_echo",
                "description": "Threadline internal tool",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "value": {
                            "type": "string"
                        }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }
            }
        ],
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
        .send_text(&assistant_text_completed_event("response-1", "first completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-2", "second completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-1", "first completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-2", "second completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
    assert_eq!(
        payload["context_management"],
        json!({
            "type": "compaction",
            "compact_threshold": 12345
        })
    );
    let tools = payload["tools"].as_array().expect("tools array");
    assert!(tools.iter().any(|tool| tool["name"] == "user_tool"));
    assert!(!tools.iter().any(|tool| tool["name"] == "threadline_echo"));
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

    let response = post_responses(app, auxiliary_summary_request(Some("response-1"))).await;
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
    let tools = forwarded["tools"].as_array().expect("tools array");
    assert!(tools.iter().any(|tool| tool["name"] == "user_tool"));
    assert!(!tools.iter().any(|tool| tool["name"] == "threadline_echo"));
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
async fn request_routing_diagnostics_distinguish_summary_without_logging_raw_request_content() {
    let trace_guard = TraceCaptureGuard::begin().await;
    let summary_server = Arc::new(ScriptedWebSocketServer::start().await);
    let normal_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&summary_server),
            turn_state: None,
        },
        PlannedConnection {
            server: Arc::clone(&normal_server),
            turn_state: None,
        },
    ]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));
    let raw_request_secret = "secret-123";
    let raw_request_account = "acct_123456789";

    let summary = post_responses(app.clone(), auxiliary_summary_request(Some("response-1"))).await;
    assert_eq!(summary.status(), StatusCode::OK);
    let _ = summary_server
        .recv_client_message()
        .await
        .expect("summary request");
    summary_server
        .send_text(
            &assistant_text_completed_event("response-summary-diagnostics", "summary completion")
                .to_string(),
        )
        .await;
    let _ = to_bytes(summary.into_body(), usize::MAX)
        .await
        .expect("summary body");

    let normal = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input": format!("Account {raw_request_account} credential {raw_request_secret}")
        }),
    )
    .await;
    assert_eq!(normal.status(), StatusCode::OK);
    let _ = normal_server
        .recv_client_message()
        .await
        .expect("normal request");
    normal_server
        .send_text(
            &assistant_text_completed_event("response-normal-diagnostics", "normal completion")
                .to_string(),
        )
        .await;
    let _ = to_bytes(normal.into_body(), usize::MAX)
        .await
        .expect("normal body");

    let logs = trace_guard.logs();
    let summary_line = logs
        .lines()
        .find(|line| {
            line.contains("responses_request_routed")
                && line.contains("request_class=\"auxiliary_summary\"")
        })
        .expect("summary routing diagnostics trace line");
    assert!(summary_line.contains("previous_response_id_present=true"));
    assert!(summary_line.contains("context_management_present=true"));
    assert!(!summary_line.contains("response-1"));
    assert!(!summary_line.contains(auxiliary_summary_text()));

    let normal_line = logs
        .lines()
        .find(|line| {
            line.contains("responses_request_routed") && line.contains("request_class=\"normal\"")
        })
        .expect("normal routing diagnostics trace line");
    assert!(normal_line.contains("previous_response_id_present=false"));
    assert!(normal_line.contains("context_management_present=false"));
    assert!(!normal_line.contains(raw_request_secret));
    assert!(!normal_line.contains(raw_request_account));
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
        .send_text(
            &assistant_text_completed_event("response-summary", "summary completion").to_string(),
        )
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-1", "hello").to_string())
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
        assistant_text_completed_event("response-1", "hello")
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
    let completed_event = assistant_text_completed_event("response-1", "hello from completed");
    server
        .send_text(&serde_json::to_string_pretty(&completed_event).expect("pretty completed json"))
        .await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    assert_eq!(
        frames.len(),
        3,
        "expected synthetic delta, completed SSE, and bare DONE frame, got body: {body_text}"
    );

    let (delta_event, delta_data) = sse_event_and_data(frames[0]);
    let delta_payload: Value = serde_json::from_str(delta_data).expect("delta json");
    let (event, data) = sse_event_and_data(frames[1]);
    let payload: Value = serde_json::from_str(data).expect("completed json");

    assert_eq!(delta_event, "response.output_text.delta");
    assert_eq!(
        delta_payload,
        json!({
            "type":"response.output_text.delta",
            "delta":"hello from completed",
            "output_index":0,
            "content_index":0
        })
    );
    assert_eq!(event, "response.completed");
    assert_eq!(payload, completed_event);

    assert_done_frame(frames[2]);
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
    let completed_event = assistant_text_completed_event("response-1", "hello from completed");
    server
        .send_text(&serde_json::to_string_pretty(&completed_event).expect("pretty completed json"))
        .await;

    let mut body_stream = response.into_body().into_data_stream();
    let first = next_body_chunk(&mut body_stream).await;
    let first_text = String::from_utf8(first.to_vec()).expect("utf8 first chunk");
    assert!(
        !first_text.contains("data: [DONE]"),
        "expected the first synthetic delta chunk to exclude the bare DONE sentinel"
    );
    let (event, data) = sse_event_and_data(first_text.trim_end());
    let payload: Value = serde_json::from_str(data).expect("delta json");
    assert_eq!(event, "response.output_text.delta");
    assert_eq!(
        payload,
        json!({
            "type":"response.output_text.delta",
            "delta":"hello from completed",
            "output_index":0,
            "content_index":0
        }),
        "expected the first chunk to contain only the synthetic response.output_text.delta SSE frame"
    );

    let second = match body_stream.next().await {
        Some(Ok(chunk)) => chunk,
        Some(Err(error)) => panic!("expected a completed chunk, got body error: {error}"),
        None => panic!(
            "expected a separate completed chunk after the synthetic delta chunk, but reached EOF after first chunk: {first_text:?}"
        ),
    };
    let second_text = String::from_utf8(second.to_vec()).expect("utf8 second chunk");
    let (second_event, second_data) = sse_event_and_data(second_text.trim_end());
    let second_payload: Value = serde_json::from_str(second_data).expect("completed json");
    assert_eq!(second_event, "response.completed");
    assert_eq!(second_payload, completed_event);

    let third = match body_stream.next().await {
        Some(Ok(chunk)) => chunk,
        Some(Err(error)) => panic!("expected a bare DONE chunk, got body error: {error}"),
        None => panic!(
            "expected a separate bare DONE chunk after the completed chunk, but reached EOF after second chunk: {second_text:?}"
        ),
    };
    let fourth = body_stream.next().await;

    assert_eq!(
        third,
        Bytes::from_static(b"data: [DONE]\n\n"),
        "expected the third chunk to be exactly the bare downstream DONE sentinel"
    );
    assert!(fourth.is_none(), "expected EOF after the bare DONE chunk");
}

#[tokio::test]
async fn completed_marker_can_be_reused_after_completed_chunk_before_done_or_eof() {
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-2", "followup completion").to_string())
        .await;

    let mut active_body = active.into_body().into_data_stream();
    let first_chunk = next_body_chunk(&mut active_body).await;
    let first_text = String::from_utf8(first_chunk.to_vec()).expect("utf8 first chunk");
    let (event, data) = sse_event_and_data(first_text.trim_end());
    let payload: Value = serde_json::from_str(data).expect("delta json");
    assert_eq!(event, "response.output_text.delta");
    assert_eq!(payload["delta"], "followup completion");

    let completed_chunk = next_body_chunk(&mut active_body).await;
    let completed_text = String::from_utf8(completed_chunk.to_vec()).expect("utf8 completed chunk");
    let (completed_event, completed_data) = sse_event_and_data(completed_text.trim_end());
    let completed_payload: Value = serde_json::from_str(completed_data).expect("completed json");
    assert_eq!(completed_event, "response.completed");
    assert_eq!(
        completed_payload,
        assistant_text_completed_event("response-2", "followup completion")
    );

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
        .send_text(&assistant_text_completed_event("response-3", "resume completion").to_string())
        .await;
    let _ = to_bytes(resumed.into_body(), usize::MAX)
        .await
        .expect("resumed body");
}

#[tokio::test]
async fn recoverable_upstream_close_releases_prior_marker_after_completed_chunk_before_body_drop() {
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
        .await;

    let mut initial_body = initial.into_body().into_data_stream();
    let first_chunk = next_body_chunk(&mut initial_body).await;
    let first_text = String::from_utf8(first_chunk.to_vec()).expect("utf8 first chunk");
    let (event, data) = sse_event_and_data(first_text.trim_end());
    let payload: Value = serde_json::from_str(data).expect("delta json");
    assert_eq!(event, "response.output_text.delta");
    assert_eq!(payload["delta"], "seed completion");

    first_server.send_close(1000, "done").await;
    sleep(Duration::from_millis(50)).await;

    let completed_chunk = next_body_chunk(&mut initial_body).await;
    let completed_text = String::from_utf8(completed_chunk.to_vec()).expect("utf8 completed chunk");
    let (completed_event, completed_data) = sse_event_and_data(completed_text.trim_end());
    let completed_payload: Value = serde_json::from_str(completed_data).expect("completed json");
    assert_eq!(completed_event, "response.completed");
    assert_eq!(
        completed_payload,
        assistant_text_completed_event("response-1", "seed completion")
    );

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
        .send_text(&assistant_text_completed_event("response-2", "resume completion").to_string())
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
        3,
        "expected synthetic delta, completed SSE, and bare DONE frame, got body: {body_text}"
    );

    let (delta_event, delta_data) = sse_event_and_data(frames[0]);
    let delta_payload: Value = serde_json::from_str(delta_data).expect("delta json");
    assert_eq!(delta_event, "response.output_text.delta");
    assert_eq!(
        delta_payload,
        json!({
            "type": "response.output_text.delta",
            "delta": "done",
            "output_index": 1,
            "content_index": 0
        })
    );

    let (event, data) = sse_event_and_data(frames[1]);
    let payload: Value = serde_json::from_str(data).expect("completed json");
    assert_eq!(event, "response.completed");
    assert_eq!(payload["response"]["id"], "response-1");
    assert_eq!(
        payload["response"]["output"],
        json!([
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
        ])
    );
    assert_done_frame(frames[2]);
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
    assert_eq!(
        capture.downstream_events[1].payload,
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-internal-visible-text",
                "output": [
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
        })
    );
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
async fn terminal_failed_and_incomplete_payloads_preserve_vscode_terminal_fields() {
    let failed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let failed_connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&failed_server),
        turn_state: None,
    }]);
    let failed_app = build_test_router(ThreadlineConfig::default(), Arc::new(failed_connector));

    let failed_response = post_responses(
        failed_app,
        json!({"model":"gpt-5.4","input":"failed-terminal-fields"}),
    )
    .await;
    assert_eq!(failed_response.status(), StatusCode::OK);
    let _ = failed_server
        .recv_client_message()
        .await
        .expect("failed request");
    failed_server
        .send_text(
            r#"{"type":"response.failed","response":{"id":"response-failed-fields","model":"gpt-5.4","usage":{"input_tokens":10,"output_tokens":4,"total_tokens":14},"output":[{"id":"assistant-visible","type":"message","role":"assistant","content":[{"type":"output_text","text":"visible failed text"}]}]},"error":{"code":"upstream_response_failed","message":"failed"}}"#,
        )
        .await;

    let failed_body = timeout(
        Duration::from_secs(2),
        to_bytes(failed_response.into_body(), usize::MAX),
    )
    .await
    .expect("failed body timeout")
    .expect("failed body");
    let failed_text = String::from_utf8(failed_body.to_vec()).expect("utf8 failed body");
    let failed_frames = split_sse_frames(&failed_text);
    let (failed_event, failed_data) =
        sse_event_and_data(failed_frames.first().expect("failed frame"));
    let failed_payload: Value = serde_json::from_str(failed_data).expect("failed payload json");

    assert_eq!(failed_event, "response.failed");
    assert_eq!(failed_payload["response"]["id"], "response-failed-fields");
    assert_eq!(failed_payload["response"]["model"], "gpt-5.4");
    assert_eq!(failed_payload["response"]["usage"]["total_tokens"], 14);
    assert_eq!(
        assistant_output_text_from_completed(
            &json!({"response": failed_payload["response"].clone()})
        ),
        "visible failed text"
    );
    assert_done_frame(failed_frames[1]);

    let incomplete_server = Arc::new(ScriptedWebSocketServer::start().await);
    let incomplete_connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&incomplete_server),
        turn_state: None,
    }]);
    let incomplete_app =
        build_test_router(ThreadlineConfig::default(), Arc::new(incomplete_connector));

    let incomplete_response = post_responses(
        incomplete_app,
        json!({"model":"gpt-5.4","input":"incomplete-terminal-fields"}),
    )
    .await;
    assert_eq!(incomplete_response.status(), StatusCode::OK);
    let _ = incomplete_server
        .recv_client_message()
        .await
        .expect("incomplete request");
    incomplete_server
        .send_text(
            r#"{"type":"response.incomplete","response":{"id":"response-incomplete-fields","model":"gpt-5.4","usage":{"input_tokens":8,"output_tokens":3,"total_tokens":11},"output":[{"id":"assistant-partial","type":"message","role":"assistant","content":[{"type":"output_text","text":"visible partial text"}]}],"incomplete_details":{"reason":"max_output_tokens"}}}"#,
        )
        .await;
    incomplete_server.send_close(1000, "incomplete").await;

    let incomplete_body = timeout(
        Duration::from_secs(2),
        to_bytes(incomplete_response.into_body(), usize::MAX),
    )
    .await
    .expect("incomplete body timeout")
    .expect("incomplete body");
    let incomplete_text =
        String::from_utf8(incomplete_body.to_vec()).expect("utf8 incomplete body");
    let incomplete_frames = split_sse_frames(&incomplete_text);
    let (incomplete_event, incomplete_data) =
        sse_event_and_data(incomplete_frames.first().expect("incomplete frame"));
    let incomplete_payload: Value =
        serde_json::from_str(incomplete_data).expect("incomplete payload json");

    assert_eq!(incomplete_event, "response.incomplete");
    assert_eq!(
        incomplete_payload["response"]["id"],
        "response-incomplete-fields"
    );
    assert_eq!(incomplete_payload["response"]["model"], "gpt-5.4");
    assert_eq!(incomplete_payload["response"]["usage"]["total_tokens"], 11);
    assert_eq!(
        incomplete_payload["response"]["incomplete_details"]["reason"],
        "max_output_tokens"
    );
    assert_eq!(
        assistant_output_text_from_completed(
            &json!({"response": incomplete_payload["response"].clone()})
        ),
        "visible partial text"
    );
    assert_done_frame(incomplete_frames[1]);
}

#[tokio::test]
async fn upstream_incomplete_emits_terminal_response_incomplete_without_marker() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(
        app.clone(),
        json!({"model":"gpt-5.4","input":"terminal-incomplete"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server
        .recv_client_message()
        .await
        .expect("incomplete request");
    server
        .send_text(
            r#"{"type":"response.incomplete","response":{"id":"response-incomplete","model":"gpt-5.4","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5},"output":[{"id":"assistant-partial","type":"message","role":"assistant","content":[{"type":"output_text","text":"partial answer"}]}],"incomplete_details":{"reason":"max_output_tokens"}}}"#,
        )
        .await;
    server.send_close(1000, "incomplete").await;

    let body = timeout(
        Duration::from_secs(2),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("incomplete body timeout")
    .expect("incomplete body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("incomplete frame"));
    let payload: Value = serde_json::from_str(data).expect("incomplete json");

    assert_eq!(frames.len(), 2);
    assert_eq!(event, "response.incomplete");
    assert_eq!(payload["response"]["id"], "response-incomplete");
    assert_eq!(
        payload["response"]["incomplete_details"]["reason"],
        "max_output_tokens"
    );
    assert_done_frame(frames[1]);

    let rejected = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"invalid-incomplete-resume",
            "previous_response_id":"response-incomplete"
        }),
    )
    .await;

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let rejected_body = to_bytes(rejected.into_body(), usize::MAX)
        .await
        .expect("rejected body");
    let rejected_payload: Value = serde_json::from_slice(&rejected_body).expect("rejected json");
    assert_eq!(
        rejected_payload["error"]["code"],
        "previous_response_not_found"
    );
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-2", "resume completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
async fn upstream_error_event_emits_response_failed_and_done_without_successful_completion() {
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

    assert!(
        !body_text.contains("event: error\n"),
        "raw upstream error must not be forwarded as a raw error event: {body_text}"
    );

    let (event, data) = sse_event_and_data(frames.first().expect("failed frame"));
    let payload: Value = serde_json::from_str(data).expect("failed json");

    assert_eq!(
        frames.len(),
        2,
        "raw upstream error must be normalized into downstream response.failed plus DONE frames: {body_text}"
    );
    assert_eq!(event, "response.failed");
    assert_eq!(payload["type"], "response.failed");
    assert_eq!(payload["response"]["status"], "failed");
    assert_eq!(payload["response"]["error"]["code"], "upstream_error_event");
    assert!(
        payload["response"]["error"]["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()),
        "raw upstream error must surface a stable Threadline error message: {payload:?}"
    );
    assert!(
        !body_text.contains("event: response.completed\n"),
        "raw upstream error must not emit successful completion semantics: {body_text}"
    );
    assert_done_frame(frames[1]);
}

#[tokio::test]
async fn upstream_done_or_eof_without_completed_emits_response_failed_not_done_only() {
    for case_name in ["done", "eof"] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector = RecordingConnector::new(vec![PlannedConnection {
            server: Arc::clone(&server),
            turn_state: None,
        }]);
        let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

        let response = post_responses(
            app,
            json!({"model":"gpt-5.4","input":format!("terminal-{case_name}")}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = server
            .recv_client_message()
            .await
            .expect("terminal request");

        match case_name {
            "done" => {
                server.send_text("[DONE]").await;
                server.send_close(1000, "done before completed").await;
            }
            "eof" => {
                server.abort_connection().await;
            }
            _ => unreachable!("unexpected terminal case"),
        }

        let body = timeout(
            Duration::from_secs(2),
            to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("terminal body timeout")
        .expect("terminal body");
        let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
        let frames = split_sse_frames(&body_text);
        let (event, data) = sse_event_and_data(frames.first().expect("failed frame"));
        let payload: Value = serde_json::from_str(data).expect("failed json");

        assert_eq!(
            frames.len(),
            2,
            "expected terminal failed event plus DONE for {case_name}: {body_text}"
        );
        assert_eq!(event, "response.failed");
        assert_eq!(payload["type"], "response.failed");
        assert_eq!(payload["response"]["status"], "failed");
        assert!(
            payload["response"]["error"]["code"]
                .as_str()
                .is_some_and(|code| !code.is_empty()),
            "expected a stable failure code for {case_name}: {payload:?}"
        );
        assert_done_frame(frames[1]);
    }
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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
        .send_text(
            &assistant_text_completed_event("response-parent", "parent completion").to_string(),
        )
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
        .send_text(
            &assistant_text_completed_event("response-child", "child completion").to_string(),
        )
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
        .send_text(
            &assistant_text_completed_event("response-third", "third completion").to_string(),
        )
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
        .send_text(
            &assistant_text_completed_event("response-fourth", "fourth completion").to_string(),
        )
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

type SharedTraceBytes = Arc<StdMutex<Vec<u8>>>;
type ActiveTraceBytes = StdMutex<Option<SharedTraceBytes>>;

struct SharedLogBuffer {
    bytes: SharedTraceBytes,
}

impl SharedLogBuffer {
    fn new() -> Self {
        Self {
            bytes: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    fn logs(&self) -> String {
        String::from_utf8(self.bytes.lock().expect("log buffer lock").clone())
            .expect("utf8 trace logs")
    }
}

static TRACE_CAPTURE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static ACTIVE_TRACE_BUFFER: OnceLock<ActiveTraceBytes> = OnceLock::new();
static TRACE_SUBSCRIBER_INIT: OnceLock<()> = OnceLock::new();

fn trace_capture_lock() -> &'static Mutex<()> {
    TRACE_CAPTURE_LOCK.get_or_init(|| Mutex::new(()))
}

fn active_trace_buffer() -> &'static ActiveTraceBytes {
    ACTIVE_TRACE_BUFFER.get_or_init(|| StdMutex::new(None))
}

fn ensure_test_trace_subscriber() {
    TRACE_SUBSCRIBER_INIT.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .without_time()
            .with_ansi(false)
            .with_writer(GlobalTraceCapture)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("global trace subscriber should only initialize once");
    });
}

#[derive(Clone, Copy)]
struct GlobalTraceCapture;

struct GlobalTraceWriter;

impl Write for GlobalTraceWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(bytes) = active_trace_buffer()
            .lock()
            .expect("active trace buffer lock")
            .as_ref()
        {
            bytes
                .lock()
                .expect("log buffer lock")
                .extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for GlobalTraceCapture {
    type Writer = GlobalTraceWriter;

    fn make_writer(&'a self) -> Self::Writer {
        GlobalTraceWriter
    }
}

struct TraceCaptureGuard {
    _lock: tokio::sync::MutexGuard<'static, ()>,
    log_buffer: SharedLogBuffer,
}

impl TraceCaptureGuard {
    async fn begin() -> Self {
        let lock = trace_capture_lock().lock().await;
        ensure_test_trace_subscriber();
        let log_buffer = SharedLogBuffer::new();
        *active_trace_buffer()
            .lock()
            .expect("active trace buffer lock") = Some(Arc::clone(&log_buffer.bytes));
        Self {
            _lock: lock,
            log_buffer,
        }
    }

    fn logs(&self) -> String {
        self.log_buffer.logs()
    }
}

impl Drop for TraceCaptureGuard {
    fn drop(&mut self) {
        *active_trace_buffer()
            .lock()
            .expect("active trace buffer lock") = None;
    }
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

    let delta_chunk = next_body_chunk(&mut body_stream).await;
    let delta_text = String::from_utf8(delta_chunk.to_vec()).expect("utf8 delta chunk");
    let (delta_sse_event, delta_sse_data) = sse_event_and_data(delta_text.trim_end());
    let delta_payload: Value = serde_json::from_str(delta_sse_data).expect("delta payload json");

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
                event: delta_sse_event.to_string(),
                payload: delta_payload,
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

fn assistant_output_text_from_completed(payload: &Value) -> String {
    let mut text = String::new();

    let Some(output) = payload["response"]["output"].as_array() else {
        return text;
    };

    for item in output {
        if item["type"] != "message" || item["role"] != "assistant" {
            continue;
        }

        let Some(content) = item["content"].as_array() else {
            continue;
        };

        for part in content {
            if part["type"] != "output_text" {
                continue;
            }

            if let Some(segment) = part["text"].as_str() {
                text.push_str(segment);
            }
        }
    }

    text
}

fn output_text_delta_strings(events: &[DownstreamSseEvent]) -> Vec<String> {
    events
        .iter()
        .filter(|event| event.event == "response.output_text.delta")
        .filter_map(|event| {
            event.payload["delta"]
                .as_str()
                .map(|delta| delta.to_string())
        })
        .collect()
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

    for (index, upstream_event) in capture.upstream_events[..capture.upstream_events.len() - 1]
        .iter()
        .enumerate()
    {
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
    assert_eq!(capture.downstream_events[5].event, "response.completed");
    assert_eq!(
        capture.downstream_events[5].payload,
        capture.upstream_events[5]
    );
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
async fn completed_response_preserves_assistant_text_and_compaction_output() {
    let capture = capture_compaction_stream("name").await;

    assert_eq!(
        capture.downstream_events[2].event,
        "response.output_text.delta"
    );
    assert_eq!(
        capture.downstream_events[2].payload,
        json!({
            "type": "response.output_text.delta",
            "delta": "done",
            "output_index": 1,
            "content_index": 0
        })
    );
    assert_eq!(capture.downstream_events[3].event, "response.completed");
    assert_eq!(
        capture.downstream_events[3].payload,
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-compaction",
                "output": [
                    {
                        "id": "cmp_1",
                        "type": "compaction",
                        "name": "threadline_echo",
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
        })
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn completed_response_preserves_compaction_output() {
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-completed-compaction-only",
            "output": [
                {
                    "id": "cmp-completed-only",
                    "type": "compaction",
                    "tool_name": "threadline_echo",
                    "encrypted_content": "opaque-completed-only"
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![completed_event.clone()]).await;

    assert_eq!(capture.downstream_events.len(), 1);
    assert!(output_text_delta_strings(&capture.downstream_events).is_empty());
    assert_eq!(capture.downstream_events[0].event, "response.completed");
    assert_eq!(capture.downstream_events[0].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn compaction_output_item_done_counts_as_observable_output_when_forwarded() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"compaction-only"})).await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = server
        .recv_client_message()
        .await
        .expect("compaction-only request");

    server
        .send_text(
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"cmp_1","type":"compaction","tool_name":"threadline_echo","encrypted_content":"opaque-done"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-compaction-only"}}"#)
        .await;

    let body = timeout(
        Duration::from_secs(2),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("compaction-only body timeout")
    .expect("compaction-only body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 3);
    let done_frame = sse_event_and_data(frames[0]);
    let completed_frame = sse_event_and_data(frames[1]);

    assert_eq!(done_frame.0, "response.output_item.done");
    assert_eq!(
        serde_json::from_str::<Value>(done_frame.1).expect("compaction done json"),
        json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {
                "id": "cmp_1",
                "type": "compaction",
                "tool_name": "threadline_echo",
                "encrypted_content": "opaque-done"
            }
        })
    );
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(
        serde_json::from_str::<Value>(completed_frame.1).expect("compaction completed json"),
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-compaction-only"
            }
        })
    );
    assert_done_frame(frames[2]);
}

#[tokio::test]
async fn compaction_only_completed_output_counts_as_observable_output() {
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-compaction-observable-only",
            "output": [
                {
                    "id": "cmp-observable-only",
                    "type": "compaction",
                    "name": "threadline_echo",
                    "encrypted_content": "opaque-observable-only"
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![completed_event.clone()]).await;

    assert_eq!(capture.downstream_events.len(), 1);
    assert_eq!(capture.downstream_events[0].event, "response.completed");
    assert_eq!(capture.downstream_events[0].payload, completed_event);
    assert_ne!(capture.downstream_events[0].event, "response.failed");
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
    assert_eq!(
        capture.downstream_events[2].payload,
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-output-text-done-only",
                "output": [
                    {
                        "id": "assistant-item-done-only",
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "hello from output_text.done",
                                "annotations": []
                            }
                        ]
                    }
                ]
            }
        })
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn completed_without_visible_message_inserts_synthetic_assistant_message_from_done_text() {
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
            "id": "response-synthetic-completed-message",
            "output": [
                {
                    "type": "function_call",
                    "name": "threadline_echo",
                    "call_id": "call-1"
                },
                {
                    "id": "cmp-1",
                    "type": "compaction",
                    "encrypted_content": "opaque"
                }
            ]
        }
    });

    let capture =
        capture_completed_output_stream(vec![output_text_done_event.clone(), completed_event])
            .await;

    assert_eq!(capture.downstream_events.len(), 3);
    assert_eq!(
        capture.downstream_events[0].event,
        "response.output_text.delta"
    );
    assert_eq!(
        capture.downstream_events[0].payload["delta"],
        "hello from output_text.done"
    );
    assert_eq!(
        capture.downstream_events[1].event,
        "response.output_text.done"
    );
    assert_eq!(capture.downstream_events[1].payload, output_text_done_event);
    assert_eq!(capture.downstream_events[2].event, "response.completed");
    assert_eq!(
        capture.downstream_events[2].payload,
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-synthetic-completed-message",
                "output": [
                    {
                        "id": "cmp-1",
                        "type": "compaction",
                        "encrypted_content": "opaque"
                    },
                    {
                        "id": "assistant-item-done-only",
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "hello from output_text.done",
                                "annotations": []
                            }
                        ]
                    }
                ]
            }
        })
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn codex_output_item_done_message_becomes_vscode_completed_output() {
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
    assert_eq!(
        capture.downstream_events[2].payload,
        json!({
            "type": "response.completed",
            "response": {
                "id": "response-output-item-done-message",
                "output": [
                    {
                        "id": "assistant-item-done-message",
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "hello from output_item.done",
                                "annotations": []
                            }
                        ]
                    }
                ]
            }
        })
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn direct_output_text_delta_backfills_empty_completed_output() {
    let delta_event = json!({
        "type": "response.output_text.delta",
        "delta": "hello from direct delta",
        "item_id": "assistant-item-direct-delta",
        "output_index": 0,
        "content_index": 0
    });
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-direct-delta-backfill"
        }
    });

    let capture =
        capture_completed_output_stream(vec![delta_event.clone(), completed_event.clone()]).await;

    assert_eq!(capture.downstream_events.len(), 2);
    assert_eq!(
        output_text_delta_strings(&capture.downstream_events),
        vec!["hello from direct delta"]
    );
    assert_eq!(capture.downstream_events[0].payload, delta_event);
    assert_eq!(capture.downstream_events[1].event, "response.completed");
    assert_eq!(
        assistant_output_text_from_completed(&capture.downstream_events[1].payload),
        "hello from direct delta"
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn visible_text_sources_are_not_duplicated_across_delta_done_item_and_completed() {
    let delta_event = json!({
        "type": "response.output_text.delta",
        "delta": "hello from every source",
        "item_id": "assistant-item-shared",
        "output_index": 0,
        "content_index": 0
    });
    let output_text_done_event = json!({
        "type": "response.output_text.done",
        "item_id": "assistant-item-shared",
        "output_index": 0,
        "content_index": 0,
        "text": "hello from every source"
    });
    let output_item_done_event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "id": "assistant-item-shared",
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": "hello from every source"
                }
            ]
        }
    });
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-visible-dedupe",
            "output": [
                {
                    "id": "assistant-item-shared",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "hello from every source"
                        }
                    ]
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![
        delta_event.clone(),
        output_text_done_event.clone(),
        output_item_done_event.clone(),
        completed_event.clone(),
    ])
    .await;

    assert_eq!(
        output_text_delta_strings(&capture.downstream_events),
        vec!["hello from every source"]
    );
    assert_eq!(capture.downstream_events.len(), 4);
    assert_eq!(capture.downstream_events[0].payload, delta_event);
    assert_eq!(capture.downstream_events[1].payload, output_text_done_event);
    assert_eq!(capture.downstream_events[2].payload, output_item_done_event);
    assert_eq!(capture.downstream_events[3].event, "response.completed");
    assert_eq!(
        assistant_output_text_from_completed(&capture.downstream_events[3].payload),
        "hello from every source"
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn empty_response_completed_emits_no_observable_output_failure() {
    let capture = capture_completed_output_stream(vec![json!({
        "type": "response.completed",
        "response": {
            "id": "response-empty-terminal"
        }
    })])
    .await;

    assert_eq!(capture.downstream_events.len(), 1);
    assert_eq!(capture.downstream_events[0].event, "response.failed");
    assert_eq!(
        capture.downstream_events[0].payload,
        no_observable_output_failed_event("response-empty-terminal")
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn no_observable_output_diagnostics_do_not_log_arguments_or_encrypted_content() {
    let trace_guard = TraceCaptureGuard::begin().await;
    let response_id = "response-no-observable-diagnostics";
    let raw_arguments = "{\"api_key\":\"secret-123\"}";
    let encrypted_content = "opaque-encrypted-payload";
    let capture = capture_completed_output_stream(vec![json!({
        "type": "response.completed",
        "response": {
            "id": response_id,
            "output": [
                {
                    "id": "fc-internal-only",
                    "type": "function_call",
                    "call_id": "call-internal-only",
                    "name": "threadline_echo",
                    "arguments": raw_arguments
                },
                {
                    "id": "state-marker-internal-only",
                    "type": "state_marker",
                    "encrypted_content": encrypted_content
                }
            ]
        }
    })])
    .await;

    assert_eq!(capture.downstream_events.len(), 1);
    assert_eq!(capture.downstream_events[0].event, "response.failed");
    assert_eq!(
        capture.downstream_events[0].payload,
        no_observable_output_failed_event(response_id)
    );

    let logs = trace_guard.logs();
    let guard_line = logs
        .lines()
        .find(|line| {
            line.contains("responses_translation_no_observable_output_guard")
                && line.contains(response_id)
        })
        .expect("guard diagnostics trace line");

    assert!(guard_line.contains(response_id));
    assert!(guard_line.contains("completed_output_item_types"));
    assert!(guard_line.contains("function_call"));
    assert!(guard_line.contains("state_marker"));
    assert!(!guard_line.contains(raw_arguments));
    assert!(!guard_line.contains(encrypted_content));
    assert!(!guard_line.contains("arguments="));
    assert!(!guard_line.contains("encrypted_content="));
}

#[tokio::test]
async fn external_function_call_completed_output_remains_visible() {
    let tool_done_event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "id": "fc-visible-only",
            "type": "function_call",
            "call_id": "call-visible-only",
            "name": "apply_patch",
            "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"
        }
    });
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-visible-tool-only",
            "output": [
                {
                    "id": "fc-visible-only",
                    "type": "function_call",
                    "call_id": "call-visible-only",
                    "name": "apply_patch",
                    "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"
                }
            ]
        }
    });

    let capture =
        capture_completed_output_stream(vec![tool_done_event.clone(), completed_event.clone()])
            .await;

    assert_eq!(capture.downstream_events.len(), 2);
    assert_eq!(
        capture.downstream_events[0].event,
        "response.output_item.done"
    );
    assert_eq!(capture.downstream_events[0].payload, tool_done_event);
    assert_eq!(capture.downstream_events[1].event, "response.completed");
    assert_eq!(capture.downstream_events[1].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn unknown_marker_like_completed_output_remains_non_observable() {
    let capture = capture_completed_output_stream(vec![json!({
        "type": "response.completed",
        "response": {
            "id": "response-unknown-marker",
            "output": [
                {
                    "id": "marker-1",
                    "type": "state_marker",
                    "encrypted_content": "opaque-marker"
                }
            ]
        }
    })])
    .await;

    assert_eq!(capture.downstream_events.len(), 1);
    assert_eq!(capture.downstream_events[0].event, "response.failed");
    assert_eq!(
        capture.downstream_events[0].payload,
        no_observable_output_failed_event("response-unknown-marker")
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
    assert!(output_text_delta_strings(&capture.downstream_events).is_empty());
}

#[tokio::test]
async fn internal_function_call_completed_output_remains_sanitized_and_non_observable() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(
        app.clone(),
        json!({"model":"gpt-5.4","input":"internal-only-completed"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server
        .recv_client_message()
        .await
        .expect("internal-only request");

    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-internal-only",
            "output": [
                {
                    "type": "function_call",
                    "name": "threadline_echo",
                    "call_id": "call-1",
                    "arguments": "{\"value\":\"alpha\"}"
                }
            ]
        }
    });
    server.send_text(&completed_event.to_string()).await;

    let body = timeout(
        Duration::from_secs(2),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("internal-only body timeout")
    .expect("internal-only body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("terminal failed frame"));
    let payload: Value = serde_json::from_str(data).expect("terminal failed json");

    assert_eq!(frames.len(), 2);
    assert_eq!(event, "response.failed");
    assert_eq!(payload["type"], "response.failed");
    assert_eq!(
        payload["response"]["error"]["code"],
        "threadline_no_observable_output"
    );
    assert_done_frame(frames[1]);

    let rejected = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"invalid-internal-only-resume",
            "previous_response_id":"response-internal-only"
        }),
    )
    .await;

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let rejected_body = to_bytes(rejected.into_body(), usize::MAX)
        .await
        .expect("rejected body");
    let rejected_payload: Value = serde_json::from_slice(&rejected_body).expect("rejected json");
    assert_eq!(
        rejected_payload["error"]["code"],
        "previous_response_not_found"
    );
}

#[tokio::test]
async fn auxiliary_summary_compaction_only_completed_preserves_transient_behavior() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app.clone(), auxiliary_summary_request(None)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("summary request");

    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-summary",
            "output": [
                {
                    "id": "cmp-1",
                    "type": "compaction",
                    "name": "threadline_echo",
                    "encrypted_content": "opaque-summary"
                }
            ]
        }
    });
    server.send_text(&completed_event.to_string()).await;

    let body = timeout(
        Duration::from_secs(2),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("summary body timeout")
    .expect("summary body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("summary completed frame"));
    let payload: Value = serde_json::from_str(data).expect("summary completed json");

    assert_eq!(frames.len(), 2);
    assert_eq!(event, "response.completed");
    assert_eq!(payload, completed_event);
    assert_done_frame(frames[1]);

    let rejected = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"invalid-summary-resume",
            "previous_response_id":"response-summary"
        }),
    )
    .await;

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let rejected_body = to_bytes(rejected.into_body(), usize::MAX)
        .await
        .expect("rejected body");
    let rejected_payload: Value = serde_json::from_slice(&rejected_body).expect("rejected json");
    assert_eq!(
        rejected_payload["error"]["code"],
        "previous_response_not_found"
    );
}

#[tokio::test]
async fn image_generation_completed_output_remains_successful_without_text() {
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-image-generation",
            "output": [
                {
                    "id": "img-1",
                    "type": "image_generation_call",
                    "result": "image-asset-1"
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![completed_event.clone()]).await;

    assert_eq!(capture.downstream_events.len(), 1);
    assert!(output_text_delta_strings(&capture.downstream_events).is_empty());
    assert_eq!(capture.downstream_events[0].event, "response.completed");
    assert_eq!(capture.downstream_events[0].payload, completed_event);
    assert_eq!(capture.done_frame, "data: [DONE]");
}

#[tokio::test]
async fn forwarded_tool_event_does_not_hide_upstream_response_failed() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(
        app,
        json!({"model":"gpt-5.4","input":"visible-tool-then-upstream-failed"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = server
        .recv_client_message()
        .await
        .expect("visible tool request");

    let tool_done_event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "id": "fc-visible-failed",
            "type": "function_call",
            "call_id": "call-visible-failed",
            "name": "apply_patch",
            "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"
        }
    });
    let failed_event = json!({
        "type": "response.failed",
        "response": {
            "id": "response-visible-tool-failed"
        },
        "error": {
            "code": "upstream_response_failed",
            "message": "failed"
        }
    });
    server.send_text(&tool_done_event.to_string()).await;
    server.send_text(&failed_event.to_string()).await;

    let body = timeout(
        Duration::from_secs(2),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("body timeout")
    .expect("body bytes");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 3);
    let tool_frame = sse_event_and_data(frames[0]);
    let tool_payload: Value = serde_json::from_str(tool_frame.1).expect("tool json");
    assert_eq!(tool_frame.0, "response.output_item.done");
    assert_eq!(tool_payload, tool_done_event);

    let failed_frame = sse_event_and_data(frames[1]);
    let failed_payload: Value = serde_json::from_str(failed_frame.1).expect("failed json");
    assert_eq!(failed_frame.0, "response.failed");
    assert_eq!(failed_payload["type"], "response.failed");
    assert_eq!(
        failed_payload["response"]["id"],
        "response-visible-tool-failed"
    );
    assert_eq!(
        failed_payload["response"]["error"]["code"],
        "upstream_response_failed"
    );
    assert_eq!(failed_payload["response"]["error"]["message"], "failed");
    assert_done_frame(frames[2]);
}

#[tokio::test]
async fn missing_visible_text_identity_fields_do_not_duplicate_or_drop_distinct_text() {
    let delta_event = json!({
        "type": "response.output_text.delta",
        "delta": "repeat"
    });
    let output_text_done_event = json!({
        "type": "response.output_text.done",
        "text": "repeat"
    });
    let output_item_done_event = json!({
        "type": "response.output_item.done",
        "item": {
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": " and distinct"
                }
            ]
        }
    });
    let completed_event = json!({
        "type": "response.completed",
        "response": {
            "id": "response-missing-identity",
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "repeat and distinct"
                        }
                    ]
                }
            ]
        }
    });

    let capture = capture_completed_output_stream(vec![
        delta_event,
        output_text_done_event,
        output_item_done_event,
        completed_event,
    ])
    .await;

    assert_eq!(
        output_text_delta_strings(&capture.downstream_events),
        vec!["repeat", " and distinct"]
    );
    assert_eq!(
        assistant_output_text_from_completed(
            &capture
                .downstream_events
                .last()
                .expect("completed event")
                .payload
        ),
        "repeat and distinct"
    );
    assert_eq!(capture.done_frame, "data: [DONE]");
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
async fn completed_output_marker_is_reusable_after_completed_before_done() {
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
        .send_text(&assistant_text_completed_event("response-1", "seed completion").to_string())
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

    let second_chunk = next_body_chunk(&mut active_body).await;
    let second_text = String::from_utf8(second_chunk.to_vec()).expect("utf8 second chunk");
    let (second_event, second_data) = sse_event_and_data(second_text.trim_end());
    let second_payload: Value = serde_json::from_str(second_data).expect("completed json");
    assert_eq!(second_event, "response.completed");
    assert_eq!(second_payload, completed_event);

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
        let response_id = completed_event["response"]["id"]
            .as_str()
            .expect("completed response id");
        assert_eq!(
            capture.downstream_events.len(),
            1,
            "expected malformed case {case_name} to emit only the terminal failure without a synthetic delta"
        );
        assert_eq!(
            capture.downstream_events[0].event, "response.failed",
            "expected malformed case {case_name} to downgrade the malformed completion into response.failed"
        );
        assert_eq!(
            capture.downstream_events[0].payload,
            no_observable_output_failed_event(response_id),
            "expected malformed case {case_name} to emit the stable no-observable-output failure payload"
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
