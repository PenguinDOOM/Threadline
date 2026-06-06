use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures_util::future::BoxFuture;
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
            source: AuthSource::ExplicitOverride,
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
}

impl RecordingConnector {
    fn new(plans: Vec<PlannedConnection>) -> Self {
        Self {
            plans: Arc::new(Mutex::new(plans.into())),
            sessions: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn recorded_sessions(&self) -> Vec<UpstreamSessionDescriptor> {
        self.sessions.lock().await.clone()
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

            Ok(ConnectedUpstream {
                websocket: Arc::new(LiveUpstreamWebSocket::from_stream(stream)),
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
        post_responses(app.clone(), json!({"model":"ignored","input":"first"})).await;
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
            "model":"ignored",
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
async fn missing_previous_response_id_returns_stable_not_found() {
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(FailingConnector));

    let response = post_responses(
        app,
        json!({
            "model":"ignored",
            "input":"missing",
            "previous_response_id":"response-missing"
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");
}

#[tokio::test]
async fn concurrent_marker_reuse_returns_conflict_and_client_drop_releases_the_lease() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let initial = post_responses(app.clone(), json!({"model":"ignored","input":"seed"})).await;
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
            "model":"ignored",
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
            "model":"ignored",
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
            "model":"ignored",
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

    let active = post_responses(app.clone(), json!({"model":"ignored","input":"first"})).await;
    assert_eq!(active.status(), StatusCode::OK);

    let exhausted = post_responses(app, json!({"model":"ignored","input":"second"})).await;
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

    let response = post_responses(app, json!({"model":"ignored","input":"connect"})).await;

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

    let response = post_responses(app, json!({"model":"ignored","input":"pretty-delta"})).await;
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
        2,
        "expected delta and completed SSE frames, got body: {body_text}"
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
}

#[tokio::test]
async fn upstream_pretty_response_completed_is_compacted_before_downstream_sse() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"ignored","input":"pretty-completed"})).await;
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
        1,
        "expected exactly one completed SSE frame, got body: {body_text}"
    );

    let (event, data) = sse_event_and_data(frames[0]);
    let payload: Value = serde_json::from_str(data).expect("completed json");

    assert_eq!(event, "response.completed");
    assert_eq!(
        payload,
        json!({"type":"response.completed","response":{"id":"response-1"}})
    );
}

#[tokio::test]
async fn upstream_response_failed_emits_a_stable_sse_error() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"ignored","input":"failure"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("failure request");
    server
        .send_text(r#"{"type":"response.failed","response":{"id":"response-1"},"error":{"message":"failed"}}"#)
        .await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("error frame"));
    let payload: Value = serde_json::from_str(data).expect("error json");

    assert_eq!(event, "error");
    assert_eq!(payload["error"]["code"], "upstream_response_failed");
}

#[tokio::test]
async fn upstream_error_event_emits_a_single_compact_sse_error() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"ignored","input":"error"})).await;
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

    assert_eq!(event, "error");
    assert_eq!(payload["error"]["code"], "upstream_error_event");
}

#[tokio::test]
async fn done_sentinel_is_not_forwarded_as_downstream_data() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(ThreadlineConfig::default(), Arc::new(connector));

    let response = post_responses(app, json!({"model":"ignored","input":"done"})).await;
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

    let initial = post_responses(app.clone(), json!({"model":"ignored","input":"seed"})).await;
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
            "model":"ignored",
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
            "model":"ignored",
            "input":"retry",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(retried.status(), StatusCode::NOT_FOUND);
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

    let first = post_responses(app.clone(), json!({"model":"ignored","input":"first"})).await;
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
            "model":"ignored",
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
            "model":"ignored",
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
            "model":"ignored",
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
async fn byok_request_fields_are_preserved_in_upstream_response_create() {
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
            "model":"ignored",
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
            "max_output_tokens":321
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
    assert_eq!(response_payload["max_output_tokens"], Value::from(321));

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
            "model":"ignored",
            "input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]}],
            "max_output_tokens":321
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
    assert_eq!(missing_payload["max_output_tokens"], Value::from(321));

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
            "model":"ignored",
            "input":[{"role":"user","content":[{"type":"input_text","text":"hello again"}]}],
            "instructions":null,
            "max_output_tokens":654
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
    assert_eq!(null_payload["max_output_tokens"], Value::from(654));

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
            "model":"ignored",
            "input":[{"role":"user","content":[{"type":"input_text","text":"preserve me"}]}],
            "instructions":"explicit downstream instructions",
            "max_output_tokens":987
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
    assert_eq!(payload["max_output_tokens"], Value::from(987));

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-3"}}"#)
        .await;
    let _ = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("explicit instructions body");
}
