use std::collections::VecDeque;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};
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
    wait_until_closed_before_return: bool,
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
            let websocket = Arc::new(LiveUpstreamWebSocket::from_stream(stream));

            if plan.wait_until_closed_before_return {
                timeout(Duration::from_secs(1), async {
                    while !websocket.is_closed() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("disconnected websocket should close promptly");
            }

            Ok(ConnectedUpstream {
                websocket,
                session,
                turn_state: plan.turn_state,
            })
        })
    }
}

fn build_test_router(connector: Arc<dyn UpstreamConnector>) -> axum::Router {
    build_router_with_services(
        ThreadlineConfig::default(),
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

async fn seed_marker(app: axum::Router, server: &ScriptedWebSocketServer, marker: &str) {
    let response = post_responses(app, json!({"model":"ignored","input":"seed"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(&format!(
            "{{\"type\":\"response.completed\",\"response\":{{\"id\":\"{marker}\"}}}}"
        ))
        .await;
    let _ = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("seed body");
}

#[tokio::test]
async fn reconnect_fallback_is_not_attempted_for_non_continuation_requests() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
        wait_until_closed_before_return: false,
    }]);
    let app = build_test_router(Arc::new(connector.clone()));

    let response = post_responses(app, json!({"model":"ignored","input":"first"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("initial request timeout")
        .expect("initial request");
    server.send_close(1000, "closed-before-event").await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("error frame"));
    let payload: Value = serde_json::from_str(data).expect("error json");

    assert_eq!(frames.len(), 1);
    assert_eq!(event, "error");
    assert_eq!(
        payload,
        json!({
            "error": {
                "code": "upstream_websocket_closed",
                "message": "The upstream Codex websocket closed before Threadline finished streaming the response.",
                "type": "bad_gateway_error"
            }
        })
    );
    assert_eq!(
        data,
        json!({
            "error": {
                "code": "upstream_websocket_closed",
                "message": "The upstream Codex websocket closed before Threadline finished streaming the response.",
                "type": "bad_gateway_error"
            }
        })
        .to_string()
    );
    assert!(!body_text.contains("data: [DONE]"));

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn reconnect_fallback_reuses_the_same_session_once_before_the_first_upstream_event() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let first_attempt_server = Arc::new(ScriptedWebSocketServer::start().await);
    let reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&seed_server),
            turn_state: Some("turn-state-1".to_string()),
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&first_attempt_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&reconnect_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;
    seed_server.send_close(1000, "seed complete").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = post_responses(
        app,
        json!({
            "model":"ignored",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let first_attempt_payload: Value = serde_json::from_str(&message_text(
        timeout(
            Duration::from_secs(1),
            first_attempt_server.recv_client_message(),
        )
        .await
        .expect("first continuation timeout")
        .expect("first continuation request"),
    ))
    .expect("first continuation json");
    assert_eq!(
        first_attempt_payload["response"]["previous_response_id"],
        "response-1"
    );
    first_attempt_server
        .send_close(1000, "closed-before-event")
        .await;

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body")
    });

    let reconnect_message = match timeout(
        Duration::from_secs(1),
        reconnect_server.recv_client_message(),
    )
    .await
    {
        Ok(message) => message.expect("reconnect request"),
        Err(error) => {
            body_task.abort();
            panic!("reconnect timeout: {error}");
        }
    };
    let reconnect_payload: Value =
        serde_json::from_str(&message_text(reconnect_message)).expect("reconnect json");
    assert_eq!(
        reconnect_payload["response"]["previous_response_id"],
        "response-1"
    );
    reconnect_server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-2"}}"#)
        .await;

    let body = timeout(Duration::from_secs(1), body_task)
        .await
        .expect("body timeout")
        .expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("completed frame"));
    let payload: Value = serde_json::from_str(data).expect("completed json");

    assert_eq!(frames.len(), 1);
    assert_eq!(event, "response.completed");
    assert_eq!(
        payload,
        json!({"type":"response.completed","response":{"id":"response-2"}})
    );
    assert_eq!(
        data,
        json!({"type":"response.completed","response":{"id":"response-2"}}).to_string()
    );
    assert!(!body_text.contains("data: [DONE]"));

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 3);
    assert_eq!(sessions[1].session_id, sessions[2].session_id);
    assert_eq!(sessions[1].thread_id, sessions[2].thread_id);
    assert_eq!(sessions[1].turn_state.as_deref(), Some("turn-state-1"));
    assert_eq!(sessions[2].turn_state.as_deref(), Some("turn-state-1"));
    assert_ne!(sessions[1].window_id, sessions[2].window_id);
}

#[tokio::test]
async fn reconnect_fallback_is_not_attempted_after_any_upstream_event() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let continuation_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&seed_server),
            turn_state: Some("turn-state-1".to_string()),
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&continuation_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;
    seed_server.send_close(1000, "seed complete").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = post_responses(
        app,
        json!({
            "model":"ignored",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = timeout(
        Duration::from_secs(1),
        continuation_server.recv_client_message(),
    )
    .await
    .expect("continuation request timeout")
    .expect("continuation request");
    continuation_server
        .send_text(r#"{"type":"response.created","response":{"id":"response-created"}}"#)
        .await;
    continuation_server
        .send_close(1000, "closed-after-event")
        .await;

    let body = timeout(
        Duration::from_secs(1),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("body timeout")
    .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (created_event, created_data) = sse_event_and_data(frames.first().expect("created frame"));
    let (error_event, error_data) = sse_event_and_data(frames.get(1).expect("error frame"));
    let created_payload: Value = serde_json::from_str(created_data).expect("created json");
    let error_payload: Value = serde_json::from_str(error_data).expect("error json");

    assert_eq!(frames.len(), 2);
    assert_eq!(created_event, "response.created");
    assert_eq!(
        created_payload,
        json!({"type":"response.created","response":{"id":"response-created"}})
    );
    assert_eq!(error_event, "error");
    assert_eq!(error_payload["error"]["code"], "upstream_websocket_closed");
    assert!(!body_text.contains("data: [DONE]"));

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 2);
}

#[tokio::test]
async fn reconnect_fallback_attempts_only_once() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let first_attempt_server = Arc::new(ScriptedWebSocketServer::start().await);
    let reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&seed_server),
            turn_state: Some("turn-state-1".to_string()),
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&first_attempt_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&reconnect_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;
    seed_server.send_close(1000, "seed complete").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = post_responses(
        app,
        json!({
            "model":"ignored",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = timeout(
        Duration::from_secs(1),
        first_attempt_server.recv_client_message(),
    )
    .await
    .expect("first continuation timeout")
    .expect("first continuation request");
    first_attempt_server
        .send_close(1000, "closed-before-event")
        .await;

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body")
    });

    match timeout(
        Duration::from_secs(1),
        reconnect_server.recv_client_message(),
    )
    .await
    {
        Ok(message) => {
            let _ = message.expect("reconnect request");
        }
        Err(error) => {
            body_task.abort();
            panic!("reconnect timeout: {error}");
        }
    }
    reconnect_server.send_close(1000, "closed-again").await;

    let body = timeout(Duration::from_secs(1), body_task)
        .await
        .expect("body timeout")
        .expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("error frame"));
    let payload: Value = serde_json::from_str(data).expect("error json");

    assert_eq!(frames.len(), 1);
    assert_eq!(event, "error");
    assert_eq!(payload["error"]["code"], "upstream_websocket_closed");
    assert!(!body_text.contains("data: [DONE]"));

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 3);
}

#[tokio::test]
async fn reconnect_fallback_attempts_only_once_after_pre_stream_send_failure() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let first_attempt_server =
        Arc::new(ScriptedWebSocketServer::start_disconnect_after_handshake().await);
    let reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let unexpected_third_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&seed_server),
            turn_state: Some("turn-state-1".to_string()),
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&first_attempt_server),
            turn_state: None,
            wait_until_closed_before_return: true,
        },
        PlannedConnection {
            server: Arc::clone(&reconnect_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&unexpected_third_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;
    seed_server.send_close(1000, "seed complete").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = post_responses(
        app,
        json!({
            "model":"ignored",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body")
    });

    let reconnect_message = timeout(
        Duration::from_secs(1),
        reconnect_server.recv_client_message(),
    )
    .await
    .expect("reconnect timeout")
    .expect("reconnect request");
    let reconnect_payload: Value =
        serde_json::from_str(&message_text(reconnect_message)).expect("reconnect json");
    assert_eq!(
        reconnect_payload["response"]["previous_response_id"],
        "response-1"
    );
    reconnect_server
        .send_close(1000, "closed-before-event-again")
        .await;

    let no_second_reconnect = timeout(
        Duration::from_millis(250),
        unexpected_third_server.recv_client_message(),
    )
    .await;
    assert!(no_second_reconnect.is_err());

    let body = timeout(Duration::from_secs(1), body_task)
        .await
        .expect("body timeout")
        .expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("error frame"));
    let payload: Value = serde_json::from_str(data).expect("error json");

    assert_eq!(frames.len(), 1);
    assert_eq!(event, "error");
    assert_eq!(
        payload,
        json!({
            "error": {
                "code": "upstream_websocket_closed",
                "message": "The upstream Codex websocket closed before Threadline finished streaming the response.",
                "type": "bad_gateway_error"
            }
        })
    );
    assert_eq!(
        data,
        json!({
            "error": {
                "code": "upstream_websocket_closed",
                "message": "The upstream Codex websocket closed before Threadline finished streaming the response.",
                "type": "bad_gateway_error"
            }
        })
        .to_string()
    );
    assert!(!body_text.contains("data: [DONE]"));

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 3);
}
