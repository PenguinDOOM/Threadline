use std::collections::VecDeque;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};
use tokio_tungstenite::connect_async;
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

fn new_session_descriptor() -> UpstreamSessionDescriptor {
    UpstreamSessionDescriptor {
        session_id: Uuid::now_v7().to_string(),
        thread_id: Uuid::now_v7().to_string(),
        window_id: Uuid::now_v7().to_string(),
        turn_state: None,
    }
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

fn assert_response_failed_payload(payload: &Value, expected_code: &str) {
    assert_eq!(payload["type"], "response.failed");
    assert_eq!(payload["response"]["status"], "failed");
    assert_eq!(payload["response"]["error"]["code"], expected_code);
    assert!(
        payload["response"]["error"]["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()),
        "expected a stable non-empty terminal failure message: {payload:?}"
    );
}

async fn seed_marker(app: axum::Router, server: &ScriptedWebSocketServer, marker: &str) {
    let response = post_responses(app, json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(&assistant_text_completed_event(marker, "seed completion").to_string())
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

    let response = post_responses(app, json!({"model":"gpt-5.4","input":"first"})).await;
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
    let (event, data) = sse_event_and_data(frames.first().expect("failed frame"));
    let payload: Value = serde_json::from_str(data).expect("failed json");

    assert_eq!(frames.len(), 2);
    assert_eq!(event, "response.failed");
    assert_response_failed_payload(&payload, "upstream_websocket_closed");
    assert!(
        !body_text.contains("event: error\n"),
        "expected terminal websocket close to use the downstream response.failed contract: {body_text}"
    );
    assert_done_frame(frames[1]);

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn live_retained_continuation_close_before_first_send_returns_previous_response_not_found_without_reconnect_or_resend()
 {
    let retained_server = Arc::new(ScriptedWebSocketServer::start().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&retained_server),
            turn_state: Some("turn-state-1".to_string()),
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&unexpected_reconnect_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &retained_server, "response-1").await;

    let response_task = tokio::spawn({
        let app = app.clone();
        async move {
            post_responses(
                app,
                json!({
                    "model":"gpt-5.4",
                    "input":"followup",
                    "previous_response_id":"response-1"
                }),
            )
            .await
        }
    });

    retained_server.abort_connection().await;

    let response = timeout(Duration::from_secs(1), response_task)
        .await
        .expect("continuation response timeout")
        .expect("continuation response task");
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "retained close before first send should fail before SSE starts"
    );

    let retained_message = timeout(
        Duration::from_millis(250),
        retained_server.recv_client_message(),
    )
    .await
    .expect("retained close should resolve the pending client receive");
    assert!(
        retained_message.is_none(),
        "expected the retained upstream to close before resending the same previous_response_id"
    );

    let no_reconnect = timeout(
        Duration::from_millis(250),
        unexpected_reconnect_server.recv_client_message(),
    )
    .await;
    assert!(no_reconnect.is_err());

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn reconnect_fallback_is_not_attempted_after_any_upstream_event() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&seed_server),
        turn_state: Some("turn-state-1".to_string()),
        wait_until_closed_before_return: false,
    }]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;

    let response = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = timeout(Duration::from_secs(1), seed_server.recv_client_message())
        .await
        .expect("continuation request timeout")
        .expect("continuation request");
    seed_server
        .send_text(r#"{"type":"response.created","response":{"id":"response-created"}}"#)
        .await;
    seed_server.send_close(1000, "closed-after-event").await;

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
    let (failed_event, failed_data) = sse_event_and_data(frames.get(1).expect("failed frame"));
    let created_payload: Value = serde_json::from_str(created_data).expect("created json");
    let failed_payload: Value = serde_json::from_str(failed_data).expect("failed json");

    assert_eq!(frames.len(), 3);
    assert_eq!(created_event, "response.created");
    assert_eq!(
        created_payload,
        json!({"type":"response.created","response":{"id":"response-created"}})
    );
    assert_eq!(failed_event, "response.failed");
    assert_response_failed_payload(&failed_payload, "upstream_websocket_closed");
    assert!(
        !body_text.contains("event: error\n"),
        "expected terminal websocket close to use the downstream response.failed contract: {body_text}"
    );
    assert_done_frame(frames[2]);

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn retained_continuation_close_after_send_before_first_upstream_event_replays_stale_marker_without_reconnect()
 {
    let retained_server = Arc::new(ScriptedWebSocketServer::start().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&retained_server),
            turn_state: Some("turn-state-1".to_string()),
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&unexpected_reconnect_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &retained_server, "response-1").await;

    let response = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        timeout(
            Duration::from_secs(1),
            to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("body timeout")
        .expect("body bytes")
    });

    let retained_message = timeout(
        Duration::from_secs(1),
        retained_server.recv_client_message(),
    )
    .await
    .expect("continuation request timeout")
    .expect("continuation request");
    let retained_message = retained_message.into_text().expect("text request");
    let retained_payload: Value = serde_json::from_str(&retained_message).expect("request json");
    assert_eq!(retained_payload["previous_response_id"], "response-1");

    retained_server
        .send_close(1000, "closed-before-first-event")
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (failed_event, failed_data) = sse_event_and_data(frames.first().expect("failed frame"));
    let failed_payload: Value = serde_json::from_str(failed_data).expect("failed json");

    assert_eq!(frames.len(), 2);
    assert_eq!(failed_event, "response.failed");
    assert_response_failed_payload(&failed_payload, "previous_response_not_found");
    assert_done_frame(frames[1]);

    let no_reconnect = timeout(
        Duration::from_millis(250),
        unexpected_reconnect_server.recv_client_message(),
    )
    .await;
    assert!(no_reconnect.is_err());

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn stale_continuation_with_spare_reconnect_plans_returns_previous_response_not_found_without_reconnect()
 {
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
            "model":"gpt-5.4",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let no_first_attempt = timeout(
        Duration::from_millis(250),
        first_attempt_server.recv_client_message(),
    )
    .await;
    assert!(no_first_attempt.is_err());

    let no_reconnect = timeout(
        Duration::from_millis(250),
        reconnect_server.recv_client_message(),
    )
    .await;
    assert!(no_reconnect.is_err());

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn stale_continuation_returns_previous_response_not_found_before_sse_without_reconnect_or_resend()
 {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        PlannedConnection {
            server: Arc::clone(&seed_server),
            turn_state: Some("turn-state-1".to_string()),
            wait_until_closed_before_return: false,
        },
        PlannedConnection {
            server: Arc::clone(&unexpected_reconnect_server),
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
            "model":"gpt-5.4",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "stale retained continuation should fail before SSE starts"
    );

    let no_reconnect = timeout(
        Duration::from_millis(250),
        unexpected_reconnect_server.recv_client_message(),
    )
    .await;
    assert!(no_reconnect.is_err());

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn summary_request_first_send_failure_does_not_reconnect_as_continuation() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let first_attempt_server =
        Arc::new(ScriptedWebSocketServer::start_disconnect_after_handshake().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
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
            server: Arc::clone(&unexpected_reconnect_server),
            turn_state: None,
            wait_until_closed_before_return: false,
        },
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;
    seed_server.send_close(1000, "seed complete").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = post_responses(app, auxiliary_summary_request(Some("response-1"))).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    let no_reconnect = timeout(
        Duration::from_millis(250),
        unexpected_reconnect_server.recv_client_message(),
    )
    .await;
    assert!(no_reconnect.is_err());

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 2);
}
