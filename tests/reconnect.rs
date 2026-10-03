use std::collections::VecDeque;
use std::sync::{Arc, Weak};

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
use threadline::ws_pump::{LiveUpstreamWebSocket, UpstreamWatchdogPolicy};

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
    watchdog_policy: Option<UpstreamWatchdogPolicy>,
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
            let websocket = Arc::new(match plan.watchdog_policy {
                Some(policy) => {
                    LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                        stream,
                        Duration::from_millis(5),
                        policy,
                    )
                }
                None => LiveUpstreamWebSocket::from_stream(stream),
            });
            websockets.lock().await.push(Arc::downgrade(&websocket));

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

fn short_watchdog_policy() -> UpstreamWatchdogPolicy {
    UpstreamWatchdogPolicy::new(Duration::from_millis(30), Duration::from_secs(1))
        .expect("valid watchdog policy")
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
        "model": "gpt-6-sol",
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
    let response = post_responses(app, json!({"model":"gpt-6-sol","input":"seed"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(&assistant_text_completed_event(marker, "seed completion").to_string())
        .await;
    let _ = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("seed body");
}

async fn seed_marker_without_reader(
    app: axum::Router,
    server: &ScriptedWebSocketServer,
    marker: &str,
) {
    let response = post_responses(app, json!({"model":"gpt-6-sol","input":"seed"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    server
        .send_text(&assistant_text_completed_event(marker, "seed completion").to_string())
        .await;
    let _ = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("seed body");
}

#[path = "reconnect/timeouts.rs"]
mod timeouts;

#[path = "reconnect/no_replay.rs"]
mod no_replay;

#[path = "reconnect/closed_continuation.rs"]
mod closed_continuation;

fn planned_connection(
    server: &Arc<ScriptedWebSocketServer>,
    turn_state: Option<&str>,
    wait_until_closed_before_return: bool,
    watchdog_policy: Option<UpstreamWatchdogPolicy>,
) -> PlannedConnection {
    PlannedConnection {
        server: Arc::clone(server),
        turn_state: turn_state.map(ToString::to_string),
        wait_until_closed_before_return,
        watchdog_policy,
    }
}

async fn assert_no_reconnect(server: &ScriptedWebSocketServer) {
    let no_reconnect = timeout(Duration::from_millis(250), server.recv_client_message()).await;
    assert!(no_reconnect.is_err());
}
