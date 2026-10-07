use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    ConnectedUpstream, InternalToolExecutor, ThreadlineServices, UpstreamAuthProvider,
    UpstreamConnector,
};
use threadline::tools::{InternalToolCall, PendingInternalToolOutput};
use threadline::ws_pump::{LiveUpstreamWebSocket, UpstreamWatchdogPolicy};

const PROBE_MODEL: &str = "threadline-lifetime-probe";
const PROBE_SEED: &str = "THREADLINE_FIXTURE_SEED_INPUT_A7";
const PROBE_SEED_ANSWER: &str = "THREADLINE_FIXTURE_SEED_REPLY_B8";
const PROBE_CONTINUE: &str = "THREADLINE_FIXTURE_CONTINUE_INPUT_C9";
const PROBE_FINAL: &str = "THREADLINE_FIXTURE_FINAL_REPLY_D0";

#[derive(Default)]
struct ProbeToolExecutor {
    attempts: AtomicUsize,
}

impl InternalToolExecutor for ProbeToolExecutor {
    fn execute(
        &self,
        _call: InternalToolCall,
    ) -> BoxFuture<'static, Result<PendingInternalToolOutput, ThreadlineError>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ThreadlineError::InternalToolFailed) })
    }
}

fn probe_log(value: Value) {
    use std::io::Write;
    println!("{value}");
    std::io::stdout()
        .flush()
        .expect("flush safe probe evidence");
}

fn probe_input_contains(input: &Value, token: &str) -> bool {
    match input {
        Value::String(text) => text.contains(token),
        Value::Array(items) => items.iter().any(|item| probe_input_contains(item, token)),
        Value::Object(item) => ["content", "text"].iter().any(|key| {
            item.get(*key)
                .is_some_and(|value| probe_input_contains(value, token))
        }),
        _ => false,
    }
}

#[derive(Clone)]
struct StaticAuthProvider;

#[derive(Default)]
struct CountingToolExecutor {
    executions: AtomicUsize,
}

impl InternalToolExecutor for CountingToolExecutor {
    fn execute(
        &self,
        call: InternalToolCall,
    ) -> BoxFuture<'static, Result<PendingInternalToolOutput, ThreadlineError>> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { call.execute() })
    }
}

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

fn build_counting_test_router(
    connector: RecordingConnector,
) -> (axum::Router, Arc<CountingToolExecutor>) {
    let executor = Arc::new(CountingToolExecutor::default());
    let app = build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::with_internal_tool_executor(
            Arc::new(StaticAuthProvider),
            Arc::new(connector),
            Arc::clone(&executor) as Arc<dyn InternalToolExecutor>,
        ),
    );
    (app, executor)
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

async fn begin_continuation(
    app: axum::Router,
    server: &ScriptedWebSocketServer,
    marker: &str,
    tools: Option<Value>,
) -> tokio::task::JoinHandle<Response<Body>> {
    let mut payload = json!({"model":"gpt-6-sol","input":"continue","previous_response_id":marker});
    if let Some(tools) = tools {
        payload["tools"] = tools;
    }
    let response = tokio::spawn(post_responses(app, payload));
    let message = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("continuation received before headers")
        .expect("continuation create");
    let request: Value =
        serde_json::from_str(&message.into_text().expect("text create")).expect("create json");
    assert_eq!(request["previous_response_id"], marker);
    response
}

#[tokio::test]
async fn recovery_prelude_capacity_handoff_is_fifo_and_exactly_once() {
    for byte_boundary in [false, true] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector =
            RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
        let app = build_test_router(Arc::new(connector.clone()));
        seed_marker(app.clone(), &server, "seed-marker").await;
        let response = begin_continuation(app, &server, "seed-marker", None).await;
        let events: Vec<Value> = (0..if byte_boundary { 2 } else { 9 }).map(|sequence| json!({"type":"response.in_progress","sequence_number":sequence,"response":{"id": if byte_boundary { "x".repeat(40 * 1024) } else { "active".to_string() },"output":[]}})).collect();
        for event in &events {
            server.send_text(&event.to_string()).await;
        }
        let response = timeout(Duration::from_secs(1), response)
            .await
            .expect("capacity commits headers without semantic event")
            .expect("response task");
        assert_eq!(response.status(), StatusCode::OK);
        server
            .send_text(r#"{"type":"error","error":{"code":"websocket_connection_limit_reached"}}"#)
            .await;
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = String::from_utf8(bytes.to_vec()).expect("utf8");
        let frames = split_sse_frames(&body);
        assert_eq!(frames.len(), events.len() + 2);
        for (frame, expected) in frames.iter().zip(&events) {
            let event: Value =
                serde_json::from_str(sse_event_and_data(frame).1).expect("lifecycle event");
            assert_eq!(&event, expected);
        }
        let failed: Value =
            serde_json::from_str(sse_event_and_data(frames[events.len()]).1).expect("failure");
        assert_response_failed_payload(&failed, "websocket_connection_limit_reached");
        assert_done_frame(frames[events.len() + 1]);
    }
}

#[tokio::test]
async fn recovery_idle_queued_limit_uses_original_local_tool_eligibility() {
    for hosted in [false, true] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector =
            RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
        let app = build_test_router(Arc::new(connector.clone()));
        seed_marker(app.clone(), &server, "seed-marker").await;
        server
            .send_text(r#"{"type":"error","error":{"code":"websocket_connection_limit_reached"}}"#)
            .await;
        server.send_ping(b"idle-limit-barrier").await;
        let pong = timeout(Duration::from_secs(1), server.recv_client_message())
            .await
            .expect("idle Ping processed after limit")
            .expect("Pong");
        assert_eq!(
            pong,
            tokio_tungstenite::tungstenite::Message::Pong(b"idle-limit-barrier".to_vec())
        );
        let tools = hosted.then(|| json!([{"type":"web_search"}]));
        let response = begin_continuation(app, &server, "seed-marker", tools).await;
        let response = timeout(Duration::from_secs(1), response)
            .await
            .expect("idle limit headers")
            .expect("response task");
        assert_eq!(
            response.status(),
            if hosted {
                StatusCode::OK
            } else {
                StatusCode::BAD_REQUEST
            }
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(body.contains(if hosted {
            "websocket_connection_limit_reached"
        } else {
            "previous_response_not_found"
        }));
        assert_eq!(connector.recorded_sessions().await.len(), 1);
        assert!(connector.recorded_websockets().await[0].upgrade().is_none());
        assert!(
            timeout(Duration::from_secs(1), server.recv_client_message())
                .await
                .expect("no additional create")
                .is_none()
        );
    }
}

#[tokio::test]
#[ignore = "Explicit user-coordinated loopback probe; never normal-suite client evidence"]
async fn vscode_lifetime_recovery_manual_probe() {
    let trial = match std::env::var("THREADLINE_LIFETIME_PROBE").as_deref() {
        Ok("success") => "success",
        Ok("retry-failure") => "retry-failure",
        Ok("wait") => "wait",
        Ok("cancel") => "cancel",
        _ => panic!("set THREADLINE_LIFETIME_PROBE to success, retry-failure, wait, or cancel"),
    };
    timeouts::run_manual_probe(trial).await;
}
