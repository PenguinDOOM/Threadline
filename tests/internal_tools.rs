use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures_util::{StreamExt, future::BoxFuture};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
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
use threadline::jobs::{ThreadlineJobManager, ThreadlineJobManagerConfig};
use threadline::responses::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};
use threadline::tools::{
    InternalToolCall, event_contains_internal_tool_name, inject_internal_tools,
};
use threadline::ws_pump::LiveUpstreamWebSocket;

const JOB_START_NEXT_ACTION_HINT: &str = "This job is running in the background. Continue other useful work if available, then poll status or read output later when needed.";

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
}

impl RecordingConnector {
    fn new(plans: Vec<PlannedConnection>) -> Self {
        Self {
            plans: Arc::new(Mutex::new(plans.into())),
        }
    }
}

impl UpstreamConnector for RecordingConnector {
    fn connect(
        &self,
        _auth: LoadedUpstreamAuth,
        session: Option<UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
        let plans = Arc::clone(&self.plans);
        Box::pin(async move {
            let session = session.unwrap_or_else(new_session_descriptor);
            let plan = plans
                .lock()
                .await
                .pop_front()
                .expect("planned websocket connection");

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

fn shell_program() -> String {
    if cfg!(windows) {
        "pwsh".to_string()
    } else {
        "sh".to_string()
    }
}

fn shell_command(script: &str) -> Vec<String> {
    if cfg!(windows) {
        vec![
            "pwsh".to_string(),
            "-NoProfile".to_string(),
            "-Command".to_string(),
            script.to_string(),
        ]
    } else {
        vec!["sh".to_string(), "-lc".to_string(), script.to_string()]
    }
}

fn shell_job_manager() -> ThreadlineJobManager {
    ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        allowed_commands: vec![shell_program()],
    })
}

async fn wait_for_terminal_result(
    manager: &ThreadlineJobManager,
    job_id: &str,
    timeout: Duration,
) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        let result = manager.get_result_json(job_id);
        if result["status"] == "completed"
            || result["status"] == "failed"
            || result["status"] == "cancelled"
        {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for terminal job result"
        );
        sleep(Duration::from_millis(10)).await;
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

type SharedBytes = Arc<StdMutex<Vec<u8>>>;
type ActiveTraceBytes = StdMutex<Option<SharedBytes>>;

#[derive(Clone)]
struct SharedLogBuffer {
    bytes: SharedBytes,
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

struct SharedLogWriter {
    bytes: SharedBytes,
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

impl Write for SharedLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedLogBuffer {
    type Writer = SharedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SharedLogWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

async fn next_sse_frame(
    body_stream: &mut (impl futures_util::Stream<Item = Result<Bytes, axum::Error>> + Unpin),
    pending: &mut String,
) -> String {
    loop {
        if let Some(frame_end) = pending.find("\n\n") {
            let frame = pending[..frame_end].to_string();
            pending.drain(..frame_end + 2);
            if !frame.trim().is_empty() {
                return frame;
            }
            continue;
        }

        let chunk = match body_stream.next().await {
            Some(Ok(chunk)) => chunk,
            Some(Err(error)) => panic!("expected SSE chunk, got body error: {error}"),
            None => panic!("expected another SSE frame before EOF"),
        };
        pending.push_str(std::str::from_utf8(&chunk).expect("utf8 sse chunk"));
    }
}

#[tokio::test]
async fn internal_tool_outputs_are_sent_after_intermediate_response_completes() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "run internal tool loop",
            "max_output_tokens": 512,
            "max_tokens": 256,
            "max_completion_tokens": 128,
            "truncation": "auto",
            "tools": [
                {
                    "type": "function",
                    "name": "downstream_tool",
                    "description": "preserve me",
                    "parameters": {"type": "object"},
                    "strict": true
                }
            ]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let first_request: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("initial request"),
    ))
    .expect("initial request json");
    assert_eq!(first_request["type"], "response.create");
    assert_eq!(first_request["instructions"], "");
    assert!(first_request.get("response").is_none());
    assert!(first_request.get("max_output_tokens").is_none());
    assert!(first_request.get("max_tokens").is_none());
    assert!(first_request.get("max_completion_tokens").is_none());
    assert!(first_request.get("truncation").is_none());

    let tools = first_request["tools"].as_array().expect("tools array");
    assert_eq!(tools[0]["name"], "downstream_tool");
    assert_eq!(tools[0]["strict"], true);
    assert!(
        tools
            .iter()
            .any(|tool| { tool["name"] == "threadline_echo" && tool["type"] == "function" })
    );

    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-2","name":"threadline_echo","arguments":"{\"value\":\"beta\"}"}}"#,
        )
        .await;

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let followup_request: Value = serde_json::from_str(&message_text(
        server
            .recv_client_message()
            .await
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup_request["type"], "response.create");
    assert_eq!(followup_request["store"], false);
    assert_eq!(followup_request["instructions"], "");
    assert!(followup_request.get("response").is_none());
    assert!(followup_request.get("max_output_tokens").is_none());
    assert!(followup_request.get("max_tokens").is_none());
    assert!(followup_request.get("max_completion_tokens").is_none());
    assert!(followup_request.get("truncation").is_none());
    assert_eq!(
        followup_request["previous_response_id"],
        "response-intermediate"
    );

    let followup_input = followup_request["input"]
        .as_array()
        .expect("followup input array");
    assert_eq!(followup_input.len(), 2);
    assert_eq!(followup_input[0]["type"], "function_call_output");
    assert_eq!(followup_input[0]["call_id"], "call-1");
    assert_eq!(followup_input[0]["output"], "alpha");
    assert_eq!(followup_input[1]["call_id"], "call-2");
    assert_eq!(followup_input[1]["output"], "beta");

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (delta_event, delta_data) = sse_event_and_data(frames[0]);
    let delta_payload: Value = serde_json::from_str(delta_data).expect("delta json");
    let (completed_event, completed_data) = sse_event_and_data(frames[1]);
    let completed_payload: Value = serde_json::from_str(completed_data).expect("completed json");

    assert_eq!(frames.len(), 3);
    assert_eq!(delta_event, "response.output_text.delta");
    assert_eq!(
        delta_payload,
        json!({"type":"response.output_text.delta","delta":"final answer"})
    );
    assert_eq!(completed_event, "response.completed");
    assert_eq!(
        completed_payload,
        json!({"type":"response.completed","response":{"id":"response-final"}})
    );
    assert_eq!(
        completed_data,
        json!({"type":"response.completed","response":{"id":"response-final"}}).to_string()
    );
    assert_done_frame(frames[2]);
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
    assert!(!body_text.contains("event: response.output_item.done"));
    assert!(server.take_pending_client_messages().await.is_empty());
}

#[tokio::test]
async fn internal_tool_pre_done_events_are_hidden_from_downstream() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "run internal tool loop",
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(
            r#"{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let followup_request: Value = serde_json::from_str(&message_text(
        server
            .recv_client_message()
            .await
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup_request["type"], "response.create");
    assert!(followup_request.get("response").is_none());

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (delta_event, delta_data) = sse_event_and_data(frames[0]);
    let (completed_event, completed_data) = sse_event_and_data(frames[1]);

    assert_eq!(frames.len(), 3);
    assert_eq!(delta_event, "response.output_text.delta");
    assert_eq!(
        serde_json::from_str::<Value>(delta_data).expect("delta json"),
        json!({"type":"response.output_text.delta","delta":"final answer"})
    );
    assert_eq!(completed_event, "response.completed");
    assert_eq!(
        serde_json::from_str::<Value>(completed_data).expect("completed json"),
        json!({"type":"response.completed","response":{"id":"response-final"}})
    );
    assert_done_frame(frames[2]);
    assert!(!body_text.contains("event: response.output_item.added"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
}

#[tokio::test]
async fn non_internal_tool_events_continue_streaming_without_local_followup() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "run downstream tool",
            "tools": [
                {
                    "type": "function",
                    "name": "downstream_tool",
                    "description": "visible tool",
                    "parameters": {"type": "object"}
                }
            ]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(concat!(
            "{\n",
            "  \"type\": \"response.output_item.done\",\n",
            "  \"item\": {\n",
            "    \"type\": \"function_call\",\n",
            "    \"call_id\": \"call-visible\",\n",
            "    \"name\": \"downstream_tool\",\n",
            "    \"arguments\": \"{}\"\n",
            "  }\n",
            "}"
        ))
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-visible"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let tool_frame = sse_event_and_data(frames[0]);
    let completed_frame = sse_event_and_data(frames[1]);
    let tool_payload = json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-visible",
            "name": "downstream_tool",
            "arguments": "{}"
        }
    });
    let completed_payload = json!({
        "type": "response.completed",
        "response": {"id": "response-visible"}
    });

    assert_eq!(frames.len(), 3);
    assert_eq!(tool_frame.0, "response.output_item.done");
    assert_eq!(tool_frame.1, tool_payload.to_string());
    assert_eq!(
        serde_json::from_str::<Value>(tool_frame.1).expect("tool payload json"),
        tool_payload
    );
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(completed_frame.1, completed_payload.to_string());
    assert_eq!(
        serde_json::from_str::<Value>(completed_frame.1).expect("completed payload json"),
        completed_payload
    );
    assert_done_frame(frames[2]);
    assert!(!body_text.contains("  \"type\": \"response.output_item.done\""));
    assert!(server.take_pending_client_messages().await.is_empty());
}

#[tokio::test]
async fn non_internal_tool_added_and_done_events_stream_before_response_completed() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "stream observed downstream tool events",
            "tools": [
                {
                    "type": "function",
                    "name": "downstream_tool",
                    "description": "visible tool",
                    "parameters": {"type": "object"}
                }
            ]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let mut body_stream = response.into_body().into_data_stream();
    let mut pending = String::new();

    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(
            r#"{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call-visible","name":"downstream_tool","arguments":"{}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-visible","name":"downstream_tool","arguments":"{}"}}"#,
        )
        .await;
    let added_payload = json!({
        "type": "response.output_item.added",
        "item": {
            "type": "function_call",
            "call_id": "call-visible",
            "name": "downstream_tool",
            "arguments": "{}"
        }
    });
    let done_payload = json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-visible",
            "name": "downstream_tool",
            "arguments": "{}"
        }
    });
    let completed_payload = json!({
        "type": "response.completed",
        "response": {"id": "response-visible"}
    });

    let added_frame = next_sse_frame(&mut body_stream, &mut pending).await;
    let (added_event, added_data) = sse_event_and_data(&added_frame);
    assert_eq!(added_event, "response.output_item.added");
    assert_eq!(added_data, added_payload.to_string());

    let done_frame = next_sse_frame(&mut body_stream, &mut pending).await;
    let (done_event, done_data) = sse_event_and_data(&done_frame);
    assert_eq!(done_event, "response.output_item.done");
    assert_eq!(done_data, done_payload.to_string());

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-visible"}}"#)
        .await;

    let completed_frame = next_sse_frame(&mut body_stream, &mut pending).await;
    let (completed_event, completed_data) = sse_event_and_data(&completed_frame);
    assert_eq!(completed_event, "response.completed");
    assert_eq!(completed_data, completed_payload.to_string());

    let done_sentinel = next_sse_frame(&mut body_stream, &mut pending).await;
    assert_done_frame(&done_sentinel);
    assert!(
        body_stream.next().await.is_none(),
        "expected EOF after DONE"
    );
    assert!(server.take_pending_client_messages().await.is_empty());
}

#[tokio::test]
async fn internal_tool_added_and_done_events_stay_hidden_until_intermediate_completion() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "run hidden internal tool loop",
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(
            r#"{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;

    assert!(
        server.take_pending_client_messages().await.is_empty(),
        "expected no follow-up request before the intermediate completion arrives"
    );

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let followup_request: Value = serde_json::from_str(&message_text(
        server
            .recv_client_message()
            .await
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup_request["type"], "response.create");

    let followup_input = followup_request["input"]
        .as_array()
        .expect("followup input array");
    assert_eq!(followup_input.len(), 1);
    assert_eq!(followup_input[0]["type"], "function_call_output");
    assert_eq!(followup_input[0]["call_id"], "call-1");
    assert_eq!(followup_input[0]["output"], "alpha");

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let delta_frame = sse_event_and_data(frames[0]);
    let completed_frame = sse_event_and_data(frames[1]);

    assert_eq!(frames.len(), 3);
    assert_eq!(delta_frame.0, "response.output_text.delta");
    assert_eq!(
        serde_json::from_str::<Value>(delta_frame.1).expect("delta json"),
        json!({"type":"response.output_text.delta","delta":"final answer"})
    );
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(
        serde_json::from_str::<Value>(completed_frame.1).expect("completed json"),
        json!({"type":"response.completed","response":{"id":"response-final"}})
    );
    assert_done_frame(frames[2]);
    assert!(!body_text.contains("response.output_item.added"));
    assert!(!body_text.contains("response.output_item.done"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
}

#[tokio::test]
async fn internal_tool_argument_deltas_are_not_forwarded_downstream() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "hide internal tool argument deltas",
            "tools": [
                {
                    "type": "function",
                    "name": "apply_patch",
                    "description": "visible tool",
                    "parameters": {"type": "object"}
                }
            ]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(
            r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call-internal","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call-visible","name":"apply_patch","arguments":""}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.function_call_arguments.delta","output_index":0,"item_id":"item-internal","delta":"{\"value\":\"secret-internal\"}"}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.function_call_arguments.delta","output_index":1,"item_id":"item-visible","delta":"{\"input\":\"*** Begin "}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.function_call_arguments.delta","item_id":"item-uncorrelated","delta":"{\"input\":\"still-visible-without-index\"}"}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call-internal","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","call_id":"call-visible","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let followup_request: Value = serde_json::from_str(&message_text(
        server
            .recv_client_message()
            .await
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup_request["type"], "response.create");
    assert_eq!(
        followup_request["input"]
            .as_array()
            .expect("followup input")[0]["output"],
        "alpha"
    );

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    let visible_added = json!({
        "type": "response.output_item.added",
        "output_index": 1,
        "item": {
            "type": "function_call",
            "call_id": "call-visible",
            "name": "apply_patch",
            "arguments": ""
        }
    });
    let visible_delta = json!({
        "type": "response.function_call_arguments.delta",
        "output_index": 1,
        "item_id": "item-visible",
        "delta": "{\"input\":\"*** Begin "
    });
    let uncorrelated_delta = json!({
        "type": "response.function_call_arguments.delta",
        "item_id": "item-uncorrelated",
        "delta": "{\"input\":\"still-visible-without-index\"}"
    });
    let visible_done = json!({
        "type": "response.output_item.done",
        "output_index": 1,
        "item": {
            "type": "function_call",
            "call_id": "call-visible",
            "name": "apply_patch",
            "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"
        }
    });
    let final_delta = json!({
        "type": "response.output_text.delta",
        "delta": "final answer"
    });
    let final_completed = json!({
        "type": "response.completed",
        "response": {"id": "response-final"}
    });

    assert_eq!(frames.len(), 7);

    let added_frame = sse_event_and_data(frames[0]);
    assert_eq!(added_frame.0, "response.output_item.added");
    assert_eq!(added_frame.1, visible_added.to_string());

    let visible_delta_frame = sse_event_and_data(frames[1]);
    assert_eq!(
        visible_delta_frame.0,
        "response.function_call_arguments.delta"
    );
    assert_eq!(visible_delta_frame.1, visible_delta.to_string());

    let uncorrelated_delta_frame = sse_event_and_data(frames[2]);
    assert_eq!(
        uncorrelated_delta_frame.0,
        "response.function_call_arguments.delta"
    );
    assert_eq!(uncorrelated_delta_frame.1, uncorrelated_delta.to_string());

    let done_frame = sse_event_and_data(frames[3]);
    assert_eq!(done_frame.0, "response.output_item.done");
    assert_eq!(done_frame.1, visible_done.to_string());

    let final_delta_frame = sse_event_and_data(frames[4]);
    assert_eq!(final_delta_frame.0, "response.output_text.delta");
    assert_eq!(final_delta_frame.1, final_delta.to_string());

    let completed_frame = sse_event_and_data(frames[5]);
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(completed_frame.1, final_completed.to_string());

    assert_done_frame(frames[6]);
    assert!(!body_text.contains("call-internal"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("secret-internal"));
    assert!(!body_text.contains("response-intermediate"));
}

#[tokio::test]
async fn internal_tool_done_suppression_emits_stable_trace_event() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "trace suppressed internal tool completion",
            "stream": true
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let trace_capture = TraceCaptureGuard::begin().await;

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(
            r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call-internal","name":"threadline_echo","arguments":"{\"value\":\"secret-internal\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let followup_request: Value = serde_json::from_str(&message_text(
        server
            .recv_client_message()
            .await
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup_request["type"], "response.create");
    assert_eq!(
        followup_request["input"]
            .as_array()
            .expect("followup input")[0]["output"],
        "secret-internal"
    );

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 3);

    let final_delta = sse_event_and_data(frames[0]);
    assert_eq!(final_delta.0, "response.output_text.delta");

    let final_completed = sse_event_and_data(frames[1]);
    assert_eq!(final_completed.0, "response.completed");

    assert_done_frame(frames[2]);
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("secret-internal"));

    let logs = trace_capture.logs();
    assert!(
        logs.contains("responses_translation_event_suppressed")
            && logs.contains("event_type=response.output_item.done"),
        "expected stable suppression trace for successful internal tool completion, logs were: {logs}"
    );
    assert!(!logs.contains("secret-internal"));
}

#[tokio::test]
async fn internal_tool_followup_completed_only_text_is_synthesized_as_final_delta() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "synthesize final follow-up completed-only assistant text",
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(
            r#"{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let followup_request: Value = serde_json::from_str(&message_text(
        server
            .recv_client_message()
            .await
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup_request["type"], "response.create");

    let followup_input = followup_request["input"]
        .as_array()
        .expect("followup input array");
    assert_eq!(followup_input.len(), 1);
    assert_eq!(followup_input[0]["type"], "function_call_output");
    assert_eq!(followup_input[0]["call_id"], "call-1");
    assert_eq!(followup_input[0]["output"], "alpha");

    let final_completed = json!({
        "type": "response.completed",
        "response": {
            "id": "response-final",
            "output": [
                {
                    "id": "msg-final",
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "final follow-up answer"
                        }
                    ]
                }
            ]
        }
    });
    server.send_text(&final_completed.to_string()).await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 3);

    let synthetic_delta_frame = sse_event_and_data(frames[0]);
    assert_eq!(synthetic_delta_frame.0, "response.output_text.delta");
    assert_eq!(
        serde_json::from_str::<Value>(synthetic_delta_frame.1).expect("synthetic delta json"),
        json!({
            "type": "response.output_text.delta",
            "delta": "final follow-up answer",
            "item_id": "msg-final",
            "output_index": 0,
            "content_index": 0
        })
    );

    let completed_frame = sse_event_and_data(frames[1]);
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(
        serde_json::from_str::<Value>(completed_frame.1).expect("completed json"),
        final_completed
    );

    assert_done_frame(frames[2]);
    assert!(!body_text.contains("response.output_item.added"));
    assert!(!body_text.contains("response.output_item.done"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
}

#[tokio::test]
async fn visible_followup_function_call_argument_delta_is_forwarded_when_output_index_is_reused() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "reuse output index after internal tool follow-up",
            "tools": [
                {
                    "type": "function",
                    "name": "apply_patch",
                    "description": "visible tool",
                    "parameters": {"type": "object"}
                }
            ]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(
            r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call-internal","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call-internal","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let followup_request: Value = serde_json::from_str(&message_text(
        server
            .recv_client_message()
            .await
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup_request["type"], "response.create");
    assert_eq!(
        followup_request["previous_response_id"],
        "response-intermediate"
    );
    assert_eq!(
        followup_request["input"]
            .as_array()
            .expect("followup input")[0]["output"],
        "alpha"
    );

    server
        .send_text(
            r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call-visible","name":"apply_patch","arguments":""}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.function_call_arguments.delta","output_index":0,"item_id":"item-visible","delta":"{\"input\":\"*** Begin Patch"}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call-visible","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    let visible_added = json!({
        "type": "response.output_item.added",
        "output_index": 0,
        "item": {
            "type": "function_call",
            "call_id": "call-visible",
            "name": "apply_patch",
            "arguments": ""
        }
    });
    let visible_delta = json!({
        "type": "response.function_call_arguments.delta",
        "output_index": 0,
        "item_id": "item-visible",
        "delta": "{\"input\":\"*** Begin Patch"
    });
    let visible_done = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "type": "function_call",
            "call_id": "call-visible",
            "name": "apply_patch",
            "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"
        }
    });
    let final_completed = json!({
        "type": "response.completed",
        "response": {"id": "response-final"}
    });

    assert_eq!(frames.len(), 5);

    let added_frame = sse_event_and_data(frames[0]);
    assert_eq!(added_frame.0, "response.output_item.added");
    assert_eq!(added_frame.1, visible_added.to_string());

    let delta_frame = sse_event_and_data(frames[1]);
    assert_eq!(delta_frame.0, "response.function_call_arguments.delta");
    assert_eq!(delta_frame.1, visible_delta.to_string());

    let done_frame = sse_event_and_data(frames[2]);
    assert_eq!(done_frame.0, "response.output_item.done");
    assert_eq!(done_frame.1, visible_done.to_string());

    let completed_frame = sse_event_and_data(frames[3]);
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(completed_frame.1, final_completed.to_string());

    assert_done_frame(frames[4]);
    assert!(!body_text.contains("call-internal"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
}

#[test]
fn start_job_tool_returns_stable_disabled_json_by_default() {
    let event = json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-start",
            "name": "threadline_start_job",
            "arguments": {
                "command": ["echo", "hello"]
            }
        }
    });

    let call = InternalToolCall::from_event(&event)
        .expect("tool parse")
        .expect("internal tool call");
    let output = call.execute().expect("tool output").into_followup_input();

    assert_eq!(output["type"], "function_call_output");
    assert_eq!(output["call_id"], "call-start");

    let payload: Value = serde_json::from_str(output["output"].as_str().expect("output string"))
        .expect("json payload");
    assert_eq!(payload["ok"], false);
    assert_eq!(payload["code"], "jobs_disabled");
    assert_eq!(payload.get("next_action_hint"), None);

    let invalid_event = json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-start-invalid",
            "name": "threadline_start_job",
            "arguments": {
                "command": "echo hello"
            }
        }
    });

    let invalid_call = InternalToolCall::from_event(&invalid_event)
        .expect("invalid tool parse")
        .expect("invalid internal tool call");
    let invalid_output = invalid_call
        .execute()
        .expect("invalid tool output")
        .into_followup_input();
    let invalid_payload: Value = serde_json::from_str(
        invalid_output["output"]
            .as_str()
            .expect("invalid output string"),
    )
    .expect("invalid json payload");
    assert_eq!(invalid_payload["ok"], false);
    assert_eq!(invalid_payload["code"], "invalid_job_request");
    assert_eq!(invalid_payload.get("next_action_hint"), None);
}

#[test]
fn injected_job_tool_definitions_include_contract_phrases_and_preserve_schema() {
    let mut payload = serde_json::Map::new();
    inject_internal_tools(&mut payload);

    let tools = payload["tools"].as_array().expect("tools array");
    let find_tool = |name: &str| {
        tools
            .iter()
            .find(|tool| tool["name"] == name)
            .unwrap_or_else(|| panic!("missing tool definition: {name}"))
    };

    let start = find_tool("threadline_start_job");
    let start_description = start["description"].as_str().expect("start description");
    assert!(start_description.contains("background"));
    assert!(start_description.contains("return immediately"));
    assert!(start_description.contains("busy-poll"));
    assert_eq!(start["parameters"]["required"], json!(["command"]));
    assert_eq!(start["parameters"]["additionalProperties"], false);
    assert_eq!(start["parameters"]["properties"]["command"]["minItems"], 1);

    let poll = find_tool("threadline_poll_job");
    let poll_description = poll["description"].as_str().expect("poll description");
    assert!(poll_description.contains("natural checkpoint"));
    assert!(poll_description.contains("tight loop"));
    assert_eq!(poll["parameters"]["required"], json!(["job_id"]));
    assert_eq!(poll["parameters"]["additionalProperties"], false);

    let read_output = find_tool("threadline_read_job_output");
    let read_description = read_output["description"]
        .as_str()
        .expect("read description");
    assert!(read_description.contains("next_offset"));
    assert!(read_description.contains("truncated_before"));
    assert_eq!(read_output["parameters"]["required"], json!(["job_id"]));
    assert_eq!(read_output["parameters"]["additionalProperties"], false);
    assert_eq!(
        read_output["parameters"]["properties"]["offset"]["minimum"],
        0
    );

    let result = find_tool("threadline_get_job_result");
    let result_description = result["description"].as_str().expect("result description");
    assert!(result_description.contains("before final claims"));
    assert!(result_description.contains("success or failure"));
    assert_eq!(result["parameters"]["required"], json!(["job_id"]));
    assert_eq!(result["parameters"]["additionalProperties"], false);

    let cancel = find_tool("threadline_cancel_job");
    let cancel_description = cancel["description"].as_str().expect("cancel description");
    assert!(cancel_description.contains("stuck"));
    assert!(cancel_description.contains("poll or get the result"));
    assert_eq!(cancel["parameters"]["required"], json!(["job_id"]));
    assert_eq!(cancel["parameters"]["additionalProperties"], false);
}

#[tokio::test]
async fn start_job_tool_serializes_success_hint_in_function_call_output() {
    let manager = shell_job_manager();
    let command = if cfg!(windows) {
        shell_command("Write-Output 'tool success'; Start-Sleep -Milliseconds 50")
    } else {
        shell_command("printf 'tool success\n'; sleep 0.05")
    };

    let call = InternalToolCall::from_event(&json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-start-success",
            "name": "threadline_start_job",
            "arguments": {"command": command}
        }
    }))
    .expect("start parse")
    .expect("start call");

    let output = call
        .execute_with_job_manager(&manager)
        .expect("start output")
        .into_followup_input();
    let payload: Value =
        serde_json::from_str(output["output"].as_str().expect("start output string"))
            .expect("start json payload");

    assert_eq!(output["type"], "function_call_output");
    assert_eq!(output["call_id"], "call-start-success");
    assert_eq!(payload["ok"], true);
    assert_eq!(payload["status"], "starting");
    assert_eq!(payload["next_action_hint"], JOB_START_NEXT_ACTION_HINT);

    let job_id = payload["job_id"].as_str().expect("job id").to_string();
    let result = wait_for_terminal_result(&manager, &job_id, Duration::from_millis(1500)).await;
    assert_eq!(result["status"], "completed");
    assert_eq!(result["result"]["success"], true);
}

#[tokio::test]
async fn job_tool_outputs_are_serialized_as_function_call_output_json() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        allowed_commands: vec![],
    });
    let started = manager.spawn_job("tool-job", move |context| async move {
        context.mark_running();
        context.push_stdout("hello\n");
        context.complete(json!({"summary": "done"}));
    });
    let job_id = started["job_id"].as_str().expect("job id").to_string();
    sleep(Duration::from_millis(20)).await;

    let poll_call = InternalToolCall::from_event(&json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-poll",
            "name": "threadline_poll_job",
            "arguments": {"job_id": job_id}
        }
    }))
    .expect("poll parse")
    .expect("poll call");
    let poll_output = poll_call
        .execute_with_job_manager(&manager)
        .expect("poll output")
        .into_followup_input();
    let poll_payload: Value =
        serde_json::from_str(poll_output["output"].as_str().expect("poll output string"))
            .expect("poll json");
    assert_eq!(poll_payload["status"], "completed");

    let read_call = InternalToolCall::from_event(&json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-read",
            "name": "threadline_read_job_output",
            "arguments": {"job_id": job_id, "offset": 0}
        }
    }))
    .expect("read parse")
    .expect("read call");
    let read_output = read_call
        .execute_with_job_manager(&manager)
        .expect("read output")
        .into_followup_input();
    let read_payload: Value =
        serde_json::from_str(read_output["output"].as_str().expect("read output string"))
            .expect("read json");
    assert_eq!(read_payload["items"][0]["text"], "hello\n");

    let result_call = InternalToolCall::from_event(&json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-result",
            "name": "threadline_get_job_result",
            "arguments": {"job_id": job_id}
        }
    }))
    .expect("result parse")
    .expect("result call");
    let result_output = result_call
        .execute_with_job_manager(&manager)
        .expect("result output")
        .into_followup_input();
    let result_payload: Value = serde_json::from_str(
        result_output["output"]
            .as_str()
            .expect("result output string"),
    )
    .expect("result json");
    assert_eq!(result_payload["result"]["summary"], "done");
}

#[test]
fn compaction_item_with_threadline_like_name_is_not_an_internal_tool_call() {
    let event = json!({
        "type": "response.output_item.done",
        "item": {
            "type": "compaction",
            "id": "cmp_1",
            "name": "threadline_echo",
            "tool_name": "threadline_echo",
            "encrypted_content": "opaque"
        }
    });

    assert!(
        InternalToolCall::from_event(&event)
            .expect("compaction parse")
            .is_none(),
        "expected compaction items to bypass internal function-call handling"
    );
}

#[test]
fn internal_tool_name_detection_does_not_match_non_function_compaction_items() {
    let compaction_added = json!({
        "type": "response.output_item.added",
        "output_index": 0,
        "item": {
            "type": "compaction",
            "id": "cmp_1",
            "name": "threadline_echo",
            "encrypted_content": "opaque"
        }
    });
    let compaction_done = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "type": "compaction",
            "id": "cmp_1",
            "tool_name": "threadline_echo",
            "encrypted_content": "opaque"
        }
    });
    let internal_done = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "type": "function_call",
            "call_id": "call-internal",
            "name": "threadline_echo",
            "arguments": "{\"value\":\"opaque\"}"
        }
    });

    assert!(
        !event_contains_internal_tool_name(&compaction_added),
        "expected non-function compaction added event to remain visible across translation"
    );
    assert!(
        !event_contains_internal_tool_name(&compaction_done),
        "expected non-function compaction done event to remain visible across translation"
    );
    assert!(
        event_contains_internal_tool_name(&internal_done),
        "expected actual internal function_call item to stay suppressed"
    );
}

#[test]
fn actual_internal_function_call_with_threadline_name_remains_suppressed() {
    let event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "type": "function_call",
            "call_id": "call-internal",
            "name": "threadline_echo",
            "arguments": "{\"value\":\"secret-internal\"}"
        }
    });

    assert!(
        InternalToolCall::from_event(&event)
            .expect("internal parse")
            .is_some(),
        "expected actual threadline function_call item to remain an internal tool"
    );
    assert!(
        event_contains_internal_tool_name(&event),
        "expected actual threadline function_call item to remain suppressible"
    );
}
