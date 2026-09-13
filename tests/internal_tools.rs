use axum::body::{Body, Bytes, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures_util::{Stream, StreamExt, future::BoxFuture, future::poll_fn};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::pin::Pin;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::Instant;
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, sleep, timeout};
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
    ConnectedUpstream, InternalToolExecutor, ThreadlineServices, UpstreamAuthProvider,
    UpstreamConnector,
};
use threadline::tools::{
    InternalToolCall, PendingInternalToolOutput, event_contains_internal_tool_name,
    inject_internal_tools,
};
use threadline::ws_pump::{LiveUpstreamWebSocket, UpstreamInboundLimits, UpstreamWatchdogPolicy};

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
    websockets: Arc<Mutex<Vec<Weak<LiveUpstreamWebSocket>>>>,
    inbound_limits: UpstreamInboundLimits,
    watchdog_policy: Option<UpstreamWatchdogPolicy>,
}

impl RecordingConnector {
    fn new(plans: Vec<PlannedConnection>) -> Self {
        Self {
            plans: Arc::new(Mutex::new(plans.into())),
            websockets: Arc::new(Mutex::new(Vec::new())),
            inbound_limits: UpstreamInboundLimits::DEFAULT,
            watchdog_policy: None,
        }
    }

    fn with_inbound_limits(
        plans: Vec<PlannedConnection>,
        inbound_limits: UpstreamInboundLimits,
    ) -> Self {
        Self {
            inbound_limits,
            ..Self::new(plans)
        }
    }

    fn with_watchdog_policy(
        plans: Vec<PlannedConnection>,
        watchdog_policy: UpstreamWatchdogPolicy,
    ) -> Self {
        Self {
            watchdog_policy: Some(watchdog_policy),
            ..Self::new(plans)
        }
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
        let websockets = Arc::clone(&self.websockets);
        let inbound_limits = self.inbound_limits;
        let watchdog_policy = self.watchdog_policy;
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

            let websocket = Arc::new(match watchdog_policy {
                Some(policy) => {
                    LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                        stream,
                        Duration::from_millis(5),
                        policy,
                    )
                }
                None => LiveUpstreamWebSocket::from_stream_with_limits(stream, inbound_limits),
            });
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
struct PausedInternalToolExecutor {
    started: Arc<Notify>,
    resume: Arc<Notify>,
    executions: Arc<AtomicUsize>,
}

impl PausedInternalToolExecutor {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
            executions: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl InternalToolExecutor for PausedInternalToolExecutor {
    fn execute(
        &self,
        call: InternalToolCall,
    ) -> BoxFuture<'static, Result<PendingInternalToolOutput, ThreadlineError>> {
        let started = Arc::clone(&self.started);
        let resume = Arc::clone(&self.resume);
        let executions = Arc::clone(&self.executions);
        Box::pin(async move {
            executions.fetch_add(1, Ordering::SeqCst);
            started.notify_one();
            resume.notified().await;
            call.execute()
        })
    }
}

#[derive(Clone)]
struct PausedFailingInternalToolExecutor {
    started: Arc<Notify>,
    resume: Arc<Notify>,
    executions: Arc<AtomicUsize>,
}

impl PausedFailingInternalToolExecutor {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
            executions: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl InternalToolExecutor for PausedFailingInternalToolExecutor {
    fn execute(
        &self,
        _call: InternalToolCall,
    ) -> BoxFuture<'static, Result<PendingInternalToolOutput, ThreadlineError>> {
        let started = Arc::clone(&self.started);
        let resume = Arc::clone(&self.resume);
        let executions = Arc::clone(&self.executions);
        Box::pin(async move {
            executions.fetch_add(1, Ordering::SeqCst);
            started.notify_one();
            resume.notified().await;
            Err(ThreadlineError::InternalToolFailed)
        })
    }
}

fn build_test_router(connector: Arc<dyn UpstreamConnector>) -> axum::Router {
    build_test_router_with_config(ThreadlineConfig::default(), connector)
}

fn build_test_router_with_internal_tool_executor(
    connector: Arc<dyn UpstreamConnector>,
    internal_tool_executor: Arc<dyn InternalToolExecutor>,
) -> axum::Router {
    build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::with_internal_tool_executor(
            Arc::new(StaticAuthProvider),
            connector,
            internal_tool_executor,
        ),
    )
}

fn build_test_router_with_config(
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

fn auxiliary_summary_text() -> &'static str {
    concat!(
        "The conversation has grown too large for the context window and must be compacted now",
        "\n\n",
        "Your ONLY task right now is to produce a comprehensive summary",
        "\n",
        "Output your summary wrapped in <summary> and </summary> tags"
    )
}

fn short_watchdog_policy() -> UpstreamWatchdogPolicy {
    UpstreamWatchdogPolicy::new(Duration::from_millis(30), Duration::from_secs(1))
        .expect("valid watchdog policy")
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

fn downstream_function_tool(name: &str) -> Value {
    json!({
        "type": "function",
        "name": name,
        "description": format!("{name} description"),
        "parameters": {
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }
    })
}

fn auxiliary_summary_request_with_tools(tools: Vec<Value>) -> Value {
    json!({
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
        ],
        "tools": tools
    })
}

fn new_auto_system_summary_text() -> &'static str {
    "Your task is to create a comprehensive, detailed summary of the entire conversation that captures all essential information needed to seamlessly continue the work without any loss of context"
}

fn new_auto_compressed_history_text() -> &'static str {
    concat!(
        "The following is a compressed version of the preceeding history in the current conversation. ",
        "The first message is kept, some history may be truncated after that:"
    )
}

fn new_auto_final_summary_prompt_text() -> &'static str {
    concat!(
        "Summarize the conversation history so far, paying special attention to the most recent agent commands and tool results that triggered this summarization. ",
        "Structure your summary using the enhanced format provided in the system message.\n",
        "Focus particularly on:\n",
        "- The specific agent commands/tools that were just executed\n",
        "- The results returned from these recent tool calls (truncate if very long but preserve key information)\n",
        "- What the agent was actively working on when the token budget was exceeded\n",
        "- How these recent operations connect to the overall user goals\n",
        "Include all important tool calls and their results as part of the appropriate sections, with special emphasis on the most recent operations."
    )
}

fn new_auto_summary_request_with_tools(tools: Vec<Value>) -> Value {
    json!({
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
            {
                "type": "message",
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": new_auto_system_summary_text()
                    }
                ]
            },
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": new_auto_compressed_history_text()
                    }
                ]
            },
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": new_auto_final_summary_prompt_text()
                    }
                ]
            }
        ],
        "tools": tools
    })
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
        max_active_jobs: 16,
        max_retained_jobs: 128,
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
    assert_eq!(completed_payload["response"]["id"], "response-final");
    assert_eq!(
        completed_payload["response"]["output"][0]["content"][0]["text"],
        "final answer"
    );
    assert_done_frame(frames[2]);
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
    assert!(!body_text.contains("event: response.output_item.done"));
    assert!(server.take_pending_client_messages().await.is_empty());
}

#[tokio::test]
async fn internal_tool_followup_preserves_persistent_reasoning_context_and_outputs() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router_with_config(
        ThreadlineConfig {
            persistent_reasoning_enabled: true,
            ..ThreadlineConfig::default()
        },
        Arc::new(connector),
    );

    let response = post_responses(
        app,
        json!({
            "model":"threadline-main-gpt-5.6-terra",
            "input":"run persistent internal tool loop",
            "reasoning":{"effort":"high","summary":"auto"}
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
    let expected_reasoning = json!({"context":"all_turns","effort":"high","summary":"auto"});
    assert_eq!(first_request["type"], "response.create");
    assert_eq!(first_request["model"], "gpt-5.6-terra");
    assert_eq!(first_request["reasoning"], expected_reasoning);

    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-persistent","name":"threadline_echo","arguments":"{\"value\":\"persistent output\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.completed","response":{"id":"response-persistent-intermediate"}}"#,
        )
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
        followup_request["reasoning"],
        json!({"context":"all_turns","effort":"high","summary":"auto"})
    );
    assert_eq!(
        followup_request["previous_response_id"],
        "response-persistent-intermediate"
    );
    assert_eq!(
        followup_request["input"],
        json!([{
            "type":"function_call_output",
            "call_id":"call-persistent",
            "output":"persistent output"
        }])
    );

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"persistent final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-persistent-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    assert!(body_text.contains("persistent final answer"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-persistent-intermediate"));
    assert!(!body_text.contains("event: response.output_item.done"));
    assert!(server.take_pending_client_messages().await.is_empty());
}

#[tokio::test]
async fn internal_tool_intermediate_text_does_not_leak_and_followup_fallback_still_runs() {
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
            "input": "hide intermediate assistant text during internal tool follow-up"
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
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_text.done","item_id":"msg-intermediate","output_index":0,"content_index":0,"text":"hidden intermediate assistant text"}"#,
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
            .expect("followup input array")[0]["output"],
        "alpha"
    );

    let final_completed = json!({
        "type": "response.completed",
        "response": {
            "id": "response-final",
            "output": [
                {
                    "type": "function_call",
                    "name": "threadline_echo",
                    "call_id": "call-hidden-final"
                },
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
            "output_index": 1,
            "content_index": 0
        })
    );

    let completed_frame = sse_event_and_data(frames[1]);
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(
        serde_json::from_str::<Value>(completed_frame.1).expect("completed json"),
        json!({
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
        })
    );

    assert_done_frame(frames[2]);
    assert!(!body_text.contains("hidden intermediate assistant text"));
    assert!(!body_text.contains("response.output_text.done"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
}

#[tokio::test]
async fn internal_tool_intermediate_output_item_done_text_does_not_leak() {
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
            "input": "hide intermediate output_item.done assistant text during internal tool follow-up"
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
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"msg-intermediate","type":"message","role":"assistant","content":[{"type":"output_text","text":"hidden intermediate assistant text"}]}}"#,
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
            .expect("followup input array")[0]["output"],
        "alpha"
    );

    let final_done_event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "id": "msg-final",
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": "final follow-up answer from output_item.done"
                }
            ]
        }
    });
    server.send_text(&final_done_event.to_string()).await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 4);

    let delta_frame = sse_event_and_data(frames[0]);
    assert_eq!(delta_frame.0, "response.output_text.delta");
    assert_eq!(
        serde_json::from_str::<Value>(delta_frame.1).expect("delta json"),
        json!({
            "type": "response.output_text.delta",
            "delta": "final follow-up answer from output_item.done",
            "item_id": "msg-final",
            "output_index": 0,
            "content_index": 0
        })
    );

    let done_frame = sse_event_and_data(frames[1]);
    assert_eq!(done_frame.0, "response.output_item.done");
    assert_eq!(done_frame.1, final_done_event.to_string());

    let completed_frame = sse_event_and_data(frames[2]);
    let completed_payload: Value = serde_json::from_str(completed_frame.1).expect("completed json");
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(completed_payload["response"]["id"], "response-final");
    assert_eq!(
        completed_payload["response"]["output"][0]["content"][0]["text"],
        "final follow-up answer from output_item.done"
    );

    assert_done_frame(frames[3]);
    assert!(!body_text.contains("hidden intermediate assistant text"));
    assert!(!body_text.contains("msg-intermediate"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
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
    let completed_payload: Value = serde_json::from_str(completed_data).expect("completed json");
    assert_eq!(completed_payload["response"]["id"], "response-final");
    assert_eq!(
        completed_payload["response"]["output"][0]["content"][0]["text"],
        "final answer"
    );
    assert_done_frame(frames[2]);
    assert!(!body_text.contains("event: response.output_item.added"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
}

#[tokio::test]
async fn summary_request_does_not_inject_threadline_internal_tools() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        auxiliary_summary_request_with_tools(vec![downstream_function_tool("downstream_tool")]),
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
    let tools = first_request["tools"].as_array().expect("tools array");

    assert!(tools.iter().any(|tool| tool["name"] == "downstream_tool"));
    assert!(
        !tools.iter().any(|tool| {
            tool["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("threadline_"))
        }),
        "expected classified summary request to skip internal tool injection: {tools:?}"
    );

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"summary answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-summary"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    assert!(!body_text.contains("threadline_"));
}

#[tokio::test]
async fn summary_request_strips_downstream_threadline_prefixed_tools() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        auxiliary_summary_request_with_tools(vec![
            downstream_function_tool("downstream_tool"),
            downstream_function_tool("threadline_echo"),
            downstream_function_tool("external_web_search"),
        ]),
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
    let tools = first_request["tools"].as_array().expect("tools array");
    let tool_names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect();

    assert_eq!(
        tool_names.len(),
        2,
        "expected only non-Threadline tools upstream"
    );
    assert!(tool_names.contains(&"downstream_tool"));
    assert!(tool_names.contains(&"external_web_search"));
    assert!(
        !tool_names
            .iter()
            .any(|name| name.starts_with("threadline_")),
        "expected classified summary request to strip downstream threadline_* tools: {tool_names:?}"
    );

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"summary answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-summary"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    assert!(!body_text.contains("threadline_"));
}

#[tokio::test]
async fn summary_request_does_not_execute_threadline_tool_call_events() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        auxiliary_summary_request_with_tools(vec![downstream_function_tool("downstream_tool")]),
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
    let tools = first_request["tools"].as_array().expect("tools array");

    assert!(
        !tools.iter().any(|tool| {
            tool["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("threadline_"))
        }),
        "expected classified summary request to exclude internal tools before streaming"
    );

    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.completed","response":{"id":"response-summary-intermediate"}}"#,
        )
        .await;

    let maybe_followup =
        tokio::time::timeout(Duration::from_millis(100), server.recv_client_message()).await;
    let saw_followup_request = matches!(maybe_followup, Ok(Some(_)));

    if saw_followup_request {
        server
            .send_text(r#"{"type":"response.output_text.delta","delta":"summary answer"}"#)
            .await;
        server
            .send_text(
                r#"{"type":"response.completed","response":{"id":"response-summary-final"}}"#,
            )
            .await;
    }

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");

    assert!(
        !saw_followup_request,
        "expected no local internal-tool followup request during summary stream"
    );
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("call-1"));
}

#[tokio::test]
async fn summary_request_new_auto_shape_does_not_inject_or_execute_threadline_tools() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        new_auto_summary_request_with_tools(vec![
            downstream_function_tool("downstream_tool"),
            downstream_function_tool("threadline_echo"),
        ]),
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
    let tools = first_request["tools"].as_array().expect("tools array");

    assert!(tools.iter().any(|tool| tool["name"] == "downstream_tool"));
    assert!(
        !tools.iter().any(|tool| {
            tool["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("threadline_"))
        }),
        "expected new auto summary request to strip threadline_* tools before streaming"
    );

    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-summary-new-auto"}}"#)
        .await;

    let maybe_followup =
        tokio::time::timeout(Duration::from_millis(100), server.recv_client_message()).await;
    assert!(
        !matches!(maybe_followup, Ok(Some(_))),
        "expected new auto summary request to avoid internal tool follow-up traffic"
    );

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response.output_item.done"));
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
        "response": {
            "id": "response-visible"
        }
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
        "response": {
            "id": "response-visible"
        }
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
async fn explicit_upstream_failure_remains_terminal_failure_after_forwarded_external_tool_event() {
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
            "input": "forward tool before terminal upstream failure",
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
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-visible","name":"downstream_tool","arguments":"{}"}}"#,
        )
        .await;
    server
        .send_text(
            r#"{"type":"response.failed","response":{"id":"response-visible-failed"},"error":{"code":"upstream_response_failed","message":"failed after visible tool"}}"#,
        )
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 3);
    let tool_frame = sse_event_and_data(frames[0]);
    assert_eq!(tool_frame.0, "response.output_item.done");
    assert_eq!(
        serde_json::from_str::<Value>(tool_frame.1).expect("tool payload json"),
        json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "call_id": "call-visible",
                "name": "downstream_tool",
                "arguments": "{}"
            }
        })
    );

    let failed_frame = sse_event_and_data(frames[1]);
    let failed_payload: Value = serde_json::from_str(failed_frame.1).expect("failed json");
    assert_eq!(failed_frame.0, "response.failed");
    assert_eq!(failed_payload["response"]["id"], "response-visible-failed");
    assert_eq!(
        failed_payload["response"]["error"]["code"],
        "upstream_response_failed"
    );
    assert_eq!(
        failed_payload["response"]["error"]["message"],
        "failed after visible tool"
    );
    assert_done_frame(frames[2]);
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
    let completed_payload: Value = serde_json::from_str(completed_frame.1).expect("completed json");
    assert_eq!(completed_payload["response"]["id"], "response-final");
    assert_eq!(
        completed_payload["response"]["output"][0]["content"][0]["text"],
        "final answer"
    );
    assert_done_frame(frames[2]);
    assert!(!body_text.contains("response.output_item.added"));
    assert!(!body_text.contains("response.output_item.done"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
}

#[tokio::test]
async fn intermediate_internal_tool_completion_keeps_marker_active_until_followup_finishes() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let seed = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(seed.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(
            r#"{"type":"response.completed","response":{"id":"response-1","output":[{"id":"assistant-seed","type":"message","role":"assistant","content":[{"type":"output_text","text":"seed answer"}]}]}}"#,
        )
        .await;
    let _ = to_bytes(seed.into_body(), usize::MAX)
        .await
        .expect("seed body");

    let active = post_responses(
        app.clone(),
        json!({
            "model": "gpt-5.4",
            "input": "run hidden internal tool loop",
            "previous_response_id": "response-1"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        to_bytes(active.into_body(), usize::MAX)
            .await
            .expect("body bytes")
    });

    let active_request: Value = serde_json::from_str(&message_text(
        server.recv_client_message().await.expect("active request"),
    ))
    .expect("active request json");
    assert_eq!(active_request["previous_response_id"], "response-1");

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
    assert_eq!(
        followup_request["previous_response_id"],
        "response-intermediate"
    );

    let conflict = post_responses(
        app.clone(),
        json!({
            "model": "gpt-5.4",
            "input": "conflict-before-followup-finish",
            "previous_response_id": "response-1"
        }),
    )
    .await;
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    assert!(
        server.take_pending_client_messages().await.is_empty(),
        "expected no extra upstream request while the follow-up response is still active"
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
    let delta_frame = sse_event_and_data(frames[0]);
    assert_eq!(delta_frame.0, "response.output_text.delta");
    assert_eq!(
        serde_json::from_str::<Value>(delta_frame.1).expect("delta json"),
        json!({"type":"response.output_text.delta","delta":"final answer"})
    );

    let completed_frame = sse_event_and_data(frames[1]);
    assert_eq!(completed_frame.0, "response.completed");
    let completed_payload: Value = serde_json::from_str(completed_frame.1).expect("completed json");
    assert_eq!(completed_payload["response"]["id"], "response-final");
    assert_eq!(
        completed_payload["response"]["output"][0]["content"][0]["text"],
        "final answer"
    );

    assert_done_frame(frames[2]);
}

#[tokio::test]
async fn internal_tool_intermediate_completion_sends_followup_without_downstream_failure() {
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
            "input": "hold downstream terminal state until internal follow-up request exists"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let mut body_stream = response.into_body().into_data_stream();
    let mut pending = String::new();

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

    let premature_frame = timeout(
        Duration::from_millis(100),
        next_sse_frame(&mut body_stream, &mut pending),
    )
    .await;
    assert!(
        premature_frame.is_err(),
        "expected no downstream terminal frame before the internal follow-up request is observed"
    );

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

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let delta_frame = next_sse_frame(&mut body_stream, &mut pending).await;
    let completed_frame = next_sse_frame(&mut body_stream, &mut pending).await;
    let done_sentinel = next_sse_frame(&mut body_stream, &mut pending).await;

    assert_eq!(
        sse_event_and_data(&delta_frame).0,
        "response.output_text.delta"
    );
    assert_eq!(sse_event_and_data(&completed_frame).0, "response.completed");
    assert_done_frame(&done_sentinel);
    assert!(
        body_stream.next().await.is_none(),
        "expected EOF after DONE"
    );
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
    let completed_payload: Value = serde_json::from_str(completed_frame.1).expect("completed json");
    assert_eq!(completed_payload["response"]["id"], "response-final");
    assert_eq!(
        completed_payload["response"]["output"][0]["content"][0]["text"],
        "final answer"
    );

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
async fn internal_tool_followup_output_item_done_message_becomes_final_completed_output() {
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
            "input": "surface follow-up output_item.done assistant text as final completed output"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        timeout(
            Duration::from_secs(2),
            to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("body timeout")
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

    let final_done_event = json!({
        "type": "response.output_item.done",
        "output_index": 0,
        "item": {
            "id": "msg-final",
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "output_text",
                    "text": "final follow-up answer from output_item.done"
                }
            ]
        }
    });
    server.send_text(&final_done_event.to_string()).await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 4);

    let delta_frame = sse_event_and_data(frames[0]);
    assert_eq!(delta_frame.0, "response.output_text.delta");
    assert_eq!(
        serde_json::from_str::<Value>(delta_frame.1).expect("delta json"),
        json!({
            "type": "response.output_text.delta",
            "delta": "final follow-up answer from output_item.done",
            "item_id": "msg-final",
            "output_index": 0,
            "content_index": 0
        })
    );

    let done_frame = sse_event_and_data(frames[1]);
    assert_eq!(done_frame.0, "response.output_item.done");
    assert_eq!(done_frame.1, final_done_event.to_string());

    let completed_frame = sse_event_and_data(frames[2]);
    let completed_payload: Value = serde_json::from_str(completed_frame.1).expect("completed json");
    assert_eq!(completed_frame.0, "response.completed");
    assert_eq!(completed_payload["response"]["id"], "response-final");
    assert_eq!(
        completed_payload["response"]["output"][0]["content"][0]["text"],
        "final follow-up answer from output_item.done"
    );

    assert_done_frame(frames[3]);
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
}

#[tokio::test]
async fn intermediate_internal_tool_completion_does_not_record_marker() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app.clone(),
        json!({
            "model": "gpt-5.4",
            "input": "do not record intermediate internal completion markers"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        timeout(
            Duration::from_secs(2),
            to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("body timeout")
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

    let _ = server
        .recv_client_message()
        .await
        .expect("followup request");

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let _ = body_task.await.expect("body task");

    let rejected = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "invalid-intermediate-resume",
            "previous_response_id": "response-intermediate"
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
async fn internal_tool_followup_failure_emits_response_failed_without_internal_leak() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![PlannedConnection {
        server: Arc::clone(&server),
        turn_state: None,
    }]);
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app.clone(),
        json!({
            "model": "gpt-5.4",
            "input": "normalize internal-tool follow-up failure"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        timeout(
            Duration::from_secs(2),
            to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("body timeout")
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

    let _ = server
        .recv_client_message()
        .await
        .expect("followup request");

    server
        .send_text(
            r#"{"type":"response.failed","response":{"id":"response-followup-failed","model":"gpt-5.4","usage":{"input_tokens":4,"output_tokens":0,"total_tokens":4}},"error":{"code":"upstream_response_failed","message":"followup failed"}}"#,
        )
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 2);

    let failed_frame = sse_event_and_data(frames[0]);
    let failed_payload: Value = serde_json::from_str(failed_frame.1).expect("failed json");
    assert_eq!(failed_frame.0, "response.failed");
    assert_eq!(failed_payload["response"]["id"], "response-followup-failed");
    assert_eq!(
        failed_payload["response"]["error"]["code"],
        "upstream_response_failed"
    );
    assert_done_frame(frames[1]);
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));

    let rejected = post_responses(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "invalid-followup-failed-resume",
            "previous_response_id": "response-followup-failed"
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
        "response": {
            "id": "response-final"
        }
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

#[tokio::test]
async fn internal_tool_followup_empty_final_does_not_reuse_intermediate_observability() {
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
            "input": "final empty completion after internal follow-up"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body_task = tokio::spawn(async move {
        timeout(
            Duration::from_secs(2),
            to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("body timeout")
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
    assert_eq!(
        followup_request["previous_response_id"],
        "response-intermediate"
    );

    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final-empty"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);

    assert_eq!(frames.len(), 2);
    let failed_frame = sse_event_and_data(frames[0]);
    let failed_payload: Value = serde_json::from_str(failed_frame.1).expect("failed json");
    assert_eq!(failed_frame.0, "response.failed");
    assert_eq!(failed_payload["response"]["id"], "response-final-empty");
    assert_eq!(
        failed_payload["response"]["error"]["code"],
        "threadline_no_observable_output"
    );
    assert_done_frame(frames[1]);
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
async fn start_job_tool_serializes_capacity_rejection_without_success_hint() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 0,
        max_retained_jobs: 1,
        allowed_commands: vec![shell_program()],
    });
    let call = InternalToolCall::from_event(&json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-capacity-rejected",
            "name": "threadline_start_job",
            "arguments": {"command": shell_command("Write-Output 'not started'")}
        }
    }))
    .expect("start parse")
    .expect("start call");

    let output = call
        .execute_with_job_manager(&manager)
        .expect("capacity output")
        .into_followup_input();
    let payload: Value =
        serde_json::from_str(output["output"].as_str().expect("capacity output string"))
            .expect("capacity json");

    assert_eq!(output["type"], "function_call_output");
    assert_eq!(output["call_id"], "call-capacity-rejected");
    assert_eq!(payload["ok"], false);
    assert_eq!(payload["code"], "job_capacity_exceeded");
    assert_eq!(payload.get("next_action_hint"), None);
}

#[tokio::test]
async fn poll_job_tool_serializes_expired_job_not_found_with_its_call_id() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::ZERO,
        max_active_jobs: 1,
        max_retained_jobs: 1,
        allowed_commands: vec![shell_program()],
    });
    let started = manager.spawn_job("expires-before-poll", |context| async move {
        context.complete(json!({"summary": "expired"}));
    });
    let job_id = started["job_id"].as_str().expect("job id");
    tokio::task::yield_now().await;

    let call = InternalToolCall::from_event(&json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "call_id": "call-expired-job",
            "name": "threadline_poll_job",
            "arguments": {"job_id": job_id}
        }
    }))
    .expect("poll parse")
    .expect("poll call");
    let output = call
        .execute_with_job_manager(&manager)
        .expect("expired job output")
        .into_followup_input();
    let payload: Value = serde_json::from_str(output["output"].as_str().expect("output string"))
        .expect("output JSON");

    assert_eq!(output["type"], "function_call_output");
    assert_eq!(output["call_id"], "call-expired-job");
    assert_eq!(payload["ok"], false);
    assert_eq!(payload["code"], "job_not_found");
    assert_eq!(payload.get("next_action_hint"), None);
}

#[tokio::test]
async fn job_retention_diagnostics_report_only_safe_required_fields() {
    let log_buffer = SharedLogBuffer::new();
    let trace_dispatch = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .without_time()
            .with_ansi(false)
            .with_writer(log_buffer.clone())
            .finish(),
    );
    tracing::dispatcher::with_default(&trace_dispatch, tracing::callsite::rebuild_interest_cache);
    let active_capacity_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 1,
        max_retained_jobs: 3,
        allowed_commands: vec![shell_program()],
    });
    let (first_release_tx, first_release_rx) = tokio::sync::oneshot::channel();
    let (first_finished_tx, first_finished_rx) = tokio::sync::oneshot::channel();
    let first = tracing::dispatcher::with_default(&trace_dispatch, || {
        active_capacity_manager.spawn_job("active-name-sentinel", move |context| async move {
            context.mark_running();
            let _ = tokio::time::timeout(Duration::from_secs(1), first_release_rx).await;
            context.complete(json!({"result": "active-result-sentinel"}));
            let _ = first_finished_tx.send(());
        })
    });
    assert_eq!(first["ok"], true);
    let command_output_sentinel = "command-output-sentinel";
    let environment_sentinel = "environment-sentinel";
    let environment_variable = "THREADLINE_TEST_RETENTION_ENV";
    let command = if cfg!(windows) {
        format!(
            "$env:{environment_variable}=\"{environment_sentinel}\"; Write-Output \"{command_output_sentinel} $env:{environment_variable}\""
        )
    } else {
        format!(
            "{environment_variable}='{environment_sentinel}'; export {environment_variable}; printf '%s %s\\n' '{command_output_sentinel}' \"${environment_variable}\""
        )
    };
    let active_rejected = tracing::dispatcher::with_default(&trace_dispatch, || {
        active_capacity_manager.start_command_json(shell_command(&command))
    });
    assert_eq!(active_rejected["code"], "job_capacity_exceeded");
    first_release_tx.send(()).expect("release active worker");
    first_finished_rx.await.expect("active worker finished");

    let retained_capacity_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 2,
        max_retained_jobs: 1,
        allowed_commands: vec![shell_program()],
    });
    let (retained_release_tx, retained_release_rx) = tokio::sync::oneshot::channel();
    let (retained_finished_tx, retained_finished_rx) = tokio::sync::oneshot::channel();
    let (retained_started_tx, retained_started_rx) = tokio::sync::oneshot::channel();
    let retained = tracing::dispatcher::with_default(&trace_dispatch, || {
        retained_capacity_manager.spawn_job("retained-name-sentinel", move |context| async move {
            context.mark_running();
            let _ = retained_started_tx.send(());
            let _ = tokio::time::timeout(Duration::from_secs(1), retained_release_rx).await;
            context.complete(json!({"result": "retained-result-sentinel"}));
            let _ = retained_finished_tx.send(());
        })
    });
    assert_eq!(retained["ok"], true);
    retained_started_rx.await.expect("retained worker started");

    let retained_rejected = tracing::dispatcher::with_default(&trace_dispatch, || {
        retained_capacity_manager.spawn_job("retained-rejected-name-sentinel", |_| async move {
            panic!("retained rejection must not execute");
        })
    });
    assert_eq!(retained_rejected["code"], "job_capacity_exceeded");
    retained_release_tx
        .send(())
        .expect("release retained worker");
    retained_finished_rx
        .await
        .expect("retained worker finished");

    let eviction_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 2,
        max_retained_jobs: 1,
        allowed_commands: vec![shell_program()],
    });
    let evicted = eviction_manager.start_command_json(shell_command(&command));
    let evicted_id = evicted["job_id"]
        .as_str()
        .expect("evicted job id")
        .to_string();
    let eviction_result =
        wait_for_terminal_result(&eviction_manager, &evicted_id, Duration::from_secs(2)).await;
    assert_eq!(eviction_result["result"]["kind"], "command");
    assert_eq!(
        eviction_result["result"]["command"],
        json!(shell_command(&command))
    );
    let eviction_output = eviction_manager.read_output_json(&evicted_id, 0);
    assert!(
        eviction_output
            .to_string()
            .contains(command_output_sentinel)
            && eviction_output.to_string().contains(environment_sentinel),
        "evicted command did not produce its child-environment output: {eviction_output}"
    );
    sleep(Duration::from_millis(10)).await;
    let admitted_after_eviction = tracing::dispatcher::with_default(&trace_dispatch, || {
        eviction_manager.spawn_job("eviction-trigger-name-sentinel", |context| async move {
            context.complete(json!({"result": "eviction-trigger-result-sentinel"}));
        })
    });
    assert_eq!(admitted_after_eviction["ok"], true);

    let prune_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(1),
        max_active_jobs: 1,
        max_retained_jobs: 1,
        allowed_commands: vec![shell_program()],
    });
    let prunable = prune_manager.start_command_json(shell_command(&command));
    let prunable_id = prunable["job_id"]
        .as_str()
        .expect("prunable job id")
        .to_string();
    let prune_result =
        wait_for_terminal_result(&prune_manager, &prunable_id, Duration::from_secs(2)).await;
    assert_eq!(prune_result["result"]["kind"], "command");
    assert_eq!(
        prune_result["result"]["command"],
        json!(shell_command(&command))
    );
    let prune_output = prune_manager.read_output_json(&prunable_id, 0);
    assert!(
        prune_output.to_string().contains(command_output_sentinel)
            && prune_output.to_string().contains(environment_sentinel),
        "pruned command did not produce its child-environment output: {prune_output}"
    );
    sleep(Duration::from_millis(1100)).await;
    let prunable_poll = tracing::dispatcher::with_default(&trace_dispatch, || {
        prune_manager.poll_json(&prunable_id)
    });
    assert_eq!(prunable_poll["code"], "job_not_found");

    let logs = log_buffer.logs();
    let retention_events = logs
        .lines()
        .filter(|line| {
            line.contains("job_capacity_rejected")
                || line.contains("job_retention_evicted")
                || line.contains("job_retention_pruned")
        })
        .collect::<Vec<_>>();
    assert!(
        retention_events.len() == 4,
        "expected active and retained rejections plus one eviction and one prune: {logs}"
    );

    let active_rejection = retention_events[0];
    assert!(
        active_rejection.contains("job_capacity_rejected")
            && active_rejection.contains("reason=\"active\"")
            && active_rejection.contains("active_count=1")
            && active_rejection.contains("entry_count=1")
            && active_rejection.contains("max_active_jobs=1")
            && active_rejection.contains("max_retained_jobs=3"),
        "active capacity diagnostic fields were missing: {active_rejection}"
    );
    let retained_rejection = retention_events[1];
    assert!(
        retained_rejection.contains("job_capacity_rejected")
            && retained_rejection.contains("reason=\"retained\"")
            && retained_rejection.contains("active_count=1")
            && retained_rejection.contains("entry_count=1")
            && retained_rejection.contains("max_active_jobs=2")
            && retained_rejection.contains("max_retained_jobs=1"),
        "retained capacity diagnostic fields were missing: {retained_rejection}"
    );
    let eviction = retention_events[2];
    assert!(
        eviction.contains("job_retention_evicted")
            && eviction.contains(&evicted_id)
            && eviction.contains("terminal_state=Some(Completed)")
            && eviction.contains("age_secs=")
            && eviction.contains("entry_count=0")
            && eviction.contains("retained_limit=1"),
        "eviction diagnostic fields were missing: {eviction}"
    );
    let prune = retention_events[3];
    assert!(
        prune.contains("job_retention_pruned")
            && prune.contains("removed_count=1")
            && prune.contains("remaining_count=0")
            && prune.contains("retention_ttl_secs=1"),
        "prune diagnostic fields were missing: {prune}"
    );

    for event in retention_events {
        for sentinel in [
            "active-name-sentinel",
            "active-result-sentinel",
            "retained-name-sentinel",
            "retained-rejected-name-sentinel",
            "retained-result-sentinel",
            "evicted-name-sentinel",
            "eviction-trigger-name-sentinel",
            "eviction-trigger-result-sentinel",
            "prune-sentinel-job",
            command_output_sentinel,
            environment_sentinel,
            environment_variable,
            &command,
        ] {
            assert!(
                !event.contains(sentinel),
                "sentinel leaked into retention diagnostic: {event}"
            );
        }
    }
}

#[tokio::test]
async fn job_tool_outputs_are_serialized_as_function_call_output_json() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 16,
        max_retained_jobs: 128,
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

#[tokio::test]
async fn overflow_after_internal_tool_execution_skips_followup_and_hides_intermediate_events() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::with_inbound_limits(
        vec![PlannedConnection {
            server: Arc::clone(&server),
            turn_state: None,
        }],
        UpstreamInboundLimits::new(1, 4096).expect("valid limits"),
    );
    let app = build_test_router(Arc::new(connector));

    let response = post_responses(
        app,
        json!({"model":"gpt-5.4","input":"run internal tool loop"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("initial request");

    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server.send_ping(b"tool queued").await;
    assert!(matches!(
        server.recv_client_message().await,
        Some(Message::Pong(payload)) if payload == b"tool queued"
    ));

    let pending = Arc::new(Notify::new());
    let pending_for_task = Arc::clone(&pending);
    let body_task = tokio::spawn(async move {
        let mut body_stream = response.into_body().into_data_stream();
        let failed = poll_fn(
            |context| match Pin::new(&mut body_stream).poll_next(context) {
                Poll::Pending => {
                    pending_for_task.notify_one();
                    Poll::Pending
                }
                Poll::Ready(Some(chunk)) => Poll::Ready(chunk.expect("failed SSE chunk")),
                Poll::Ready(None) => panic!("expected terminal failure frame"),
            },
        )
        .await;
        let done = body_stream
            .next()
            .await
            .expect("DONE frame")
            .expect("valid DONE frame");
        assert!(body_stream.next().await.is_none());
        (failed, done)
    });
    pending.notified().await;

    server
        .send_text_burst(&[
            r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#,
            r#"{"type":"response.output_text.delta","delta":"overflow"}"#,
        ])
        .await;
    timeout(Duration::from_secs(1), server.wait_for_client_disconnect())
        .await
        .expect("overflow should close the pump before the internal follow-up");

    let (failed, done) = body_task.await.expect("body task");
    let failed_text = String::from_utf8(failed.to_vec()).expect("failure utf8");
    assert!(failed_text.contains("event: response.failed"));
    assert!(failed_text.contains("upstream_inbound_buffer_overflow"));
    assert!(!failed_text.contains("response-intermediate"));
    assert!(!failed_text.contains("threadline_echo"));
    assert_eq!(
        String::from_utf8(done.to_vec()).expect("DONE utf8"),
        "data: [DONE]\n\n"
    );
    assert!(server.take_pending_client_messages().await.is_empty());
}

#[tokio::test]
async fn overflow_while_internal_tool_execution_is_pending_skips_followup_and_later_tools() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::with_inbound_limits(
        vec![PlannedConnection {
            server: Arc::clone(&server),
            turn_state: None,
        }],
        UpstreamInboundLimits::new(1, 4096).expect("valid limits"),
    );
    let executor = Arc::new(PausedInternalToolExecutor::new());
    let app = build_test_router_with_internal_tool_executor(
        Arc::new(connector),
        Arc::clone(&executor) as Arc<dyn InternalToolExecutor>,
    );

    let response = post_responses(
        app,
        json!({"model":"gpt-5.4","input":"run internal tool loop"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("initial request");
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("overflow body")
    });
    executor.started.notified().await;

    server
        .send_text_burst(&[
            r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#,
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-2","name":"threadline_echo","arguments":"{\"value\":\"beta\"}"}}"#,
        ])
        .await;
    timeout(Duration::from_secs(1), server.wait_for_client_disconnect())
        .await
        .expect("overflow should close the pump while tool execution is pending");
    executor.resume.notify_one();

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("failure utf8");
    assert_eq!(body_text.matches("event: response.failed").count(), 1);
    assert_eq!(body_text.matches("data: [DONE]").count(), 1);
    assert!(body_text.contains("upstream_inbound_buffer_overflow"));
    assert!(!body_text.contains("response-intermediate"));
    assert!(!body_text.contains("threadline_echo"));
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(server.take_pending_client_messages().await.is_empty());
}

#[tokio::test]
async fn paused_internal_tool_execution_stays_live_across_matching_pongs_before_followup() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::with_watchdog_policy(
        vec![PlannedConnection {
            server: Arc::clone(&server),
            turn_state: None,
        }],
        short_watchdog_policy(),
    );
    let executor = Arc::new(PausedInternalToolExecutor::new());
    let app = build_test_router_with_internal_tool_executor(
        Arc::new(connector),
        Arc::clone(&executor) as Arc<dyn InternalToolExecutor>,
    );

    let response = post_responses(
        app.clone(),
        json!({"model":"gpt-5.4","input":"run paused internal tool"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("initial request");
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;

    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body")
    });
    timeout(Duration::from_secs(1), executor.started.notified())
        .await
        .expect("internal tool execution should start");
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let liveness_started = Instant::now();
    for _ in 0..12 {
        let ping = timeout(Duration::from_secs(1), server.recv_client_message())
            .await
            .expect("watchdog ping should arrive")
            .expect("client should remain connected");
        let Message::Ping(payload) = ping else {
            panic!("expected watchdog ping, got {ping:?}");
        };
        server.send_pong(payload).await;
    }
    assert!(
        liveness_started.elapsed() >= Duration::from_millis(60),
        "matching Pongs must keep the paused tool alive for at least two Pong deadlines"
    );
    assert!(
        !body_task.is_finished(),
        "internal tool wait must not produce a downstream terminal event"
    );

    executor.resume.notify_one();
    let followup: Value = serde_json::from_str(&message_text(
        timeout(Duration::from_secs(1), server.recv_client_message())
            .await
            .expect("followup request should arrive")
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup["type"], "response.create");
    assert_eq!(followup["previous_response_id"], "response-intermediate");
    server
        .send_text(
            r#"{"type":"response.completed","response":{"id":"response-final","output":[{"id":"msg-final","type":"message","role":"assistant","content":[{"type":"output_text","text":"final answer"}]}]}}"#,
        )
        .await;

    let body = timeout(Duration::from_secs(1), body_task)
        .await
        .expect("final response body timeout")
        .expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("response utf8");
    assert_eq!(body_text.matches("event: response.completed").count(), 1);
    assert_eq!(body_text.matches("data: [DONE]").count(), 1);
    assert!(!body_text.contains("event: response.failed"));
    assert!(body_text.contains("final answer"));
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);

    let intermediate = post_responses(
        app,
        json!({
            "model":"gpt-5.4",
            "input":"resume intermediate",
            "previous_response_id":"response-intermediate"
        }),
    )
    .await;
    assert_eq!(intermediate.status(), StatusCode::BAD_REQUEST);
    let intermediate_body = to_bytes(intermediate.into_body(), usize::MAX)
        .await
        .expect("intermediate marker body");
    let intermediate_payload: Value =
        serde_json::from_slice(&intermediate_body).expect("intermediate marker json");
    assert_eq!(
        intermediate_payload["error"]["code"],
        "previous_response_not_found"
    );
}

#[tokio::test]
async fn liveness_expiry_while_internal_tool_waits_skips_queued_work_and_keeps_prior_marker() {
    let server =
        Arc::new(ScriptedWebSocketServer::start_after_first_client_message_without_reader().await);
    let replacement_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::with_watchdog_policy(
        vec![
            PlannedConnection {
                server: Arc::clone(&server),
                turn_state: None,
            },
            PlannedConnection {
                server: Arc::clone(&replacement_server),
                turn_state: None,
            },
        ],
        UpstreamWatchdogPolicy::new(Duration::from_millis(100), Duration::from_secs(1))
            .expect("valid watchdog policy"),
    );
    let connector_observer = connector.clone();
    let executor = Arc::new(PausedInternalToolExecutor::new());
    let app = build_router_with_services(
        ThreadlineConfig {
            retained_session_capacity: 1,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::with_internal_tool_executor(
            Arc::new(StaticAuthProvider),
            Arc::new(connector),
            Arc::clone(&executor) as Arc<dyn InternalToolExecutor>,
        ),
    );

    let seed = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(seed.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(
            r#"{"type":"response.completed","response":{"id":"response-1","output":[{"id":"msg-seed","type":"message","role":"assistant","content":[{"type":"output_text","text":"seed answer"}]}]}}"#,
        )
        .await;
    let _ = to_bytes(seed.into_body(), usize::MAX)
        .await
        .expect("seed body");
    let websocket = connector_observer
        .recorded_websockets()
        .await
        .into_iter()
        .next()
        .expect("seed connection observer");
    let active = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"active internal tool",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    let body_task = tokio::spawn(async move {
        to_bytes(active.into_body(), usize::MAX)
            .await
            .expect("active body")
    });
    timeout(Duration::from_secs(1), executor.started.notified())
        .await
        .expect("internal tool execution should start");
    server
        .send_text_burst(&[
            r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#,
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-2","name":"threadline_echo","arguments":"{\"value\":\"beta\"}"}}"#,
        ])
        .await;
    timeout(Duration::from_secs(1), async {
        loop {
            let Some(upstream) = websocket.upgrade() else {
                panic!("retained upstream dropped before recording liveness timeout");
            };
            if matches!(
                upstream.terminal_state(),
                threadline::ws_pump::UpstreamTerminalState::LivenessTimeout(_)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("watchdog should record liveness timeout while the tool is paused");

    executor.resume.notify_one();
    let body = timeout(Duration::from_secs(1), body_task)
        .await
        .expect("liveness failure body timeout")
        .expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("failure utf8");
    assert_eq!(body_text.matches("event: response.failed").count(), 1);
    assert_eq!(body_text.matches("data: [DONE]").count(), 1);
    assert!(body_text.contains("upstream_liveness_timeout"));
    assert!(!body_text.contains("event: response.completed"));
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(server.take_pending_client_messages().await.is_empty());

    for marker in ["response-1", "response-intermediate"] {
        let stale = post_responses(
            app.clone(),
            json!({
                "model":"gpt-5.4",
                "input":"retry marker",
                "previous_response_id":marker
            }),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::BAD_REQUEST, "marker {marker}");
        let stale_body = to_bytes(stale.into_body(), usize::MAX)
            .await
            .expect("stale marker body");
        let stale_payload: Value = serde_json::from_slice(&stale_body).expect("stale marker json");
        assert_eq!(
            stale_payload["error"]["code"], "previous_response_not_found",
            "marker {marker}"
        );
    }

    let replacement = post_responses(app, json!({"model":"gpt-5.4","input":"new session"})).await;
    assert_eq!(replacement.status(), StatusCode::OK);
    let _ = replacement_server
        .recv_client_message()
        .await
        .expect("released retained entry should admit a new connection");
    drop(replacement);

    timeout(Duration::from_secs(1), async {
        loop {
            if websocket.upgrade().is_none() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("explicit liveness finalization should release the prior live upstream");
}

#[tokio::test]
async fn liveness_expiry_while_failing_internal_tool_waits_prioritizes_timeout() {
    let server =
        Arc::new(ScriptedWebSocketServer::start_after_first_client_message_without_reader().await);
    let connector = RecordingConnector::with_watchdog_policy(
        vec![PlannedConnection {
            server: Arc::clone(&server),
            turn_state: None,
        }],
        UpstreamWatchdogPolicy::new(Duration::from_millis(100), Duration::from_secs(1))
            .expect("valid watchdog policy"),
    );
    let connector_observer = connector.clone();
    let executor = Arc::new(PausedFailingInternalToolExecutor::new());
    let app = build_test_router_with_internal_tool_executor(
        Arc::new(connector),
        Arc::clone(&executor) as Arc<dyn InternalToolExecutor>,
    );

    let response = post_responses(
        app,
        json!({"model":"gpt-5.4","input":"failing internal tool"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("initial request");
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    let body_task = tokio::spawn(async move {
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body")
    });
    timeout(Duration::from_secs(1), executor.started.notified())
        .await
        .expect("internal tool execution should start");

    timeout(Duration::from_secs(1), async {
        loop {
            let Some(upstream) = connector_observer
                .recorded_websockets()
                .await
                .into_iter()
                .next()
                .and_then(|websocket| websocket.upgrade())
            else {
                panic!("upstream dropped before recording liveness timeout");
            };
            if matches!(
                upstream.terminal_state(),
                threadline::ws_pump::UpstreamTerminalState::LivenessTimeout(_)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("watchdog should record liveness timeout while the tool is paused");

    executor.resume.notify_one();
    let body = timeout(Duration::from_secs(1), body_task)
        .await
        .expect("liveness failure body timeout")
        .expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("failure utf8");
    assert_eq!(body_text.matches("event: response.failed").count(), 1);
    assert_eq!(body_text.matches("data: [DONE]").count(), 1);
    assert!(body_text.contains("upstream_liveness_timeout"));
    assert!(!body_text.contains("internal_tool_failed"));
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(server.take_pending_client_messages().await.is_empty());
}

#[tokio::test]
async fn liveness_timeout_after_internal_tool_followup_starts_invalidates_retained_aliases() {
    let server =
        Arc::new(ScriptedWebSocketServer::start_after_first_client_message_without_reader().await);
    let connector = RecordingConnector::with_watchdog_policy(
        vec![PlannedConnection {
            server: Arc::clone(&server),
            turn_state: None,
        }],
        UpstreamWatchdogPolicy::new(Duration::from_millis(500), Duration::from_secs(1))
            .expect("valid watchdog policy"),
    );
    let connector_observer = connector.clone();
    let app = build_test_router(Arc::new(connector));

    let seed = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(seed.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(
            r#"{"type":"response.completed","response":{"id":"response-1","output":[{"id":"msg-seed","type":"message","role":"assistant","content":[{"type":"output_text","text":"seed answer"}]}]}}"#,
        )
        .await;
    let _ = to_bytes(seed.into_body(), usize::MAX)
        .await
        .expect("seed body");
    let websocket = connector_observer
        .recorded_websockets()
        .await
        .into_iter()
        .next()
        .expect("seed connection observer");
    let observed_upstream = websocket
        .upgrade()
        .expect("seed websocket should remain retained before the active turn");

    let active = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"active internal tool",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    let body_task = tokio::spawn(async move {
        to_bytes(active.into_body(), usize::MAX)
            .await
            .expect("active body")
    });
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                observed_upstream.terminal_state(),
                threadline::ws_pump::UpstreamTerminalState::LivenessTimeout(_)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("started follow-up should encounter the watchdog timeout");

    let body = timeout(Duration::from_secs(1), body_task)
        .await
        .expect("liveness failure body timeout")
        .expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("failure utf8");
    assert_eq!(body_text.matches("event: response.failed").count(), 1);
    assert_eq!(body_text.matches("data: [DONE]").count(), 1);
    assert!(body_text.contains("upstream_liveness_timeout"));
    assert!(!body_text.contains("event: response.completed"));

    for marker in ["response-1", "response-intermediate"] {
        let stale = post_responses(
            app.clone(),
            json!({
                "model":"gpt-5.4",
                "input":"retry marker",
                "previous_response_id":marker
            }),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::BAD_REQUEST, "marker {marker}");
        let stale_body = to_bytes(stale.into_body(), usize::MAX)
            .await
            .expect("stale marker body");
        let stale_payload: Value = serde_json::from_slice(&stale_body).expect("stale marker json");
        assert_eq!(
            stale_payload["error"]["code"], "previous_response_not_found",
            "marker {marker}"
        );
    }
    assert_eq!(connector_observer.recorded_websockets().await.len(), 1);
    drop(observed_upstream);
    timeout(Duration::from_secs(1), async {
        loop {
            if websocket.upgrade().is_none() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal invalidation should release the live upstream");
}

#[tokio::test]
async fn liveness_timeout_after_completed_internal_tool_followup_keeps_prior_aliases() {
    let server = Arc::new(ScriptedWebSocketServer::start_with_stoppable_reader().await);
    let connector = RecordingConnector::with_watchdog_policy(
        vec![PlannedConnection {
            server: Arc::clone(&server),
            turn_state: None,
        }],
        short_watchdog_policy(),
    );
    let connector_observer = connector.clone();
    let app = build_test_router(Arc::new(connector));

    let seed = post_responses(app.clone(), json!({"model":"gpt-5.4","input":"seed"})).await;
    assert_eq!(seed.status(), StatusCode::OK);
    let _ = server.recv_client_message().await.expect("seed request");
    server
        .send_text(
            r#"{"type":"response.completed","response":{"id":"response-accepted","output":[{"id":"msg-seed","type":"message","role":"assistant","content":[{"type":"output_text","text":"seed answer"}]}]}}"#,
        )
        .await;
    let _ = to_bytes(seed.into_body(), usize::MAX)
        .await
        .expect("seed body");
    let websocket = connector_observer
        .recorded_websockets()
        .await
        .into_iter()
        .next()
        .expect("seed connection observer");

    let active = post_responses(
        app.clone(),
        json!({
            "model":"gpt-5.4",
            "input":"active internal tool",
            "previous_response_id":"response-accepted"
        }),
    )
    .await;
    assert_eq!(active.status(), StatusCode::OK);
    let body_task = tokio::spawn(async move {
        to_bytes(active.into_body(), usize::MAX)
            .await
            .expect("active body")
    });
    server
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#)
        .await;

    let followup: Value = serde_json::from_str(&message_text(
        timeout(Duration::from_secs(1), server.recv_client_message())
            .await
            .expect("followup request should arrive")
            .expect("followup request"),
    ))
    .expect("followup request json");
    assert_eq!(followup["type"], "response.create");
    assert_eq!(followup["previous_response_id"], "response-accepted");

    server
        .send_text(r#"{"type":"response.created","response":{"id":"response-failed"}}"#)
        .await;
    server.stop_reader();

    let body = timeout(Duration::from_secs(1), body_task)
        .await
        .expect("liveness failure body timeout")
        .expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("failure utf8");
    assert_eq!(body_text.matches("event: response.created").count(), 1);
    assert_eq!(body_text.matches("event: response.failed").count(), 1);
    assert_eq!(body_text.matches("data: [DONE]").count(), 1);
    assert!(body_text.contains("upstream_liveness_timeout"));
    assert!(!body_text.contains("event: response.completed"));
    assert!(!body_text.contains("response-intermediate"));

    timeout(Duration::from_secs(1), async {
        loop {
            let Some(upstream) = websocket.upgrade() else {
                return;
            };
            if matches!(
                upstream.terminal_state(),
                threadline::ws_pump::UpstreamTerminalState::LivenessTimeout(_)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("same upstream should record a liveness timeout");
    assert_eq!(connector_observer.recorded_websockets().await.len(), 1);

    for marker in [
        "response-accepted",
        "response-intermediate",
        "response-failed",
    ] {
        let stale = post_responses(
            app.clone(),
            json!({
                "model":"gpt-5.4",
                "input":"retry marker",
                "previous_response_id":marker
            }),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::BAD_REQUEST, "marker {marker}");
        let stale_body = to_bytes(stale.into_body(), usize::MAX)
            .await
            .expect("stale marker body");
        let stale_payload: Value = serde_json::from_slice(&stale_body).expect("stale marker json");
        assert_eq!(
            stale_payload["error"]["code"], "previous_response_not_found",
            "marker {marker}"
        );
    }
    assert_eq!(connector_observer.recorded_websockets().await.len(), 1);

    timeout(Duration::from_secs(1), async {
        loop {
            if websocket.upgrade().is_none() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recoverable finalization should release live upstream ownership");
}
