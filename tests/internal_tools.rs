use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
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
use threadline::jobs::{ThreadlineJobManager, ThreadlineJobManagerConfig};
use threadline::responses::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};
use threadline::tools::InternalToolCall;
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
            "model": "ignored",
            "input": "run internal tool loop",
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

    let tools = first_request["response"]["tools"]
        .as_array()
        .expect("tools array");
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
    assert_eq!(
        followup_request["response"]["previous_response_id"],
        "response-intermediate"
    );

    let followup_input = followup_request["response"]["input"]
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
    assert!(body_text.contains("event: response.output_text.delta"));
    assert!(body_text.contains("final answer"));
    assert!(body_text.contains("response-final"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(!body_text.contains("response-intermediate"));
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
            "model": "ignored",
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

    server
        .send_text(r#"{"type":"response.output_text.delta","delta":"final answer"}"#)
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-final"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    assert!(!body_text.contains("event: response.output_item.added"));
    assert!(!body_text.contains("threadline_echo"));
    assert!(body_text.contains("final answer"));
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
            "model": "ignored",
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
        .send_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-visible","name":"downstream_tool","arguments":"{}"}}"#,
        )
        .await;
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"response-visible"}}"#)
        .await;

    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    assert!(body_text.contains("event: response.output_item.done"));
    assert!(body_text.contains("downstream_tool"));
    assert!(body_text.contains("response-visible"));
    assert!(server.take_pending_client_messages().await.is_empty());
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
