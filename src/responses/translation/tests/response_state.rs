use super::lifecycle::{armed_followup_lease, followup_stream_state};
use super::*;

use crate::responses::translation::{progression, receive, recovery};
use crate::ws_pump::{ResponseState, UpstreamInboundLimits, UpstreamWatchdogPolicy};
use futures_util::SinkExt;
use tokio::io::{DuplexStream, duplex};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{Message, protocol::Role};

async fn diagnostic_stream_pair() -> (
    Arc<LiveUpstreamWebSocket>,
    ResponseStreamState,
    WebSocketStream<DuplexStream>,
) {
    let (client_io, server_io) = duplex(8192);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let upstream = Arc::new(LiveUpstreamWebSocket::from_stream_with_close_diagnostics(
        client,
        UpstreamWatchdogPolicy::DEFAULT,
        UpstreamInboundLimits::DEFAULT,
        true,
    ));
    let registry = Arc::new(RetainedSessionRegistry::new(1));
    let lease = armed_followup_lease(&registry, Arc::clone(&upstream)).await;
    let state = followup_stream_state(Arc::clone(&upstream), lease);
    (upstream, state, server)
}

async fn parse_received_event(state: &mut ResponseStreamState) -> serde_json::Value {
    let text = match receive::receive_upstream_text(state).await {
        Ok(Some(text)) => text,
        _ => panic!("expected upstream event"),
    };
    match progression::parse_upstream_event(state, &text).await {
        Ok(parsed) => parsed,
        Err(_) => panic!("expected valid event"),
    }
}

async fn send_server_event(server: &mut WebSocketStream<DuplexStream>, event: serde_json::Value) {
    server.send(Message::Text(event.to_string())).await.unwrap();
}

fn assert_diagnostic_state(upstream: &LiveUpstreamWebSocket, expected: ResponseState) {
    assert_eq!(upstream.diagnostic_response_state(), Some(expected));
}

async fn receive_unclassified_event(
    upstream: &LiveUpstreamWebSocket,
    server: &mut WebSocketStream<DuplexStream>,
    text: &str,
) -> String {
    server.send(Message::Text(text.to_owned())).await.unwrap();
    let raw = upstream.recv_text().await.unwrap().unwrap();
    assert_diagnostic_state(upstream, ResponseState::Unknown);
    raw
}

async fn assert_parser_classification(
    text: &str,
    expected: ResponseState,
    response_already_started: bool,
) {
    let (upstream, mut state, mut server) = diagnostic_stream_pair().await;
    if response_already_started {
        let raw =
            receive_unclassified_event(&upstream, &mut server, r#"{"type":"response.created"}"#)
                .await;
        assert!(
            progression::parse_upstream_event(&mut state, &raw)
                .await
                .is_ok()
        );
        assert_diagnostic_state(&upstream, ResponseState::InProgress);
    } else {
        assert_diagnostic_state(&upstream, ResponseState::NotStarted);
    }
    let raw = receive_unclassified_event(&upstream, &mut server, text).await;
    let _parsed = progression::parse_upstream_event(&mut state, &raw).await;
    assert_diagnostic_state(&upstream, expected);
}

async fn send_gated_diagnostic_create(
    upstream: &LiveUpstreamWebSocket,
    request: &serde_json::Map<String, serde_json::Value>,
    server: &mut WebSocketStream<DuplexStream>,
    started: &tokio::sync::Notify,
    open: &std::sync::atomic::AtomicBool,
    waker: &futures_util::task::AtomicWaker,
) {
    let writing = started.notified();
    tokio::pin!(writing);
    writing.as_mut().enable();
    assert_diagnostic_state(upstream, ResponseState::NotStarted);
    crate::responses::upstream::send_response_create(upstream, request)
        .await
        .unwrap();
    writing.await;
    assert_diagnostic_state(upstream, ResponseState::CreatePending);
    open.store(true, std::sync::atomic::Ordering::SeqCst);
    waker.wake();
    assert!(matches!(
        server.next().await.unwrap().unwrap(),
        Message::Text(_)
    ));
    assert_diagnostic_state(upstream, ResponseState::CreateSent);
}

#[tokio::test]
async fn close_diagnostics_normal_parser_classifies_only_fixed_event_types() {
    for (text, expected) in [
        (r#"{"type":"response.created"}"#, ResponseState::InProgress),
        (
            r#"{"type":"response.in_progress"}"#,
            ResponseState::InProgress,
        ),
        (r#"{"type":"response.completed"}"#, ResponseState::Completed),
        (r#"{"type":"response.failed"}"#, ResponseState::Failed),
        (
            r#"{"type":"response.incomplete"}"#,
            ResponseState::Incomplete,
        ),
        (
            r#"{"type":"response.output_text.delta"}"#,
            ResponseState::InProgress,
        ),
        (
            r#"{"type":"response.output_text.done"}"#,
            ResponseState::InProgress,
        ),
        (
            r#"{"type":"response.output_item.added"}"#,
            ResponseState::InProgress,
        ),
        (
            r#"{"type":"response.output_item.done"}"#,
            ResponseState::InProgress,
        ),
        (
            r#"{"type":"response.content_part.added"}"#,
            ResponseState::InProgress,
        ),
        (
            r#"{"type":"response.content_part.done"}"#,
            ResponseState::InProgress,
        ),
        (
            r#"{"type":"response.function_call_arguments.delta"}"#,
            ResponseState::InProgress,
        ),
        (
            r#"{"type":"response.function_call_arguments.done"}"#,
            ResponseState::InProgress,
        ),
        (r#"{"type":"error"}"#, ResponseState::Unknown),
        (
            r#"{"type":"Authorization-secret","id":"response-secret"}"#,
            ResponseState::Unknown,
        ),
        (r#"{"type":42}"#, ResponseState::Unknown),
        ("{}", ResponseState::Unknown),
        ("[]", ResponseState::Unknown),
        ("null", ResponseState::Unknown),
        ("invalid-json-secret", ResponseState::Unknown),
        ("[DONE]", ResponseState::Unknown),
    ] {
        assert_parser_classification(text, expected, true).await;
    }
    assert_parser_classification(
        r#"{"type":"response.output_text.delta"}"#,
        ResponseState::NotStarted,
        false,
    )
    .await;
}

#[tokio::test]
async fn close_diagnostics_unclassified_data_survives_dequeue_and_control_traffic() {
    for binary in [false, true] {
        for classified in [false, true] {
            let (upstream, mut state, mut server) = diagnostic_stream_pair().await;
            let completed = r#"{"type":"response.completed"}"#;
            server
                .send(if binary {
                    Message::Binary(completed.as_bytes().to_vec())
                } else {
                    Message::Text(completed.into())
                })
                .await
                .unwrap();
            let raw = upstream.recv_text().await.unwrap().unwrap();
            assert_diagnostic_state(&upstream, ResponseState::Unknown);
            server
                .send(Message::Pong(b"nonce-secret".to_vec()))
                .await
                .unwrap();
            server
                .send(Message::Ping(b"ping-secret".to_vec()))
                .await
                .unwrap();
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Pong(b"ping-secret".to_vec())
            );
            assert_diagnostic_state(&upstream, ResponseState::Unknown);
            if classified {
                assert!(
                    progression::parse_upstream_event(&mut state, &raw)
                        .await
                        .is_ok()
                );
                assert_diagnostic_state(&upstream, ResponseState::Completed);
            }
            drop(server);
            assert_eq!(upstream.recv_text().await.unwrap(), None);
            let (calls, output) = upstream.diagnostic_output();
            assert_eq!(calls, 1);
            assert!(output.contains("source=read_error"));
            assert!(output.contains(if classified {
                "response_state=completed"
            } else {
                "response_state=unknown"
            }));
            assert!(!output.contains("secret"));
        }
    }
}

#[tokio::test]
async fn close_diagnostics_prelude_classifies_fifo_once_and_keeps_discard_unknown() {
    for failure in [false, true] {
        let (upstream, mut state, mut server) = diagnostic_stream_pair().await;
        state.headers_committed = false;
        state.replay_prohibited = false;
        state.replay_stale_marker_on_pre_first_event_close = true;
        for event_type in ["response.created", "response.in_progress"] {
            send_server_event(&mut server, json!({"type": event_type})).await;
        }
        let terminal_event = if failure {
            json!({"type":"error","error":{"code":"previous_response_not_found"}})
        } else {
            json!({"type":"response.completed","response":{"id":"resp_final"}})
        };
        send_server_event(&mut server, terminal_event).await;
        let result = recovery::preflight(&mut state).await;
        assert_diagnostic_state(&upstream, ResponseState::Unknown);
        if failure {
            assert!(matches!(
                result,
                Err(ThreadlineError::PreviousResponseNotFound)
            ));
            assert!(state.pending_upstream_events.is_empty());
            assert!(state.upstream.is_none());
        } else {
            assert!(result.is_ok());
            assert_eq!(state.pending_upstream_events.len(), 3);
            for event_type in [
                "response.created",
                "response.in_progress",
                "response.completed",
            ] {
                let parsed = parse_received_event(&mut state).await;
                assert_eq!(parsed["type"], event_type);
                assert_diagnostic_state(
                    &upstream,
                    if event_type == "response.completed" {
                        ResponseState::Completed
                    } else {
                        ResponseState::Unknown
                    },
                );
            }
        }
    }
}

struct AwaitedDiagnosticTool {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl crate::responses::InternalToolExecutor for AwaitedDiagnosticTool {
    fn execute(
        &self,
        call: crate::tools::InternalToolCall,
    ) -> BoxFuture<'static, Result<crate::tools::PendingInternalToolOutput, ThreadlineError>> {
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            call.execute()
        })
    }
}

#[tokio::test]
async fn close_diagnostics_internal_tool_await_and_followup_track_each_response() {
    let (pump, mut server, started, open, waker) =
        LiveUpstreamWebSocket::test_diagnostic_write_gate().await;
    let upstream = Arc::new(pump);
    let registry = Arc::new(RetainedSessionRegistry::new(1));
    let lease = armed_followup_lease(&registry, Arc::clone(&upstream)).await;
    let mut state = followup_stream_state(Arc::clone(&upstream), lease);
    let tool_started = Arc::new(tokio::sync::Notify::new());
    let tool_release = Arc::new(tokio::sync::Notify::new());
    state.services = ThreadlineServices::with_internal_tool_executor(
        Arc::new(UnusedAuthProvider),
        Arc::new(UnusedConnector),
        Arc::new(AwaitedDiagnosticTool {
            started: Arc::clone(&tool_started),
            release: Arc::clone(&tool_release),
        }),
    );
    send_gated_diagnostic_create(
        &upstream,
        &state.base_request,
        &mut server,
        &started,
        &open,
        &waker,
    )
    .await;
    server
        .send(Message::Text(
            json!({"type":"response.created"}).to_string(),
        ))
        .await
        .unwrap();
    let created = parse_received_event(&mut state).await;
    progression::process_upstream_event(&mut state, created).await;
    assert_diagnostic_state(&upstream, ResponseState::InProgress);
    assert_diagnostic_tool_await(
        &upstream,
        &mut state,
        &mut server,
        &tool_started,
        &tool_release,
    )
    .await;
    let completed = parse_received_event(&mut state).await;
    assert_eq!(completed["type"], "response.completed");
    assert_diagnostic_state(&upstream, ResponseState::Completed);
    complete_diagnostic_tool_followup(
        &registry,
        &mut state,
        &mut server,
        completed,
        &started,
        &open,
        &waker,
    )
    .await;
    drop(state);
    assert_diagnostic_state(&upstream, ResponseState::InProgress);
}

async fn complete_diagnostic_tool_followup(
    registry: &Arc<RetainedSessionRegistry>,
    state: &mut ResponseStreamState,
    server: &mut WebSocketStream<DuplexStream>,
    completed: serde_json::Value,
    started: &tokio::sync::Notify,
    open: &std::sync::atomic::AtomicBool,
    waker: &futures_util::task::AtomicWaker,
) {
    use std::sync::atomic::Ordering;
    let upstream = Arc::clone(state.upstream.as_ref().expect("active upstream"));
    open.store(false, Ordering::SeqCst);
    let writing = started.notified();
    tokio::pin!(writing);
    writing.as_mut().enable();
    progression::process_upstream_event(state, completed).await;
    writing.await;
    assert_diagnostic_state(&upstream, ResponseState::CreatePending);
    open.store(true, Ordering::SeqCst);
    waker.wake();
    let Message::Text(followup) = server.next().await.unwrap().unwrap() else {
        panic!("follow-up Text");
    };
    let followup: serde_json::Value = serde_json::from_str(&followup).unwrap();
    assert_eq!(followup["previous_response_id"], "resp_middle");
    assert_eq!(followup["input"][0]["type"], "function_call_output");
    assert_diagnostic_state(&upstream, ResponseState::CreateSent);
    send_server_event(server, json!({"type":"response.completed","response":{"id":"resp_final","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}})).await;
    let final_event = parse_received_event(state).await;
    progression::process_upstream_event(state, final_event).await;
    assert_diagnostic_state(&upstream, ResponseState::Completed);
    state.lease.release();
    let mut next_lease = registry.acquire_previous("resp_final").await.unwrap();
    next_lease.arm_active_turn();
    let mut next_body = followup_stream_state(Arc::clone(&upstream), next_lease);
    assert_diagnostic_state(&upstream, ResponseState::Completed);
    crate::responses::upstream::send_response_create(&upstream, &next_body.base_request)
        .await
        .unwrap();
    assert!(matches!(
        server.next().await.unwrap().unwrap(),
        Message::Text(_)
    ));
    assert_diagnostic_state(&upstream, ResponseState::CreateSent);
    server
        .send(Message::Text(
            json!({"type":"response.created"}).to_string(),
        ))
        .await
        .unwrap();
    parse_received_event(&mut next_body).await;
    assert_diagnostic_state(&upstream, ResponseState::InProgress);
}

async fn assert_diagnostic_tool_await(
    upstream: &LiveUpstreamWebSocket,
    state: &mut ResponseStreamState,
    server: &mut WebSocketStream<DuplexStream>,
    started: &tokio::sync::Notify,
    release: &tokio::sync::Notify,
) {
    send_server_event(server, json!({"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_test","name":"threadline_echo","arguments":json!({"value":"done"}).to_string()}})).await;
    let tool = parse_received_event(state).await;
    let execution = progression::process_upstream_event(state, tool);
    tokio::pin!(execution);
    assert!(futures_util::poll!(&mut execution).is_pending());
    started.notified().await;
    assert_diagnostic_state(upstream, ResponseState::InProgress);
    server
        .send(Message::Text(
            json!({"type":"response.completed","response":{"id":"resp_middle"}}).to_string(),
        ))
        .await
        .unwrap();
    server
        .send(Message::Ping(b"completion-barrier".to_vec()))
        .await
        .unwrap();
    assert_eq!(
        server.next().await.unwrap().unwrap(),
        Message::Pong(b"completion-barrier".to_vec())
    );
    assert_eq!(
        upstream.diagnostic_response_state(),
        Some(ResponseState::Unknown)
    );
    release.notify_one();
    execution.await;
}

#[tokio::test]
async fn close_diagnostics_early_response_cannot_be_rewound_by_old_write() {
    for (overlap, completed) in [(false, false), (false, true), (true, true)] {
        assert_early_response_case(overlap, completed).await;
    }
}

async fn assert_early_response_case(overlap: bool, completed: bool) {
    use std::sync::atomic::Ordering;
    let (pump, mut server, started, open, waker) =
        LiveUpstreamWebSocket::test_diagnostic_write_gate().await;
    let upstream = Arc::new(pump);
    let registry = Arc::new(RetainedSessionRegistry::new(1));
    let lease = armed_followup_lease(&registry, Arc::clone(&upstream)).await;
    let mut state = followup_stream_state(Arc::clone(&upstream), lease);
    let writing = started.notified();
    tokio::pin!(writing);
    writing.as_mut().enable();
    {
        let create =
            crate::responses::upstream::send_response_create(&upstream, &state.base_request);
        tokio::pin!(create);
        assert_diagnostic_state(&upstream, ResponseState::NotStarted);
        assert!(matches!(
            futures_util::poll!(&mut create),
            std::task::Poll::Ready(Ok(()))
        ));
    }
    writing.await;
    for event_type in ["response.created", "response.completed"]
        .into_iter()
        .take(if completed { 2 } else { 1 })
    {
        send_server_event(&mut server, json!({"type":event_type})).await;
        parse_received_event(&mut state).await;
        assert_diagnostic_state(
            &upstream,
            if event_type == "response.created" {
                ResponseState::InProgress
            } else {
                ResponseState::Completed
            },
        );
    }
    if overlap {
        crate::responses::upstream::send_response_create(&upstream, &state.base_request)
            .await
            .unwrap();
        assert_diagnostic_state(&upstream, ResponseState::Unknown);
    }
    open.store(true, Ordering::SeqCst);
    waker.wake();
    for _ in 0..if overlap { 2 } else { 1 } {
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));
    }
    assert_diagnostic_state(
        &upstream,
        if overlap {
            ResponseState::Unknown
        } else if completed {
            ResponseState::Completed
        } else {
            ResponseState::InProgress
        },
    );
}
#[tokio::test]
async fn close_diagnostics_next_create_and_generic_send_keep_ambiguity_sticky() {
    for cause in ["unclassified", "unfinished", "generic"] {
        let (upstream, mut state, mut server) = diagnostic_stream_pair().await;
        send_server_event(
            &mut server,
            json!({"type": if cause == "unfinished" { "response.created" } else { "response.completed" }}),
        )
        .await;
        let raw = upstream.recv_text().await.unwrap().unwrap();
        if cause != "unclassified" {
            assert!(
                progression::parse_upstream_event(&mut state, &raw)
                    .await
                    .is_ok()
            );
        }
        if cause == "generic" {
            upstream.send_text("generic-send-secret").await.unwrap();
        } else {
            crate::responses::upstream::send_response_create(&upstream, &state.base_request)
                .await
                .unwrap();
        }
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));
        if cause == "unclassified" {
            assert!(
                progression::parse_upstream_event(&mut state, &raw)
                    .await
                    .is_ok()
            );
        }
        send_server_event(&mut server, json!({"type":"response.completed"})).await;
        parse_received_event(&mut state).await;
        assert_diagnostic_state(&upstream, ResponseState::Unknown);
    }
}

#[tokio::test]
async fn close_diagnostics_two_pumps_do_not_share_response_observations() {
    let (first, mut first_state, mut first_server) = diagnostic_stream_pair().await;
    let (second, mut second_state, mut second_server) = diagnostic_stream_pair().await;
    send_server_event(&mut first_server, json!({"type":"response.completed"})).await;
    send_server_event(&mut second_server, json!({"type":"response.created"})).await;
    parse_received_event(&mut first_state).await;
    parse_received_event(&mut second_state).await;
    assert_eq!(
        first.diagnostic_response_state(),
        Some(ResponseState::Completed)
    );
    assert_eq!(
        second.diagnostic_response_state(),
        Some(ResponseState::InProgress)
    );
    drop(first_server);
    assert_eq!(first.recv_text().await.unwrap(), None);
    assert_eq!(first.diagnostic_output().0, 1);
    assert_eq!(
        second.diagnostic_response_state(),
        Some(ResponseState::InProgress)
    );
    assert!(matches!(
        second.terminal_state(),
        UpstreamTerminalState::Open
    ));
    assert_eq!(second.diagnostic_output().0, 0);
}
