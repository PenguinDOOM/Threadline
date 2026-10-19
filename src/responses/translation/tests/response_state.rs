use super::lifecycle::{armed_followup_lease, followup_stream_state};
use super::*;

use crate::responses::translation::{progression, receive, recovery};
use crate::ws_pump::{
    FiniteTrace, finite_drive, finite_read_barrier, finite_send, finite_warning_capture,
    synthetic_completed, synthetic_created, synthetic_delta,
};
use crate::ws_pump::{ResponseState, UpstreamInboundLimits, UpstreamWatchdogPolicy};
use futures_util::SinkExt;
use tokio::io::{DuplexStream, duplex};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{Message, protocol::Role};

#[path = "response_state_overflow.rs"]
mod overflow;

async fn measurement_stream_pair(
    trace: &FiniteTrace,
    aggregate: bool,
) -> (
    Arc<LiveUpstreamWebSocket>,
    ResponseStreamState,
    WebSocketStream<DuplexStream>,
) {
    let (client_io, server_io) = duplex(8192);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let upstream = Arc::new(LiveUpstreamWebSocket::test_measurement_stream(
        client,
        aggregate,
        trace.clone(),
        UpstreamInboundLimits::DEFAULT,
    ));
    let registry = Arc::new(RetainedSessionRegistry::new(1));
    let lease = armed_followup_lease(&registry, Arc::clone(&upstream)).await;
    let state = followup_stream_state(Arc::clone(&upstream), lease);
    (upstream, state, server)
}

async fn stop_measurement_pump(upstream: Arc<LiveUpstreamWebSocket>, trace: &FiniteTrace) {
    let mut upstream = Arc::try_unwrap(upstream)
        .ok()
        .expect("stream and registry released upstream ownership");
    upstream.test_stop_and_join().await;
    upstream.test_drain_after_terminal(trace).await;
    trace.event("resource_released", None, None, None, None);
}

async fn measurement_attempt<F: std::future::Future>(
    trace: &FiniteTrace,
    future: F,
) -> Result<F::Output, u64> {
    use futures_util::FutureExt;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        std::panic::AssertUnwindSafe(future).catch_unwind(),
    )
    .await;
    match result {
        Ok(Ok(output)) => Ok(output),
        result => {
            let reason = if result.is_err() { 5 } else { 6 };
            trace.invalidate(reason);
            Err(reason)
        }
    }
}

#[tokio::test]
async fn response_stream_measurement_classification_keeps_only_fixed_categories() {
    let trace = FiniteTrace::configured("privacy_categories", 1, 0, 4, false, false, true);
    let (upstream, mut state, mut server) = measurement_stream_pair(&trace, true).await;
    for event in [
        json!({"type":"response.created","prompt":"prompt-secret"}),
        json!({"type":"response.output_text.delta","delta":"file-secret"}),
        json!({"type":"response.function_call_arguments.delta","delta":"arguments-secret"}),
        json!({"type":"Authorization-secret","id":"real-id-secret"}),
    ] {
        send_server_event(&mut server, event).await;
        let _ = parse_received_event(&mut state).await;
    }
    trace.assert_measurement_records(Some([1, 1, 1, 1]));
    drop(state);
    drop(server);
    stop_measurement_pump(upstream, &trace).await;
    trace.assert_valid();
}

async fn legal_series_measurement(
    name: &'static str,
    aggregate: bool,
    detail: bool,
    kind: crate::ws_pump::SyntheticSeriesKind,
    trace: Option<FiniteTrace>,
) {
    let series = crate::ws_pump::measurement_series(kind, 16, 199);
    let trace = trace.unwrap_or_else(|| {
        FiniteTrace::configured(name, 1, 0, series.events.len(), false, false, detail)
    });
    trace.set_series(series.delta_start, 16);
    let (upstream, state, mut server) = measurement_stream_pair(&trace, aggregate).await;
    let mut body = Box::pin(response_stream(state));
    let result = measurement_attempt(&trace, async {
        let start = std::time::Instant::now();
        let mut reconstructed = String::new();
        for (sequence, event) in series.events.iter().enumerate() {
            finite_send(&mut server, &trace, sequence, event.clone()).await;
            let chunk = body.next().await.unwrap().unwrap();
            let event = finite_sse_value(&chunk);
            assert_eq!(event["sequence_number"], sequence);
            if let Some(fragment) = event["delta"].as_str() {
                reconstructed.push_str(fragment);
            }
            if sequence == series.events.len() - 1 {
                assert_eq!(event["type"], "response.completed");
                match kind {
                    crate::ws_pump::SyntheticSeriesKind::Text => assert_eq!(
                        event["response"]["output"][0]["content"][0]["text"],
                        series.semantic_text
                    ),
                    crate::ws_pump::SyntheticSeriesKind::Arguments => {
                        let arguments: serde_json::Value = serde_json::from_str(
                            event["response"]["output"][0]["arguments"]
                                .as_str()
                                .unwrap(),
                        )
                        .unwrap();
                        assert_eq!(arguments["s"], series.semantic_text);
                    }
                }
            }
        }
        assert_eq!(body.next().await.unwrap().unwrap(), "data: [DONE]\n\n");
        assert!(body.next().await.is_none());
        let elapsed_ns = start.elapsed().as_nanos();
        match kind {
            crate::ws_pump::SyntheticSeriesKind::Text => {
                assert_eq!(reconstructed, series.semantic_text)
            }
            crate::ws_pump::SyntheticSeriesKind::Arguments => assert_eq!(
                serde_json::from_str::<serde_json::Value>(&reconstructed).unwrap()["s"],
                series.semantic_text
            ),
        }
        assert_eq!(upstream.test_queue_sample().0, 0);
        assert_eq!(upstream.diagnostic_response_state().is_some(), aggregate);
        let activity = aggregate.then(|| upstream.diagnostic_activity());
        if let Some(activity) = &activity {
            assert!(activity.contains(&format!("enqueued_messages={}", series.events.len())));
            assert!(activity.contains(&format!("dequeued_messages={}", series.events.len())));
        }
        (elapsed_ns, activity)
    })
    .await;
    drop(body);
    drop(server);
    stop_measurement_pump(upstream, &trace).await;
    let invalid_reason = trace.invalid_reason().or(result.as_ref().err().copied());
    let elapsed_ns = result.as_ref().ok().map(|value| value.0);
    let activity = result.as_ref().ok().and_then(|value| value.1.clone());
    trace.write_scenario(
        name,
        &json!({"generator":series.metadata,"runtime_flavor":"current_thread","workers":1,
        "aggregate":aggregate,"detail":detail,"limits":{"messages":256,"bytes":16777216},"elapsed_ns":elapsed_ns,
        "valid":result.is_ok() && invalid_reason.is_none(),"invalid_reason":invalid_reason,"trace_record_limit":trace.record_budget(),"completed":result.is_ok(),"overflow":false,"delivered_sequence_inclusive":result.is_ok().then_some([0,series.events.len()-1]),"done_chunks":usize::from(result.is_ok()),
        "aggregate_snapshot":activity,"cleanup":"body and registry references released; pump abort joined; channel empty",
        "purpose":"minimal instrumentation contract, not observer-effect performance experiment"}),
    );
    trace.flush();
    trace.assert_valid();
    if result.is_ok() {
        trace.assert_measurement_records(Some(match kind {
            crate::ws_pump::SyntheticSeriesKind::Text => [16, 0, 7, 0],
            crate::ws_pump::SyntheticSeriesKind::Arguments => [0, 16, 5, 0],
        }));
    }
    assert!(result.is_ok(), "invalid measurement: {result:?}");
}

#[tokio::test]
async fn response_stream_measurement_modes_preserve_legal_series_and_fixed_schema() {
    use crate::ws_pump::{
        SyntheticSeriesKind::{Arguments, Text},
        measurement_series,
    };
    for kind in [Text, Arguments] {
        for deltas in [16, 1536] {
            for size in [199, 4096] {
                let series = measurement_series(kind, deltas, size);
                assert_eq!(
                    crate::ws_pump::measurement_encoded_bytes(&series.events) as u64,
                    series.metadata["wire_bytes"].as_u64().unwrap()
                );
                assert_eq!(
                    series.events.len(),
                    deltas + if matches!(kind, Text) { 7 } else { 5 }
                );
                assert!(
                    series.events[series.delta_start..series.delta_start + deltas]
                        .iter()
                        .all(|event| event.len() == size)
                );
                let expected = match (kind, deltas, size) {
                    (Text, 16, 199) => 4956,
                    (Text, 16, 4096) => 67308,
                    (Arguments, 16, 199) => 4505,
                    (Arguments, 16, 4096) => 66857,
                    (Text, 1536, 199) => 350004,
                    (Text, 1536, 4096) => 6335796,
                    (Arguments, 1536, 199) => 340431,
                    (Arguments, 1536, 4096) => 6326223,
                    _ => unreachable!(),
                };
                assert_eq!(series.metadata["wire_bytes"], expected);
            }
        }
    }
    for (name, aggregate, detail, kind) in [
        ("normal_text_a", false, false, Text),
        ("normal_text_b", true, false, Text),
        ("normal_text_c", true, true, Text),
        ("normal_arguments_a", false, false, Arguments),
        ("normal_arguments_b", true, false, Arguments),
        ("normal_arguments_c", true, true, Arguments),
    ] {
        legal_series_measurement(name, aggregate, detail, kind, None).await;
    }
}

#[tokio::test]
async fn response_stream_measurement_budget_exhaustion_preserves_failed_run_evidence() {
    use futures_util::FutureExt;
    let name = "budget_exceeded_text_c";
    let trace = FiniteTrace::configured(name, 1, 0, 23, false, false, true).with_record_budget(257);
    let temporary = std::env::var_os("THREADLINE_TEST_TRACE_DIR").is_none();
    let directory = std::env::var_os("THREADLINE_TEST_TRACE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let directory = std::env::temp_dir().join(format!(
                "threadline-measurement-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&directory).unwrap();
            trace.set_output_directory(&directory);
            directory
        });
    let result = std::panic::AssertUnwindSafe(legal_series_measurement(
        name,
        true,
        true,
        crate::ws_pump::SyntheticSeriesKind::Text,
        Some(trace.clone()),
    ))
    .catch_unwind()
    .await;
    assert!(result.is_err(), "trace budget excess must fail measurement");
    assert_invalid_measurement_evidence(&directory, name);
    if temporary {
        std::fs::remove_dir_all(directory).unwrap();
    }
}

fn assert_invalid_measurement_evidence(directory: &std::path::Path, name: &str) {
    let read_json = |suffix| -> serde_json::Value {
        serde_json::from_reader(
            std::fs::File::open(directory.join(format!("{name}.{suffix}"))).unwrap(),
        )
        .unwrap()
    };
    let scenario = read_json("scenario.json");
    assert_eq!(scenario["valid"], false);
    assert_eq!(scenario["invalid_reason"], 1);
    assert_eq!(scenario["completed"], true);
    assert_eq!(scenario["overflow"], false);
    assert_eq!(scenario["detail"], true);
    assert_eq!(scenario["aggregate"], true);
    assert_eq!(scenario["trace_record_limit"], 257);
    assert!(scenario["generator"].is_object());
    assert!(
        scenario["cleanup"]
            .as_str()
            .unwrap()
            .contains("pump abort joined")
    );
    let status = read_json("status.json");
    assert_eq!(status["valid"], false);
    assert_eq!(status["invalid_reason"], 1);
    assert!(status["missing_records"].as_u64().unwrap() > 0);
    let records = std::fs::read_to_string(directory.join(format!("{name}.jsonl"))).unwrap();
    let records: Vec<serde_json::Value> = records
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(!records.is_empty());
    assert_eq!(records.len() as u64, status["recorded"].as_u64().unwrap());
    let schema = read_json("schema.json");
    let cleanup = schema["event_kinds_one_based"]
        .as_array()
        .unwrap()
        .iter()
        .position(|kind| kind == "resource_released")
        .unwrap()
        + 1;
    assert!(records.iter().any(|record| record[4] == cleanup));
}

#[tokio::test]
async fn response_stream_measurement_overflow_preserves_frozen_aggregate_and_physical_drain() {
    use crate::ws_pump::{SyntheticSeriesKind::Text, measurement_series};
    let series = measurement_series(Text, 1536, 199);
    let trace = FiniteTrace::configured(
        "overflow_text_c",
        1,
        0,
        series.events.len(),
        false,
        false,
        true,
    );
    trace.set_series(series.delta_start, 1536);
    let (upstream, state, mut server) = measurement_stream_pair(&trace, true).await;
    let mut body = Box::pin(response_stream(state));
    let rejected = series.delta_start + 256;
    let result = measurement_attempt(&trace, async {
        for sequence in 0..series.delta_start {
            finite_send(
                &mut server,
                &trace,
                sequence,
                series.events[sequence].clone(),
            )
            .await;
            assert_eq!(
                finite_sse_value(&body.next().await.unwrap().unwrap())["sequence_number"],
                sequence
            );
        }
        for sequence in series.delta_start..series.delta_start + 256 {
            finite_send(
                &mut server,
                &trace,
                sequence,
                series.events[sequence].clone(),
            )
            .await;
        }
        finite_read_barrier(&mut server, &trace).await;
        assert_eq!(upstream.test_queue_sample().0, 256);
        assert_eq!(upstream.terminal_state(), UpstreamTerminalState::Open);
        finite_send(
            &mut server,
            &trace,
            rejected,
            series.events[rejected].clone(),
        )
        .await;
        while !upstream.is_closed() {
            tokio::task::yield_now().await;
        }
        let frozen = upstream.diagnostic_output();
        assert!(frozen.1.contains("cause=message_count"));
        assert!(frozen.1.contains("enqueued_messages=259"));
        assert!(frozen.1.contains("dequeued_messages=3"));
        assert_eq!(
            finite_sse_value(&body.next().await.unwrap().unwrap())["type"],
            "response.failed"
        );
        assert_eq!(body.next().await.unwrap().unwrap(), "data: [DONE]\n\n");
        assert!(body.next().await.is_none());
        assert_eq!(upstream.test_drain_after_terminal(&trace).await, 256);
        assert_eq!(upstream.diagnostic_output(), frozen);
        frozen.1
    })
    .await;
    drop(body);
    drop(server);
    stop_measurement_pump(upstream, &trace).await;
    let invalid_reason = trace.invalid_reason().or(result.as_ref().err().copied());
    trace.scenario(&json!({"generator":series.metadata,"runtime_flavor":"current_thread","workers":1,
        "aggregate":true,"detail":true,"limits":{"messages":256,"bytes":16777216},
        "supplied_sequence_inclusive":[0,rejected],"delivered_sequence_inclusive":[0,2],"rejected_sequence":rejected,
        "physical_post_terminal_drain_inclusive":[3,258],"unsupplied_sequence_inclusive":[260,1542],
        "valid":result.is_ok() && invalid_reason.is_none(),"invalid_reason":invalid_reason,"trace_record_limit":trace.record_budget(),"completed":false,"overflow":result.is_ok(),"failed_chunks":usize::from(result.is_ok()),"done_chunks":usize::from(result.is_ok()),"aggregate_snapshot":result.as_ref().ok(),
        "cleanup":"body and registry references released; pump abort joined; 256 queued envelopes drained; byte permits returned"}));
    trace.flush();
    trace.assert_valid();
    if result.is_ok() {
        trace.assert_measurement_records(Some([0, 0, 3, 0]));
    }
    assert!(result.is_ok(), "invalid overflow measurement: {result:?}");
}

async fn diagnostic_stream_pair() -> (
    Arc<LiveUpstreamWebSocket>,
    ResponseStreamState,
    WebSocketStream<DuplexStream>,
) {
    diagnostic_stream_pair_with_trace(None).await
}

async fn diagnostic_stream_pair_with_trace(
    trace: Option<FiniteTrace>,
) -> (
    Arc<LiveUpstreamWebSocket>,
    ResponseStreamState,
    WebSocketStream<DuplexStream>,
) {
    let (client_io, server_io) = duplex(8192);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let upstream = Arc::new(match trace {
        Some(trace) => LiveUpstreamWebSocket::test_traced_stream(client, trace),
        None => LiveUpstreamWebSocket::from_stream_with_close_diagnostics(
            client,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::DEFAULT,
            true,
        ),
    });
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

async fn diagnostic_read_barrier(server: &mut WebSocketStream<DuplexStream>) {
    server.send(Message::Ping(vec![7])).await.unwrap();
    assert_eq!(
        server.next().await.unwrap().unwrap(),
        Message::Pong(vec![7])
    );
}

fn finite_sse_value(bytes: &[u8]) -> serde_json::Value {
    let text = std::str::from_utf8(bytes).unwrap();
    let data = text
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap();
    serde_json::from_str(data).unwrap()
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_paused_sse_preserves_burst_fifo_and_terminal_snapshot() {
    let (upstream, state, mut server) = diagnostic_stream_pair().await;
    let mut body = Box::pin(response_stream(state));
    assert!(futures_util::poll!(body.next()).is_pending());
    tokio::time::advance(std::time::Duration::from_millis(100)).await;
    assert!(futures_util::poll!(body.next()).is_pending());
    assert!(upstream.diagnostic_activity().contains(
        "consumer_phase=awaiting_upstream last_consumer_poll_age_ms=0 last_dequeue_age_ms=-"
    ));
    send_server_event(&mut server, json!({"type":"response.created"})).await;
    assert!(
        String::from_utf8(body.next().await.unwrap().unwrap().to_vec())
            .unwrap()
            .contains("response.created")
    );
    let events = [
        json!({"type":"response.output_text.delta","delta":"first"}),
        json!({"type":"response.output_text.delta","delta":"second"}),
        json!({"type":"response.completed","response":{"id":"resp_burst","output":[]}}),
    ];
    let bytes: usize = events.iter().map(|event| event.to_string().len()).sum();
    for event in events {
        send_server_event(&mut server, event).await;
    }
    diagnostic_read_barrier(&mut server).await;
    tokio::time::advance(std::time::Duration::from_millis(250)).await;
    diagnostic_read_barrier(&mut server).await;
    assert!(upstream.diagnostic_activity().contains("consumer_phase=awaiting_downstream_poll last_consumer_poll_age_ms=250 last_dequeue_age_ms=250 internal_tool_age_ms=- queue_messages_high_water=3"));
    server.send(Message::Close(None)).await.unwrap();
    while !upstream.is_closed() {
        tokio::task::yield_now().await;
    }
    let before = upstream.diagnostic_output();
    assert_eq!(before.0, 1);
    assert!(before.1.contains(&format!(
        "queue_bytes_high_water={bytes} last_data_age_ms=250"
    )));
    assert!(before.1.contains("response_state=unknown last_classified_response_state=in_progress unclassified_data_count=3 response_ambiguous=false"));
    let mut chunks = Vec::new();
    while let Some(chunk) = body.next().await {
        chunks.push(String::from_utf8(chunk.unwrap().to_vec()).unwrap());
    }
    assert_eq!(
        chunks
            .iter()
            .filter(|chunk| chunk.starts_with("event: response.output_text.delta"))
            .count(),
        2
    );
    assert!(chunks[0].contains("first"));
    assert!(chunks[1].contains("second"));
    assert_eq!(
        chunks
            .iter()
            .filter(|chunk| chunk.starts_with("event: response.completed"))
            .count(),
        1
    );
    assert_eq!(
        chunks
            .iter()
            .filter(|chunk| chunk.contains("data: [DONE]"))
            .count(),
        1
    );
    assert!(!chunks.iter().any(|chunk| chunk.contains("response.failed")));
    assert_eq!(upstream.diagnostic_output(), before);
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_retained_idle_data_is_distinct_from_control_traffic() {
    let (upstream, mut state, mut server) = diagnostic_stream_pair().await;
    send_server_event(&mut server, json!({"type":"response.completed","response":{"id":"resp_idle","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}})).await;
    let completed = parse_received_event(&mut state).await;
    progression::process_upstream_event(
        &mut state,
        completed,
        &mut super::super::InternalToolLedger::default(),
    )
    .await;
    assert!(
        upstream
            .diagnostic_activity()
            .contains("consumer_phase=retained_idle")
    );
    state.lease.release();
    server
        .send(Message::Text("idle-data".into()))
        .await
        .unwrap();
    diagnostic_read_barrier(&mut server).await;
    tokio::time::advance(std::time::Duration::from_millis(400)).await;
    diagnostic_read_barrier(&mut server).await;
    server.send(Message::Close(None)).await.unwrap();
    while !upstream.is_closed() {
        tokio::task::yield_now().await;
    }
    let before = upstream.diagnostic_output();
    assert!(before.1.contains("last_rx_age_ms=0 last_rx_kind=close"));
    assert!(before.1.contains("last_data_age_ms=400"));
    assert!(before.1.contains("response_state=unknown last_classified_response_state=completed unclassified_data_count=1 response_ambiguous=false consumer_phase=retained_idle"));
    assert_eq!(
        upstream.recv_text().await.unwrap().as_deref(),
        Some("idle-data")
    );
    upstream.observe_response_event(crate::ws_pump::ResponseEvent::Unknown);
    assert_eq!(upstream.diagnostic_output(), before);
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
            assert!(matches!(result, Err(ThreadlineError::UpstreamErrorEvent)));
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

#[tokio::test(start_paused = true)]
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
    progression::process_upstream_event(
        &mut state,
        created,
        &mut super::super::InternalToolLedger::default(),
    )
    .await;
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
    progression::process_upstream_event(
        state,
        completed,
        &mut super::super::InternalToolLedger::default(),
    )
    .await;
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
    progression::process_upstream_event(
        state,
        final_event,
        &mut super::super::InternalToolLedger::default(),
    )
    .await;
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
    let tool_event = json!({"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_test","name":"threadline_echo","arguments":json!({"value":"done"}).to_string()}});
    let tool_bytes = tool_event.to_string().len();
    send_server_event(server, tool_event).await;
    let tool = parse_received_event(state).await;
    let mut ledger = super::super::InternalToolLedger::default();
    let execution = progression::process_upstream_event(state, tool, &mut ledger);
    tokio::pin!(execution);
    assert!(futures_util::poll!(&mut execution).is_pending());
    started.notified().await;
    assert_diagnostic_state(upstream, ResponseState::InProgress);
    let completed =
        json!({"type":"response.completed","response":{"id":"resp_middle"}}).to_string();
    let completed_bytes = completed.len();
    server.send(Message::Text(completed)).await.unwrap();
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
    tokio::time::advance(std::time::Duration::from_millis(250)).await;
    let activity = upstream.diagnostic_activity();
    assert!(activity.contains("consumer_phase=executing_internal_tool"));
    assert!(activity.contains("last_dequeue_age_ms=250 internal_tool_age_ms=250"));
    assert!(activity.contains(&format!(
        "queue_messages_high_water=1 queue_bytes_high_water={}",
        completed_bytes.max(tool_bytes)
    )));
    release.notify_one();
    execution.await;
    assert!(
        upstream
            .diagnostic_activity()
            .contains("internal_tool_age_ms=-")
    );
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
