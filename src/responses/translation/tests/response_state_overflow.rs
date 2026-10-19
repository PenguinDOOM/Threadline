use super::*;

use axum::body::Bytes;
use futures_util::{Stream, StreamExt};
use std::convert::Infallible;
use std::pin::Pin;

type ResponseBody = Pin<Box<dyn Stream<Item = Result<Bytes, Infallible>>>>;

async fn start_sse_case(
    preload: usize,
) -> (
    FiniteTrace,
    Arc<LiveUpstreamWebSocket>,
    ResponseBody,
    WebSocketStream<DuplexStream>,
) {
    let trace = FiniteTrace::new(match preload {
        0 => "sse_alternating",
        192 => "sse_preloaded",
        _ => "sse_paused",
    });
    let (upstream, state, mut server) =
        diagnostic_stream_pair_with_trace(Some(trace.clone())).await;
    let mut body: ResponseBody = Box::pin(response_stream(state));
    finite_send(&mut server, &trace, 0, synthetic_created()).await;
    let created = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(finite_sse_value(&created)["type"], "response.created");
    assert!(
        upstream
            .diagnostic_activity()
            .contains("consumer_phase=awaiting_downstream_poll")
    );
    for ordinal in 1..=preload {
        finite_send(&mut server, &trace, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(&mut server, &trace).await;
    assert_eq!(upstream.test_queue_sample().0, preload);
    trace.event(
        "resume",
        None,
        None,
        Some(upstream.test_queue_sample()),
        None,
    );
    (trace, upstream, body, server)
}

async fn assert_sse_overflow(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    server: &mut WebSocketStream<DuplexStream>,
) {
    assert!(
        upstream
            .diagnostic_activity()
            .contains("consumer_phase=awaiting_downstream_poll")
    );
    finite_send(server, trace, 257, synthetic_delta(257)).await;
    while !upstream.is_closed() {
        tokio::task::yield_now().await;
    }
    let frozen = upstream.diagnostic_output();
    assert_eq!(frozen.0, 1);
    assert!(frozen.1.contains("consumer_phase=awaiting_downstream_poll"));
    trace.diagnostic(&frozen.1);
    let failed = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(finite_sse_value(&failed)["type"], "response.failed");
    assert!(
        std::str::from_utf8(&failed)
            .unwrap()
            .contains("upstream_inbound_buffer_overflow")
    );
    let done = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(std::str::from_utf8(&done).unwrap(), "data: [DONE]\n\n");
    assert!(trace.observe(body.next()).await.is_none());
    assert_eq!(upstream.diagnostic_output(), frozen);
    trace.event(
        "outcome",
        Some(257),
        None,
        Some(upstream.test_queue_sample()),
        Some("expected_overflow"),
    );
}

async fn drain_preloaded_sse(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    preload: usize,
) {
    for ordinal in 1..=preload {
        let chunk = trace.observe(body.next()).await.unwrap().unwrap();
        let event = finite_sse_value(&chunk);
        assert_eq!(event["sequence_number"], ordinal);
        assert_eq!(event["delta"], format!("{ordinal:06}"));
    }
    assert_eq!(upstream.test_queue_sample().0, 0);
    trace.event(
        "recovered",
        None,
        None,
        Some(upstream.test_queue_sample()),
        None,
    );
}

async fn send_and_drain_sse_batches(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    server: &mut WebSocketStream<DuplexStream>,
    preload: usize,
) {
    for first in (preload + 1..=400).step_by(16) {
        for ordinal in first..first + 16 {
            finite_send(server, trace, ordinal, synthetic_delta(ordinal)).await;
        }
        finite_read_barrier(server, trace).await;
        for ordinal in first..first + 16 {
            let chunk = trace.observe(body.next()).await.unwrap().unwrap();
            let event = finite_sse_value(&chunk);
            assert_eq!(event["sequence_number"], ordinal);
            assert_eq!(event["delta"], format!("{ordinal:06}"));
        }
        assert_eq!(upstream.test_queue_sample().0, 0);
    }
}

async fn complete_sse_case(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    server: &mut WebSocketStream<DuplexStream>,
) {
    finite_send(server, trace, 401, synthetic_completed()).await;
    let completed = trace.observe(body.next()).await.unwrap().unwrap();
    let event = finite_sse_value(&completed);
    assert_eq!(event["type"], "response.completed");
    let expected: String = (1..=400).map(|ordinal| format!("{ordinal:06}")).collect();
    assert_eq!(
        event["response"]["output"][0]["content"][0]["text"],
        expected
    );
    let done = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(std::str::from_utf8(&done).unwrap(), "data: [DONE]\n\n");
    assert!(trace.observe(body.next()).await.is_none());
    assert_eq!(
        upstream.terminal_state(),
        crate::ws_pump::UpstreamTerminalState::Open
    );
    trace.event(
        "outcome",
        Some(400),
        None,
        Some(upstream.test_queue_sample()),
        Some("not_reproduced"),
    );
}

async fn run_sse_case(preload: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (trace, upstream, mut body, mut server) = start_sse_case(preload).await;
        if preload == 256 {
            assert_sse_overflow(&trace, &upstream, &mut body, &mut server).await;
            return;
        }
        drain_preloaded_sse(&trace, &upstream, &mut body, preload).await;
        send_and_drain_sse_batches(&trace, &upstream, &mut body, &mut server, preload).await;
        complete_sse_case(&trace, &upstream, &mut body, &mut server).await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn response_stream_acknowledged_and_paused_batches_preserve_fifo_or_report_overflow() {
    for preload in [0, 192, 256] {
        run_sse_case(preload).await;
    }
}

async fn fill_default_queue(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    server: &mut WebSocketStream<DuplexStream>,
    terminal_at: tokio::time::Instant,
) {
    for ordinal in 1..=256 {
        finite_send(server, trace, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(server, trace).await;
    assert_eq!(upstream.test_queue_sample(), (256, 256, 256));
    assert_eq!(tokio::time::Instant::now(), terminal_at);
    finite_send(server, trace, 257, synthetic_delta(257)).await;
    while !upstream.is_closed() {
        tokio::task::yield_now().await;
    }
    assert_eq!(tokio::time::Instant::now(), terminal_at);
}

fn assert_seventeen_millisecond_snapshot(frozen: &(usize, String)) {
    assert_eq!(frozen.0, 1);
    for field in [
        "terminal=inbound_buffer_overflow",
        "response_state=unknown",
        "last_classified_response_state=in_progress",
        "unclassified_data_count=257",
        "response_ambiguous=false",
        "consumer_phase=awaiting_upstream",
        "last_consumer_poll_age_ms=17",
        "last_dequeue_age_ms=17",
        "queued_messages=256",
        "queued_bytes=50944",
        "incoming_bytes=199",
        "queue_messages_high_water=256",
        "internal_tool_age_ms=-",
        "last_data_age_ms=0",
    ] {
        assert!(
            frozen.1.contains(field),
            "missing diagnostic field: {field}"
        );
    }
}

async fn finish_failed_sse(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    frozen: &(usize, String),
    warning_count: &impl Fn() -> usize,
) {
    trace.diagnostic(&frozen.1);
    assert_eq!(warning_count(), 1);
    let failed = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(finite_sse_value(&failed)["type"], "response.failed");
    let done = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(std::str::from_utf8(&done).unwrap(), "data: [DONE]\n\n");
    assert!(trace.observe(body.next()).await.is_none());
    assert_eq!(upstream.test_drain_after_terminal(trace).await, 255);
    upstream.observe_response_event(crate::ws_pump::ResponseEvent::Progress);
    assert_eq!(upstream.diagnostic_output(), *frozen);
    assert_eq!(warning_count(), 1);
    trace.event(
        "outcome",
        Some(257),
        None,
        Some(upstream.test_queue_sample()),
        Some("expected_overflow"),
    );
}

async fn run_pending_receive_snapshot(warning_count: impl Fn() -> usize) {
    let trace = FiniteTrace::configured("sse_pending_seventeen_ms", 1, 0, 402, false, true, true);
    let (upstream, state, mut server) =
        diagnostic_stream_pair_with_trace(Some(trace.clone())).await;
    let mut body: ResponseBody = Box::pin(response_stream(state));
    let initial = tokio::time::Instant::now();
    finite_send(&mut server, &trace, 0, synthetic_created()).await;
    let created = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(finite_sse_value(&created)["type"], "response.created");
    assert_eq!(tokio::time::Instant::now(), initial);
    {
        let waiting = trace.observe(body.next());
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
    }
    assert!(
        upstream
            .diagnostic_activity()
            .contains("consumer_phase=awaiting_upstream")
    );
    tokio::time::advance(std::time::Duration::from_millis(17)).await;
    let terminal_at = initial + std::time::Duration::from_millis(17);
    assert_eq!(tokio::time::Instant::now(), terminal_at);
    fill_default_queue(&trace, &upstream, &mut server, terminal_at).await;
    let frozen = upstream.diagnostic_output();
    assert_seventeen_millisecond_snapshot(&frozen);
    finish_failed_sse(&trace, &upstream, &mut body, &frozen, &warning_count).await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn response_stream_pending_receive_preserves_seventeen_millisecond_overflow_snapshot() {
    let (_keepalive, _guard, warning_count) = finite_warning_capture();
    finite_drive(run_pending_receive_snapshot(warning_count)).await;
}
