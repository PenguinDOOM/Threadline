use super::*;

#[tokio::test(start_paused = true)]
async fn close_diagnostics_traffic_intervals_saturate_and_freeze_with_terminal() {
    let (owned, mut receiver) = test_diagnostic_pump_state(true);
    let diagnostics = &owned.diagnostics;
    let terminal = &owned.terminal_state;
    let response = &diagnostics.response;
    let snapshot = || {
        diagnostics
            .snapshot(diagnostics.started_at.unwrap(), Instant::now())
            .to_string()
    };
    let payload = "prompt-secret arguments-secret file-secret";
    for index in 0..2 {
        if index > 0 {
            advance(Duration::from_millis(17)).await;
        }
        response.record_consumer_poll(terminal);
        assert!(try_enqueue_inbound(
            &owned.inbound_tx,
            &owned.byte_budget,
            owned.limits,
            payload.into(),
            terminal,
            diagnostics
        ));
        let envelope = receiver.try_recv().unwrap();
        response.record_dequeue(terminal, envelope.payload.len());
    }
    let output = snapshot();
    for expected in ["enqueued_messages=2".to_string(),format!("enqueued_bytes={}",2*payload.len()),
        "dequeued_messages=2".into(),format!("dequeued_bytes={}",2*payload.len()),
        "consumer_poll_gap_count=1 consumer_poll_gap_total_ns=17000000 consumer_poll_gap_max_ns=17000000 consumer_poll_gap_saturated=false".into(),
        "dequeue_gap_count=1 dequeue_gap_total_ns=17000000 dequeue_gap_max_ns=17000000 dequeue_gap_saturated=false".into()] {
        assert!(output.contains(&expected), "missing {expected}");
    }
    assert!(!output.contains("secret"));
    response.test_seed_traffic_saturation();
    advance(Duration::from_nanos(1)).await;
    response.record_consumer_poll(terminal);
    assert!(try_enqueue_inbound(
        &owned.inbound_tx,
        &owned.byte_budget,
        owned.limits,
        payload.into(),
        terminal,
        diagnostics
    ));
    let envelope = receiver.try_recv().unwrap();
    response.record_dequeue(terminal, envelope.payload.len());
    let saturated = snapshot();
    assert!(saturated.contains("traffic_saturated=true"));
    assert!(saturated.contains("consumer_poll_gap_saturated=true"));
    assert!(saturated.contains("dequeue_gap_saturated=true"));
    assert!(saturated.contains(&format!(
        "enqueued_messages={} enqueued_bytes={} dequeued_messages={} dequeued_bytes={}",
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX
    )));
    assert!(commit_terminal(
        terminal,
        UpstreamTerminalState::Closed(empty_close_metadata()),
        diagnostics,
        Some(UpstreamCloseSource::PeerCloseFrame),
        None
    ));
    response.record_consumer_poll(terminal);
    response.record_dequeue(terminal, payload.len());
    assert_eq!(snapshot(), saturated);
}

pub(super) fn terminal_observation_cases() -> [UpstreamTerminalState; 3] {
    [
        UpstreamTerminalState::Closed(empty_close_metadata()),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::WriteDeadline,
            timeout: Duration::from_secs(1),
            elapsed: Duration::from_secs(1),
            outbound_kind: Some(UpstreamOutboundKind::Text),
        }),
        UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
            cause: InboundBufferOverflowCause::MessageCount,
            queued_messages: 1,
            queued_bytes: 1,
            incoming_bytes: 1,
            max_messages: 1,
            max_bytes: 1,
        }),
    ]
}
