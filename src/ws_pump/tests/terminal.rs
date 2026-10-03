use super::*;

#[tokio::test(start_paused = true)]
async fn close_diagnostics_timeout_and_overflow_remain_sticky_and_warn_once() {
    let _no_subscriber_dispatch =
        tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let events = OverflowLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(events.clone());
    let _subscriber = tracing::subscriber::set_default(subscriber);
    for overflow_first in [false, true] {
        assert_sticky_terminal_diagnostics(overflow_first).await;
    }
    assert_timeout_and_overflow_events(&events);
}

async fn assert_sticky_terminal_diagnostics(overflow_first: bool) {
    let diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let limits = UpstreamInboundLimits::new(1, 4).unwrap();
    let (sender, _receiver) = mpsc::channel(1);
    let budget = Arc::new(Semaphore::new(4));
    let overflow = || {
        assert!(record_transport_overflow(
            &state,
            &sender,
            &budget,
            limits,
            &TungsteniteError::Capacity(CapacityError::MessageTooLong {
                size: 126,
                max_size: 125
            }),
            &diagnostics
        ));
    };
    let liveness_timeout = || {
        record_liveness_timeout(
            &state,
            UpstreamLivenessTimeoutCause::WriteDeadline,
            Duration::from_secs(7),
            Duration::from_secs(8),
            Some(UpstreamOutboundKind::Text),
            &diagnostics,
        );
    };
    advance(Duration::from_millis(1500)).await;
    if overflow_first {
        overflow();
    } else {
        liveness_timeout();
    }
    let first = state.lock().unwrap().clone();
    assert_eq!(
        matches!(first, UpstreamTerminalState::InboundBufferOverflow(_)),
        overflow_first
    );
    advance(Duration::from_secs(2)).await;
    overflow();
    liveness_timeout();
    assert_fallback_preserves_terminal(&state, first, &diagnostics, overflow_first);
}

fn assert_fallback_preserves_terminal(
    state: &Arc<StdMutex<UpstreamTerminalState>>,
    first: UpstreamTerminalState,
    diagnostics: &CloseDiagnostics,
    overflow_first: bool,
) {
    record_close(
        state,
        empty_close_metadata(),
        UpstreamCloseSource::PumpExitFallback,
        diagnostics,
    );
    assert_eq!(*state.lock().unwrap(), first);
    assert_sticky_terminal_output(diagnostics, overflow_first);
}

fn assert_sticky_terminal_output(diagnostics: &CloseDiagnostics, overflow_first: bool) {
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    let output = String::from_utf8(capture.bytes.clone()).unwrap();
    assert!(output.contains("connection_age_ms=1500"));
    assert!(!output.contains("source="));
    if overflow_first {
        assert!(output.contains("terminal=inbound_buffer_overflow"));
        assert!(output.contains("cause=transport_size queued_messages=0 queued_bytes=0 incoming_bytes=126 max_messages=1 max_bytes=4"));
    } else {
        assert!(output.contains("terminal=liveness_timeout"));
        assert!(
            output.contains(
                "cause=write_deadline timeout_ms=7000 elapsed_ms=8000 outbound_kind=text"
            )
        );
    }
}

fn assert_timeout_and_overflow_events(events: &OverflowLogCapture) {
    let events = events.0.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].get("cause"), Some(&"WriteDeadline".to_string()));
    assert_eq!(events[0].get("timeout_ms"), Some(&"7000".to_string()));
    assert_eq!(events[0].get("elapsed_ms"), Some(&"8000".to_string()));
    assert_eq!(
        events[0].get("outbound_kind"),
        Some(&"Some(Text)".to_string())
    );
    assert_eq!(
        events[1].get("overflow_cause"),
        Some(&"TransportSize".to_string())
    );
    assert_eq!(events[1].get("incoming_bytes"), Some(&"126".to_string()));
    assert_eq!(events[1].get("recoverable"), Some(&"false".to_string()));
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_first_commit_freezes_lifetime_and_unlocks_before_writing() {
    let diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    diagnostics.capture().terminal_state = Some(Arc::clone(&state));
    advance(Duration::from_millis(1234)).await;
    let metadata = UpstreamCloseMetadata {
        code: Some(1001),
        reason: Some("going away".to_string()),
        error: None,
    };
    assert!(commit_terminal(
        &state,
        UpstreamTerminalState::Closed(metadata.clone()),
        &diagnostics,
        Some(UpstreamCloseSource::PeerCloseFrame),
        None
    ));
    advance(Duration::from_secs(10)).await;
    assert!(!commit_terminal(
        &state,
        UpstreamTerminalState::Closed(outbound_channel_closed_metadata()),
        &diagnostics,
        Some(UpstreamCloseSource::PumpExitFallback),
        None
    ));
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::Closed(metadata)
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    assert_eq!(
        String::from_utf8(capture.bytes.clone()).unwrap(),
        "[threadline] websocket closed source=peer_close_frame code=1001 reason=going away error=- connection_age_ms=1234 io_kind=- raw_os_error=-\n"
    );
}

#[test]
fn close_diagnostics_disabled_never_calls_writer_and_preserves_metadata() {
    let diagnostics = CloseDiagnostics::new(false);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let metadata = UpstreamCloseMetadata {
        code: Some(1001),
        reason: Some("private-reason".to_string()),
        error: Some("private-error".to_string()),
    };
    assert!(commit_terminal(
        &state,
        UpstreamTerminalState::Closed(metadata.clone()),
        &diagnostics,
        Some(UpstreamCloseSource::ReadError),
        None
    ));
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::Closed(metadata)
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 0);
    assert!(capture.bytes.is_empty());
}

#[test]
fn close_diagnostics_failing_writer_does_not_retry_log_or_change_terminal() {
    let diagnostics = CloseDiagnostics::new(true);
    diagnostics.capture().fail = true;
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let capture = OverflowLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let metadata = UpstreamCloseMetadata {
        code: None,
        reason: None,
        error: None,
    };
    tracing::subscriber::with_default(subscriber, || {
        assert!(commit_terminal(
            &state,
            UpstreamTerminalState::Closed(metadata.clone()),
            &diagnostics,
            Some(UpstreamCloseSource::PumpExitFallback),
            None
        ));
        assert!(!commit_terminal(
            &state,
            UpstreamTerminalState::Closed(metadata.clone()),
            &diagnostics,
            Some(UpstreamCloseSource::StreamEof),
            None
        ));
    });
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::Closed(metadata)
    );
    assert!(capture.0.lock().unwrap().is_empty());
    let writer = diagnostics.capture();
    assert_eq!(writer.calls, 1);
    assert!(writer.bytes.is_empty());
}

#[tokio::test]
async fn close_diagnostics_write_failure_uses_send_and_flush_paths_with_pending_reader() {
    for outbound_kind in [
        UpstreamOutboundKind::Text,
        UpstreamOutboundKind::ControlFlush,
    ] {
        assert_write_failure_diagnostics(outbound_kind).await;
    }
}

async fn assert_write_failure_diagnostics(outbound_kind: UpstreamOutboundKind) {
    let (client, _server, _, _, _) = flush_gate_pair(1024, true).await;
    let (mut writer, mut reader) = client.split();
    {
        let read = reader.next();
        pin_mut!(read);
        assert!(
            futures_util::poll!(read).is_pending(),
            "read must be pending before the write error"
        );
    }
    let limits = UpstreamInboundLimits::DEFAULT;
    let (inbound_tx, _inbound_rx) = mpsc::channel(limits.max_messages());
    let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let diagnostics = CloseDiagnostics::new(true);
    let pump_state = PumpState {
        inbound_tx: &inbound_tx,
        byte_budget: &byte_budget,
        limits,
        terminal_state: &state,
        watchdog_policy: UpstreamWatchdogPolicy::DEFAULT,
        diagnostics: &diagnostics,
    };
    let mut next_ping_due = Instant::now() + UPSTREAM_PING_INTERVAL;
    assert!(
        !timeout(
            Duration::from_secs(2),
            drive_write_operation(
                &mut writer,
                &mut reader,
                &pump_state,
                Message::Text("frame-secret".to_string()),
                outbound_kind,
                PumpLivenessState {
                    pending_challenge: &mut None,
                    control_flush_needed: &mut false,
                    next_ping_due: &mut next_ping_due,
                    ping_interval: UPSTREAM_PING_INTERVAL,
                },
            )
        )
        .await
        .expect("write fails without waiting for a deadline")
    );
    assert_write_failure_terminal(&state, &diagnostics);
}

fn assert_write_failure_terminal(
    state: &Arc<StdMutex<UpstreamTerminalState>>,
    diagnostics: &CloseDiagnostics,
) {
    let first = state.lock().unwrap().clone();
    assert!(
        matches!(&first, UpstreamTerminalState::Closed(UpstreamCloseMetadata { code: None, reason: None, error: Some(error) }) if error.contains("write-secret"))
    );
    record_close(
        state,
        empty_close_metadata(),
        UpstreamCloseSource::PumpExitFallback,
        diagnostics,
    );
    assert_eq!(*state.lock().unwrap(), first);
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    let output = String::from_utf8(capture.bytes.clone()).unwrap();
    assert!(output.contains("source=write_error code=- reason=- error=io"));
    assert!(output.contains("io_kind=broken_pipe raw_os_error=-"));
    assert!(!output.contains("secret"));
    assert!(!output.contains(['\r', '\x1b']));
    assert_eq!(output.lines().count(), 1);
}
