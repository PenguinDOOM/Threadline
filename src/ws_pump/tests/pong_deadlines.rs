use super::*;

#[tokio::test(start_paused = true)]
async fn close_diagnostics_pong_tracks_only_existing_acknowledgements() {
    let (mut owned, _inbound_rx) = test_diagnostic_pump_state(true);
    let mut pump_state = owned.borrowed();
    assert_regular_pong_activity(&mut pump_state).await;
    let mut challenge = Some(PendingPongChallenge {
        nonce: b"early-secret".to_vec(),
        sent_at: None,
        acknowledged_early: false,
    });
    for _ in 0..2 {
        advance(Duration::from_millis(10)).await;
        assert!(handle_inbound_message(
            Some(Ok(Message::Pong(b"early-secret".to_vec()))),
            &mut pump_state,
            &mut challenge,
            &mut false
        ));
        assert!(challenge.as_ref().unwrap().acknowledged_early);
        assert_eq!(pump_state.diagnostics.last_pong_at, Some(Instant::now()));
        assert_eq!(
            pump_state.diagnostics.last_pong_at,
            pump_state.diagnostics.last_rx_at
        );
    }
    assert!(!finish_write_operation(
        Err(TungsteniteError::ConnectionClosed),
        &mut pump_state,
        UpstreamOutboundKind::Ping
    ));
    assert_eq!(owned.diagnostics.last_ping_at, None);
    assert_eq!(owned.diagnostics.last_tx_at, None);
    assert_eq!(owned.diagnostics.last_pong_at, Some(Instant::now()));
    let output = String::from_utf8(owned.diagnostics.capture().bytes.clone()).unwrap();
    assert!(
        output.contains("last_rx_age_ms=0 last_tx_age_ms=- last_ping_age_ms=- last_pong_age_ms=0")
    );
    assert!(!output.contains("secret"));
}

async fn assert_regular_pong_activity(pump_state: &mut PumpState<'_>) {
    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"unsolicited".to_vec()))),
        pump_state,
        &mut None,
        &mut false
    ));
    assert_eq!(pump_state.diagnostics.last_pong_at, None);
    let mut challenge = Some(PendingPongChallenge {
        nonce: b"matching-secret".to_vec(),
        sent_at: Some(Instant::now()),
        acknowledged_early: false,
    });
    advance(Duration::from_millis(10)).await;
    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"mismatch".to_vec()))),
        pump_state,
        &mut challenge,
        &mut false
    ));
    assert_eq!(pump_state.diagnostics.last_rx_at, Some(Instant::now()));
    assert_eq!(pump_state.diagnostics.last_pong_at, None);
    advance(Duration::from_millis(10)).await;
    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"matching-secret".to_vec()))),
        pump_state,
        &mut challenge,
        &mut false
    ));
    let acknowledged_at = Instant::now();
    assert!(challenge.is_none());
    assert_eq!(pump_state.diagnostics.last_pong_at, Some(acknowledged_at));
    advance(Duration::from_millis(10)).await;
    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"matching-secret".to_vec()))),
        pump_state,
        &mut challenge,
        &mut false
    ));
    assert_eq!(pump_state.diagnostics.last_rx_at, Some(Instant::now()));
    assert_eq!(pump_state.diagnostics.last_pong_at, Some(acknowledged_at));
}

#[tokio::test]
async fn websocket_pump_times_out_when_a_sent_challenge_never_receives_a_matching_pong() {
    let (client_io, _server_io) = duplex(1024);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let policy = UpstreamWatchdogPolicy::new(Duration::from_millis(20), Duration::from_millis(100))
        .expect("valid short policy");
    let pump = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_millis(1),
            policy,
        ),
    );
    let waiting_recv = {
        let pump = Arc::clone(&pump);
        tokio::spawn(async move { pump.recv_text().await })
    };

    assert_eq!(
        timeout(Duration::from_secs(1), waiting_recv)
            .await
            .expect("Pong timeout should release the receiver")
            .expect("receiver task should not panic"),
        Err(UpstreamWebSocketError::LivenessTimeout)
    );
    assert!(matches!(
        pump.terminal_state(),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::PongDeadline,
            timeout,
            outbound_kind: None,
            ..
        }) if timeout == Duration::from_millis(20)
    ));
    assert_eq!(
        pump.close_metadata().await,
        Some(liveness_timeout_close_metadata())
    );
}

#[tokio::test]
async fn websocket_pump_times_out_a_stalled_ping_write() {
    let (client, _server, write_started, _write_open, _write_waker) =
        write_gate_pair(1024, None).await;
    let policy = UpstreamWatchdogPolicy::new(Duration::from_millis(100), Duration::from_millis(20))
        .expect("valid short policy");
    let pump = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_millis(1),
            policy,
        ),
    );
    timeout(Duration::from_secs(1), write_started.notified())
        .await
        .expect("Ping write should reach the controlled gate");

    assert_eq!(
        timeout(Duration::from_secs(1), pump.recv_text())
            .await
            .expect("write timeout should release the receiver"),
        Err(UpstreamWebSocketError::LivenessTimeout)
    );
    assert!(matches!(
        pump.terminal_state(),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::WriteDeadline,
            timeout,
            outbound_kind: Some(UpstreamOutboundKind::Ping),
            ..
        }) if timeout == Duration::from_millis(20)
    ));
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_rejects_matching_pong_at_the_deadline() {
    let limits = UpstreamInboundLimits::new(1, 16).expect("valid limits");
    let (inbound_tx, _inbound_rx) = mpsc::channel(limits.max_messages());
    let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(8))
        .expect("valid watchdog policy");
    let mut diagnostics = CloseDiagnostics::new(true);
    let mut pump_state = PumpState {
        inbound_tx: &inbound_tx,
        byte_budget: &byte_budget,
        limits,
        terminal_state: &terminal_state,
        watchdog_policy: policy,
        diagnostics: &mut diagnostics,
    };
    let mut challenge = Some(PendingPongChallenge {
        nonce: b"current-challenge".to_vec(),
        sent_at: Some(Instant::now()),
        acknowledged_early: false,
    });
    let mut control_flush_needed = false;

    advance(Duration::from_secs(5)).await;
    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"current-challenge".to_vec()))),
        &mut pump_state,
        &mut challenge,
        &mut control_flush_needed,
    ));
    assert!(challenge.is_some());
    assert_eq!(pump_state.diagnostics.last_rx_at, Some(Instant::now()));
    assert_eq!(pump_state.diagnostics.last_pong_at, None);
    assert!(!check_liveness_deadline(
        &terminal_state,
        challenge.as_ref(),
        None,
        policy,
        &diagnostics,
    ));
    assert!(matches!(
        *terminal_state.lock().expect("terminal state lock"),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::PongDeadline,
            ..
        })
    ));
    let output = String::from_utf8(diagnostics.capture().bytes.clone()).unwrap();
    assert!(output.contains("connection_age_ms=5000 last_rx_age_ms=0 last_tx_age_ms=- last_ping_age_ms=- last_pong_age_ms=-"));
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_latches_matching_pong_before_ping_send_completes() {
    let limits = UpstreamInboundLimits::new(1, 16).expect("valid limits");
    let (inbound_tx, _inbound_rx) = mpsc::channel(limits.max_messages());
    let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(8))
        .expect("valid watchdog policy");
    let mut diagnostics = CloseDiagnostics::new(true);
    let mut pump_state = PumpState {
        inbound_tx: &inbound_tx,
        byte_budget: &byte_budget,
        limits,
        terminal_state: &terminal_state,
        watchdog_policy: policy,
        diagnostics: &mut diagnostics,
    };
    let mut challenge = Some(PendingPongChallenge {
        nonce: b"current-challenge".to_vec(),
        sent_at: None,
        acknowledged_early: false,
    });
    let mut control_flush_needed = false;

    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"current-challenge".to_vec()))),
        &mut pump_state,
        &mut challenge,
        &mut control_flush_needed,
    ));
    assert_eq!(
        pump_state.diagnostics.last_pong_at,
        pump_state.diagnostics.last_rx_at
    );
    assert_eq!(pump_state.diagnostics.last_ping_at, None);
    assert_eq!(pump_state.diagnostics.last_tx_at, None);
    assert!(
        challenge
            .as_ref()
            .expect("challenge remains until send completion")
            .acknowledged_early
    );
    assert!(matches!(
        *terminal_state.lock().expect("terminal state lock"),
        UpstreamTerminalState::Open
    ));
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_accepts_matching_pong_before_ping_flush_completes() {
    let (client, server, flush_started, flush_open, flush_waker) =
        flush_gate_pair(1024, false).await;
    let (mut server_writer, mut server_reader) = server.split();
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(10))
        .expect("valid watchdog policy");
    let flush_waiter = {
        let flush_started = Arc::clone(&flush_started);
        tokio::spawn(async move { flush_started.notified().await })
    };
    let pump = LiveUpstreamWebSocket::from_stream_with_policy_and_limits(
        client,
        Duration::from_secs(10),
        policy,
        UpstreamInboundLimits::DEFAULT,
        CloseDiagnostics::new(true),
    );

    tokio::task::yield_now().await;
    advance(Duration::from_secs(10)).await;
    let nonce = match server_reader
        .next()
        .await
        .expect("client Ping bytes")
        .expect("valid client Ping")
    {
        Message::Ping(nonce) => nonce,
        message => panic!("expected client Ping, got {message:?}"),
    };
    flush_waiter.await.expect("flush waiter should complete");
    server_writer
        .send(Message::Pong(nonce))
        .await
        .expect("send matching Pong before flush completion");
    server_writer
        .send(Message::Text("early-pong-observed".to_string()))
        .await
        .expect("send ordered read barrier after early Pong");
    assert_eq!(
        pump.recv_text()
            .await
            .expect("pump should process the early Pong before the read barrier"),
        Some("early-pong-observed".to_string())
    );

    advance(Duration::from_secs(2)).await;
    flush_open.store(true, Ordering::SeqCst);
    flush_waker.wake();
    assert_read_progress_after_early_pong(&pump, &mut server_writer, &mut server_reader).await;

    assert_early_pong_activity_ages(pump, &mut server_writer).await;
}

async fn assert_early_pong_activity_ages(
    mut pump: LiveUpstreamWebSocket,
    server_writer: &mut TestPeerWriter,
) {
    server_writer
        .send(Message::Close(None))
        .await
        .expect("send terminal close");
    assert_eq!(pump.recv_text().await.unwrap(), None);
    (&mut pump.task).await.expect("pump completes");
    let capture = pump.diagnostics.capture();
    assert_eq!(capture.calls, 1);
    assert_eq!(
        String::from_utf8(capture.bytes.clone()).unwrap(),
        "[threadline] websocket closed source=peer_close_frame code=- reason=- error=- protocol_kind=- connection_age_ms=18000 last_rx_age_ms=0 last_tx_age_ms=6000 last_ping_age_ms=6000 last_pong_age_ms=8000 io_kind=- raw_os_error=-\n"
    );
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_rejects_matching_pong_at_exact_deadline_on_live_socket() {
    let (client, server) = raw_pair(1024).await;
    let (mut server_writer, mut server_reader) = server.split();
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(10))
        .expect("valid watchdog policy");
    let pump = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(1),
            policy,
        ),
    );

    advance(Duration::from_secs(1)).await;
    let nonce = match server_reader
        .next()
        .await
        .expect("client Ping")
        .expect("valid client Ping")
    {
        Message::Ping(nonce) => nonce,
        message => panic!("expected client Ping, got {message:?}"),
    };
    tokio::task::yield_now().await;
    let pong_at_deadline = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = server_writer.send(Message::Pong(nonce)).await;
    });
    tokio::task::yield_now().await;
    advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    assert!(matches!(
        pump.terminal_state(),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::PongDeadline,
            timeout,
            outbound_kind: None,
            ..
        }) if timeout == Duration::from_secs(5)
    ));
    pong_at_deadline
        .await
        .expect("deadline Pong task should finish");
}

#[test]
fn liveness_timeout_is_sticky_and_logs_once_without_nonce_contents() {
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let capture = OverflowLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    tracing::subscriber::with_default(subscriber, || {
        record_liveness_timeout(
            &terminal_state,
            UpstreamLivenessTimeoutCause::WriteDeadline,
            Duration::from_secs(7),
            Duration::from_secs(8),
            Some(UpstreamOutboundKind::Text),
            &CloseDiagnostics::DISABLED,
        );
        record_liveness_timeout(
            &terminal_state,
            UpstreamLivenessTimeoutCause::PongDeadline,
            Duration::from_secs(1),
            Duration::from_secs(2),
            None,
            &CloseDiagnostics::DISABLED,
        );
    });

    assert!(matches!(
        *terminal_state.lock().expect("terminal state lock"),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::WriteDeadline,
            timeout,
            elapsed,
            outbound_kind: Some(UpstreamOutboundKind::Text),
        }) if timeout == Duration::from_secs(7) && elapsed == Duration::from_secs(8)
    ));
    let events = capture.0.lock().expect("liveness log capture lock");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].get("timeout_ms"), Some(&"7000".to_string()));
    assert_eq!(events[0].get("elapsed_ms"), Some(&"8000".to_string()));
    assert!(
        events[0]
            .values()
            .all(|value| !value.contains("current-challenge"))
    );
}

#[test]
fn liveness_timeout_does_not_replace_inbound_overflow() {
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::InboundBufferOverflow(
        InboundBufferOverflow {
            cause: InboundBufferOverflowCause::MessageCount,
            queued_messages: 1,
            queued_bytes: 1,
            incoming_bytes: 1,
            max_messages: 1,
            max_bytes: 1,
        },
    )));

    record_liveness_timeout(
        &terminal_state,
        UpstreamLivenessTimeoutCause::PongDeadline,
        Duration::from_secs(1),
        Duration::from_secs(1),
        None,
        &CloseDiagnostics::DISABLED,
    );

    assert!(matches!(
        *terminal_state.lock().expect("terminal state lock"),
        UpstreamTerminalState::InboundBufferOverflow(_)
    ));
}
