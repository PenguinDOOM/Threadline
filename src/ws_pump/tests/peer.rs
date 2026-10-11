use super::*;

#[tokio::test(start_paused = true)]
async fn close_diagnostics_active_lifetime_does_not_abort_before_normal_peer_close() {
    use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};
    let (client, mut server) = raw_pair(1024).await;
    let pump = LiveUpstreamWebSocket::from_stream_with_policy_and_limits(
        client,
        Duration::from_secs(7200),
        UpstreamWatchdogPolicy::DEFAULT,
        UpstreamInboundLimits::DEFAULT,
        CloseDiagnostics::new(true),
    );
    pump.send_response_create_text("{}".into()).await.unwrap();
    assert!(matches!(
        server.next().await.unwrap().unwrap(),
        Message::Text(_)
    ));
    server.send(Message::Text("started".into())).await.unwrap();
    assert_eq!(pump.recv_text().await.unwrap().as_deref(), Some("started"));
    pump.observe_response_event(ResponseEvent::Started);
    pump.observe_consumer_phase(ConsumerPhase::AwaitingUpstream);
    advance(Duration::from_secs(3600)).await;
    assert_eq!(pump.terminal_state(), UpstreamTerminalState::Open);
    assert_eq!(
        pump.diagnostic_response_state(),
        Some(ResponseState::InProgress)
    );
    server
        .send(Message::Text("unclassified".into()))
        .await
        .unwrap();
    server.send(Message::Ping(vec![8])).await.unwrap();
    assert_eq!(
        server.next().await.unwrap().unwrap(),
        Message::Pong(vec![8])
    );
    server
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        })))
        .await
        .unwrap();
    while !pump.is_closed() {
        tokio::task::yield_now().await;
    }
    let before = pump.diagnostic_output();
    assert_eq!(before.0, 1);
    assert!(before.1.contains("source=peer_close_frame code=1000"));
    assert!(before.1.contains("connection_age_ms=3600000"));
    assert!(before.1.contains("response_state=unknown last_classified_response_state=in_progress unclassified_data_count=1 response_ambiguous=false consumer_phase=awaiting_upstream"));
    assert_eq!(
        pump.recv_text().await.unwrap().as_deref(),
        Some("unclassified")
    );
    pump.observe_response_event(ResponseEvent::Completed);
    assert_eq!(pump.diagnostic_output(), before);
}

#[tokio::test]
async fn close_diagnostics_disabled_skips_consumer_and_queue_observation() {
    let mut diagnostics = CloseDiagnostics::new(false);
    let terminal = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    diagnostics.response.record_consumer_poll(&terminal);
    diagnostics
        .response
        .record_consumer_phase(&terminal, ConsumerPhase::ExecutingInternalTool);
    assert_eq!(diagnostics.record_rx_kind(RxKind::Text), None);
    let (sender, mut receiver) = mpsc::channel(2);
    let budget = Arc::new(Semaphore::new(8));
    let limits = UpstreamInboundLimits::new(2, 8).unwrap();
    for text in ["one", "two"] {
        assert!(try_enqueue_inbound(
            &sender,
            &budget,
            limits,
            text.into(),
            &terminal,
            &diagnostics
        ));
    }
    assert!(!try_enqueue_inbound(
        &sender,
        &budget,
        limits,
        "x".into(),
        &terminal,
        &diagnostics
    ));
    for text in ["one", "two"] {
        let envelope = receiver.try_recv().unwrap();
        diagnostics
            .response
            .record_dequeue(&terminal, envelope.payload.len());
        assert_eq!(&*envelope.payload, text);
    }
    assert_eq!(budget.available_permits(), 8);
    assert!(diagnostics.response.record.is_none());
    assert!(diagnostics.started_at.is_none());
    assert!(diagnostics.last_data_at.is_none());
    assert_eq!(diagnostics.capture().calls, 0);
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_consumer_ages_and_high_water_freeze_after_terminal() {
    let (client, _server) = raw_pair(1024).await;
    let mut pump = LiveUpstreamWebSocket::from_stream_with_close_diagnostics(
        client,
        UpstreamWatchdogPolicy::DEFAULT,
        UpstreamInboundLimits::DEFAULT,
        true,
    );
    pump.observe_consumer_poll();
    pump.observe_consumer_phase(ConsumerPhase::ExecutingInternalTool);
    let (sender, receiver) = mpsc::channel(2);
    let budget = Arc::new(Semaphore::new(16));
    let limits = UpstreamInboundLimits::new(2, 16).unwrap();
    for text in ["one", "second"] {
        assert!(try_enqueue_inbound(
            &sender,
            &budget,
            limits,
            text.into(),
            &pump.terminal_state,
            &pump.diagnostics
        ));
    }
    pump.inbound_rx = Mutex::new(receiver);
    advance(Duration::from_millis(200)).await;
    assert_eq!(pump.recv_text().await.unwrap().as_deref(), Some("one"));
    advance(Duration::from_millis(300)).await;
    record_close(
        &pump.terminal_state,
        empty_close_metadata(),
        UpstreamCloseSource::StreamEof,
        &pump.diagnostics,
    );
    let before = pump.diagnostic_output();
    assert!(before.1.contains("consumer_phase=executing_internal_tool last_consumer_poll_age_ms=500 last_dequeue_age_ms=300 internal_tool_age_ms=500 queue_messages_high_water=2 queue_bytes_high_water=9 last_data_age_ms=-"));
    assert_eq!(pump.recv_text().await.unwrap().as_deref(), Some("second"));
    pump.observe_consumer_poll();
    pump.observe_consumer_phase(ConsumerPhase::RetainedIdle);
    assert_eq!(pump.diagnostic_output(), before);
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_unknown_retains_classification_evidence_after_terminal() {
    let diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    diagnostics.response.record_unclassified(&state);
    diagnostics
        .response
        .record_response_event(&state, ResponseEvent::Completed);
    diagnostics.response.record_unclassified(&state);
    record_close(
        &state,
        empty_close_metadata(),
        UpstreamCloseSource::StreamEof,
        &diagnostics,
    );
    let before = diagnostics.capture().bytes.clone();
    let output = String::from_utf8(before.clone()).unwrap();
    assert!(output.contains("response_state=unknown last_classified_response_state=completed unclassified_data_count=1 response_ambiguous=false"));
    diagnostics
        .response
        .record_response_event(&state, ResponseEvent::Unknown);
    diagnostics.response.record_generic_send(&state);
    assert_eq!(diagnostics.capture().bytes, before);
    assert_eq!(
        diagnostics.response.response_state(),
        Some(ResponseState::Unknown)
    );
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_overflow_records_the_successfully_received_message() {
    let mut diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let limits = UpstreamInboundLimits::new(1, 4).unwrap();
    let (inbound_tx, _inbound_rx) = mpsc::channel(limits.max_messages());
    let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let mut pump_state = PumpState {
        inbound_tx: &inbound_tx,
        byte_budget: &byte_budget,
        limits,
        terminal_state: &state,
        watchdog_policy: UpstreamWatchdogPolicy::DEFAULT,
        diagnostics: &mut diagnostics,
    };
    advance(Duration::from_millis(100)).await;
    assert!(handle_inbound_message(
        Some(Ok(Message::Text("one".to_string()))),
        &mut pump_state,
        &mut None,
        &mut false
    ));
    advance(Duration::from_millis(50)).await;
    assert!(!handle_inbound_message(
        Some(Ok(Message::Binary(vec![1, 2]))),
        &mut pump_state,
        &mut None,
        &mut false
    ));
    assert_eq!(diagnostics.last_rx, Some((Instant::now(), RxKind::Binary)));
    assert!(matches!(
        *state.lock().unwrap(),
        UpstreamTerminalState::InboundBufferOverflow(_)
    ));
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    let output = String::from_utf8(capture.bytes.clone()).unwrap();
    assert!(output.contains("terminal=inbound_buffer_overflow connection_age_ms=150 last_rx_age_ms=0 last_rx_kind=binary last_tx_age_ms=- last_ping_age_ms=- last_pong_age_ms=-"));
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_rx_tracks_control_messages_and_excludes_frame_error_eof() {
    use tokio_tungstenite::tungstenite::protocol::frame::Frame;
    for (message, expected_kind) in [
        (Message::Ping(vec![1]), RxKind::Ping),
        (Message::Pong(vec![2]), RxKind::Pong),
        (Message::Close(None), RxKind::Close),
    ] {
        let mut diagnostics = CloseDiagnostics::new(true);
        let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        advance(Duration::from_millis(25)).await;
        let received_at = Instant::now();
        let keep_running = handle_test_inbound(Some(Ok(message)), &state, &mut diagnostics);
        assert_eq!(diagnostics.last_rx, Some((received_at, expected_kind)));
        assert_eq!(diagnostics.last_pong_at, None);
        if !keep_running {
            assert!(
                String::from_utf8(diagnostics.capture().bytes.clone())
                    .unwrap()
                    .contains("last_rx_age_ms=0 last_rx_kind=close")
            );
        }
    }
    for inbound in [
        Some(Ok(Message::Frame(Frame::pong(Vec::new())))),
        Some(Err(TungsteniteError::ConnectionClosed)),
        None,
    ] {
        let mut diagnostics = CloseDiagnostics::new(true);
        let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        advance(Duration::from_millis(25)).await;
        assert!(handle_test_inbound(
            Some(Ok(Message::Ping(vec![1]))),
            &state,
            &mut diagnostics
        ));
        let received_at = diagnostics.last_rx;
        advance(Duration::from_millis(75)).await;
        handle_test_inbound(inbound, &state, &mut diagnostics);
        assert_eq!(diagnostics.last_rx, received_at);
        record_close(
            &state,
            empty_close_metadata(),
            UpstreamCloseSource::PumpExitFallback,
            &diagnostics,
        );
        assert!(String::from_utf8(diagnostics.capture().bytes.clone()).unwrap().contains("connection_age_ms=100 last_rx_age_ms=75 last_rx_kind=ping last_tx_age_ms=- last_ping_age_ms=- last_pong_age_ms=-"));
    }
}

pub(super) async fn reply_to_ready_challenges(
    mut server_writer: TestPeerWriter,
    mut server_reader: TestPeerReader,
    challenge_sent_tx: oneshot::Sender<()>,
) {
    let mut challenge_sent_tx = Some(challenge_sent_tx);
    while let Some(message) = server_reader.next().await {
        match message.expect("read client frame") {
            Message::Ping(nonce) => {
                server_writer
                    .send(Message::Pong(nonce))
                    .await
                    .expect("send matching Pong");
                server_writer
                    .send(Message::Text("ready-inbound".to_string()))
                    .await
                    .expect("send ready inbound Text");
                if let Some(challenge_sent_tx) = challenge_sent_tx.take() {
                    challenge_sent_tx
                        .send(())
                        .expect("report challenge response");
                }
            }
            Message::Text(_) | Message::Pong(_) | Message::Binary(_) | Message::Close(_) => {}
            Message::Frame(_) => {}
        }
    }
}

pub(super) async fn reply_to_delayed_challenge(
    mut server_writer: TestPeerWriter,
    mut server_reader: TestPeerReader,
    first_ping_tx: oneshot::Sender<()>,
    ack_rx: oneshot::Receiver<()>,
    next_ping_tx: oneshot::Sender<()>,
) {
    let nonce = match server_reader
        .next()
        .await
        .expect("first client frame")
        .expect("valid first client frame")
    {
        Message::Ping(nonce) => nonce,
        message => panic!("expected first client Ping, got {message:?}"),
    };
    first_ping_tx.send(()).expect("report first Ping");
    ack_rx.await.expect("release delayed matching Pong");
    server_writer
        .send(Message::Pong(nonce))
        .await
        .expect("send matching Pong");
    server_writer
        .send(Message::Text("after-delayed-ack".to_string()))
        .await
        .expect("send inbound Text");
    match server_reader
        .next()
        .await
        .expect("next client frame")
        .expect("valid next client frame")
    {
        Message::Ping(_) => next_ping_tx.send(()).expect("report next Ping"),
        message => panic!("expected next client Ping, got {message:?}"),
    }
}

pub(super) async fn reply_to_multiple_challenges(
    mut server_writer: TestPeerWriter,
    mut server_reader: TestPeerReader,
    completed_tx: oneshot::Sender<Vec<Vec<u8>>>,
    challenge_tx: mpsc::UnboundedSender<()>,
) {
    let mut challenges = Vec::new();
    while let Some(message) = server_reader.next().await {
        match message.expect("read client websocket frame") {
            Message::Ping(payload) => {
                challenges.push(payload.clone());
                server_writer
                    .send(Message::Pong(payload))
                    .await
                    .expect("reply with matching Pong");
                challenge_tx.send(()).expect("report matching Pong write");
                if challenges.len() == 3 {
                    server_writer
                        .send(Message::Text("response-completed".to_string()))
                        .await
                        .expect("send completion text");
                    let _ = completed_tx.send(challenges);
                    while server_reader.next().await.is_some() {}
                    return;
                }
            }
            Message::Close(_) => return,
            _ => {}
        }
    }
}

pub(super) async fn send_irrelevant_challenge_traffic(
    mut server_writer: TestPeerWriter,
    mut server_reader: TestPeerReader,
    traffic_tx: oneshot::Sender<()>,
) {
    match server_reader
        .next()
        .await
        .expect("client Ping")
        .expect("valid client Ping")
    {
        Message::Ping(_) => {}
        message => panic!("expected client Ping, got {message:?}"),
    }
    for payload in [
        b"mismatch".as_slice(),
        b"stale".as_slice(),
        b"unsolicited".as_slice(),
    ] {
        server_writer
            .send(Message::Pong(payload.to_vec()))
            .await
            .expect("send irrelevant Pong");
    }
    server_writer
        .send(Message::Text("irrelevant-data".to_string()))
        .await
        .expect("send data traffic");
    server_writer
        .send(Message::Ping(b"server-ping".to_vec()))
        .await
        .expect("send server Ping");
    traffic_tx.send(()).expect("report peer traffic");
    while server_reader.next().await.is_some() {}
}

pub(super) async fn assert_single_held_text_and_automatic_pong(server_reader: &mut TestPeerReader) {
    let mut saw_text = 0;
    let mut saw_latest_automatic_pong = false;
    while let Ok(Some(Ok(message))) = timeout(Duration::from_millis(50), server_reader.next()).await
    {
        match message {
            Message::Text(text) if text == "held-text" => saw_text += 1,
            Message::Pong(payload) if payload.as_slice() == b"server-ping-two" => {
                saw_latest_automatic_pong = true;
            }
            Message::Pong(payload) => assert!(
                !payload.is_empty(),
                "the pump must not add a manual empty Pong"
            ),
            _ => {}
        }
    }
    assert_eq!(
        saw_text, 1,
        "held Text should appear on the wire exactly once"
    );
    assert!(
        saw_latest_automatic_pong,
        "the most recent processed server Ping should receive an automatic Pong"
    );
}

pub(super) async fn assert_ready_traffic_progress(
    server_writer: &mut TestPeerWriter,
    server_reader: &mut TestPeerReader,
) {
    let mut saw_due_ping = None;
    let mut saw_queued_text = false;
    for _ in 0..8 {
        let message = timeout(Duration::from_secs(1), server_reader.next())
            .await
            .expect("ready traffic must not starve client progress")
            .expect("client should keep the connection open")
            .expect("client frame should be valid");
        match message {
            Message::Ping(nonce) => {
                assert!(
                    saw_due_ping.is_none(),
                    "only one client challenge is in flight"
                );
                server_writer
                    .send(Message::Pong(nonce.clone()))
                    .await
                    .expect("acknowledge due client Ping");
                saw_due_ping = Some(nonce);
            }
            Message::Text(text) if text == "queued-text" => saw_queued_text = true,
            Message::Pong(payload) => assert!(
                !payload.is_empty(),
                "the pump must not add a manual empty Pong"
            ),
            _ => {}
        }
        if saw_due_ping.is_some() && saw_queued_text {
            break;
        }
    }
    assert!(
        saw_due_ping.is_some(),
        "due client Ping should not be starved"
    );
    assert!(
        saw_queued_text,
        "queued Text should not be starved by sustained ready inbound traffic"
    );
}

pub(super) async fn assert_read_progress_after_early_pong(
    pump: &LiveUpstreamWebSocket,
    server_writer: &mut TestPeerWriter,
    server_reader: &mut TestPeerReader,
) {
    pump.send_text("flush-complete-barrier")
        .await
        .expect("queue outbound barrier after releasing flush gate");
    assert!(matches!(
        server_reader
            .next()
            .await
            .expect("outbound barrier frame")
            .expect("valid outbound barrier frame"),
        Message::Text(text) if text == "flush-complete-barrier"
    ));
    advance(Duration::from_secs(6)).await;
    assert!(matches!(pump.terminal_state(), UpstreamTerminalState::Open));
    server_writer
        .send(Message::Text("post-deadline-data".to_string()))
        .await
        .expect("send data after the original Pong deadline");
    assert_eq!(
        pump.recv_text()
            .await
            .expect("healthy pump should continue reading after the deadline window"),
        Some("post-deadline-data".to_string())
    );
}

pub(super) async fn assert_irrelevant_frames_remain_live(
    pump_state: &mut PumpState<'_>,
    challenge: &mut Option<PendingPongChallenge>,
    control_flush_needed: &mut bool,
) {
    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"stale-challenge".to_vec()))),
        pump_state,
        challenge,
        control_flush_needed,
    ));
    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"unsolicited-pong".to_vec()))),
        pump_state,
        challenge,
        control_flush_needed,
    ));
    assert!(handle_inbound_message(
        Some(Ok(Message::Text("traffic".to_string()))),
        pump_state,
        challenge,
        control_flush_needed,
    ));
    assert!(handle_inbound_message(
        Some(Ok(Message::Ping(b"server-ping".to_vec()))),
        pump_state,
        challenge,
        control_flush_needed,
    ));
}
