use super::*;

#[tokio::test]
async fn websocket_pump_sends_active_ping_after_interval() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("local addr");
    let (ping_seen_tx, ping_seen_rx) = oneshot::channel();

    let accept_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept client");
        let websocket = accept_async(stream).await.expect("accept websocket");
        let (_writer, mut reader) = websocket.split();

        while let Some(message) = reader.next().await {
            match message.expect("read websocket message") {
                Message::Ping(payload) => {
                    assert!(!payload.is_empty());
                    assert!(payload.len() <= 125);
                    let _ = ping_seen_tx.send(());
                    break;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    let (stream, _) = connect_async(format!("ws://{address}"))
        .await
        .expect("connect websocket");
    let pump =
        LiveUpstreamWebSocket::from_stream_with_ping_interval(stream, Duration::from_millis(20));

    timeout(Duration::from_secs(2), ping_seen_rx)
        .await
        .expect("active ping should be sent")
        .expect("server should report active ping");

    drop(pump);
    accept_task.await.expect("accept task");
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_processes_delayed_matching_pong_before_deadline_and_schedules_from_ack() {
    let (client, server) = raw_pair(1024).await;
    let (server_writer, server_reader) = server.split();
    let (first_ping_tx, first_ping_rx) = oneshot::channel();
    let (ack_tx, ack_rx) = oneshot::channel();
    let (next_ping_tx, mut next_ping_rx) = oneshot::channel();
    let peer = tokio::spawn(reply_to_delayed_challenge(
        server_writer,
        server_reader,
        first_ping_tx,
        ack_rx,
        next_ping_tx,
    ));
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(5))
        .expect("valid watchdog policy");
    let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
        client,
        Duration::from_secs(1),
        policy,
    );

    advance(Duration::from_secs(1)).await;
    first_ping_rx.await.expect("first Ping should be sent");
    advance(Duration::from_secs(2)).await;
    ack_tx.send(()).expect("allow matching Pong");
    assert_eq!(
        pump.recv_text().await.expect("receive inbound Text"),
        Some("after-delayed-ack".to_string())
    );
    assert!(matches!(pump.terminal_state(), UpstreamTerminalState::Open));
    assert!(next_ping_rx.try_recv().is_err());

    advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert!(next_ping_rx.try_recv().is_err());
    advance(Duration::from_millis(1)).await;
    next_ping_rx
        .await
        .expect("next Ping should be scheduled from ACK time");

    drop(pump);
    peer.await.expect("peer task should complete");
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_stays_open_across_multiple_matching_challenges_without_text() {
    let (client, server) = raw_pair(1024).await;
    let (server_writer, server_reader) = server.split();
    let (completed_tx, completed_rx) = oneshot::channel();
    let (challenge_tx, mut challenge_rx) = mpsc::unbounded_channel();
    let peer = tokio::spawn(reply_to_multiple_challenges(
        server_writer,
        server_reader,
        completed_tx,
        challenge_tx,
    ));
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(5))
        .expect("valid watchdog policy");
    let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
        client,
        Duration::from_secs(1),
        policy,
    );

    for _ in 0..3 {
        advance(Duration::from_secs(6)).await;
        challenge_rx
            .recv()
            .await
            .expect("peer should reply to the current challenge");
        tokio::task::yield_now().await;
    }

    let challenges = completed_rx
        .await
        .expect("peer should observe three challenges");
    assert_eq!(challenges.len(), 3);
    assert!(
        challenges
            .iter()
            .all(|nonce| !nonce.is_empty() && nonce.len() <= 125)
    );
    assert_ne!(challenges[0], challenges[1]);
    assert_ne!(challenges[1], challenges[2]);
    assert!(!pump.is_closed(), "{:?}", pump.terminal_state());
    assert_eq!(
        pump.recv_text().await.expect("receive completion text"),
        Some("response-completed".to_string())
    );
    drop(pump);
    peer.await.expect("peer task should complete");
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_ignores_stale_and_unsolicited_pong_until_the_original_deadline() {
    let limits = UpstreamInboundLimits::new(2, 16).expect("valid limits");
    let (inbound_tx, mut inbound_rx) = mpsc::channel(limits.max_messages());
    let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(8))
        .expect("valid watchdog policy");
    let pump_state = PumpState {
        inbound_tx: &inbound_tx,
        byte_budget: &byte_budget,
        limits,
        terminal_state: &terminal_state,
        watchdog_policy: policy,
        diagnostics: &CloseDiagnostics::DISABLED,
    };
    let sent_at = Instant::now();
    let mut challenge = Some(PendingPongChallenge {
        nonce: b"current-challenge".to_vec(),
        sent_at: Some(sent_at),
        acknowledged_early: false,
    });
    let mut control_flush_needed = false;

    assert_irrelevant_frames_remain_live(&pump_state, &mut challenge, &mut control_flush_needed)
        .await;
    assert!(control_flush_needed);
    assert_eq!(
        inbound_rx
            .recv()
            .await
            .map(|envelope| envelope.payload.into_string()),
        Some("traffic".to_string())
    );

    advance(Duration::from_secs(5)).await;
    assert!(!check_liveness_deadline(
        &terminal_state,
        challenge.as_ref(),
        None,
        policy,
        &CloseDiagnostics::DISABLED,
    ));
    assert!(matches!(
        *terminal_state.lock().expect("terminal state lock"),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::PongDeadline,
            timeout,
            outbound_kind: None,
            ..
        }) if timeout == Duration::from_secs(5)
    ));
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_times_out_at_original_deadline_despite_irrelevant_peer_traffic() {
    let (client, server) = raw_pair(1024).await;
    let (server_writer, server_reader) = server.split();
    let (traffic_tx, traffic_rx) = oneshot::channel();
    let peer = tokio::spawn(send_irrelevant_challenge_traffic(
        server_writer,
        server_reader,
        traffic_tx,
    ));
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(8))
        .expect("valid watchdog policy");
    let pump = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(1),
            policy,
        ),
    );

    advance(Duration::from_secs(1)).await;
    traffic_rx.await.expect("peer should receive client Ping");
    assert_eq!(
        pump.recv_text()
            .await
            .expect("data traffic should be delivered"),
        Some("irrelevant-data".to_string())
    );
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

    drop(pump);
    peer.await.expect("peer task should complete");
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_dispatches_due_ping_before_queued_text_under_ready_traffic() {
    let (client, server) = raw_pair(4096).await;
    let (mut server_writer, mut server_reader) = server.split();
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(5))
        .expect("valid watchdog policy");
    let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
        client,
        Duration::from_secs(1),
        policy,
    );
    tokio::task::yield_now().await;

    for index in 0..4 {
        server_writer
            .send(Message::Text(format!("ready-{index}")))
            .await
            .expect("send ready inbound text");
    }

    advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    pump.send_text("queued-text")
        .await
        .expect("queue outbound text after Ping is due");
    let first = server_reader
        .next()
        .await
        .expect("due Ping frame")
        .expect("valid due Ping frame");
    let nonce = match first {
        Message::Ping(nonce) => nonce,
        message => panic!("expected due Ping before queued Text, got {message:?}"),
    };
    server_writer
        .send(Message::Pong(nonce))
        .await
        .expect("acknowledge due Ping");
    assert!(matches!(
        server_reader
            .next()
            .await
            .expect("queued Text frame")
            .expect("valid queued Text frame"),
        Message::Text(text) if text == "queued-text"
    ));

    drop(pump);
}
