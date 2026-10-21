use super::*;

#[tokio::test]
async fn websocket_pump_receives_text_while_outbound_write_is_pending() {
    let (client, server, write_started, write_open, write_waker) = write_gate_pair(64, None).await;
    let (mut server_writer, mut server_reader) = server.split();
    let pump = LiveUpstreamWebSocket::from_stream(client);

    pump.send_text("outbound")
        .await
        .expect("queue outbound text");
    timeout(Duration::from_secs(1), write_started.notified())
        .await
        .expect("writer should become pending at the gate");

    server_writer
        .send(Message::Text("reader-progress".to_string()))
        .await
        .expect("send inbound text");
    assert_eq!(
        timeout(Duration::from_secs(1), pump.recv_text())
            .await
            .expect("reader should progress while writer is pending")
            .expect("receive inbound text"),
        Some("reader-progress".to_string())
    );

    write_open.store(true, Ordering::SeqCst);
    write_waker.wake();
    assert!(matches!(
        timeout(Duration::from_secs(1), server_reader.next())
            .await
            .expect("outbound text should finish after the gate opens"),
        Some(Ok(Message::Text(text))) if text == "outbound"
    ));
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_reads_text_while_automatic_control_flush_is_pending() {
    let (client, server, write_started, _write_open, _write_waker) =
        write_gate_pair(64, None).await;
    let (mut server_writer, _server_reader) = server.split();
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(8), Duration::from_secs(5))
        .expect("valid watchdog policy");
    let pump = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(60),
            policy,
        ),
    );

    server_writer
        .send(Message::Ping(b"server-ping".to_vec()))
        .await
        .expect("send server Ping");
    timeout(Duration::from_secs(1), write_started.notified())
        .await
        .expect("automatic control flush should reach the controlled gate");
    server_writer
        .send(Message::Text("reader-progress".to_string()))
        .await
        .expect("send inbound text while flush is pending");
    assert_eq!(
        pump.recv_text().await.expect("receive inbound text"),
        Some("reader-progress".to_string())
    );

    advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        pump.recv_text().await,
        Err(UpstreamWebSocketError::LivenessTimeout)
    );
    assert!(matches!(
        pump.terminal_state(),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::WriteDeadline,
            outbound_kind: Some(UpstreamOutboundKind::ControlFlush),
            ..
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_reads_matching_pong_and_text_during_sustained_outbound_traffic() {
    let (client, server) = raw_pair(4096).await;
    let (server_writer, server_reader) = server.split();
    let policy = UpstreamWatchdogPolicy::new(Duration::from_millis(1), Duration::from_secs(1))
        .expect("valid watchdog policy");
    let pump = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_millis(1),
            policy,
        ),
    );
    let (challenge_sent_tx, challenge_sent_rx) = oneshot::channel();

    tokio::task::yield_now().await;

    let server_task = tokio::spawn(reply_to_ready_challenges(
        server_writer,
        server_reader,
        challenge_sent_tx,
    ));
    let producer_pump = Arc::clone(&pump);
    let producer = tokio::spawn(async move {
        for _ in 0..256 {
            if producer_pump.send_text("outbound").await.is_err() {
                break;
            }
        }
    });

    advance(Duration::from_millis(1)).await;
    challenge_sent_rx
        .await
        .expect("server should receive challenge");
    advance(Duration::from_millis(1)).await;
    assert_eq!(
        pump.recv_text()
            .await
            .expect("healthy pump should receive Text"),
        Some("ready-inbound".to_string())
    );
    assert!(
        matches!(pump.terminal_state(), UpstreamTerminalState::Open),
        "{:?}",
        pump.terminal_state()
    );

    producer.abort();
    server_task.abort();
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_keeps_a_held_text_deadline_while_reading_server_traffic() {
    let (client, server, write_started, _write_open, _write_waker) =
        write_gate_pair(1024, None).await;
    let (mut server_writer, _server_reader) = server.split();
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5))
        .expect("valid watchdog policy");
    let pump = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(60),
            policy,
        ),
    );

    pump.send_text("held-text").await.expect("queue held text");
    timeout(Duration::from_secs(1), write_started.notified())
        .await
        .expect("Text write should reach the controlled gate");
    for payload in [b"server-ping-one".as_slice(), b"server-ping-two".as_slice()] {
        server_writer
            .send(Message::Ping(payload.to_vec()))
            .await
            .expect("send server Ping while Text write is pending");
    }
    server_writer
        .send(Message::Text("reader-progress".to_string()))
        .await
        .expect("send inbound Text while write is pending");
    assert_eq!(
        pump.recv_text().await.expect("receive inbound Text"),
        Some("reader-progress".to_string())
    );

    advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        pump.recv_text().await,
        Err(UpstreamWebSocketError::LivenessTimeout)
    );
    assert!(matches!(
        pump.terminal_state(),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::WriteDeadline,
            timeout,
            elapsed,
            outbound_kind: Some(UpstreamOutboundKind::Text),
        }) if timeout == Duration::from_secs(5) && elapsed >= timeout
    ));
}

#[tokio::test]
async fn websocket_pump_flushes_automatic_pong_and_one_held_text_after_the_gate_opens() {
    let (client, server, write_started, write_open, write_waker) =
        write_gate_pair(1024, None).await;
    let (mut server_writer, mut server_reader) = server.split();
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5))
        .expect("valid watchdog policy");
    let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
        client,
        Duration::from_secs(60),
        policy,
    );

    pump.send_text("held-text").await.expect("queue held text");
    timeout(Duration::from_secs(1), write_started.notified())
        .await
        .expect("Text write should reach the controlled gate");
    server_writer
        .send(Message::Ping(b"server-ping-one".to_vec()))
        .await
        .expect("send first server Ping");
    server_writer
        .send(Message::Ping(b"server-ping-two".to_vec()))
        .await
        .expect("send second server Ping");
    server_writer
        .send(Message::Text("reader-progress".to_string()))
        .await
        .expect("send inbound Text while write is pending");
    assert_eq!(
        timeout(Duration::from_secs(1), pump.recv_text())
            .await
            .expect("reader should progress while write is pending")
            .expect("receive inbound Text"),
        Some("reader-progress".to_string())
    );

    write_open.store(true, Ordering::SeqCst);
    write_waker.wake();
    assert_single_held_text_and_automatic_pong(&mut server_reader).await;
    assert!(!pump.is_closed(), "{:?}", pump.terminal_state());
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_services_due_ping_and_queued_text_with_sustained_ready_traffic() {
    let (client, server) = raw_pair(4096).await;
    let (mut server_writer, mut server_reader) = server.split();
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5))
        .expect("valid watchdog policy");
    let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
        client,
        Duration::from_secs(1),
        policy,
    );

    pump.send_text("queued-text")
        .await
        .expect("queue outbound Text");
    for index in 0..128 {
        server_writer
            .send(Message::Text(format!("ready-{index}")))
            .await
            .expect("keep inbound traffic ready");
    }
    server_writer
        .send(Message::Ping(b"ready-server-ping".to_vec()))
        .await
        .expect("send server Ping while traffic is ready");
    advance(Duration::from_secs(1)).await;

    assert_ready_traffic_progress(&mut server_writer, &mut server_reader).await;
    assert!(!pump.is_closed(), "{:?}", pump.terminal_state());
}
