use super::*;

#[tokio::test]
async fn websocket_pump_records_metadata_when_outbound_channel_closes() {
    let mut pump = connect_test_pump().await;
    let (replacement_tx, replacement_rx) = mpsc::channel(1);
    drop(replacement_rx);
    let original_tx = std::mem::replace(&mut pump.outbound_tx, replacement_tx);
    drop(original_tx);

    let metadata = timeout(Duration::from_secs(2), async {
        loop {
            if pump.is_closed()
                && let Some(metadata) = pump.close_metadata().await
            {
                break metadata;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pump should close after outbound sender drop");

    assert_eq!(metadata, outbound_channel_closed_metadata());
}

#[tokio::test]
async fn websocket_pump_releases_cancelled_receivers_and_dropped_handles() {
    let limits = UpstreamInboundLimits::new(1, 4).expect("valid limits");
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    let (inbound_tx, inbound_rx) = mpsc::channel(1);
    let budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let pump = Arc::new(LiveUpstreamWebSocket {
        outbound_tx,
        inbound_rx: Mutex::new(inbound_rx),
        terminal_state: Arc::clone(&terminal_state),
        task: tokio::spawn(std::future::pending()),
        after_text_send: None,
        pending_text_send: None,
        diagnostics: CloseDiagnostics::DISABLED,
    });
    let (started_tx, started_rx) = oneshot::channel();
    let pending_recv = {
        let pump = Arc::clone(&pump);
        tokio::spawn(async move {
            let _ = started_tx.send(());
            pump.recv_text().await
        })
    };
    started_rx.await.expect("recv task should start");
    tokio::task::yield_now().await;
    pending_recv.abort();
    assert!(
        pending_recv
            .await
            .expect_err("recv task should be cancelled")
            .is_cancelled()
    );

    assert!(try_enqueue_inbound(
        &inbound_tx,
        &budget,
        limits,
        "four".to_string(),
        &terminal_state,
        &CloseDiagnostics::DISABLED,
    ));
    assert_eq!(budget.available_permits(), 0);
    drop(inbound_tx);
    drop(pump);
    assert_eq!(budget.available_permits(), limits.max_bytes());
}

#[tokio::test]
async fn websocket_pump_returns_overflow_when_a_waiting_send_is_released() {
    let (outbound_tx, outbound_rx) = mpsc::channel(1);
    outbound_tx
        .try_send(OutboundCommand::Text("queued".to_string()))
        .expect("fill outbound queue");
    let (_inbound_tx, inbound_rx) = mpsc::channel(1);
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let pump = Arc::new(LiveUpstreamWebSocket {
        outbound_tx,
        inbound_rx: Mutex::new(inbound_rx),
        terminal_state: Arc::clone(&terminal_state),
        task: tokio::spawn(std::future::pending()),
        after_text_send: None,
        pending_text_send: None,
        diagnostics: CloseDiagnostics::DISABLED,
    });
    let (started_tx, started_rx) = oneshot::channel();
    let waiting_send = {
        let pump = Arc::clone(&pump);
        tokio::spawn(async move {
            let _ = started_tx.send(());
            pump.send_text("waiting").await
        })
    };
    started_rx.await.expect("send task should start");
    tokio::task::yield_now().await;
    *terminal_state.lock().expect("terminal state lock") =
        UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
            cause: InboundBufferOverflowCause::MessageCount,
            queued_messages: 1,
            queued_bytes: 0,
            incoming_bytes: 1,
            max_messages: 1,
            max_bytes: 1,
        });
    drop(outbound_rx);

    assert_eq!(
        waiting_send.await.expect("send task should complete"),
        Err(UpstreamWebSocketError::InboundBufferOverflow)
    );
}

#[tokio::test]
async fn websocket_pump_returns_liveness_timeout_when_a_waiting_send_is_released() {
    let (outbound_tx, outbound_rx) = mpsc::channel(1);
    outbound_tx
        .try_send(OutboundCommand::Text("queued".to_string()))
        .expect("fill outbound queue");
    let (_inbound_tx, inbound_rx) = mpsc::channel(1);
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let pump = Arc::new(LiveUpstreamWebSocket {
        outbound_tx,
        inbound_rx: Mutex::new(inbound_rx),
        terminal_state: Arc::clone(&terminal_state),
        task: tokio::spawn(std::future::pending()),
        after_text_send: None,
        pending_text_send: None,
        diagnostics: CloseDiagnostics::DISABLED,
    });
    let (started_tx, started_rx) = oneshot::channel();
    let waiting_send = {
        let pump = Arc::clone(&pump);
        tokio::spawn(async move {
            let _ = started_tx.send(());
            pump.send_text("waiting").await
        })
    };
    started_rx.await.expect("send task should start");
    tokio::task::yield_now().await;
    record_liveness_timeout(
        &terminal_state,
        UpstreamLivenessTimeoutCause::WriteDeadline,
        Duration::from_secs(1),
        Duration::from_secs(1),
        Some(UpstreamOutboundKind::Text),
        &CloseDiagnostics::DISABLED,
    );
    drop(outbound_rx);

    assert_eq!(
        waiting_send.await.expect("send task should complete"),
        Err(UpstreamWebSocketError::LivenessTimeout)
    );
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_liveness_expiry_releases_gated_io_and_waiting_callers() {
    let io_token = Arc::new(());
    let io_released = Arc::downgrade(&io_token);
    let (client, _server, write_started, _, _) = write_gate_pair(1024, Some(io_token)).await;
    let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5))
        .expect("valid watchdog policy");
    let pump = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(60),
            policy,
        ),
    );

    pump.send_text("held").await.expect("queue held Text");
    write_started.notified().await;
    fill_gated_outbound_channel(&pump);

    let waiting_send = {
        let pump = Arc::clone(&pump);
        tokio::spawn(async move { pump.send_text("waiting").await })
    };
    let waiting_recv = {
        let pump = Arc::clone(&pump);
        tokio::spawn(async move { pump.recv_text().await })
    };
    tokio::task::yield_now().await;

    advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    assert_expired_waiters_released(&pump, &io_released, waiting_send, waiting_recv).await;
}

async fn assert_expired_waiters_released(
    pump: &LiveUpstreamWebSocket,
    io_released: &std::sync::Weak<()>,
    waiting_send: JoinHandle<Result<(), UpstreamWebSocketError>>,
    waiting_recv: JoinHandle<Result<Option<String>, UpstreamWebSocketError>>,
) {
    assert!(
        io_released.upgrade().is_none(),
        "expiry must drop gated socket IO"
    );
    assert_eq!(
        waiting_send
            .await
            .expect("waiting send task should complete"),
        Err(UpstreamWebSocketError::LivenessTimeout)
    );
    assert_eq!(
        waiting_recv
            .await
            .expect("waiting receive task should complete"),
        Err(UpstreamWebSocketError::LivenessTimeout)
    );
    assert!(matches!(
        pump.terminal_state(),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::WriteDeadline,
            outbound_kind: Some(UpstreamOutboundKind::Text),
            ..
        })
    ));
}
