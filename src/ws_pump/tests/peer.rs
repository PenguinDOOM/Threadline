use super::*;

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
    pump_state: &PumpState<'_>,
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
