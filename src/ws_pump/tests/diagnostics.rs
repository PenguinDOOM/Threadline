use super::*;

#[test]
fn close_diagnostics_response_observation_lifecycle() {
    let diagnostics = CloseDiagnostics::new(true).response;
    let terminal = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    assert_eq!(
        diagnostics.response_state(),
        Some(ResponseState::NotStarted)
    );
    for (event, expected) in [
        (ResponseEvent::Started, ResponseState::InProgress),
        (ResponseEvent::Progress, ResponseState::InProgress),
        (ResponseEvent::Completed, ResponseState::Completed),
        (ResponseEvent::Failed, ResponseState::Failed),
        (ResponseEvent::Incomplete, ResponseState::Incomplete),
        (ResponseEvent::Unknown, ResponseState::Unknown),
    ] {
        diagnostics.record_unclassified(&terminal);
        assert_eq!(diagnostics.response_state(), Some(ResponseState::Unknown));
        diagnostics.record_response_event(&terminal, event);
        assert_eq!(diagnostics.response_state(), Some(expected));
    }
    let diagnostics = CloseDiagnostics::new(true).response;
    diagnostics.record_create_pending(&terminal);
    assert_eq!(
        diagnostics.response_state(),
        Some(ResponseState::CreatePending)
    );
    diagnostics.record_create_sent(&terminal);
    assert_eq!(
        diagnostics.response_state(),
        Some(ResponseState::CreateSent)
    );
    assert!(CloseDiagnostics::new(false).response.record.is_none());
}

#[test]
fn close_diagnostics_response_observation_freezes_before_unlocked_write() {
    for observer_first in [true, false] {
        for next in terminal_observation_cases() {
            assert_terminal_observation_freezes(observer_first, next);
        }
    }
}

#[test]
fn close_diagnostics_counter_faults_and_disabled_observation_are_conservative() {
    for overflow in [false, true] {
        let diagnostics = CloseDiagnostics::new(true);
        let terminal = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        if overflow {
            diagnostics
                .response
                .test_set_unclassified(&terminal, usize::MAX);
            diagnostics.response.record_unclassified(&terminal);
            diagnostics.response.test_set_unclassified(&terminal, 1);
        }
        diagnostics
            .response
            .record_response_event(&terminal, ResponseEvent::Completed);
        assert_eq!(
            diagnostics.response.response_state(),
            Some(ResponseState::Unknown)
        );
        diagnostics.response.record_create_pending(&terminal);
        diagnostics.response.record_create_sent(&terminal);
        assert_eq!(
            diagnostics.response.response_state(),
            Some(ResponseState::Unknown)
        );
    }
    let diagnostics = CloseDiagnostics::new(false);
    let terminal = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let guard = terminal.lock().unwrap();
    diagnostics.response.record_create_pending(&terminal);
    diagnostics.response.record_create_sent(&terminal);
    diagnostics.response.record_generic_send(&terminal);
    diagnostics.response.record_unclassified(&terminal);
    diagnostics
        .response
        .record_response_event(&terminal, ResponseEvent::Unknown);
    assert!(diagnostics.response.record.is_none());
    assert_eq!(diagnostics.response.response_state(), None);
    assert!(diagnostics.started_at.is_none());
    assert_eq!(diagnostics.capture().calls, 0);
    drop(guard);
}

#[tokio::test]
async fn close_diagnostics_cancelled_enqueue_keeps_pending_and_next_create_unknown() {
    let (pump, mut server, started, open, waker) =
        LiveUpstreamWebSocket::test_diagnostic_write_gate().await;
    let writing = started.notified();
    tokio::pin!(writing);
    writing.as_mut().enable();
    pump.outbound_tx
        .try_send(OutboundCommand::Text("writer-blocker".into()))
        .unwrap();
    writing.await;
    fill_gated_outbound_channel(&pump);
    {
        let create = pump.send_response_create_text("cancelled-create".into());
        tokio::pin!(create);
        assert!(futures_util::poll!(&mut create).is_pending());
        assert_eq!(
            pump.diagnostic_response_state(),
            Some(ResponseState::CreatePending)
        );
    }
    assert_eq!(
        pump.diagnostic_response_state(),
        Some(ResponseState::CreatePending)
    );
    open.store(true, Ordering::SeqCst);
    waker.wake();
    for _ in 0..=OUTBOUND_CHANNEL_CAPACITY {
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));
    }
    pump.send_response_create_text("next-create".into())
        .await
        .unwrap();
    assert!(matches!(
        server.next().await.unwrap().unwrap(),
        Message::Text(_)
    ));
    assert_eq!(
        pump.diagnostic_response_state(),
        Some(ResponseState::Unknown)
    );
}

#[tokio::test]
async fn close_diagnostics_enqueue_overflow_keeps_unclassified_input_unknown() {
    for (binary, max_bytes) in [(false, 1), (true, 1), (false, 1024)] {
        let (client, mut server) = raw_pair(8192).await;
        let mut pump = LiveUpstreamWebSocket::from_stream_with_close_diagnostics(
            client,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::new(1, max_bytes).unwrap(),
            true,
        );
        let payload = r#"{"type":"response.completed","id":"overflow-secret"}"#;
        let message = if binary {
            Message::Binary(payload.as_bytes().to_vec())
        } else {
            Message::Text(payload.into())
        };
        server.send(message.clone()).await.unwrap();
        if max_bytes == 1024 {
            server.send(message).await.unwrap();
        }
        (&mut pump.task).await.unwrap();
        assert!(matches!(
            pump.terminal_state(),
            UpstreamTerminalState::InboundBufferOverflow(_)
        ));
        let (calls, output) = pump.diagnostic_output();
        assert_eq!(calls, 1);
        assert!(output.contains("terminal=inbound_buffer_overflow"));
        assert!(output.contains("response_state=unknown"));
        assert!(!output.contains("secret"));
    }
}

#[tokio::test]
async fn close_diagnostics_queued_completion_stays_unknown_through_control_and_reset() {
    for binary in [false, true] {
        let (client, mut server) = raw_pair(8192).await;
        let mut pump = LiveUpstreamWebSocket::from_stream_with_close_diagnostics(
            client,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::DEFAULT,
            true,
        );
        let completed = r#"{"type":"response.completed","id":"response-secret"}"#;
        server
            .send(if binary {
                Message::Binary(completed.as_bytes().to_vec())
            } else {
                Message::Text(completed.into())
            })
            .await
            .unwrap();
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
        assert_eq!(
            pump.diagnostic_response_state(),
            Some(ResponseState::Unknown)
        );
        drop(server);
        (&mut pump.task).await.unwrap();
        let (calls, output) = pump.diagnostic_output();
        assert_eq!(calls, 1);
        assert!(output.contains("protocol_kind=reset_without_closing_handshake"));
        assert!(output.contains("response_state=unknown"));
        assert!(!output.contains("secret"));
    }
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_activity_ages_share_reset_terminal_time() {
    use tokio_tungstenite::tungstenite::error::ProtocolError;

    let (client, _server) = raw_pair(1024).await;
    let (mut writer, mut reader) = client.split();
    let (mut owned, mut inbound_rx) = test_diagnostic_pump_state(true);
    let mut pump_state = owned.borrowed();
    assert_explicit_send_activity(&mut writer, &mut reader, &mut pump_state).await;
    assert_received_activity(&mut pump_state, &mut inbound_rx).await;
    advance(Duration::from_millis(600)).await;
    assert!(!handle_inbound_message(
        Some(Err(TungsteniteError::Protocol(
            ProtocolError::ResetWithoutClosingHandshake
        ))),
        &mut pump_state,
        &mut None,
        &mut false,
    ));
    let capture = owned.diagnostics.capture();
    assert_eq!(capture.calls, 1);
    assert_eq!(
        String::from_utf8(capture.bytes.clone()).unwrap(),
        "[threadline] websocket closed source=read_error code=- reason=- error=protocol protocol_kind=reset_without_closing_handshake connection_age_ms=1000 last_rx_age_ms=600 last_rx_kind=binary last_tx_age_ms=800 last_ping_age_ms=800 last_pong_age_ms=750 response_state=unknown io_kind=- raw_os_error=-\n"
    );
}

async fn assert_explicit_send_activity(
    writer: &mut SplitSink<WebSocketStream<DuplexStream>, Message>,
    reader: &mut SplitStream<WebSocketStream<DuplexStream>>,
    pump_state: &mut PumpState<'_>,
) {
    advance(Duration::from_millis(100)).await;
    assert!(
        drive_test_write_operation(
            writer,
            reader,
            pump_state,
            Message::Text("payload-secret".to_string()),
            UpstreamOutboundKind::Text,
        )
        .await
    );
    let text_at = Instant::now();
    assert_eq!(pump_state.diagnostics.last_tx_at, Some(text_at));
    assert_eq!(pump_state.diagnostics.last_ping_at, None);
    advance(Duration::from_millis(100)).await;
    assert!(
        drive_test_write_operation(
            writer,
            reader,
            pump_state,
            Message::Ping(b"nonce-secret".to_vec()),
            UpstreamOutboundKind::Ping,
        )
        .await
    );
    assert_eq!(pump_state.diagnostics.last_tx_at, Some(Instant::now()));
    assert_eq!(
        pump_state.diagnostics.last_ping_at,
        pump_state.diagnostics.last_tx_at
    );
}

async fn assert_received_activity(
    pump_state: &mut PumpState<'_>,
    inbound_rx: &mut mpsc::Receiver<InboundEnvelope>,
) {
    let mut challenge = Some(PendingPongChallenge {
        nonce: b"nonce-secret".to_vec(),
        sent_at: Some(Instant::now()),
        acknowledged_early: false,
    });
    advance(Duration::from_millis(50)).await;
    assert!(handle_inbound_message(
        Some(Ok(Message::Pong(b"nonce-secret".to_vec()))),
        pump_state,
        &mut challenge,
        &mut false,
    ));
    assert!(challenge.is_none());
    assert_eq!(
        pump_state.diagnostics.last_pong_at,
        pump_state
            .diagnostics
            .last_rx
            .map(|(received_at, _)| received_at)
    );
    advance(Duration::from_millis(100)).await;
    assert!(handle_inbound_message(
        Some(Ok(Message::Text("text-secret".to_string()))),
        pump_state,
        &mut None,
        &mut false,
    ));
    assert_eq!(
        pump_state.diagnostics.last_rx,
        Some((Instant::now(), RxKind::Text))
    );
    assert_eq!(
        inbound_rx.try_recv().unwrap().payload.into_string(),
        "text-secret"
    );
    advance(Duration::from_millis(50)).await;
    assert!(handle_inbound_message(
        Some(Ok(Message::Binary(b"binary-secret".to_vec()))),
        pump_state,
        &mut None,
        &mut false,
    ));
    assert_eq!(
        pump_state.diagnostics.last_rx,
        Some((Instant::now(), RxKind::Binary))
    );
    assert_eq!(
        inbound_rx.try_recv().unwrap().payload.into_string(),
        "binary-secret"
    );
}

#[test]
fn close_diagnostics_eof_and_fallback_keep_the_first_observed_source() {
    for (inbound, source) in [
        (None, "stream_eof"),
        (Some(Ok(Message::Close(None))), "peer_close_frame"),
    ] {
        let mut diagnostics = CloseDiagnostics::new(true);
        let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        assert!(!handle_test_inbound(inbound, &state, &mut diagnostics));
        assert!(!handle_test_inbound(None, &state, &mut diagnostics));
        record_close(
            &state,
            empty_close_metadata(),
            UpstreamCloseSource::PumpExitFallback,
            &diagnostics,
        );
        assert_eq!(
            *state.lock().unwrap(),
            if source == "stream_eof" {
                UpstreamTerminalState::TransportClosed {
                    cause: UpstreamCloseCause::Eof,
                    metadata: empty_close_metadata(),
                }
            } else {
                UpstreamTerminalState::Closed(empty_close_metadata())
            }
        );
        let capture = diagnostics.capture();
        assert_eq!(capture.calls, 1);
        let output = String::from_utf8(capture.bytes.clone()).unwrap();
        assert!(output.starts_with(&format!("[threadline] websocket closed source={source} code=- reason=- error=- protocol_kind=- connection_age_ms=")));
    }
    let diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    record_close(
        &state,
        empty_close_metadata(),
        UpstreamCloseSource::PumpExitFallback,
        &diagnostics,
    );
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::Closed(empty_close_metadata())
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    assert!(
        String::from_utf8(capture.bytes.clone())
            .unwrap()
            .contains("source=pump_exit_fallback")
    );
    assert!(
        String::from_utf8(capture.bytes.clone())
            .unwrap()
            .contains("error=- protocol_kind=-")
    );
}

#[test]
fn close_diagnostics_read_errors_use_typed_fields_and_preserve_original_metadata() {
    let cases = close_diagnostic_io_error_cases()
        .into_iter()
        .chain(close_diagnostic_protocol_error_cases());
    for (error, expected_error, expected_protocol_kind, expected_io_kind, expected_raw_os_error) in
        cases
    {
        assert_read_error_diagnostic_case(
            error,
            expected_error,
            expected_protocol_kind,
            expected_io_kind,
            expected_raw_os_error,
        );
    }
}

type ReadErrorDiagnosticCase = (
    TungsteniteError,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);

fn close_diagnostic_io_error_cases() -> [ReadErrorDiagnosticCase; 2] {
    let raw_os_error = std::io::Error::from_raw_os_error(10054);
    let raw_os_io_kind = safe_io_error_kind(raw_os_error.kind());

    [
        (
            TungsteniteError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "io-secret\r\n\x1b[31mBearer secret",
            )),
            "io",
            "-",
            "connection_reset",
            "-",
        ),
        (
            TungsteniteError::Io(raw_os_error),
            "io",
            "-",
            raw_os_io_kind,
            "10054",
        ),
    ]
}

fn close_diagnostic_protocol_error_cases() -> [ReadErrorDiagnosticCase; 5] {
    use tokio_tungstenite::tungstenite::error::ProtocolError;

    [
        (
            TungsteniteError::Protocol(ProtocolError::InvalidHeader(
                "x-protocol-secret".parse().unwrap(),
            )),
            "protocol",
            "invalid_header",
            "-",
            "-",
        ),
        (
            TungsteniteError::Protocol(ProtocolError::ResetWithoutClosingHandshake),
            "protocol",
            "reset_without_closing_handshake",
            "-",
            "-",
        ),
        (
            TungsteniteError::Protocol(ProtocolError::InvalidOpcode(17)),
            "protocol",
            "invalid_opcode",
            "-",
            "-",
        ),
        (
            TungsteniteError::Protocol(ProtocolError::InvalidCloseSequence),
            "protocol",
            "invalid_close_sequence",
            "-",
            "-",
        ),
        (
            TungsteniteError::Protocol(ProtocolError::MaskedFrameFromServer),
            "protocol",
            "masked_frame_from_server",
            "-",
            "-",
        ),
    ]
}

fn assert_read_error_diagnostic_case(
    error: TungsteniteError,
    expected_error: &str,
    expected_protocol_kind: &str,
    expected_io_kind: &str,
    expected_raw_os_error: &str,
) {
    let original = error.to_string();
    let cause = match &error {
        TungsteniteError::Protocol(_) => UpstreamCloseCause::ProtocolError,
        TungsteniteError::Io(_) => UpstreamCloseCause::Io,
        _ => UpstreamCloseCause::Other,
    };
    let mut diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    assert!(!handle_test_inbound(
        Some(Err(error)),
        &state,
        &mut diagnostics
    ));
    record_close(
        &state,
        empty_close_metadata(),
        UpstreamCloseSource::PumpExitFallback,
        &diagnostics,
    );
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::TransportClosed {
            cause,
            metadata: UpstreamCloseMetadata {
                code: None,
                reason: None,
                error: Some(original),
            }
        }
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    let output = String::from_utf8(capture.bytes.clone()).unwrap();
    let age = output
        .split_once("connection_age_ms=")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .expect("connection age field");
    assert!(!age.is_empty() && age.bytes().all(|byte| byte.is_ascii_digit()));
    assert_eq!(
        output,
        format!(
            "[threadline] websocket closed source=read_error code=- reason=- error={expected_error} protocol_kind={expected_protocol_kind} connection_age_ms={age} last_rx_age_ms=- last_rx_kind=- last_tx_age_ms=- last_ping_age_ms=- last_pong_age_ms=- response_state=not_started io_kind={expected_io_kind} raw_os_error={expected_raw_os_error}\n"
        )
    );
    assert!(output.ends_with('\n'));
    assert_eq!(output.bytes().filter(|byte| *byte == b'\n').count(), 1);
    assert!(!output.contains("secret"));
    assert!(!output.contains(['\r', '\x1b']));
    assert_eq!(output.lines().count(), 1);
}

#[test]
fn close_diagnostics_redact_hostile_reasons_frames_and_error_payloads() {
    let hostile = "Authorization: Bearer frame-secret account-secret session-secret thread-secret response-secret https://secret.invalid prompt-secret tool-secret reasoning-secret\r\n\x1b[31m";
    for reason in [
        hostile,
        "going away ",
        " normal closure",
        "",
        "going away",
        "normal closure",
    ] {
        assert_redacted_peer_close(hostile, reason);
    }
    for frame in [
        Message::Text(hostile.to_string()),
        Message::Binary(hostile.as_bytes().to_vec()),
        Message::Ping(hostile.as_bytes().to_vec()),
    ] {
        assert_redacted_write_buffer_error(frame);
    }
}

#[tokio::test(start_paused = true)]
async fn close_diagnostics_constructor_peer_close_is_independent_of_off_subscriber() {
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    let events = OverflowLogCapture::default();
    let subscriber = tracing_subscriber::registry()
        .with(events.clone())
        .with(tracing_subscriber::filter::LevelFilter::OFF);
    let _subscriber = tracing::subscriber::set_default(subscriber);
    for enabled in [true, false] {
        let (client_io, server_io) = duplex(1024);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let mut pump = LiveUpstreamWebSocket::from_stream_with_close_diagnostics(
            client,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::DEFAULT,
            enabled,
        );
        advance(Duration::from_millis(1234)).await;
        server
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: "going away".into(),
            })))
            .await
            .expect("send peer close");
        assert_eq!(pump.recv_text().await.expect("ordinary close"), None);
        (&mut pump.task).await.expect("pump task completes");
        assert_eq!(
            pump.terminal_state(),
            UpstreamTerminalState::Closed(UpstreamCloseMetadata {
                code: Some(1001),
                reason: Some("going away".to_string()),
                error: None,
            })
        );
        let capture = pump.diagnostics.capture();
        assert_eq!(capture.calls, usize::from(enabled));
        if enabled {
            assert_eq!(
                String::from_utf8(capture.bytes.clone()).unwrap(),
                "[threadline] websocket closed source=peer_close_frame code=1001 reason=going away error=- protocol_kind=- connection_age_ms=1234 last_rx_age_ms=0 last_rx_kind=close last_tx_age_ms=- last_ping_age_ms=- last_pong_age_ms=- response_state=not_started io_kind=- raw_os_error=-\n"
            );
        } else {
            assert!(capture.bytes.is_empty());
        }
    }
    assert!(events.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn close_diagnostics_constructor_channel_close_records_once_in_both_dispatch_branches() {
    for prefer_inbound in [false, true] {
        let (client_io, server_io) = duplex(1024);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let mut pump = LiveUpstreamWebSocket::from_stream_with_close_diagnostics(
            client,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::DEFAULT,
            true,
        );
        if prefer_inbound {
            pump.send_text("dispatch-barrier")
                .await
                .expect("queue Text");
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Text("dispatch-barrier".to_string())
            );
        }
        let (replacement_tx, replacement_rx) = mpsc::channel(1);
        drop(replacement_rx);
        drop(std::mem::replace(&mut pump.outbound_tx, replacement_tx));
        assert_eq!(
            timeout(Duration::from_secs(2), pump.recv_text())
                .await
                .expect("pump releases receiver")
                .expect("ordinary close"),
            None
        );
        (&mut pump.task).await.expect("pump task completes");
        assert_eq!(
            pump.terminal_state(),
            UpstreamTerminalState::Closed(outbound_channel_closed_metadata())
        );
        let capture = pump.diagnostics.capture();
        assert_eq!(capture.calls, 1);
        let output = String::from_utf8(capture.bytes.clone()).unwrap();
        assert!(output.starts_with("[threadline] websocket closed source=outbound_channel_closed code=- reason=[redacted] error=- protocol_kind=- connection_age_ms="));
        assert_eq!(output.lines().count(), 1);
    }
}
