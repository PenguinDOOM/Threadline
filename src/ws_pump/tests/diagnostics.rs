use super::*;

#[test]
fn close_diagnostics_eof_and_fallback_keep_the_first_observed_source() {
    for (inbound, source) in [
        (None, "stream_eof"),
        (Some(Ok(Message::Close(None))), "peer_close_frame"),
    ] {
        let diagnostics = CloseDiagnostics::new(true);
        let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        assert!(!handle_test_inbound(inbound, &state, &diagnostics));
        assert!(!handle_test_inbound(None, &state, &diagnostics));
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
    let diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    assert!(!handle_test_inbound(Some(Err(error)), &state, &diagnostics));
    record_close(
        &state,
        empty_close_metadata(),
        UpstreamCloseSource::PumpExitFallback,
        &diagnostics,
    );
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::Closed(UpstreamCloseMetadata {
            code: None,
            reason: None,
            error: Some(original),
        })
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
            "[threadline] websocket closed source=read_error code=- reason=- error={expected_error} protocol_kind={expected_protocol_kind} connection_age_ms={age} io_kind={expected_io_kind} raw_os_error={expected_raw_os_error}\n"
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

fn assert_redacted_peer_close(hostile: &str, reason: &'static str) {
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    let diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    for frame in [
        Message::Text(hostile.to_string()),
        Message::Binary(hostile.as_bytes().to_vec()),
        Message::Ping(hostile.as_bytes().to_vec()),
        Message::Pong(hostile.as_bytes().to_vec()),
    ] {
        assert!(handle_test_inbound(Some(Ok(frame)), &state, &diagnostics));
        assert_eq!(diagnostics.capture().calls, 0);
    }
    assert!(!handle_test_inbound(
        Some(Ok(Message::Close(Some(CloseFrame {
            code: CloseCode::Away,
            reason: reason.into()
        })))),
        &state,
        &diagnostics
    ));
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::Closed(UpstreamCloseMetadata {
            code: Some(1001),
            reason: Some(reason.to_string()),
            error: None,
        })
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    let output = String::from_utf8(capture.bytes.clone()).unwrap();
    let expected = match reason {
        "" | "going away" | "normal closure" => reason,
        _ => "[redacted]",
    };
    assert!(output.contains(&format!("reason={expected} error=- protocol_kind=-")));
    assert!(!output.contains("secret"));
    assert!(!output.contains(['\r', '\x1b']));
    assert_eq!(output.lines().count(), 1);
}

fn assert_redacted_write_buffer_error(frame: Message) {
    let diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let error = TungsteniteError::WriteBufferFull(frame);
    let original = error.to_string();
    record_error(
        &state,
        &error,
        UpstreamCloseSource::WriteError,
        &diagnostics,
    );
    record_close(
        &state,
        empty_close_metadata(),
        UpstreamCloseSource::PumpExitFallback,
        &diagnostics,
    );
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::Closed(UpstreamCloseMetadata {
            code: None,
            reason: None,
            error: Some(original)
        })
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    let output = String::from_utf8(capture.bytes.clone()).unwrap();
    assert!(output.contains("source=write_error"));
    assert!(output.contains("error=write_buffer_full protocol_kind=-"));
    assert!(!output.contains("secret"));
    assert!(!output.contains(['\r', '\x1b']));
    assert_eq!(output.lines().count(), 1);
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
                "[threadline] websocket closed source=peer_close_frame code=1001 reason=going away error=- protocol_kind=- connection_age_ms=1234 io_kind=- raw_os_error=-\n"
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
