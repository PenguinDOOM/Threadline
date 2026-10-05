use super::*;

#[derive(Clone, Copy)]
pub(super) enum UpstreamCloseSource {
    PeerCloseFrame,
    ReadError,
    WriteError,
    StreamEof,
    OutboundChannelClosed,
    PumpExitFallback,
}

impl UpstreamCloseSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::PeerCloseFrame => "peer_close_frame",
            Self::ReadError => "read_error",
            Self::WriteError => "write_error",
            Self::StreamEof => "stream_eof",
            Self::OutboundChannelClosed => "outbound_channel_closed",
            Self::PumpExitFallback => "pump_exit_fallback",
        }
    }
}

#[derive(Clone)]
pub(super) struct CloseDiagnostics {
    pub(super) started_at: Option<Instant>,
    #[cfg(test)]
    pub(super) capture: Option<Arc<StdMutex<TestDiagnosticWriter>>>,
}

impl CloseDiagnostics {
    #[cfg(test)]
    pub(super) const DISABLED: Self = Self {
        started_at: None,
        #[cfg(test)]
        capture: None,
    };

    pub(super) fn new(enabled: bool) -> Self {
        Self {
            started_at: enabled.then(Instant::now),
            #[cfg(test)]
            capture: Some(Arc::new(StdMutex::new(TestDiagnosticWriter::default()))),
        }
    }

    #[cfg(test)]
    pub(super) fn capture(&self) -> std::sync::MutexGuard<'_, TestDiagnosticWriter> {
        self.capture
            .as_ref()
            .expect("test diagnostic capture")
            .lock()
            .expect("diagnostic capture lock")
    }

    pub(super) fn emit(&self, diagnostic: &SafeTerminalDiagnostic, age_ms: u128) {
        #[cfg(test)]
        write_terminal_diagnostic(&mut *self.capture(), diagnostic, age_ms);
        #[cfg(not(test))]
        write_terminal_diagnostic(&mut std::io::stderr().lock(), diagnostic, age_ms);
    }
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct TestDiagnosticWriter {
    pub(super) calls: usize,
    pub(super) bytes: Vec<u8>,
    pub(super) fail: bool,
    pub(super) terminal_state: Option<Arc<StdMutex<UpstreamTerminalState>>>,
}

#[cfg(test)]
impl std::io::Write for TestDiagnosticWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.calls += 1;
        if let Some(state) = &self.terminal_state {
            assert!(
                state.try_lock().is_ok(),
                "writer must not hold terminal lock"
            );
        }
        if self.fail {
            return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        panic!("diagnostics must not flush or retry")
    }
}

pub(super) struct SafeTransportError {
    kind: &'static str,
    protocol_kind: Option<&'static str>,
    io_kind: Option<&'static str>,
    raw_os_error: Option<i32>,
}

fn safe_protocol_error_kind(
    error: &tokio_tungstenite::tungstenite::error::ProtocolError,
) -> &'static str {
    use tokio_tungstenite::tungstenite::error::ProtocolError;

    match error {
        ProtocolError::WrongHttpMethod => "wrong_http_method",
        ProtocolError::WrongHttpVersion => "wrong_http_version",
        ProtocolError::MissingConnectionUpgradeHeader => "missing_connection_upgrade_header",
        ProtocolError::MissingUpgradeWebSocketHeader => "missing_upgrade_web_socket_header",
        ProtocolError::MissingSecWebSocketVersionHeader => "missing_sec_web_socket_version_header",
        ProtocolError::MissingSecWebSocketKey => "missing_sec_web_socket_key",
        ProtocolError::SecWebSocketAcceptKeyMismatch => "sec_web_socket_accept_key_mismatch",
        ProtocolError::SecWebSocketSubProtocolError(_) => "sec_web_socket_sub_protocol_error",
        ProtocolError::JunkAfterRequest => "junk_after_request",
        ProtocolError::CustomResponseSuccessful => "custom_response_successful",
        ProtocolError::InvalidHeader(_) => "invalid_header",
        ProtocolError::HandshakeIncomplete => "handshake_incomplete",
        ProtocolError::HttparseError(_) => "httparse_error",
        ProtocolError::SendAfterClosing => "send_after_closing",
        ProtocolError::ReceivedAfterClosing => "received_after_closing",
        ProtocolError::NonZeroReservedBits => "non_zero_reserved_bits",
        ProtocolError::UnmaskedFrameFromClient => "unmasked_frame_from_client",
        ProtocolError::MaskedFrameFromServer => "masked_frame_from_server",
        ProtocolError::FragmentedControlFrame => "fragmented_control_frame",
        ProtocolError::ControlFrameTooBig => "control_frame_too_big",
        ProtocolError::UnknownControlFrameType(_) => "unknown_control_frame_type",
        ProtocolError::UnknownDataFrameType(_) => "unknown_data_frame_type",
        ProtocolError::UnexpectedContinueFrame => "unexpected_continue_frame",
        ProtocolError::ExpectedFragment(_) => "expected_fragment",
        ProtocolError::ResetWithoutClosingHandshake => "reset_without_closing_handshake",
        ProtocolError::InvalidOpcode(_) => "invalid_opcode",
        ProtocolError::InvalidCloseSequence => "invalid_close_sequence",
    }
}

pub(super) fn safe_transport_error(error: &TungsteniteError) -> SafeTransportError {
    let kind = match error {
        TungsteniteError::ConnectionClosed => "connection_closed",
        TungsteniteError::AlreadyClosed => "already_closed",
        TungsteniteError::Io(_) => "io",
        TungsteniteError::Tls(_) => "tls",
        TungsteniteError::Capacity(_) => "capacity",
        TungsteniteError::Protocol(_) => "protocol",
        TungsteniteError::WriteBufferFull(_) => "write_buffer_full",
        TungsteniteError::Utf8 => "utf8",
        TungsteniteError::AttackAttempt => "attack_attempt",
        TungsteniteError::Url(_) => "url",
        TungsteniteError::Http(_) => "http",
        TungsteniteError::HttpFormat(_) => "http_format",
    };
    let protocol_kind = match error {
        TungsteniteError::Protocol(protocol_error) => {
            Some(safe_protocol_error_kind(protocol_error))
        }
        _ => None,
    };
    let (io_kind, raw_os_error) = safe_io_error(error);
    SafeTransportError {
        kind,
        protocol_kind,
        io_kind,
        raw_os_error,
    }
}

pub(super) fn safe_io_error(error: &TungsteniteError) -> (Option<&'static str>, Option<i32>) {
    if let TungsteniteError::Io(error) = error {
        (Some(safe_io_error_kind(error.kind())), error.raw_os_error())
    } else {
        (None, None)
    }
}

pub(super) fn safe_io_error_kind(kind: std::io::ErrorKind) -> &'static str {
    use std::io::ErrorKind;
    match kind {
        ErrorKind::NotFound => "not_found",
        ErrorKind::PermissionDenied => "permission_denied",
        ErrorKind::ConnectionRefused => "connection_refused",
        ErrorKind::ConnectionReset => "connection_reset",
        ErrorKind::HostUnreachable => "host_unreachable",
        ErrorKind::NetworkUnreachable => "network_unreachable",
        ErrorKind::ConnectionAborted => "connection_aborted",
        ErrorKind::NotConnected => "not_connected",
        ErrorKind::AddrInUse => "addr_in_use",
        ErrorKind::AddrNotAvailable => "addr_not_available",
        ErrorKind::NetworkDown => "network_down",
        ErrorKind::BrokenPipe => "broken_pipe",
        ErrorKind::AlreadyExists => "already_exists",
        ErrorKind::WouldBlock => "would_block",
        ErrorKind::InvalidInput => "invalid_input",
        ErrorKind::InvalidData => "invalid_data",
        ErrorKind::TimedOut => "timed_out",
        ErrorKind::WriteZero => "write_zero",
        ErrorKind::Interrupted => "interrupted",
        ErrorKind::Unsupported => "unsupported",
        ErrorKind::UnexpectedEof => "unexpected_eof",
        ErrorKind::OutOfMemory => "out_of_memory",
        _ => "other",
    }
}

pub(super) fn safe_close_reason(reason: Option<&str>) -> &'static str {
    match reason {
        None => "-",
        Some("") => "",
        Some("going away") => "going away",
        Some("normal closure") => "normal closure",
        Some(_) => "[redacted]",
    }
}

pub(super) enum SafeTerminalDiagnostic {
    Closed {
        source: UpstreamCloseSource,
        code: Option<u16>,
        reason: &'static str,
        error: Option<SafeTransportError>,
    },
    LivenessTimeout(UpstreamLivenessTimeout),
    InboundBufferOverflow(InboundBufferOverflow),
}

pub(super) fn write_terminal_diagnostic(
    writer: &mut impl std::io::Write,
    diagnostic: &SafeTerminalDiagnostic,
    age_ms: u128,
) {
    let line = match diagnostic {
        SafeTerminalDiagnostic::Closed {
            source,
            code,
            reason,
            error,
        } => closed_diagnostic_line(*source, *code, reason, error.as_ref(), age_ms),
        SafeTerminalDiagnostic::LivenessTimeout(metadata) => {
            liveness_diagnostic_line(metadata, age_ms)
        }
        SafeTerminalDiagnostic::InboundBufferOverflow(metadata) => {
            overflow_diagnostic_line(metadata, age_ms)
        }
    };
    let _ = writer.write(line.as_bytes());
}

pub(super) fn closed_diagnostic_line(
    source: UpstreamCloseSource,
    code: Option<u16>,
    reason: &str,
    error: Option<&SafeTransportError>,
    age_ms: u128,
) -> String {
    let code = code.map_or_else(|| "-".to_string(), |code| code.to_string());
    let kind = error.map_or("-", |error| error.kind);
    let protocol_kind = error.and_then(|error| error.protocol_kind).unwrap_or("-");
    let io_kind = error.and_then(|error| error.io_kind).unwrap_or("-");
    let raw_os_error = error
        .and_then(|error| error.raw_os_error)
        .map_or_else(|| "-".to_string(), |code| code.to_string());
    format!(
        "[threadline] websocket closed source={} code={code} reason={reason} error={kind} protocol_kind={protocol_kind} connection_age_ms={age_ms} io_kind={io_kind} raw_os_error={raw_os_error}\n",
        source.as_str()
    )
}

pub(super) fn liveness_diagnostic_line(metadata: &UpstreamLivenessTimeout, age_ms: u128) -> String {
    let cause = match metadata.cause {
        UpstreamLivenessTimeoutCause::PongDeadline => "pong_deadline",
        UpstreamLivenessTimeoutCause::WriteDeadline => "write_deadline",
    };
    let outbound_kind = match metadata.outbound_kind {
        None => "-",
        Some(UpstreamOutboundKind::Text) => "text",
        Some(UpstreamOutboundKind::Ping) => "ping",
        Some(UpstreamOutboundKind::ControlFlush) => "control_flush",
    };
    format!(
        "[threadline] websocket terminal terminal=liveness_timeout connection_age_ms={age_ms} cause={cause} timeout_ms={} elapsed_ms={} outbound_kind={outbound_kind}\n",
        metadata.timeout.as_millis(),
        metadata.elapsed.as_millis()
    )
}

pub(super) fn overflow_diagnostic_line(metadata: &InboundBufferOverflow, age_ms: u128) -> String {
    let cause = match metadata.cause {
        InboundBufferOverflowCause::MessageCount => "message_count",
        InboundBufferOverflowCause::PayloadBytes => "payload_bytes",
        InboundBufferOverflowCause::TransportSize => "transport_size",
    };
    format!(
        "[threadline] websocket terminal terminal=inbound_buffer_overflow connection_age_ms={age_ms} cause={cause} queued_messages={} queued_bytes={} incoming_bytes={} max_messages={} max_bytes={}\n",
        metadata.queued_messages,
        metadata.queued_bytes,
        metadata.incoming_bytes,
        metadata.max_messages,
        metadata.max_bytes
    )
}

pub(super) fn commit_terminal(
    target: &Arc<StdMutex<UpstreamTerminalState>>,
    next: UpstreamTerminalState,
    diagnostics: &CloseDiagnostics,
    source: Option<UpstreamCloseSource>,
    error: Option<SafeTransportError>,
) -> bool {
    let mut state = target.lock().expect("terminal state lock");
    if !matches!(*state, UpstreamTerminalState::Open) {
        return false;
    }
    *state = next;
    let diagnostic = diagnostics.started_at.map(|started_at| {
        let age_ms = Instant::now()
            .saturating_duration_since(started_at)
            .as_millis();
        let diagnostic = match &*state {
            UpstreamTerminalState::Closed(metadata) => SafeTerminalDiagnostic::Closed {
                source: source.expect("ordinary close has an observation source"),
                code: metadata.code,
                reason: safe_close_reason(metadata.reason.as_deref()),
                error,
            },
            UpstreamTerminalState::LivenessTimeout(metadata) => {
                SafeTerminalDiagnostic::LivenessTimeout(metadata.clone())
            }
            UpstreamTerminalState::InboundBufferOverflow(metadata) => {
                SafeTerminalDiagnostic::InboundBufferOverflow(metadata.clone())
            }
            UpstreamTerminalState::Open => {
                unreachable!("terminal commit requires a terminal state")
            }
        };
        (diagnostic, age_ms)
    });
    drop(state);
    if let Some((diagnostic, age_ms)) = diagnostic {
        diagnostics.emit(&diagnostic, age_ms);
    }
    true
}

pub(super) fn record_close(
    target: &Arc<StdMutex<UpstreamTerminalState>>,
    metadata: UpstreamCloseMetadata,
    source: UpstreamCloseSource,
    diagnostics: &CloseDiagnostics,
) {
    commit_terminal(
        target,
        UpstreamTerminalState::Closed(metadata),
        diagnostics,
        Some(source),
        None,
    );
}

pub(super) fn record_error(
    target: &Arc<StdMutex<UpstreamTerminalState>>,
    error: &TungsteniteError,
    source: UpstreamCloseSource,
    diagnostics: &CloseDiagnostics,
) {
    let safe_error = diagnostics.started_at.map(|_| safe_transport_error(error));
    commit_terminal(
        target,
        UpstreamTerminalState::Closed(UpstreamCloseMetadata {
            code: None,
            reason: None,
            error: Some(error.to_string()),
        }),
        diagnostics,
        Some(source),
        safe_error,
    );
}
