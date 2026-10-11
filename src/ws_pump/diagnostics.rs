use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResponseState {
    NotStarted,
    CreatePending,
    CreateSent,
    InProgress,
    Completed,
    Failed,
    Incomplete,
    Unknown,
}

impl ResponseState {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::CreatePending => "create_pending",
            Self::CreateSent => "create_sent",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Incomplete => "incomplete",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ResponseEvent {
    Started,
    Completed,
    Failed,
    Incomplete,
    Progress,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConsumerPhase {
    NotStarted,
    Preflight,
    ProcessingEvent,
    AwaitingUpstream,
    AwaitingDownstreamPoll,
    ExecutingInternalTool,
    AwaitingIntermediateCompletion,
    SendingFollowup,
    RetainedIdle,
}

impl ConsumerPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Preflight => "preflight",
            Self::ProcessingEvent => "processing_event",
            Self::AwaitingUpstream => "awaiting_upstream",
            Self::AwaitingDownstreamPoll => "awaiting_downstream_poll",
            Self::ExecutingInternalTool => "executing_internal_tool",
            Self::AwaitingIntermediateCompletion => "awaiting_intermediate_completion",
            Self::SendingFollowup => "sending_followup",
            Self::RetainedIdle => "retained_idle",
        }
    }
}

pub(super) struct ResponseObservation {
    state: ResponseState,
    unclassified: usize,
    write_pending: bool,
    ambiguous: bool,
    consumer_phase: ConsumerPhase,
    last_consumer_poll_at: Option<Instant>,
    last_dequeue_at: Option<Instant>,
    internal_tool_started_at: Option<Instant>,
    queued_messages: usize,
    queued_bytes: usize,
    queue_messages_high_water: usize,
    queue_bytes_high_water: usize,
}

#[derive(Clone)]
pub(super) struct ResponseDiagnostics {
    pub(super) record: Option<Arc<StdMutex<ResponseObservation>>>,
}

impl ResponseObservation {
    fn snapshot(&self) -> ResponseState {
        if self.ambiguous || self.unclassified != 0 {
            ResponseState::Unknown
        } else {
            self.state
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RxKind {
    Text,
    Binary,
    Ping,
    Pong,
    Close,
}

impl RxKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Binary => "binary",
            Self::Ping => "ping",
            Self::Pong => "pong",
            Self::Close => "close",
        }
    }
}

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
    pub(super) last_rx: Option<(Instant, RxKind)>,
    pub(super) last_tx_at: Option<Instant>,
    pub(super) last_ping_at: Option<Instant>,
    pub(super) last_pong_at: Option<Instant>,
    pub(super) last_data_at: Option<Instant>,
    pub(super) response: ResponseDiagnostics,
    #[cfg(test)]
    pub(super) capture: Option<Arc<StdMutex<TestDiagnosticWriter>>>,
}

impl CloseDiagnostics {
    #[cfg(test)]
    pub(super) const DISABLED: Self = Self {
        started_at: None,
        last_rx: None,
        last_tx_at: None,
        last_ping_at: None,
        last_pong_at: None,
        last_data_at: None,
        response: ResponseDiagnostics { record: None },
        #[cfg(test)]
        capture: None,
    };

    pub(super) fn new(enabled: bool) -> Self {
        Self {
            started_at: enabled.then(Instant::now),
            last_rx: None,
            last_tx_at: None,
            last_ping_at: None,
            last_pong_at: None,
            last_data_at: None,
            response: ResponseDiagnostics {
                record: enabled.then(|| {
                    Arc::new(StdMutex::new(ResponseObservation {
                        state: ResponseState::NotStarted,
                        unclassified: 0,
                        write_pending: false,
                        ambiguous: false,
                        consumer_phase: ConsumerPhase::NotStarted,
                        last_consumer_poll_at: None,
                        last_dequeue_at: None,
                        internal_tool_started_at: None,
                        queued_messages: 0,
                        queued_bytes: 0,
                        queue_messages_high_water: 0,
                        queue_bytes_high_water: 0,
                    }))
                }),
            },
            #[cfg(test)]
            capture: Some(Arc::new(StdMutex::new(TestDiagnosticWriter::default()))),
        }
    }

    pub(super) fn record_rx_kind(&mut self, kind: RxKind) -> Option<Instant> {
        self.started_at?;
        let received_at = Instant::now();
        self.last_rx = Some((received_at, kind));
        if matches!(kind, RxKind::Text | RxKind::Binary) {
            self.last_data_at = Some(received_at);
        }
        Some(received_at)
    }
}

impl ResponseDiagnostics {
    pub(super) fn record_consumer_phase(
        &self,
        terminal: &Arc<StdMutex<UpstreamTerminalState>>,
        phase: ConsumerPhase,
    ) {
        self.observe(terminal, |response| {
            if response.consumer_phase != phase {
                response.internal_tool_started_at =
                    (phase == ConsumerPhase::ExecutingInternalTool).then(Instant::now);
                response.consumer_phase = phase;
            }
        });
    }

    pub(super) fn record_consumer_poll(&self, terminal: &Arc<StdMutex<UpstreamTerminalState>>) {
        self.observe(terminal, |response| {
            response.last_consumer_poll_at = Some(Instant::now());
        });
    }

    pub(super) fn record_enqueue(
        &self,
        terminal: &Arc<StdMutex<UpstreamTerminalState>>,
        bytes: usize,
        enqueue: impl FnOnce() -> Result<(), mpsc::error::TrySendError<InboundEnvelope>>,
    ) -> Result<(), mpsc::error::TrySendError<InboundEnvelope>> {
        let Some(record) = &self.record else {
            return enqueue();
        };
        let terminal = terminal.lock().expect("terminal state lock");
        if !matches!(*terminal, UpstreamTerminalState::Open) {
            return enqueue();
        }
        let mut response = record.lock().expect("response observation lock");
        enqueue()?;
        response.queued_messages += 1;
        response.queued_bytes += bytes;
        response.queue_messages_high_water = response
            .queue_messages_high_water
            .max(response.queued_messages);
        response.queue_bytes_high_water =
            response.queue_bytes_high_water.max(response.queued_bytes);
        Ok(())
    }

    pub(super) fn record_dequeue(
        &self,
        terminal: &Arc<StdMutex<UpstreamTerminalState>>,
        bytes: usize,
    ) {
        self.observe(terminal, |response| {
            response.queued_messages = response.queued_messages.saturating_sub(1);
            response.queued_bytes = response.queued_bytes.saturating_sub(bytes);
            response.last_dequeue_at = Some(Instant::now());
        });
    }

    fn observe(
        &self,
        terminal: &Arc<StdMutex<UpstreamTerminalState>>,
        update: impl FnOnce(&mut ResponseObservation),
    ) {
        let Some(response) = &self.record else {
            return;
        };
        let terminal = terminal.lock().expect("terminal state lock");
        if matches!(*terminal, UpstreamTerminalState::Open) {
            update(&mut response.lock().expect("response observation lock"));
        }
    }

    pub(super) fn record_create_pending(&self, terminal: &Arc<StdMutex<UpstreamTerminalState>>) {
        self.observe(terminal, |response| {
            if response.write_pending
                || response.unclassified != 0
                || !matches!(
                    response.state,
                    ResponseState::NotStarted
                        | ResponseState::Completed
                        | ResponseState::Failed
                        | ResponseState::Incomplete
                )
            {
                response.ambiguous = true;
            }
            response.write_pending = true;
            response.state = ResponseState::CreatePending;
        });
    }

    pub(super) fn record_create_sent(&self, terminal: &Arc<StdMutex<UpstreamTerminalState>>) {
        self.observe(terminal, |response| {
            if response.ambiguous {
                return;
            }
            if !response.write_pending {
                response.ambiguous = true;
            } else {
                response.write_pending = false;
                if response.state == ResponseState::CreatePending {
                    response.state = ResponseState::CreateSent;
                }
            }
        });
    }

    pub(super) fn record_generic_send(&self, terminal: &Arc<StdMutex<UpstreamTerminalState>>) {
        self.observe(terminal, |response| response.ambiguous = true);
    }

    pub(super) fn record_unclassified(&self, terminal: &Arc<StdMutex<UpstreamTerminalState>>) {
        self.observe(terminal, |response| {
            if let Some(count) = response.unclassified.checked_add(1) {
                response.unclassified = count;
            } else {
                response.ambiguous = true;
            }
        });
    }

    pub(super) fn record_response_event(
        &self,
        terminal: &Arc<StdMutex<UpstreamTerminalState>>,
        event: ResponseEvent,
    ) {
        self.observe(terminal, |response| {
            if let Some(count) = response.unclassified.checked_sub(1) {
                response.unclassified = count;
            } else {
                response.ambiguous = true;
            }
            response.state = match event {
                ResponseEvent::Started => ResponseState::InProgress,
                ResponseEvent::Completed => ResponseState::Completed,
                ResponseEvent::Failed => ResponseState::Failed,
                ResponseEvent::Incomplete => ResponseState::Incomplete,
                ResponseEvent::Progress => response.state,
                ResponseEvent::Unknown => ResponseState::Unknown,
            };
        });
    }

    #[cfg(test)]
    pub(super) fn response_state(&self) -> Option<ResponseState> {
        self.record.as_ref().map(|response| {
            response
                .lock()
                .expect("response observation lock")
                .snapshot()
        })
    }

    #[cfg(test)]
    pub(super) fn test_set_unclassified(
        &self,
        terminal: &Arc<StdMutex<UpstreamTerminalState>>,
        count: usize,
    ) {
        self.observe(terminal, |response| response.unclassified = count);
    }
}

impl CloseDiagnostics {
    pub(super) fn record_pong(&mut self, received_at: Option<Instant>) {
        if self.started_at.is_some() {
            self.last_pong_at = received_at;
        }
    }

    pub(super) fn record_write(&mut self, kind: UpstreamOutboundKind) {
        if self.started_at.is_none() || kind == UpstreamOutboundKind::ControlFlush {
            return;
        }
        let completed_at = Instant::now();
        self.last_tx_at = Some(completed_at);
        if kind == UpstreamOutboundKind::Ping {
            self.last_ping_at = Some(completed_at);
        }
    }

    pub(super) fn snapshot(
        &self,
        started_at: Instant,
        terminal_at: Instant,
    ) -> TerminalActivitySnapshot {
        let response = self
            .response
            .record
            .as_ref()
            .expect("enabled response observation")
            .lock()
            .expect("response observation lock");
        let age = |observed_at: Instant| {
            terminal_at
                .saturating_duration_since(observed_at)
                .as_millis()
        };
        TerminalActivitySnapshot {
            connection_age_ms: age(started_at),
            last_rx_age_ms: self.last_rx.map(|(received_at, _)| age(received_at)),
            last_rx_kind: self.last_rx.map(|(_, kind)| kind.as_str()),
            last_tx_age_ms: self.last_tx_at.map(age),
            last_ping_age_ms: self.last_ping_at.map(age),
            last_pong_age_ms: self.last_pong_at.map(age),
            response_state: response.snapshot(),
            last_classified_response_state: response.state,
            unclassified_data_count: response.unclassified,
            response_ambiguous: response.ambiguous,
            consumer_phase: response.consumer_phase,
            last_consumer_poll_age_ms: response.last_consumer_poll_at.map(age),
            last_dequeue_age_ms: response.last_dequeue_at.map(age),
            internal_tool_age_ms: response.internal_tool_started_at.map(age),
            queue_messages_high_water: response.queue_messages_high_water,
            queue_bytes_high_water: response.queue_bytes_high_water,
            last_data_age_ms: self.last_data_at.map(age),
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

    pub(super) fn emit(&self, diagnostic: &SafeTerminalDiagnostic, ages: TerminalActivitySnapshot) {
        #[cfg(test)]
        write_terminal_diagnostic(&mut *self.capture(), diagnostic, ages);
        #[cfg(not(test))]
        write_terminal_diagnostic(&mut std::io::stderr().lock(), diagnostic, ages);
    }
}

#[derive(Clone, Copy)]
pub(super) struct TerminalActivitySnapshot {
    connection_age_ms: u128,
    last_rx_age_ms: Option<u128>,
    last_rx_kind: Option<&'static str>,
    last_tx_age_ms: Option<u128>,
    last_ping_age_ms: Option<u128>,
    last_pong_age_ms: Option<u128>,
    response_state: ResponseState,
    last_classified_response_state: ResponseState,
    unclassified_data_count: usize,
    response_ambiguous: bool,
    consumer_phase: ConsumerPhase,
    last_consumer_poll_age_ms: Option<u128>,
    last_dequeue_age_ms: Option<u128>,
    internal_tool_age_ms: Option<u128>,
    queue_messages_high_water: usize,
    queue_bytes_high_water: usize,
    last_data_age_ms: Option<u128>,
}

impl std::fmt::Display for TerminalActivitySnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "connection_age_ms={}", self.connection_age_ms)?;
        write_optional_field(formatter, "last_rx_age_ms", self.last_rx_age_ms)?;
        write_optional_field(formatter, "last_rx_kind", self.last_rx_kind)?;
        write_optional_field(formatter, "last_tx_age_ms", self.last_tx_age_ms)?;
        write_optional_field(formatter, "last_ping_age_ms", self.last_ping_age_ms)?;
        write_optional_field(formatter, "last_pong_age_ms", self.last_pong_age_ms)?;
        write!(
            formatter,
            " response_state={}",
            self.response_state.as_str()
        )?;
        write!(
            formatter,
            " last_classified_response_state={} unclassified_data_count={} response_ambiguous={}",
            self.last_classified_response_state.as_str(),
            self.unclassified_data_count,
            self.response_ambiguous
        )?;
        write!(
            formatter,
            " consumer_phase={}",
            self.consumer_phase.as_str()
        )?;
        write_optional_field(
            formatter,
            "last_consumer_poll_age_ms",
            self.last_consumer_poll_age_ms,
        )?;
        write_optional_field(formatter, "last_dequeue_age_ms", self.last_dequeue_age_ms)?;
        write_optional_field(formatter, "internal_tool_age_ms", self.internal_tool_age_ms)?;
        write!(
            formatter,
            " queue_messages_high_water={} queue_bytes_high_water={}",
            self.queue_messages_high_water, self.queue_bytes_high_water
        )?;
        write_optional_field(formatter, "last_data_age_ms", self.last_data_age_ms)?;
        Ok(())
    }
}

fn write_optional_field<T: std::fmt::Display>(
    formatter: &mut std::fmt::Formatter<'_>,
    key: &str,
    value: Option<T>,
) -> std::fmt::Result {
    write!(formatter, " {key}=")?;
    match value {
        Some(value) => write!(formatter, "{value}"),
        None => formatter.write_str("-"),
    }
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct TestDiagnosticWriter {
    pub(super) calls: usize,
    pub(super) bytes: Vec<u8>,
    pub(super) fail: bool,
    pub(super) terminal_state: Option<Arc<StdMutex<UpstreamTerminalState>>>,
    pub(super) response: Option<Arc<StdMutex<ResponseObservation>>>,
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
        if let Some(response) = &self.response {
            assert!(
                response.try_lock().is_ok(),
                "writer must not hold response observation lock"
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
    ages: TerminalActivitySnapshot,
) {
    let line = match diagnostic {
        SafeTerminalDiagnostic::Closed {
            source,
            code,
            reason,
            error,
        } => closed_diagnostic_line(*source, *code, reason, error.as_ref(), ages),
        SafeTerminalDiagnostic::LivenessTimeout(metadata) => {
            liveness_diagnostic_line(metadata, ages)
        }
        SafeTerminalDiagnostic::InboundBufferOverflow(metadata) => {
            overflow_diagnostic_line(metadata, ages)
        }
    };
    let _ = writer.write(line.as_bytes());
}

pub(super) fn closed_diagnostic_line(
    source: UpstreamCloseSource,
    code: Option<u16>,
    reason: &str,
    error: Option<&SafeTransportError>,
    ages: TerminalActivitySnapshot,
) -> String {
    let code = code.map_or_else(|| "-".to_string(), |code| code.to_string());
    let kind = error.map_or("-", |error| error.kind);
    let protocol_kind = error.and_then(|error| error.protocol_kind).unwrap_or("-");
    let io_kind = error.and_then(|error| error.io_kind).unwrap_or("-");
    let raw_os_error = error
        .and_then(|error| error.raw_os_error)
        .map_or_else(|| "-".to_string(), |code| code.to_string());
    format!(
        "[threadline] websocket closed source={} code={code} reason={reason} error={kind} protocol_kind={protocol_kind} {ages} io_kind={io_kind} raw_os_error={raw_os_error}\n",
        source.as_str()
    )
}

pub(super) fn liveness_diagnostic_line(
    metadata: &UpstreamLivenessTimeout,
    ages: TerminalActivitySnapshot,
) -> String {
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
        "[threadline] websocket terminal terminal=liveness_timeout {ages} cause={cause} timeout_ms={} elapsed_ms={} outbound_kind={outbound_kind}\n",
        metadata.timeout.as_millis(),
        metadata.elapsed.as_millis()
    )
}

pub(super) fn overflow_diagnostic_line(
    metadata: &InboundBufferOverflow,
    ages: TerminalActivitySnapshot,
) -> String {
    let cause = match metadata.cause {
        InboundBufferOverflowCause::MessageCount => "message_count",
        InboundBufferOverflowCause::PayloadBytes => "payload_bytes",
        InboundBufferOverflowCause::TransportSize => "transport_size",
    };
    format!(
        "[threadline] websocket terminal terminal=inbound_buffer_overflow {ages} cause={cause} queued_messages={} queued_bytes={} incoming_bytes={} max_messages={} max_bytes={}\n",
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
        let ages = diagnostics.snapshot(started_at, Instant::now());
        let diagnostic = match &*state {
            UpstreamTerminalState::Closed(metadata)
            | UpstreamTerminalState::TransportClosed { metadata, .. } => {
                SafeTerminalDiagnostic::Closed {
                    source: source.expect("ordinary close has an observation source"),
                    code: metadata.code,
                    reason: safe_close_reason(metadata.reason.as_deref()),
                    error,
                }
            }
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
        (diagnostic, ages)
    });
    drop(state);
    if let Some((diagnostic, ages)) = diagnostic {
        diagnostics.emit(&diagnostic, ages);
    }
    true
}

pub(super) fn record_close(
    target: &Arc<StdMutex<UpstreamTerminalState>>,
    metadata: UpstreamCloseMetadata,
    source: UpstreamCloseSource,
    diagnostics: &CloseDiagnostics,
) {
    let terminal = if matches!(source, UpstreamCloseSource::StreamEof) {
        UpstreamTerminalState::TransportClosed {
            cause: UpstreamCloseCause::Eof,
            metadata,
        }
    } else {
        UpstreamTerminalState::Closed(metadata)
    };
    commit_terminal(target, terminal, diagnostics, Some(source), None);
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
        UpstreamTerminalState::TransportClosed {
            cause: match error {
                TungsteniteError::Protocol(_) => UpstreamCloseCause::ProtocolError,
                TungsteniteError::Io(_) => UpstreamCloseCause::Io,
                _ => UpstreamCloseCause::Other,
            },
            metadata: UpstreamCloseMetadata {
                code: None,
                reason: None,
                error: Some(error.to_string()),
            },
        },
        diagnostics,
        Some(source),
        safe_error,
    );
}
