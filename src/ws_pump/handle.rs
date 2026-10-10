use super::*;

impl LiveUpstreamWebSocket {
    pub fn from_stream<S>(stream: WebSocketStream<S>) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_limits(stream, UpstreamInboundLimits::DEFAULT)
    }

    pub fn from_stream_with_limits<S>(
        stream: WebSocketStream<S>,
        limits: UpstreamInboundLimits,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_policy_and_limits(
            stream,
            UPSTREAM_PING_INTERVAL,
            UpstreamWatchdogPolicy::DEFAULT,
            limits,
            CloseDiagnostics::new(false),
        )
    }

    pub fn from_stream_with_watchdog_policy_and_limits<S>(
        stream: WebSocketStream<S>,
        watchdog_policy: UpstreamWatchdogPolicy,
        limits: UpstreamInboundLimits,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_policy_and_limits(
            stream,
            UPSTREAM_PING_INTERVAL,
            watchdog_policy,
            limits,
            CloseDiagnostics::new(false),
        )
    }

    pub(crate) fn from_stream_with_close_diagnostics<S>(
        stream: WebSocketStream<S>,
        watchdog_policy: UpstreamWatchdogPolicy,
        limits: UpstreamInboundLimits,
        enabled: bool,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let diagnostics = CloseDiagnostics::new(enabled);
        Self::from_stream_with_policy_and_limits(
            stream,
            UPSTREAM_PING_INTERVAL,
            watchdog_policy,
            limits,
            diagnostics,
        )
    }

    #[cfg(test)]
    pub(super) fn from_stream_with_ping_interval<S>(
        stream: WebSocketStream<S>,
        ping_interval: Duration,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_policy_and_limits(
            stream,
            ping_interval,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::DEFAULT,
            CloseDiagnostics::new(false),
        )
    }

    pub fn from_stream_with_ping_interval_and_watchdog_policy<S>(
        stream: WebSocketStream<S>,
        ping_interval: Duration,
        watchdog_policy: UpstreamWatchdogPolicy,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_policy_and_limits(
            stream,
            ping_interval,
            watchdog_policy,
            UpstreamInboundLimits::DEFAULT,
            CloseDiagnostics::new(false),
        )
    }

    pub(super) fn from_stream_with_policy_and_limits<S>(
        stream: WebSocketStream<S>,
        ping_interval: Duration,
        watchdog_policy: UpstreamWatchdogPolicy,
        limits: UpstreamInboundLimits,
        diagnostics: CloseDiagnostics,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let (inbound_tx, inbound_rx) = mpsc::channel(limits.max_messages());
        let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        #[cfg(not(test))]
        let response_diagnostics = diagnostics.response.clone();
        #[cfg(test)]
        let test_diagnostics = diagnostics.clone();

        let task = tokio::spawn(run_pump(
            stream,
            outbound_rx,
            OwnedPumpState {
                inbound_tx,
                byte_budget,
                limits,
                terminal_state: Arc::clone(&terminal_state),
                watchdog_policy,
                diagnostics,
            },
            ping_interval,
        ));

        Self {
            outbound_tx,
            inbound_rx: Mutex::new(inbound_rx),
            terminal_state,
            #[cfg(not(test))]
            response_diagnostics,
            task,
            #[cfg(test)]
            after_text_send: None,
            #[cfg(test)]
            pending_text_send: None,
            #[cfg(test)]
            diagnostics: test_diagnostics,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_liveness_timeout_after_first_text_send(
        send_attempts: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        use std::sync::atomic::Ordering;

        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let (_inbound_tx, inbound_rx) = mpsc::channel(1);
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let callback_terminal_state = Arc::clone(&terminal_state);
        let after_text_send = Arc::new(move || {
            send_attempts.fetch_add(1, Ordering::SeqCst);
            record_liveness_timeout(
                &callback_terminal_state,
                UpstreamLivenessTimeoutCause::WriteDeadline,
                Duration::from_millis(1),
                Duration::from_millis(1),
                Some(UpstreamOutboundKind::Text),
                &CloseDiagnostics::new(false),
            );
        });

        Self {
            outbound_tx,
            inbound_rx: Mutex::new(inbound_rx),
            terminal_state,
            task: tokio::spawn(async move {
                let _outbound_rx = outbound_rx;
                std::future::pending::<()>().await;
            }),
            after_text_send: Some(after_text_send),
            pending_text_send: None,
            diagnostics: CloseDiagnostics::new(false),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_followup_send_pending_for_liveness_timeout() -> (Self, TestPendingTextSend) {
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        for _ in 0..OUTBOUND_CHANNEL_CAPACITY {
            outbound_tx
                .try_send(OutboundCommand::Text("occupied".to_string()))
                .expect("fill test outbound queue");
        }
        let (inbound_tx, inbound_rx) = mpsc::channel(2);
        let inbound_byte_budget = Arc::new(Semaphore::new(4096));
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let state = Arc::new(TestPendingTextSendState {
            outbound_rx: Mutex::new(Some(outbound_rx)),
            terminal_state: Arc::clone(&terminal_state),
            pending: Notify::new(),
            send_attempts: std::sync::atomic::AtomicUsize::new(0),
        });
        let fixture = TestPendingTextSend {
            inbound_tx,
            inbound_byte_budget,
            state: Arc::clone(&state),
        };

        (
            Self {
                outbound_tx,
                inbound_rx: Mutex::new(inbound_rx),
                terminal_state,
                task: tokio::spawn(std::future::pending()),
                after_text_send: None,
                pending_text_send: Some(state),
                diagnostics: CloseDiagnostics::new(false),
            },
            fixture,
        )
    }

    pub async fn send_text(&self, text: impl Into<String>) -> Result<(), UpstreamWebSocketError> {
        self.send_command(text, TextSendKind::Generic).await
    }

    pub(crate) async fn send_response_create_text(
        &self,
        text: String,
    ) -> Result<(), UpstreamWebSocketError> {
        self.send_command(text, TextSendKind::ResponseCreate).await
    }

    async fn send_command(
        &self,
        text: impl Into<String>,
        kind: TextSendKind,
    ) -> Result<(), UpstreamWebSocketError> {
        self.terminal_error()?;
        let text = text.into();
        let command = match kind {
            TextSendKind::ResponseCreate => {
                self.response_diagnostics()
                    .record_create_pending(&self.terminal_state);
                OutboundCommand::ResponseCreate(text)
            }
            TextSendKind::Generic => {
                self.response_diagnostics()
                    .record_generic_send(&self.terminal_state);
                OutboundCommand::Text(text)
            }
        };
        #[cfg(test)]
        if let Some(pending_text_send) = &self.pending_text_send {
            pending_text_send
                .send_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let permit = match self.outbound_tx.try_reserve() {
                Ok(permit) => permit,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    pending_text_send.pending.notify_one();
                    self.outbound_tx.reserve().await.map_err(|_| {
                        self.terminal_error()
                            .err()
                            .unwrap_or(UpstreamWebSocketError::OutboundQueueClosed)
                    })?
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(self
                        .terminal_error()
                        .err()
                        .unwrap_or(UpstreamWebSocketError::OutboundQueueClosed));
                }
            };
            permit.send(command);
            return self.terminal_error();
        }
        self.outbound_tx.send(command).await.map_err(|_| {
            self.terminal_error()
                .err()
                .unwrap_or(UpstreamWebSocketError::OutboundQueueClosed)
        })?;
        #[cfg(test)]
        if let Some(after_text_send) = &self.after_text_send {
            after_text_send();
        }
        self.terminal_error()
    }

    pub(crate) fn observe_response_event(&self, event: ResponseEvent) {
        self.response_diagnostics()
            .record_response_event(&self.terminal_state, event);
    }

    fn response_diagnostics(&self) -> &ResponseDiagnostics {
        #[cfg(test)]
        {
            &self.diagnostics.response
        }
        #[cfg(not(test))]
        {
            &self.response_diagnostics
        }
    }

    #[cfg(test)]
    pub(crate) fn diagnostic_response_state(&self) -> Option<ResponseState> {
        let _terminal = self.terminal_state.lock().unwrap();
        self.response_diagnostics().response_state()
    }

    #[cfg(test)]
    pub(crate) fn diagnostic_output(&self) -> (usize, String) {
        let capture = self.diagnostics.capture();
        (
            capture.calls,
            String::from_utf8(capture.bytes.clone()).unwrap(),
        )
    }

    pub async fn recv_text(&self) -> Result<Option<String>, UpstreamWebSocketError> {
        self.terminal_error()?;
        let envelope = self.inbound_rx.lock().await.recv().await;
        self.terminal_error()?;
        Ok(envelope.map(|envelope| envelope.payload.into_string()))
    }

    pub fn is_closed(&self) -> bool {
        !matches!(self.terminal_state(), UpstreamTerminalState::Open)
    }

    pub async fn close_metadata(&self) -> Option<UpstreamCloseMetadata> {
        match self.terminal_state() {
            UpstreamTerminalState::Closed(metadata)
            | UpstreamTerminalState::TransportClosed { metadata, .. } => Some(metadata),
            UpstreamTerminalState::LivenessTimeout(_) => Some(liveness_timeout_close_metadata()),
            UpstreamTerminalState::Open | UpstreamTerminalState::InboundBufferOverflow(_) => None,
        }
    }

    pub fn terminal_state(&self) -> UpstreamTerminalState {
        self.terminal_state
            .lock()
            .expect("terminal state lock")
            .clone()
    }

    pub(super) fn terminal_error(&self) -> Result<(), UpstreamWebSocketError> {
        let terminal_state = self.terminal_state();
        match terminal_state {
            UpstreamTerminalState::InboundBufferOverflow(_) => {
                Err(UpstreamWebSocketError::InboundBufferOverflow)
            }
            UpstreamTerminalState::LivenessTimeout(_) => {
                Err(UpstreamWebSocketError::LivenessTimeout)
            }
            state if state.is_policy_violation() => {
                Err(UpstreamWebSocketError::OutboundQueueClosed)
            }
            UpstreamTerminalState::Open
            | UpstreamTerminalState::Closed(_)
            | UpstreamTerminalState::TransportClosed { .. } => Ok(()),
        }
    }
}

impl Drop for LiveUpstreamWebSocket {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
impl TestPendingTextSend {
    pub(crate) async fn send_inbound_text(&self, text: impl Into<String>) {
        let text = text.into();
        let byte_permit = self
            .inbound_byte_budget
            .clone()
            .acquire_many_owned(text.len().try_into().expect("test inbound text length"))
            .await
            .expect("test inbound byte budget remains open");
        self.inbound_tx
            .send(InboundEnvelope {
                payload: text.into_boxed_str(),
                _byte_permit: byte_permit,
            })
            .await
            .expect("test inbound receiver remains open");
    }

    pub(crate) async fn wait_for_send_pending(&self) {
        self.state.pending.notified().await;
    }

    pub(crate) async fn trigger_liveness_timeout(&self) {
        record_liveness_timeout(
            &self.state.terminal_state,
            UpstreamLivenessTimeoutCause::WriteDeadline,
            Duration::from_millis(1),
            Duration::from_millis(1),
            Some(UpstreamOutboundKind::Text),
            &CloseDiagnostics::new(false),
        );
        self.state.outbound_rx.lock().await.take();
    }

    pub(crate) fn text_send_attempts(&self) -> usize {
        self.state
            .send_attempts
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
