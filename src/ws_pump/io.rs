use super::*;

pub(super) async fn drive_write_operation<S>(
    writer: &mut SplitSink<WebSocketStream<S>, Message>,
    reader: &mut SplitStream<WebSocketStream<S>>,
    pump_state: &PumpState<'_>,
    message: Message,
    outbound_kind: UpstreamOutboundKind,
    liveness_state: PumpLivenessState<'_>,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let operation = WriteOperation {
        started_at: Instant::now(),
        kind: outbound_kind,
    };
    let send = async {
        match outbound_kind {
            UpstreamOutboundKind::ControlFlush => writer.flush().await,
            UpstreamOutboundKind::Text | UpstreamOutboundKind::Ping => writer.send(message).await,
        }
    };
    pin_mut!(send);

    loop {
        if !operation.is_live(pump_state, liveness_state.pending_challenge.as_ref()) {
            return false;
        }
        let earliest_deadline =
            operation.deadline(pump_state, liveness_state.pending_challenge.as_ref());
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(earliest_deadline) => {},
            result = &mut send => return finish_write_operation(result, pump_state),
            inbound = reader.next() => {
                if !handle_inbound_and_schedule(inbound, pump_state, PumpLivenessState {
                    pending_challenge: liveness_state.pending_challenge,
                    control_flush_needed: liveness_state.control_flush_needed,
                    next_ping_due: liveness_state.next_ping_due,
                    ping_interval: liveness_state.ping_interval,
                }) {
                    return false;
                }
            }
        }
    }
}

struct WriteOperation {
    started_at: Instant,
    kind: UpstreamOutboundKind,
}

impl WriteOperation {
    fn is_live(
        &self,
        pump_state: &PumpState<'_>,
        challenge: Option<&PendingPongChallenge>,
    ) -> bool {
        check_liveness_deadline(
            pump_state.terminal_state,
            challenge,
            Some((self.started_at, self.kind)),
            pump_state.watchdog_policy,
            pump_state.diagnostics,
        )
    }

    fn deadline(
        &self,
        pump_state: &PumpState<'_>,
        challenge: Option<&PendingPongChallenge>,
    ) -> Instant {
        let pong_deadline = challenge
            .and_then(|challenge| challenge.sent_at)
            .map(|sent_at| sent_at + pump_state.watchdog_policy.pong_timeout());
        let write_deadline = self.started_at + pump_state.watchdog_policy.write_timeout();
        pong_deadline.map_or(write_deadline, |deadline| deadline.min(write_deadline))
    }
}

fn finish_write_operation(
    result: Result<(), TungsteniteError>,
    pump_state: &PumpState<'_>,
) -> bool {
    if let Err(error) = result {
        record_error(
            pump_state.terminal_state,
            &error,
            UpstreamCloseSource::WriteError,
            pump_state.diagnostics,
        );
        return false;
    }
    true
}

pub(super) fn handle_inbound_message(
    inbound: Option<Result<Message, TungsteniteError>>,
    pump_state: &PumpState<'_>,
    pending_challenge: &mut Option<PendingPongChallenge>,
    control_flush_needed: &mut bool,
) -> bool {
    match inbound {
        Some(Ok(message)) => {
            handle_inbound_frame(message, pump_state, pending_challenge, control_flush_needed)
        }
        Some(Err(error)) => {
            record_read_error(&error, pump_state);
            false
        }
        None => {
            record_close(
                pump_state.terminal_state,
                UpstreamCloseMetadata {
                    code: None,
                    reason: None,
                    error: None,
                },
                UpstreamCloseSource::StreamEof,
                pump_state.diagnostics,
            );
            false
        }
    }
}

fn enqueue_inbound_payload(payload: String, pump_state: &PumpState<'_>) -> bool {
    try_enqueue_inbound(
        pump_state.inbound_tx,
        pump_state.byte_budget,
        pump_state.limits,
        payload,
        pump_state.terminal_state,
        pump_state.diagnostics,
    )
}

fn handle_inbound_frame(
    message: Message,
    pump_state: &PumpState<'_>,
    pending_challenge: &mut Option<PendingPongChallenge>,
    control_flush_needed: &mut bool,
) -> bool {
    match message {
        Message::Text(text) => enqueue_inbound_payload(text.to_string(), pump_state),
        Message::Binary(bytes) => enqueue_inbound_payload(
            String::from_utf8_lossy(bytes.as_ref()).into_owned(),
            pump_state,
        ),
        Message::Ping(payload) => {
            *control_flush_needed = true;
            debug!(payload_len = payload.len(), "ws_pump_ping_received");
            true
        }
        Message::Pong(payload) => {
            acknowledge_pong(&payload, pending_challenge, pump_state.watchdog_policy);
            debug!(payload_len = payload.len(), "ws_pump_pong_received");
            true
        }
        Message::Close(frame) => {
            record_close(
                pump_state.terminal_state,
                UpstreamCloseMetadata {
                    code: frame.as_ref().map(|frame| u16::from(frame.code)),
                    reason: frame.as_ref().map(|frame| frame.reason.to_string()),
                    error: None,
                },
                UpstreamCloseSource::PeerCloseFrame,
                pump_state.diagnostics,
            );
            false
        }
        Message::Frame(_) => true,
    }
}

fn record_read_error(error: &TungsteniteError, pump_state: &PumpState<'_>) {
    if !record_transport_overflow(
        pump_state.terminal_state,
        pump_state.inbound_tx,
        pump_state.byte_budget,
        pump_state.limits,
        error,
        pump_state.diagnostics,
    ) {
        record_error(
            pump_state.terminal_state,
            error,
            UpstreamCloseSource::ReadError,
            pump_state.diagnostics,
        );
    }
}
