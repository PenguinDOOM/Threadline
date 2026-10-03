use super::*;

pub(super) fn try_enqueue_inbound(
    sender: &mpsc::Sender<InboundEnvelope>,
    byte_budget: &Arc<Semaphore>,
    limits: UpstreamInboundLimits,
    payload: String,
    terminal_state: &Arc<StdMutex<UpstreamTerminalState>>,
    diagnostics: &CloseDiagnostics,
) -> bool {
    let incoming_bytes = payload.len();
    if !payload_fits_inbound_byte_limit(incoming_bytes, limits) {
        record_inbound_overflow(
            terminal_state,
            inbound_overflow(
                InboundBufferOverflowCause::PayloadBytes,
                sender,
                byte_budget,
                limits,
                incoming_bytes,
            ),
            diagnostics,
        );
        return false;
    }
    let permit_count = u32::try_from(incoming_bytes)
        .expect("validated inbound byte limit always fits the semaphore permit count");
    let byte_permit = byte_budget.clone().try_acquire_many_owned(permit_count);
    let cause = match byte_permit {
        Ok(byte_permit) => match sender.try_send(InboundEnvelope {
            payload: payload.into_boxed_str(),
            _byte_permit: byte_permit,
        }) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Full(_)) => InboundBufferOverflowCause::MessageCount,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        },
        Err(_) => InboundBufferOverflowCause::PayloadBytes,
    };
    record_inbound_overflow(
        terminal_state,
        inbound_overflow(cause, sender, byte_budget, limits, incoming_bytes),
        diagnostics,
    );
    false
}

pub(super) fn payload_fits_inbound_byte_limit(
    incoming_bytes: usize,
    limits: UpstreamInboundLimits,
) -> bool {
    incoming_bytes <= limits.max_bytes()
}

pub(super) fn inbound_overflow(
    cause: InboundBufferOverflowCause,
    sender: &mpsc::Sender<InboundEnvelope>,
    byte_budget: &Arc<Semaphore>,
    limits: UpstreamInboundLimits,
    incoming_bytes: usize,
) -> InboundBufferOverflow {
    InboundBufferOverflow {
        cause,
        queued_messages: limits.max_messages().saturating_sub(sender.capacity()),
        queued_bytes: limits
            .max_bytes()
            .saturating_sub(byte_budget.available_permits()),
        incoming_bytes,
        max_messages: limits.max_messages(),
        max_bytes: limits.max_bytes(),
    }
}

pub(super) fn record_inbound_overflow(
    terminal_state: &Arc<StdMutex<UpstreamTerminalState>>,
    overflow: InboundBufferOverflow,
    diagnostics: &CloseDiagnostics,
) {
    if !commit_terminal(
        terminal_state,
        UpstreamTerminalState::InboundBufferOverflow(overflow.clone()),
        diagnostics,
        None,
        None,
    ) {
        return;
    }
    tracing::warn!(
        overflow_cause = ?overflow.cause,
        queued_messages = overflow.queued_messages,
        queued_bytes = overflow.queued_bytes,
        incoming_bytes = overflow.incoming_bytes,
        max_messages = overflow.max_messages,
        max_bytes = overflow.max_bytes,
        recoverable = false,
        "ws_pump_inbound_overflow"
    );
}

pub(super) fn record_transport_overflow(
    target: &Arc<StdMutex<UpstreamTerminalState>>,
    sender: &mpsc::Sender<InboundEnvelope>,
    byte_budget: &Arc<Semaphore>,
    limits: UpstreamInboundLimits,
    error: &TungsteniteError,
    diagnostics: &CloseDiagnostics,
) -> bool {
    let TungsteniteError::Capacity(CapacityError::MessageTooLong { size, .. }) = error else {
        return false;
    };
    record_inbound_overflow(
        target,
        inbound_overflow(
            InboundBufferOverflowCause::TransportSize,
            sender,
            byte_budget,
            limits,
            *size,
        ),
        diagnostics,
    );
    true
}
