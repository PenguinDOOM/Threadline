use super::*;

pub(super) fn check_liveness_deadline(
    terminal_state: &Arc<StdMutex<UpstreamTerminalState>>,
    pending_challenge: Option<&PendingPongChallenge>,
    operation: Option<(Instant, UpstreamOutboundKind)>,
    watchdog_policy: UpstreamWatchdogPolicy,
    diagnostics: &CloseDiagnostics,
) -> bool {
    let now = Instant::now();
    let Some(metadata) =
        expired_liveness_timeout(pending_challenge, operation, watchdog_policy, now)
    else {
        return true;
    };
    record_liveness_timeout(
        terminal_state,
        metadata.cause,
        metadata.timeout,
        metadata.elapsed,
        metadata.outbound_kind,
        diagnostics,
    );
    false
}

pub(super) fn expired_liveness_timeout(
    pending_challenge: Option<&PendingPongChallenge>,
    operation: Option<(Instant, UpstreamOutboundKind)>,
    watchdog_policy: UpstreamWatchdogPolicy,
    now: Instant,
) -> Option<UpstreamLivenessTimeout> {
    let pong_expired = pending_challenge
        .and_then(|challenge| challenge.sent_at)
        .map(|sent_at| (sent_at + watchdog_policy.pong_timeout(), sent_at))
        .filter(|(deadline, _)| *deadline <= now);
    let write_expired = operation
        .map(|(started_at, kind)| {
            (
                started_at + watchdog_policy.write_timeout(),
                started_at,
                kind,
            )
        })
        .filter(|(deadline, _, _)| *deadline <= now);

    let (cause, timeout, started_at, outbound_kind) = if let Some((pong_deadline, sent_at)) =
        pong_expired
        && write_expired.is_none_or(|(write_deadline, _, _)| pong_deadline <= write_deadline)
    {
        (
            UpstreamLivenessTimeoutCause::PongDeadline,
            watchdog_policy.pong_timeout(),
            sent_at,
            None,
        )
    } else if let Some((_, started_at, kind)) = write_expired {
        (
            UpstreamLivenessTimeoutCause::WriteDeadline,
            watchdog_policy.write_timeout(),
            started_at,
            Some(kind),
        )
    } else {
        return None;
    };
    Some(UpstreamLivenessTimeout {
        cause,
        timeout,
        elapsed: now.saturating_duration_since(started_at),
        outbound_kind,
    })
}

pub(super) fn record_liveness_timeout(
    terminal_state: &Arc<StdMutex<UpstreamTerminalState>>,
    cause: UpstreamLivenessTimeoutCause,
    timeout: Duration,
    elapsed: Duration,
    outbound_kind: Option<UpstreamOutboundKind>,
    diagnostics: &CloseDiagnostics,
) {
    let metadata = UpstreamLivenessTimeout {
        cause,
        timeout,
        elapsed,
        outbound_kind,
    };
    if !commit_terminal(
        terminal_state,
        UpstreamTerminalState::LivenessTimeout(metadata.clone()),
        diagnostics,
        None,
        None,
    ) {
        return;
    }
    tracing::warn!(
        cause = ?metadata.cause,
        timeout_ms = metadata.timeout.as_millis() as u64,
        elapsed_ms = metadata.elapsed.as_millis() as u64,
        outbound_kind = ?metadata.outbound_kind,
        "ws_pump_liveness_timeout"
    );
}

pub(super) fn liveness_timeout_close_metadata() -> UpstreamCloseMetadata {
    UpstreamCloseMetadata {
        code: None,
        reason: None,
        error: Some("upstream websocket liveness timeout".to_string()),
    }
}

pub(super) fn next_watchdog_nonce(counter: &mut u64) -> Vec<u8> {
    let nonce = format!("threadline-{}", *counter).into_bytes();
    *counter = counter.wrapping_add(1);
    nonce
}

pub(super) fn acknowledge_pong(
    payload: &[u8],
    pending_challenge: &mut Option<PendingPongChallenge>,
    policy: UpstreamWatchdogPolicy,
) {
    if pending_challenge.as_mut().is_some_and(|challenge| {
        challenge.nonce == payload
            && challenge
                .sent_at
                .is_none_or(|sent_at| Instant::now() < sent_at + policy.pong_timeout())
    }) {
        let challenge = pending_challenge
            .as_mut()
            .expect("matching challenge remains present");
        if challenge.sent_at.is_some() {
            *pending_challenge = None;
        } else {
            challenge.acknowledged_early = true;
        }
    }
}
