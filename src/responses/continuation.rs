use super::*;

pub(super) async fn start_continuation_upstream(
    lease: &mut RetainedSessionLease,
    upstream_request: &mut serde_json::Map<String, Value>,
    previous_response_id: &str,
    _recovery_local_tools_only: bool,
) -> Result<Arc<LiveUpstreamWebSocket>, ThreadlineError> {
    let Some(upstream) = lease.upstream() else {
        trace_stale_continuation(lease, previous_response_id, "missing_or_closed_upstream");
        lease.release();
        return Err(ThreadlineError::PreviousResponseNotFound);
    };
    check_continuation_transport(
        lease,
        &upstream,
        previous_response_id,
        "missing_or_closed_upstream",
    )?;

    upstream_request.insert(
        "previous_response_id".to_string(),
        Value::String(previous_response_id.to_string()),
    );

    lease.arm_active_turn();
    if let Err(error) = send_response_create(&upstream, upstream_request).await {
        return Err(finish_initial_send_failure(lease, &upstream, error));
    }

    tokio::task::yield_now().await;
    if let Some(error) = super::translation::queue_transport_error(upstream.terminal_state()) {
        return Err(finish_initial_send_failure(lease, &upstream, error));
    }

    Ok(upstream)
}

fn finish_initial_send_failure(
    lease: &mut RetainedSessionLease,
    upstream: &LiveUpstreamWebSocket,
    error: ThreadlineError,
) -> ThreadlineError {
    let error =
        super::translation::queue_transport_error(upstream.terminal_state()).unwrap_or(error);
    if matches!(error, ThreadlineError::UpstreamWebSocketPolicyViolation) {
        lease.finalize_policy_violation_turn();
    }
    lease.release();
    error
}

pub(super) async fn start_new_upstream(
    services: &ThreadlineServices,
    lease: &mut RetainedSessionLease,
    upstream_request: &serde_json::Map<String, Value>,
) -> Result<Arc<LiveUpstreamWebSocket>, ThreadlineError> {
    let auth = services.auth_provider().load()?;
    let upstream = ensure_upstream(services, lease, auth).await?;
    lease.arm_active_turn();
    if let Err(error) = send_response_create(&upstream, upstream_request).await {
        return Err(finish_initial_send_failure(lease, &upstream, error));
    }

    Ok(upstream)
}

pub(super) fn trace_stale_continuation(
    lease: &RetainedSessionLease,
    previous_response_id: &str,
    stale_reason: &str,
) {
    debug!(
        previous_response_id,
        session_id = %lease.session().session_id,
        thread_id = %lease.session().thread_id,
        window_id = %lease.session().window_id,
        stale_reason,
        "stale_previous_response_requires_client_replay"
    );
}

pub(super) fn check_continuation_transport(
    lease: &mut RetainedSessionLease,
    upstream: &LiveUpstreamWebSocket,
    previous_response_id: &str,
    stale_reason: &str,
) -> Result<(), ThreadlineError> {
    if let Some(error) = continuation_terminal_error(upstream.terminal_state()) {
        if matches!(error, ThreadlineError::UpstreamWebSocketPolicyViolation) {
            lease.finalize_policy_violation_turn();
            lease.release();
            return Err(error);
        }
        if matches!(error, ThreadlineError::UpstreamInboundBufferOverflow) {
            return Err(error);
        }
        trace_stale_continuation(lease, previous_response_id, stale_reason);
        lease.release();
        return Err(error);
    }
    Ok(())
}

pub(super) fn continuation_terminal_error(
    terminal_state: crate::ws_pump::UpstreamTerminalState,
) -> Option<ThreadlineError> {
    if terminal_state.is_policy_violation() {
        return Some(ThreadlineError::UpstreamWebSocketPolicyViolation);
    }
    match terminal_state {
        crate::ws_pump::UpstreamTerminalState::Open => None,
        crate::ws_pump::UpstreamTerminalState::Closed(_)
        | crate::ws_pump::UpstreamTerminalState::TransportClosed { .. } => {
            Some(ThreadlineError::PreviousResponseNotFound)
        }
        crate::ws_pump::UpstreamTerminalState::InboundBufferOverflow(_) => {
            Some(ThreadlineError::UpstreamInboundBufferOverflow)
        }
        crate::ws_pump::UpstreamTerminalState::LivenessTimeout(_) => {
            Some(ThreadlineError::PreviousResponseNotFound)
        }
    }
}

pub(super) async fn acquire_lease(
    registry: &RetainedSessionRegistry,
    previous_response_id: Option<&str>,
    downstream_thread_id: Option<&str>,
) -> Result<RetainedSessionLease, ThreadlineError> {
    match previous_response_id {
        Some(previous_response_id) => registry
            .acquire_previous(previous_response_id)
            .await
            .map_err(map_registry_error),
        None => registry
            .acquire_new_with_thread_id(downstream_thread_id.map(ToOwned::to_owned))
            .await
            .map_err(map_registry_error),
    }
}

pub(super) async fn ensure_upstream(
    services: &ThreadlineServices,
    lease: &mut RetainedSessionLease,
    auth: LoadedUpstreamAuth,
) -> Result<Arc<LiveUpstreamWebSocket>, ThreadlineError> {
    if let Some(upstream) = lease.upstream() {
        let terminal = upstream.terminal_state();
        if terminal.is_policy_violation() {
            return Err(ThreadlineError::UpstreamWebSocketPolicyViolation);
        }
        match terminal {
            crate::ws_pump::UpstreamTerminalState::Open => return Ok(upstream),
            crate::ws_pump::UpstreamTerminalState::InboundBufferOverflow(_) => {
                return Err(ThreadlineError::UpstreamInboundBufferOverflow);
            }
            crate::ws_pump::UpstreamTerminalState::LivenessTimeout(_) => {
                return Err(ThreadlineError::UpstreamLivenessTimeout);
            }
            crate::ws_pump::UpstreamTerminalState::Closed(_)
            | crate::ws_pump::UpstreamTerminalState::TransportClosed { .. } => {
                lease.detach_upstream_recoverably();
            }
        }
    }

    let connected = services
        .connector()
        .connect(auth, Some(lease.session().clone()))
        .await?;
    let turn_state = connected
        .turn_state
        .clone()
        .or_else(|| lease.session().turn_state.clone());
    lease.update_turn_state(turn_state).await;
    lease
        .replace_upstream(Some(Arc::clone(&connected.websocket)))
        .await;
    Ok(connected.websocket)
}

pub(super) fn map_registry_error(error: RegistryAcquireError) -> ThreadlineError {
    match error {
        RegistryAcquireError::PreviousResponseNotFound => ThreadlineError::PreviousResponseNotFound,
        RegistryAcquireError::RetainedSessionConflict => ThreadlineError::RetainedSessionConflict,
        RegistryAcquireError::RetainedSessionCapacityExceeded => {
            ThreadlineError::RetainedSessionCapacityExceeded
        }
        RegistryAcquireError::UpstreamInboundBufferOverflow => {
            ThreadlineError::UpstreamInboundBufferOverflow
        }
        RegistryAcquireError::UpstreamWebSocketPolicyViolation => {
            ThreadlineError::UpstreamWebSocketPolicyViolation
        }
    }
}
