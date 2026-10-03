use super::*;

pub(super) async fn start_continuation_upstream(
    lease: &mut RetainedSessionLease,
    upstream_request: &mut serde_json::Map<String, Value>,
    previous_response_id: &str,
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
        let error = rewrite_stale_continuation_first_send_error(error);
        if matches!(error, ThreadlineError::PreviousResponseNotFound) {
            trace_stale_continuation(lease, previous_response_id, "first_send_closed");
            lease.release();
            return Err(ThreadlineError::PreviousResponseNotFound);
        }

        return Err(error);
    }

    tokio::task::yield_now().await;
    check_continuation_transport(
        lease,
        &upstream,
        previous_response_id,
        "first_send_closed_after_enqueue",
    )?;

    Ok(upstream)
}

pub(super) async fn start_new_upstream(
    services: &ThreadlineServices,
    lease: &mut RetainedSessionLease,
    upstream_request: &serde_json::Map<String, Value>,
    reconnect_attempted: &mut bool,
) -> Result<Arc<LiveUpstreamWebSocket>, ThreadlineError> {
    let auth = services.auth_provider().load()?;
    let mut upstream = ensure_upstream(services, lease, auth).await?;
    lease.arm_active_turn();
    if let Err(error) = send_response_create(&upstream, upstream_request).await {
        if let Some(reconnected) = attempt_pre_first_event_reconnect(
            services,
            lease,
            upstream_request,
            None,
            false,
            reconnect_attempted,
        )
        .await?
        {
            upstream = reconnected;
        } else {
            return Err(error);
        }
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
        if matches!(error, ThreadlineError::UpstreamInboundBufferOverflow) {
            return Err(error);
        }
        trace_stale_continuation(lease, previous_response_id, stale_reason);
        lease.release();
        return Err(error);
    }
    Ok(())
}

pub(super) fn rewrite_stale_continuation_first_send_error(
    error: ThreadlineError,
) -> ThreadlineError {
    match error {
        ThreadlineError::UpstreamWebSocketClosed | ThreadlineError::UpstreamLivenessTimeout => {
            ThreadlineError::PreviousResponseNotFound
        }
        other => other,
    }
}

pub(super) fn continuation_terminal_error(
    terminal_state: crate::ws_pump::UpstreamTerminalState,
) -> Option<ThreadlineError> {
    match terminal_state {
        crate::ws_pump::UpstreamTerminalState::Open => None,
        crate::ws_pump::UpstreamTerminalState::Closed(_) => {
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

pub(super) async fn attempt_pre_first_event_reconnect(
    services: &ThreadlineServices,
    lease: &mut RetainedSessionLease,
    request_payload: &serde_json::Map<String, Value>,
    previous_response_id: Option<&str>,
    upstream_event_seen: bool,
    reconnect_attempted: &mut bool,
) -> Result<Option<Arc<LiveUpstreamWebSocket>>, ThreadlineError> {
    let Some(previous_response_id) = previous_response_id else {
        return Ok(None);
    };

    if upstream_event_seen || *reconnect_attempted {
        return Ok(None);
    }

    *reconnect_attempted = true;
    lease.detach_upstream_recoverably();
    debug!(
        previous_response_id,
        session_id = %lease.session().session_id,
        thread_id = %lease.session().thread_id,
        window_id = %lease.session().window_id,
        "reconnect_continuation_attempt"
    );

    let auth = services.auth_provider().load()?;
    let upstream = match ensure_upstream(services, lease, auth).await {
        Ok(upstream) => upstream,
        Err(error) => {
            debug!(
                previous_response_id,
                session_id = %lease.session().session_id,
                thread_id = %lease.session().thread_id,
                "reconnect_continuation_failed"
            );
            return Err(error);
        }
    };

    if let Err(error) = send_response_create(&upstream, request_payload).await {
        debug!(
            previous_response_id,
            session_id = %lease.session().session_id,
            thread_id = %lease.session().thread_id,
            "reconnect_continuation_failed"
        );
        return Err(error);
    }

    Ok(Some(upstream))
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
        match upstream.terminal_state() {
            crate::ws_pump::UpstreamTerminalState::Open => return Ok(upstream),
            crate::ws_pump::UpstreamTerminalState::InboundBufferOverflow(_) => {
                return Err(ThreadlineError::UpstreamInboundBufferOverflow);
            }
            crate::ws_pump::UpstreamTerminalState::LivenessTimeout(_) => {
                return Err(ThreadlineError::UpstreamLivenessTimeout);
            }
            crate::ws_pump::UpstreamTerminalState::Closed(_) => {
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
    }
}
