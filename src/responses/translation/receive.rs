use super::*;

pub(super) async fn receive_upstream_text(
    state: &mut ResponseStreamState,
) -> Result<Option<String>, StreamProgress> {
    if let Some(text) = state.pending_upstream_events.pop_front() {
        return Ok(Some(text));
    }
    let upstream_result = match state.upstream.as_ref() {
        Some(upstream) => upstream.recv_text().await,
        None => Ok(None),
    };
    let next = match upstream_result {
        Ok(Some(text)) => text,
        Err(crate::ws_pump::UpstreamWebSocketError::InboundBufferOverflow) => {
            return receive_overflow_terminal(state).await;
        }
        Err(crate::ws_pump::UpstreamWebSocketError::LivenessTimeout) => {
            return receive_timeout_terminal(state).await;
        }
        Ok(None) | Err(crate::ws_pump::UpstreamWebSocketError::OutboundQueueClosed) => {
            return receive_closed_transport(state).await;
        }
    };

    Ok(Some(next))
}

pub(super) async fn receive_overflow_terminal(
    state: &mut ResponseStreamState,
) -> Result<Option<String>, StreamProgress> {
    let error = ThreadlineError::UpstreamInboundBufferOverflow;
    let failed_payload = terminal_failed_payload_from_error(None, None, &error);
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        &failed_payload,
        DownstreamTraceAction::Terminal,
        None,
    ));
    discard_unaccepted_queued_output(state);
    state.upstream = None;
    state.lease.mark_upstream_terminal().await;
    state.lease.release();
    state.final_done_pending = true;
    Err(StreamProgress::Yield(sse_terminal_response_failed_chunk(
        &failed_payload,
    )))
}

pub(super) async fn receive_timeout_terminal(
    state: &mut ResponseStreamState,
) -> Result<Option<String>, StreamProgress> {
    let stale_continuation = invalidate_stale_continuation_before_first_upstream_event(state);
    let error = if stale_continuation {
        ThreadlineError::PreviousResponseNotFound
    } else {
        ThreadlineError::UpstreamLivenessTimeout
    };
    let failed_payload = terminal_failed_payload_from_error(None, None, &error);
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        &failed_payload,
        DownstreamTraceAction::Terminal,
        None,
    ));
    discard_unaccepted_queued_output(state);
    state.upstream = None;
    if !stale_continuation {
        if state.followup_send_started {
            state.lease.mark_upstream_terminal().await;
            state.lease.release();
        } else {
            state.lease.finalize_liveness_timeout_turn();
            state.lease.release();
        }
    }
    state.final_done_pending = true;
    Err(StreamProgress::Yield(sse_terminal_response_failed_chunk(
        &failed_payload,
    )))
}

pub(super) async fn receive_closed_transport(
    state: &mut ResponseStreamState,
) -> Result<Option<String>, StreamProgress> {
    let failed_payload =
        terminal_failed_payload_from_error(None, None, &ThreadlineError::UpstreamWebSocketClosed);
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        &failed_payload,
        DownstreamTraceAction::Terminal,
        None,
    ));
    state.upstream = None;
    state.lease.finalize_recoverable_turn();
    state.lease.release();
    state.final_done_pending = true;
    Err(StreamProgress::Yield(sse_terminal_response_failed_chunk(
        &failed_payload,
    )))
}

pub(super) fn invalidate_stale_continuation_before_first_upstream_event(
    state: &mut ResponseStreamState,
) -> bool {
    if recovery::replay_allowed(state, recovery::RecoveryFailure::LivenessTimeout) {
        state.lease.release();
        return true;
    }

    false
}

pub(super) fn terminal_failed_payload(
    response: Option<&Value>,
    response_id: Option<&str>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Value {
    let mut response_object = response
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    if !response_object.contains_key("id")
        && let Some(response_id) = response_id.filter(|value| !value.is_empty())
    {
        response_object.insert("id".to_string(), Value::String(response_id.to_string()));
    }

    Value::Object(serde_json::Map::from_iter([
        (
            "type".to_string(),
            Value::String("response.failed".to_string()),
        ),
        ("response".to_string(), Value::Object(response_object)),
        (
            "error".to_string(),
            Value::Object(serde_json::Map::from_iter([
                ("code".to_string(), Value::String(code.into())),
                ("message".to_string(), Value::String(message.into())),
            ])),
        ),
    ]))
}

pub(super) fn terminal_failed_payload_from_error(
    response: Option<&Value>,
    response_id: Option<&str>,
    error: &ThreadlineError,
) -> Value {
    let public = error.public_error();
    terminal_failed_payload(
        response,
        response_id,
        public.code.into_owned(),
        public.message.into_owned(),
    )
}

pub(super) fn translated_upstream_error_payload(
    parsed: &Value,
    _event_type: &str,
) -> (bool, Value) {
    let error = parsed
        .pointer("/error/error")
        .or_else(|| parsed.get("error"))
        .or_else(|| parsed.pointer("/response/error"));
    let error_code = error
        .and_then(|value| value.get("code"))
        .and_then(safe_scalar_field);
    let error_message = error
        .and_then(|value| value.get("message"))
        .and_then(safe_scalar_field);
    let is_previous_response_not_found = is_upstream_previous_response_not_found_error(
        error_code.as_deref(),
        error_message.as_deref(),
    );
    let failure_kind = recovery::failure_kind(parsed);

    debug!(
        failure_kind = ?failure_kind,
        is_previous_response_not_found,
        "upstream_error_event"
    );
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        parsed,
        DownstreamTraceAction::ErrorTranslated,
        None,
    ));
    let connection_limit = failure_kind == recovery::RecoveryFailure::ConnectionLimit;
    let public_error = if connection_limit {
        ThreadlineError::UpstreamWebSocketConnectionLimit.public_error()
    } else if is_previous_response_not_found {
        ThreadlineError::PreviousResponseNotFound.public_error()
    } else {
        ThreadlineError::UpstreamErrorEvent.public_error()
    };
    let public_message = public_error.message.clone().into_owned();
    let failed_payload = terminal_failed_payload(
        parsed.get("response"),
        response_id_from_event(parsed),
        public_error.code.into_owned(),
        if is_previous_response_not_found || connection_limit {
            public_message
        } else {
            error_message.unwrap_or(public_message)
        },
    );

    (is_previous_response_not_found, failed_payload)
}
