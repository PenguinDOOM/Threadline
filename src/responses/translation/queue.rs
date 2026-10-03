use super::*;

pub(super) fn discard_unaccepted_queued_output(state: &mut ResponseStreamState) {
    state.queued_synthetic_output_text_deltas.clear();
    state.queued_forwarded_event = None;
}

pub(super) fn final_completion_acceptance_error(
    terminal_state: crate::ws_pump::UpstreamTerminalState,
) -> Option<ThreadlineError> {
    match terminal_state {
        crate::ws_pump::UpstreamTerminalState::InboundBufferOverflow(_) => {
            Some(ThreadlineError::UpstreamInboundBufferOverflow)
        }
        crate::ws_pump::UpstreamTerminalState::LivenessTimeout(_) => {
            Some(ThreadlineError::UpstreamLivenessTimeout)
        }
        crate::ws_pump::UpstreamTerminalState::Open
        | crate::ws_pump::UpstreamTerminalState::Closed(_) => None,
    }
}

pub(super) async fn reject_unaccepted_transport_terminal(
    state: &mut ResponseStreamState,
) -> Option<Bytes> {
    if state.queued_final_completed.is_none()
        && let Some(error) = state
            .upstream
            .as_ref()
            .and_then(|upstream| final_completion_acceptance_error(upstream.terminal_state()))
    {
        let stale_continuation = matches!(error, ThreadlineError::UpstreamLivenessTimeout)
            && invalidate_stale_continuation_before_first_upstream_event(state);
        let error = if stale_continuation {
            ThreadlineError::PreviousResponseNotFound
        } else {
            error
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
            if matches!(error, ThreadlineError::UpstreamLivenessTimeout) {
                state.lease.finalize_liveness_timeout_turn();
            } else {
                state.lease.mark_upstream_terminal().await;
            }
        }
        state.lease.release();
        state.final_done_pending = true;
        return Some(sse_terminal_response_failed_chunk(&failed_payload));
    }
    None
}

pub(super) fn drain_queued_synthetic_delta(state: &mut ResponseStreamState) -> Option<Bytes> {
    if let Some(synthetic_delta) = state.queued_synthetic_output_text_deltas.pop_front() {
        let event_type = synthetic_delta
            .payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message")
            .to_string();
        state.downstream_visible_text_delta_count += 1;
        let trace_diagnostics = DownstreamTraceDiagnostics {
            response_id: synthetic_delta.response_id.clone(),
            synthetic_delta_source: Some(synthetic_delta.synthetic_delta_source),
            visible_text_delta_count: Some(1),
            visible_text_length: synthetic_delta
                .payload
                .get("delta")
                .and_then(Value::as_str)
                .map(str::len),
            ..Default::default()
        };
        trace_downstream_sse_event(&downstream_sse_trace_metadata(
            &synthetic_delta.payload,
            DownstreamTraceAction::Forwarded,
            Some(&trace_diagnostics),
        ));
        debug!(event_type, "translation_event_forwarded");
        return Some(sse_json_chunk(&event_type, &synthetic_delta.payload));
    }
    None
}

pub(super) fn drain_queued_forwarded_event(state: &mut ResponseStreamState) -> Option<Bytes> {
    if let Some(forwarded_event) = state.queued_forwarded_event.take() {
        let event_type = forwarded_event
            .payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message")
            .to_string();
        trace_downstream_sse_event(&downstream_sse_trace_metadata(
            &forwarded_event.payload,
            DownstreamTraceAction::Forwarded,
            Some(&DownstreamTraceDiagnostics::default()),
        ));
        debug!(event_type, "translation_event_forwarded");
        return Some(sse_json_chunk(&event_type, &forwarded_event.payload));
    }
    None
}

pub(super) fn drain_queued_completed_event(state: &mut ResponseStreamState) -> Option<Bytes> {
    if let Some(completed) = state.queued_final_completed.take() {
        let response_id = response_id_from_event(&completed.payload);
        let trace_diagnostics = DownstreamTraceDiagnostics {
            response_id: response_id.map(ToString::to_string),
            visible_text_delta_count: Some(state.downstream_visible_text_delta_count),
            sanitized_internal_function_call_count: Some(
                completed.diagnostics.sanitized_internal_function_call_count,
            ),
            sanitized_compaction_count: Some(completed.diagnostics.sanitized_compaction_count),
            completed_visible_message_count: Some(
                completed.diagnostics.completed_visible_message_count,
            ),
            ..Default::default()
        };
        trace_downstream_sse_event(&downstream_sse_trace_metadata(
            &completed.payload,
            DownstreamTraceAction::Terminal,
            Some(&trace_diagnostics),
        ));
        debug!(
            response_id,
            event_type = "response.completed",
            "translation_event_forwarded"
        );
        debug!(response_id, "terminal_response_forwarded");
        state.lease.release();
        state.final_done_pending = true;
        debug!(response_id, "final_done_queued");
        return Some(sse_json_chunk("response.completed", &completed.payload));
    }
    None
}

pub(super) fn drain_queued_output(state: &mut ResponseStreamState) -> Option<Bytes> {
    drain_queued_synthetic_delta(state)
        .or_else(|| drain_queued_forwarded_event(state))
        .or_else(|| drain_queued_completed_event(state))
}

pub(super) async fn reject_no_observable_output(
    state: &mut ResponseStreamState,
    parsed: &Value,
    diagnostics: &CompletedSanitizationDiagnostics,
    response_id: Option<&str>,
    event_type: &str,
) -> StreamProgress {
    let guard_diagnostics = no_observable_output_guard_diagnostics(state, parsed, diagnostics);
    trace_no_observable_output_guard(&guard_diagnostics);
    let failed_payload = no_observable_output_failed_payload(response_id);
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        &failed_payload,
        DownstreamTraceAction::Terminal,
        None,
    ));
    state.upstream = None;
    state.lease.mark_upstream_terminal().await;
    state.lease.release();
    state.final_done_pending = true;
    debug!(response_id, event_type, "translation_event_forwarded");
    debug!(response_id, "terminal_response_forwarded");
    debug!(response_id, "final_done_queued");
    StreamProgress::Yield(sse_terminal_response_failed_chunk(&failed_payload))
}

pub(super) async fn reject_final_completion(
    state: &mut ResponseStreamState,
    parsed: &Value,
    response_id: Option<&str>,
    error: ThreadlineError,
) -> StreamProgress {
    let failed_payload =
        terminal_failed_payload_from_error(parsed.get("response"), response_id, &error);
    discard_unaccepted_queued_output(state);
    state.upstream = None;
    if matches!(error, ThreadlineError::UpstreamLivenessTimeout) {
        state.lease.finalize_liveness_timeout_turn();
    } else {
        state.lease.mark_upstream_terminal().await;
    }
    state.lease.release();
    state.final_done_pending = true;
    StreamProgress::Yield(sse_terminal_response_failed_chunk(&failed_payload))
}

pub(super) fn accept_final_completion(
    state: &mut ResponseStreamState,
    response_id: Option<&str>,
    sanitized_completed: Value,
    diagnostics: CompletedSanitizationDiagnostics,
) -> Result<(), ThreadlineError> {
    if let Some(error) = state
        .upstream
        .as_ref()
        .and_then(|upstream| final_completion_acceptance_error(upstream.terminal_state()))
    {
        return Err(error);
    }

    if let Some(response_id) = response_id {
        state.lease.record_completed_marker_and_disarm(response_id);
    } else {
        state.lease.disarm_active_turn();
    }
    state.upstream = None;
    state.queued_final_completed = Some(QueuedCompletedEvent {
        payload: sanitized_completed,
        diagnostics,
    });

    Ok(())
}
