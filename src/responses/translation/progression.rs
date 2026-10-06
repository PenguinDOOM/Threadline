use super::*;

pub(super) async fn next_stream_progress(state: &mut ResponseStreamState) -> StreamProgress {
    if let Some(chunk) = reject_unaccepted_transport_terminal(state).await {
        return StreamProgress::Yield(chunk);
    }
    if let Some(chunk) = drain_queued_output(state) {
        return StreamProgress::Yield(chunk);
    }
    if state.final_done_pending {
        state.final_done_pending = false;
        state.done = true;
        debug!("downstream_sse_done_sent");
        return StreamProgress::Yield(sse_done_chunk());
    }
    if state.done {
        debug!("downstream_sse_stream_finished");
        return StreamProgress::Finished;
    }
    let next = match receive_upstream_text(state).await {
        Ok(Some(next)) => next,
        Ok(None) => return StreamProgress::Continue,
        Err(progress) => return progress,
    };
    let parsed = match parse_upstream_event(state, &next).await {
        Ok(parsed) => parsed,
        Err(progress) => return progress,
    };
    process_upstream_event(state, parsed).await
}

pub(super) async fn parse_upstream_event(
    state: &mut ResponseStreamState,
    next: &str,
) -> Result<Value, StreamProgress> {
    state.upstream_event_seen = true;

    if next.trim() == "[DONE]" {
        let failed_payload = terminal_failed_payload(
            None,
            None,
            "upstream_done_before_completed",
            "The upstream websocket emitted [DONE] before Threadline received a terminal response event.",
        );
        trace_downstream_sse_event(&downstream_sse_trace_metadata(
            &failed_payload,
            DownstreamTraceAction::Terminal,
            None,
        ));
        state.upstream = None;
        state.lease.mark_upstream_terminal().await;
        state.lease.release();
        state.final_done_pending = true;
        return Err(StreamProgress::Yield(sse_terminal_response_failed_chunk(
            &failed_payload,
        )));
    }

    let parsed = match serde_json::from_str::<Value>(next) {
        Ok(parsed) => parsed,
        Err(_) => {
            state.upstream = None;
            state.lease.mark_upstream_terminal().await;
            state.done = true;
            return Err(StreamProgress::Yield(sse_error_chunk(
                &ThreadlineError::UpstreamInvalidJson,
            )));
        }
    };

    Ok(parsed)
}

pub(super) async fn process_upstream_event(
    state: &mut ResponseStreamState,
    parsed: Value,
) -> StreamProgress {
    if !recovery::lifecycle_only(&parsed) {
        state.replay_prohibited = true;
    }
    let trace_metadata = UpstreamEventTraceMetadata::from_event(&parsed);
    trace_upstream_event(&trace_metadata);
    state.observable_output.last_upstream_event_type = Some(trace_metadata.event_type.clone());
    if let Some(progress) = handle_internal_tool_event(state, &parsed, &trace_metadata).await {
        return progress;
    }
    let event_type = parsed
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("message")
        .to_string();
    debug!(event_type, "upstream_event_received");
    if suppress_internal_tool_event(state, &parsed, &event_type, &trace_metadata) {
        return StreamProgress::Continue;
    }
    match event_type.as_str() {
        "response.completed" => handle_completed_event(state, &parsed, &event_type).await,
        "response.failed" => handle_failed_event(state, &parsed, &event_type).await,
        "response.incomplete" => handle_incomplete_event(state, &parsed, &event_type).await,
        "error" => handle_error_event(state, &parsed, &event_type).await,
        _ => handle_visible_event(state, parsed, &event_type),
    }
}

pub(super) async fn handle_completed_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
    event_type: &str,
) -> StreamProgress {
    let response_id = response_id_from_event(parsed).map(ToString::to_string);

    if !state.pending_internal_outputs.is_empty() {
        return handle_intermediate_completion(state, parsed, response_id.as_deref()).await;
    }

    if !has_accumulated_visible_assistant_text(&state.visible_assistant_text) {
        queue_visible_text_deltas(
            state,
            synthesized_completed_output_text_delta(parsed),
            "response.completed",
            response_id.as_deref(),
        );
    }

    let (sanitized_completed, diagnostics) =
        sanitized_completed_event_with_diagnostics(parsed, &state.visible_assistant_text);
    record_completed_observable_output(
        &mut state.observable_output,
        &sanitized_completed,
        &diagnostics,
    );

    if state.apply_no_observable_output_failure && !has_downstream_observable_output(state) {
        return reject_no_observable_output(
            state,
            parsed,
            &diagnostics,
            response_id.as_deref(),
            event_type,
        )
        .await;
    }

    if let Err(error) = accept_final_completion(
        state,
        response_id.as_deref(),
        sanitized_completed,
        diagnostics,
    ) {
        return reject_final_completion(state, parsed, response_id.as_deref(), error).await;
    }
    StreamProgress::Continue
}

pub(super) async fn handle_failed_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
    event_type: &str,
) -> StreamProgress {
    if recovery::failure_kind(parsed) == recovery::RecoveryFailure::ConnectionLimit {
        return handle_error_event(state, parsed, event_type).await;
    }
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        parsed,
        DownstreamTraceAction::Terminal,
        None,
    ));
    state.upstream = None;
    state.lease.finalize_recoverable_turn();
    state.lease.release();
    state.final_done_pending = true;
    debug!(event_type, "terminal_response_forwarded");
    debug!(event_type, "final_done_queued");
    StreamProgress::Yield(sse_terminal_response_failed_chunk(parsed))
}

pub(super) async fn handle_incomplete_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
    event_type: &str,
) -> StreamProgress {
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        parsed,
        DownstreamTraceAction::Terminal,
        None,
    ));
    state.upstream = None;
    state.lease.mark_upstream_terminal().await;
    state.lease.release();
    state.final_done_pending = true;
    debug!(event_type, "terminal_response_forwarded");
    debug!(event_type, "final_done_queued");
    StreamProgress::Yield(sse_terminal_response_incomplete_chunk(parsed))
}

pub(super) async fn handle_error_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
    event_type: &str,
) -> StreamProgress {
    let (is_previous_response_not_found, failed_payload) =
        translated_upstream_error_payload(parsed, event_type);
    if is_previous_response_not_found
        && recovery::failure_kind(parsed) != recovery::RecoveryFailure::ConnectionLimit
    {
        state.upstream = None;
        state.lease.finalize_recoverable_turn();
    } else {
        state.upstream = None;
        state.lease.mark_upstream_terminal().await;
    }
    state.lease.release();
    state.final_done_pending = true;
    StreamProgress::Yield(sse_terminal_response_failed_chunk(&failed_payload))
}

pub(super) fn handle_visible_event(
    state: &mut ResponseStreamState,
    parsed: Value,
    event_type: &str,
) -> StreamProgress {
    let trace_diagnostics = if event_type == "response.output_text.delta" {
        record_visible_delta_event(state, &parsed)
    } else {
        if queue_visible_done_event(state, &parsed, event_type) {
            state.queued_forwarded_event = Some(QueuedForwardedEvent { payload: parsed });
            return StreamProgress::Continue;
        }
        DownstreamTraceDiagnostics::default()
    };
    record_forwarded_observable_output(&mut state.observable_output, event_type, &parsed);
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        &parsed,
        DownstreamTraceAction::Forwarded,
        Some(&trace_diagnostics),
    ));
    debug!(event_type, "translation_event_forwarded");
    StreamProgress::Yield(sse_json_chunk(event_type, &parsed))
}
