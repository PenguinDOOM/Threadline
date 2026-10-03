use super::*;

pub(super) async fn handle_internal_tool_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
    trace_metadata: &UpstreamEventTraceMetadata,
) -> Option<StreamProgress> {
    if !state.execute_internal_tools {
        return None;
    }

    let internal_tool_call = match InternalToolCall::from_event(parsed) {
        Ok(call) => call,
        Err(error) => {
            trace_downstream_sse_event(&downstream_sse_trace_metadata(
                parsed,
                DownstreamTraceAction::ErrorTranslated,
                None,
            ));
            let failed_payload = terminal_failed_payload_from_error(
                parsed.get("response"),
                response_id_from_event(parsed),
                &error,
            );
            state.upstream = None;
            if matches!(error, ThreadlineError::UpstreamLivenessTimeout) {
                state.lease.finalize_liveness_timeout_turn();
            } else {
                state.lease.mark_upstream_terminal().await;
            }
            state.lease.release();
            state.final_done_pending = true;
            return Some(StreamProgress::Yield(sse_terminal_response_failed_chunk(
                &failed_payload,
            )));
        }
    };

    if let Some(call) = internal_tool_call {
        return Some(execute_internal_tool_event(state, parsed, call, trace_metadata).await);
    }

    None
}

pub(super) fn suppress_internal_tool_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
    event_type: &str,
    trace_metadata: &UpstreamEventTraceMetadata,
) -> bool {
    if !state.pending_internal_outputs.is_empty()
        && (matches!(
            event_type,
            "response.output_text.delta" | "response.output_text.done"
        ) || (event_type == "response.output_item.done"
            && !synthesized_output_item_done_text_delta(parsed).is_empty()))
    {
        trace_suppressed_event(trace_metadata);
        debug!(event_type, "translation_event_suppressed_internal_tool");
        return true;
    }

    if event_type.starts_with("response.output_item.") && event_contains_internal_tool_name(parsed)
    {
        if let Some(output_index) = output_index_from_event(parsed) {
            state
                .suppressed_internal_output_indexes
                .insert(output_index);
        }
        trace_suppressed_event(trace_metadata);
        debug!(event_type, "translation_event_suppressed_internal_tool");
        return true;
    }

    if event_type == "response.function_call_arguments.delta"
        && output_index_from_event(parsed).is_some_and(|output_index| {
            state
                .suppressed_internal_output_indexes
                .contains(&output_index)
        })
    {
        trace_suppressed_event(trace_metadata);
        debug!(event_type, "translation_event_suppressed_internal_tool");
        return true;
    }

    false
}

pub(super) async fn execute_internal_tool_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
    call: InternalToolCall,
    trace_metadata: &UpstreamEventTraceMetadata,
) -> StreamProgress {
    let execution_result = state.services.execute_internal_tool(call).await;
    if let Some(error) = state
        .upstream
        .as_ref()
        .and_then(|upstream| final_completion_acceptance_error(upstream.terminal_state()))
    {
        return reject_internal_tool_transport(
            state,
            parsed,
            response_id_from_event(parsed),
            error,
        )
        .await;
    }

    match execution_result {
        Ok(output) => {
            if state
                .pending_internal_outputs
                .iter()
                .any(|pending| pending.call_id() == output.call_id())
            {
                return reject_internal_tool_output(
                    state,
                    parsed,
                    response_id_from_event(parsed),
                    ThreadlineError::InternalToolFailed,
                )
                .await;
            }
            state.pending_internal_outputs.push(output);
            debug!(
                pending_internal_output_count = state.pending_internal_outputs.len(),
                "internal_tool_executed"
            );
            trace_suppressed_event(trace_metadata);
            return StreamProgress::Continue;
        }
        Err(error) => reject_internal_tool_execution(state, parsed, error).await,
    }
}

pub(super) async fn handle_intermediate_completion(
    state: &mut ResponseStreamState,
    parsed: &Value,
    response_id: Option<&str>,
) -> StreamProgress {
    let Some(response_id) = response_id else {
        return reject_internal_tool_execution(state, parsed, ThreadlineError::InternalToolFailed)
            .await;
    };

    if let Some(error) = state
        .upstream
        .as_ref()
        .and_then(|upstream| final_completion_acceptance_error(upstream.terminal_state()))
    {
        return reject_internal_tool_transport(state, parsed, Some(response_id), error).await;
    }

    let outputs = mem::take(&mut state.pending_internal_outputs);
    let output_count = outputs.len();
    debug!(
        response_id,
        pending_internal_output_count = output_count,
        "intermediate_completion_consumed"
    );
    state.suppressed_internal_output_indexes.clear();
    let followup_input = build_followup_input(outputs);
    if let Some(progress) =
        send_internal_tool_followup(state, parsed, response_id, followup_input).await
    {
        return progress;
    }
    reset_followup_visible_output(state);
    debug!(
        response_id,
        output_count,
        previous_response_id = state.previous_response_id.as_deref(),
        "internal_tool_followup_sent"
    );
    return StreamProgress::Continue;
}

pub(super) async fn reject_internal_tool_transport(
    state: &mut ResponseStreamState,
    parsed: &Value,
    response_id: Option<&str>,
    error: ThreadlineError,
) -> StreamProgress {
    let failed_payload =
        terminal_failed_payload_from_error(parsed.get("response"), response_id, &error);
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        &failed_payload,
        DownstreamTraceAction::Terminal,
        None,
    ));
    state.upstream = None;
    if matches!(error, ThreadlineError::UpstreamLivenessTimeout) {
        state.lease.finalize_liveness_timeout_turn();
    } else {
        state.lease.mark_upstream_terminal().await;
    }
    state.lease.release();
    state.final_done_pending = true;
    return StreamProgress::Yield(sse_terminal_response_failed_chunk(&failed_payload));
}

pub(super) async fn reject_internal_tool_execution(
    state: &mut ResponseStreamState,
    parsed: &Value,
    error: ThreadlineError,
) -> StreamProgress {
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        parsed,
        DownstreamTraceAction::ErrorTranslated,
        None,
    ));
    let failed_payload = terminal_failed_payload_from_error(
        parsed.get("response"),
        response_id_from_event(parsed),
        &error,
    );
    state.upstream = None;
    state.lease.mark_upstream_terminal().await;
    state.lease.release();
    state.final_done_pending = true;
    return StreamProgress::Yield(sse_terminal_response_failed_chunk(&failed_payload));
}

pub(super) async fn reject_internal_tool_output(
    state: &mut ResponseStreamState,
    parsed: &Value,
    response_id: Option<&str>,
    error: ThreadlineError,
) -> StreamProgress {
    let failed_payload =
        terminal_failed_payload_from_error(parsed.get("response"), response_id, &error);
    trace_downstream_sse_event(&downstream_sse_trace_metadata(
        &failed_payload,
        DownstreamTraceAction::Terminal,
        None,
    ));
    state.upstream = None;
    state.lease.mark_upstream_terminal().await;
    state.lease.release();
    state.final_done_pending = true;
    return StreamProgress::Yield(sse_terminal_response_failed_chunk(&failed_payload));
}

pub(super) async fn send_internal_tool_followup(
    state: &mut ResponseStreamState,
    parsed: &Value,
    response_id: &str,
    followup_input: Value,
) -> Option<StreamProgress> {
    let Some(upstream) = state.upstream.as_ref() else {
        let failed_payload = terminal_failed_payload_from_error(
            parsed.get("response"),
            Some(response_id),
            &ThreadlineError::UpstreamWebSocketClosed,
        );
        state.lease.mark_upstream_terminal().await;
        state.final_done_pending = true;
        return Some(StreamProgress::Yield(sse_terminal_response_failed_chunk(
            &failed_payload,
        )));
    };
    state.followup_send_started = true;
    let followup_result =
        send_followup_tool_outputs(upstream, &state.base_request, response_id, followup_input)
            .await;
    state.followup_send_started = false;
    if let Err(error) = followup_result {
        return Some(reject_internal_tool_output(state, parsed, Some(response_id), error).await);
    }

    None
}

pub(super) fn reset_followup_visible_output(state: &mut ResponseStreamState) -> () {
    state.downstream_visible_text_sources.clear();
    state.downstream_visible_text_delta_count = 0;
    state.visible_assistant_text.clear();
    state.last_unidentified_visible_text = None;
    state.queued_synthetic_output_text_deltas.clear();
    state.observable_output.reset();
}
