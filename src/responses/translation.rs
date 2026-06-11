use std::collections::HashSet;
use std::convert::Infallible;
use std::mem;
use std::sync::Arc;

use axum::body::Bytes;
use futures_util::stream;
use serde_json::Value;
use tracing::debug;

use crate::errors::ThreadlineError;
use crate::registry::RetainedSessionLease;
use crate::tools::{
    InternalToolCall, PendingInternalToolOutput, build_followup_input,
    event_contains_internal_tool_name,
};
use crate::ws_pump::LiveUpstreamWebSocket;

use super::downstream::{
    safe_scalar_field, sse_done_chunk, sse_error_chunk, sse_json_chunk,
    sse_terminal_response_failed_chunk,
};
use super::upstream::{ThreadlineServices, send_followup_tool_outputs};

fn response_id_from_event(event: &Value) -> Option<&str> {
    event
        .get("response")
        .and_then(|response| response.get("id"))
        .and_then(Value::as_str)
}

fn output_index_from_event(event: &Value) -> Option<u64> {
    event.get("output_index").and_then(Value::as_u64)
}

pub(super) struct ResponseStreamState {
    pub(super) services: ThreadlineServices,
    pub(super) upstream: Arc<LiveUpstreamWebSocket>,
    pub(super) lease: RetainedSessionLease,
    pub(super) base_request: serde_json::Map<String, Value>,
    pub(super) pending_internal_outputs: Vec<PendingInternalToolOutput>,
    pub(super) previous_response_id: Option<String>,
    pub(super) suppressed_internal_output_indexes: HashSet<u64>,
    pub(super) upstream_event_seen: bool,
    pub(super) reconnect_attempted: bool,
    pub(super) final_done_pending: bool,
    pub(super) done: bool,
}

pub(super) fn response_stream(
    state: ResponseStreamState,
) -> impl futures_util::Stream<Item = Result<Bytes, Infallible>> {
    stream::unfold(state, |mut state| async move {
        loop {
            if state.final_done_pending {
                state.final_done_pending = false;
                state.done = true;
                debug!("downstream_sse_done_sent");
                return Some((Ok::<Bytes, Infallible>(sse_done_chunk()), state));
            }

            if state.done {
                debug!("downstream_sse_stream_finished");
                return None;
            }

            let next = match state.upstream.recv_text().await {
                Ok(Some(text)) => text,
                Ok(None) | Err(_) => match try_reconnect_or_terminal_error(&mut state).await {
                    Ok(Some(reconnected)) => {
                        state.upstream = reconnected;
                        continue;
                    }
                    Ok(None) => {
                        state.lease.mark_upstream_recoverable().await;
                        state.done = true;
                        return Some((
                            Ok::<Bytes, Infallible>(sse_error_chunk(
                                &ThreadlineError::UpstreamWebSocketClosed,
                            )),
                            state,
                        ));
                    }
                    Err(error) => {
                        state.done = true;
                        return Some((Ok::<Bytes, Infallible>(sse_error_chunk(&error)), state));
                    }
                },
            };

            state.upstream_event_seen = true;

            let parsed = match serde_json::from_str::<Value>(&next) {
                Ok(parsed) => parsed,
                Err(_) => {
                    state.lease.mark_upstream_terminal().await;
                    state.done = true;
                    return Some((
                        Ok::<Bytes, Infallible>(sse_error_chunk(
                            &ThreadlineError::UpstreamInvalidJson,
                        )),
                        state,
                    ));
                }
            };

            let internal_tool_call = match InternalToolCall::from_event(&parsed) {
                Ok(call) => call,
                Err(error) => {
                    state.lease.mark_upstream_terminal().await;
                    state.done = true;
                    return Some((Ok::<Bytes, Infallible>(sse_error_chunk(&error)), state));
                }
            };

            if let Some(call) = internal_tool_call {
                match call.execute() {
                    Ok(output) => {
                        state.pending_internal_outputs.push(output);
                        debug!(
                            pending_internal_output_count = state.pending_internal_outputs.len(),
                            "internal_tool_executed"
                        );
                        continue;
                    }
                    Err(error) => {
                        state.lease.mark_upstream_terminal().await;
                        state.done = true;
                        return Some((Ok::<Bytes, Infallible>(sse_error_chunk(&error)), state));
                    }
                }
            }

            let event_type = parsed
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("message")
                .to_string();

            debug!(event_type, "upstream_event_received");

            if event_type.starts_with("response.output_item.")
                && event_contains_internal_tool_name(&parsed)
            {
                if let Some(output_index) = output_index_from_event(&parsed) {
                    state
                        .suppressed_internal_output_indexes
                        .insert(output_index);
                }
                debug!(event_type, "translation_event_suppressed_internal_tool");
                continue;
            }

            if event_type == "response.function_call_arguments.delta"
                && output_index_from_event(&parsed).is_some_and(|output_index| {
                    state
                        .suppressed_internal_output_indexes
                        .contains(&output_index)
                })
            {
                debug!(event_type, "translation_event_suppressed_internal_tool");
                continue;
            }

            match event_type.as_str() {
                "response.completed" => {
                    let response_id = response_id_from_event(&parsed).map(ToString::to_string);

                    if let Some(response_id) = response_id.as_deref() {
                        state.lease.record_completed_marker(response_id).await;
                    }

                    if !state.pending_internal_outputs.is_empty() {
                        let Some(response_id) = response_id.as_deref() else {
                            let error = ThreadlineError::InternalToolFailed;
                            state.lease.mark_upstream_terminal().await;
                            state.done = true;
                            return Some((Ok::<Bytes, Infallible>(sse_error_chunk(&error)), state));
                        };

                        let outputs = mem::take(&mut state.pending_internal_outputs);
                        let output_count = outputs.len();
                        debug!(
                            response_id,
                            pending_internal_output_count = output_count,
                            "intermediate_completion_consumed"
                        );
                        state.suppressed_internal_output_indexes.clear();
                        let followup_input = build_followup_input(outputs);
                        if let Err(error) = send_followup_tool_outputs(
                            &state.upstream,
                            &state.base_request,
                            response_id,
                            followup_input,
                        )
                        .await
                        {
                            state.lease.mark_upstream_terminal().await;
                            state.done = true;
                            return Some((Ok::<Bytes, Infallible>(sse_error_chunk(&error)), state));
                        }
                        debug!(
                            response_id,
                            output_count,
                            previous_response_id = state.previous_response_id.as_deref(),
                            "internal_tool_followup_sent"
                        );
                        continue;
                    }

                    debug!(response_id, event_type, "translation_event_forwarded");
                    debug!(response_id, "terminal_response_forwarded");
                    state.final_done_pending = true;
                    debug!(response_id, "final_done_queued");
                    return Some((
                        Ok::<Bytes, Infallible>(sse_json_chunk(&event_type, &parsed)),
                        state,
                    ));
                }
                "response.failed" => {
                    state.lease.mark_upstream_recoverable().await;
                    state.final_done_pending = true;
                    debug!(event_type, "terminal_response_forwarded");
                    debug!(event_type, "final_done_queued");
                    return Some((
                        Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(&parsed)),
                        state,
                    ));
                }
                "error" => {
                    let error = parsed.get("error");
                    let error_code = error
                        .and_then(|value| value.get("code"))
                        .and_then(safe_scalar_field);
                    let error_message = error
                        .and_then(|value| value.get("message"))
                        .and_then(safe_scalar_field);
                    let status = parsed
                        .get("status")
                        .or_else(|| parsed.get("status_code"))
                        .and_then(safe_scalar_field);

                    debug!(
                        event_type,
                        error_code, error_message, status, "upstream_error_event"
                    );
                    state.lease.mark_upstream_terminal().await;
                    state.done = true;
                    return Some((
                        Ok::<Bytes, Infallible>(sse_error_chunk(
                            &ThreadlineError::UpstreamErrorEvent,
                        )),
                        state,
                    ));
                }
                _ => {
                    debug!(event_type, "translation_event_forwarded");
                    return Some((
                        Ok::<Bytes, Infallible>(sse_json_chunk(&event_type, &parsed)),
                        state,
                    ));
                }
            }
        }
    })
}

async fn try_reconnect_or_terminal_error(
    state: &mut ResponseStreamState,
) -> Result<Option<Arc<LiveUpstreamWebSocket>>, ThreadlineError> {
    super::attempt_pre_first_event_reconnect(
        &state.services,
        &mut state.lease,
        &state.base_request,
        state.previous_response_id.as_deref(),
        state.upstream_event_seen,
        &mut state.reconnect_attempted,
    )
    .await
}
