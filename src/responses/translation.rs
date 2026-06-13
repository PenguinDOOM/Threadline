use std::collections::HashSet;
use std::convert::Infallible;
use std::mem;
use std::sync::Arc;

use axum::body::Bytes;
use futures_util::stream;
use serde_json::Value;
use tracing::{debug, trace};

use crate::errors::ThreadlineError;
use crate::registry::RetainedSessionLease;
use crate::tools::{
    InternalToolCall, PendingInternalToolOutput, build_followup_input,
    event_contains_internal_tool_name, is_internal_tool_name,
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

pub(super) enum ResponseStreamLease {
    Retained(RetainedSessionLease),
    TransientAuxiliary,
}

impl ResponseStreamLease {
    fn release(&mut self) {
        if let Self::Retained(lease) = self {
            lease.release();
        }
    }

    async fn record_completed_marker(&mut self, response_marker: &str) {
        if let Self::Retained(lease) = self {
            lease.record_completed_marker(response_marker).await;
        }
    }

    async fn mark_upstream_recoverable(&mut self) {
        if let Self::Retained(lease) = self {
            lease.mark_upstream_recoverable().await;
        }
    }

    async fn mark_upstream_terminal(&mut self) {
        if let Self::Retained(lease) = self {
            lease.mark_upstream_terminal().await;
        }
    }

    fn retained_mut(&mut self) -> Option<&mut RetainedSessionLease> {
        match self {
            Self::Retained(lease) => Some(lease),
            Self::TransientAuxiliary => None,
        }
    }
}

const RESPONSES_TRANSLATION_UPSTREAM_EVENT: &str = "responses_translation_upstream_event";
const RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT: &str =
    "responses_translation_downstream_sse_event";
const RESPONSES_TRANSLATION_EVENT_SUPPRESSED: &str = "responses_translation_event_suppressed";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DownstreamTraceAction {
    Forwarded,
    Suppressed,
    Terminal,
    ErrorTranslated,
}

impl DownstreamTraceAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Forwarded => "forwarded",
            Self::Suppressed => "suppressed",
            Self::Terminal => "terminal",
            Self::ErrorTranslated => "error-translated",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct UpstreamEventTraceMetadata {
    event_type: String,
    item_type: Option<String>,
    item_name: Option<String>,
    call_id: Option<String>,
    arguments_length: Option<usize>,
    delta_length: Option<usize>,
    output_index: Option<u64>,
    item_id: Option<String>,
    is_compaction: bool,
    compaction_id: Option<String>,
    has_encrypted_content: Option<bool>,
}

impl UpstreamEventTraceMetadata {
    fn from_event(event: &Value) -> Self {
        let item = event.get("item");
        let item_type = string_field(item.and_then(|value| value.get("type")))
            .or_else(|| string_field(event.get("item_type")));
        let item_id = string_field(event.get("item_id"))
            .or_else(|| string_field(item.and_then(|value| value.get("id"))));
        let is_compaction = item_type.as_deref() == Some("compaction");
        Self {
            event_type: event
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("message")
                .to_string(),
            item_type,
            item_name: string_field(item.and_then(|value| value.get("name")))
                .or_else(|| string_field(event.get("name")))
                .or_else(|| string_field(event.get("tool_name"))),
            call_id: string_field(item.and_then(|value| value.get("call_id")))
                .or_else(|| string_field(event.get("call_id"))),
            arguments_length: string_length_field(
                item.and_then(|value| value.get("arguments"))
                    .or_else(|| event.get("arguments")),
            ),
            delta_length: string_length_field(event.get("delta")),
            output_index: output_index_from_event(event),
            item_id: item_id.clone(),
            is_compaction,
            compaction_id: is_compaction.then_some(item_id).flatten(),
            has_encrypted_content: is_compaction.then_some(
                item.and_then(|value| value.get("encrypted_content"))
                    .is_some(),
            ),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DownstreamSseTraceMetadata {
    translation_action: &'static str,
    event_type: String,
    item_type: Option<String>,
    item_name: Option<String>,
    call_id: Option<String>,
    arguments_length: Option<usize>,
    delta_length: Option<usize>,
    output_index: Option<u64>,
    item_id: Option<String>,
    is_compaction: bool,
    compaction_id: Option<String>,
    has_encrypted_content: Option<bool>,
}

fn downstream_sse_trace_metadata(
    event: &Value,
    action: DownstreamTraceAction,
) -> DownstreamSseTraceMetadata {
    let metadata = UpstreamEventTraceMetadata::from_event(event);
    DownstreamSseTraceMetadata {
        translation_action: action.as_str(),
        event_type: metadata.event_type,
        item_type: metadata.item_type,
        item_name: metadata.item_name,
        call_id: metadata.call_id,
        arguments_length: metadata.arguments_length,
        delta_length: metadata.delta_length,
        output_index: metadata.output_index,
        item_id: metadata.item_id,
        is_compaction: metadata.is_compaction,
        compaction_id: metadata.compaction_id,
        has_encrypted_content: metadata.has_encrypted_content,
    }
}

fn trace_upstream_event(metadata: &UpstreamEventTraceMetadata) {
    trace!(
        event_type = %metadata.event_type,
        item_type = ?metadata.item_type,
        item_name = ?metadata.item_name,
        call_id = ?metadata.call_id,
        arguments_length = ?metadata.arguments_length,
        delta_length = ?metadata.delta_length,
        output_index = ?metadata.output_index,
        item_id = ?metadata.item_id,
        is_compaction = metadata.is_compaction,
        compaction_id = ?metadata.compaction_id,
        has_encrypted_content = ?metadata.has_encrypted_content,
        "{RESPONSES_TRANSLATION_UPSTREAM_EVENT}"
    );
}

fn trace_downstream_sse_event(metadata: &DownstreamSseTraceMetadata) {
    trace!(
        translation_action = metadata.translation_action,
        event_type = %metadata.event_type,
        item_type = ?metadata.item_type,
        item_name = ?metadata.item_name,
        call_id = ?metadata.call_id,
        arguments_length = ?metadata.arguments_length,
        delta_length = ?metadata.delta_length,
        output_index = ?metadata.output_index,
        item_id = ?metadata.item_id,
        is_compaction = metadata.is_compaction,
        compaction_id = ?metadata.compaction_id,
        has_encrypted_content = ?metadata.has_encrypted_content,
        "{RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT}"
    );
}

fn trace_suppressed_event(metadata: &UpstreamEventTraceMetadata) {
    trace!(
        translation_action = DownstreamTraceAction::Suppressed.as_str(),
        event_type = %metadata.event_type,
        item_type = ?metadata.item_type,
        item_name = ?metadata.item_name,
        call_id = ?metadata.call_id,
        arguments_length = ?metadata.arguments_length,
        delta_length = ?metadata.delta_length,
        output_index = ?metadata.output_index,
        item_id = ?metadata.item_id,
        is_compaction = metadata.is_compaction,
        compaction_id = ?metadata.compaction_id,
        has_encrypted_content = ?metadata.has_encrypted_content,
        "{RESPONSES_TRANSLATION_EVENT_SUPPRESSED}"
    );
}

fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(ToString::to_string)
}

fn string_length_field(value: Option<&Value>) -> Option<usize> {
    value.and_then(Value::as_str).map(str::len)
}

fn completed_output_blocks_synthetic_delta(output: &[Value]) -> bool {
    output
        .iter()
        .any(|item| match item.get("type").and_then(Value::as_str) {
            Some("compaction") => true,
            Some("function_call") => item
                .get("name")
                .or_else(|| item.get("tool_name"))
                .and_then(Value::as_str)
                .is_some_and(is_internal_tool_name),
            _ => false,
        })
}

fn synthesized_completed_output_text_delta(event: &Value) -> Option<Value> {
    let output = event
        .get("response")
        .and_then(|response| response.get("output"))
        .and_then(Value::as_array)?;

    if completed_output_blocks_synthetic_delta(output) {
        return None;
    }

    let mut delta = String::new();
    let mut first_item_id = None;
    let mut first_output_index = None;
    let mut first_content_index = None;

    for (output_index, item) in output.iter().enumerate() {
        if item.get("type").and_then(Value::as_str) != Some("message")
            || item.get("role").and_then(Value::as_str) != Some("assistant")
        {
            continue;
        }

        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };

        for (content_index, part) in content.iter().enumerate() {
            if part.get("type").and_then(Value::as_str) != Some("output_text") {
                continue;
            }

            let Some(text) = part.get("text").and_then(Value::as_str) else {
                continue;
            };

            if text.trim().is_empty() {
                continue;
            }

            if first_output_index.is_none() {
                first_item_id = item
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                first_output_index = Some(output_index as u64);
                first_content_index = Some(content_index as u64);
            }

            delta.push_str(text);
        }
    }

    if delta.is_empty() {
        return None;
    }

    let mut payload = serde_json::Map::new();
    payload.insert(
        "type".to_string(),
        Value::String("response.output_text.delta".to_string()),
    );
    payload.insert("delta".to_string(), Value::String(delta));
    if let Some(item_id) = first_item_id {
        payload.insert("item_id".to_string(), Value::String(item_id));
    }
    if let Some(output_index) = first_output_index {
        payload.insert("output_index".to_string(), Value::from(output_index));
    }
    if let Some(content_index) = first_content_index {
        payload.insert("content_index".to_string(), Value::from(content_index));
    }

    Some(Value::Object(payload))
}

pub(super) struct ResponseStreamState {
    pub(super) services: ThreadlineServices,
    pub(super) upstream: Arc<LiveUpstreamWebSocket>,
    pub(super) lease: ResponseStreamLease,
    pub(super) base_request: serde_json::Map<String, Value>,
    pub(super) pending_internal_outputs: Vec<PendingInternalToolOutput>,
    pub(super) previous_response_id: Option<String>,
    pub(super) execute_internal_tools: bool,
    pub(super) suppressed_internal_output_indexes: HashSet<u64>,
    pub(super) upstream_event_seen: bool,
    pub(super) reconnect_attempted: bool,
    pub(super) downstream_output_text_delta_emitted: bool,
    pub(super) queued_final_completed: Option<Value>,
    pub(super) final_done_pending: bool,
    pub(super) done: bool,
}

pub(super) fn response_stream(
    state: ResponseStreamState,
) -> impl futures_util::Stream<Item = Result<Bytes, Infallible>> {
    stream::unfold(state, |mut state| async move {
        loop {
            if let Some(completed) = state.queued_final_completed.take() {
                let response_id = response_id_from_event(&completed);
                trace_downstream_sse_event(&downstream_sse_trace_metadata(
                    &completed,
                    DownstreamTraceAction::Terminal,
                ));
                debug!(
                    response_id,
                    event_type = "response.completed",
                    "translation_event_forwarded"
                );
                debug!(response_id, "terminal_response_forwarded");
                state.final_done_pending = true;
                debug!(response_id, "final_done_queued");
                return Some((
                    Ok::<Bytes, Infallible>(sse_json_chunk("response.completed", &completed)),
                    state,
                ));
            }

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
                        state.lease.release();
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

            let trace_metadata = UpstreamEventTraceMetadata::from_event(&parsed);
            trace_upstream_event(&trace_metadata);

            if state.execute_internal_tools {
                let internal_tool_call = match InternalToolCall::from_event(&parsed) {
                    Ok(call) => call,
                    Err(error) => {
                        trace_downstream_sse_event(&downstream_sse_trace_metadata(
                            &parsed,
                            DownstreamTraceAction::ErrorTranslated,
                        ));
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
                                pending_internal_output_count =
                                    state.pending_internal_outputs.len(),
                                "internal_tool_executed"
                            );
                            trace_suppressed_event(&trace_metadata);
                            continue;
                        }
                        Err(error) => {
                            trace_downstream_sse_event(&downstream_sse_trace_metadata(
                                &parsed,
                                DownstreamTraceAction::ErrorTranslated,
                            ));
                            state.lease.mark_upstream_terminal().await;
                            state.done = true;
                            return Some((Ok::<Bytes, Infallible>(sse_error_chunk(&error)), state));
                        }
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
                trace_suppressed_event(&trace_metadata);
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
                trace_suppressed_event(&trace_metadata);
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
                            trace_downstream_sse_event(&downstream_sse_trace_metadata(
                                &parsed,
                                DownstreamTraceAction::ErrorTranslated,
                            ));
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

                    if !state.downstream_output_text_delta_emitted
                        && let Some(synthetic_delta) =
                            synthesized_completed_output_text_delta(&parsed)
                    {
                        state.lease.release();
                        trace_downstream_sse_event(&downstream_sse_trace_metadata(
                            &synthetic_delta,
                            DownstreamTraceAction::Forwarded,
                        ));
                        debug!(
                            response_id,
                            event_type = "response.output_text.delta",
                            "translation_event_forwarded"
                        );
                        state.downstream_output_text_delta_emitted = true;
                        state.queued_final_completed = Some(parsed);
                        return Some((
                            Ok::<Bytes, Infallible>(sse_json_chunk(
                                "response.output_text.delta",
                                &synthetic_delta,
                            )),
                            state,
                        ));
                    }

                    trace_downstream_sse_event(&downstream_sse_trace_metadata(
                        &parsed,
                        DownstreamTraceAction::Terminal,
                    ));
                    debug!(response_id, event_type, "translation_event_forwarded");
                    debug!(response_id, "terminal_response_forwarded");
                    state.lease.release();
                    state.final_done_pending = true;
                    debug!(response_id, "final_done_queued");
                    return Some((
                        Ok::<Bytes, Infallible>(sse_json_chunk(&event_type, &parsed)),
                        state,
                    ));
                }
                "response.failed" => {
                    trace_downstream_sse_event(&downstream_sse_trace_metadata(
                        &parsed,
                        DownstreamTraceAction::Terminal,
                    ));
                    state.lease.mark_upstream_recoverable().await;
                    state.lease.release();
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
                    trace_downstream_sse_event(&downstream_sse_trace_metadata(
                        &parsed,
                        DownstreamTraceAction::ErrorTranslated,
                    ));
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
                    if event_type == "response.output_text.delta" {
                        state.downstream_output_text_delta_emitted = true;
                    }
                    trace_downstream_sse_event(&downstream_sse_trace_metadata(
                        &parsed,
                        DownstreamTraceAction::Forwarded,
                    ));
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
    let Some(lease) = state.lease.retained_mut() else {
        return Ok(None);
    };

    super::attempt_pre_first_event_reconnect(
        &state.services,
        lease,
        &state.base_request,
        state.previous_response_id.as_deref(),
        state.upstream_event_seen,
        &mut state.reconnect_attempted,
    )
    .await
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        DownstreamTraceAction, RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT,
        RESPONSES_TRANSLATION_EVENT_SUPPRESSED, RESPONSES_TRANSLATION_UPSTREAM_EVENT,
        UpstreamEventTraceMetadata, downstream_sse_trace_metadata,
    };

    #[test]
    fn upstream_event_trace_metadata_redacts_argument_bodies_and_keeps_lengths() {
        let arguments = "{\"input\":\"*** Begin Patch\\nsecret\\n*** End Patch\"}";
        let parsed = json!({
            "type": "response.output_item.added",
            "output_index": 2,
            "item_id": "item-visible",
            "item": {
                "type": "function_call",
                "call_id": "call-visible",
                "name": "apply_patch",
                "arguments": arguments
            }
        });

        let metadata = UpstreamEventTraceMetadata::from_event(&parsed);

        assert_eq!(metadata.event_type, "response.output_item.added");
        assert_eq!(metadata.item_type.as_deref(), Some("function_call"));
        assert_eq!(metadata.item_name.as_deref(), Some("apply_patch"));
        assert_eq!(metadata.call_id.as_deref(), Some("call-visible"));
        assert_eq!(metadata.arguments_length, Some(arguments.len()));
        assert_eq!(metadata.output_index, Some(2));
        assert_eq!(metadata.item_id.as_deref(), Some("item-visible"));
    }

    #[test]
    fn downstream_sse_trace_metadata_reports_action_without_delta_body() {
        let delta = "{\"input\":\"*** Begin Patch";
        let parsed = json!({
            "type": "response.function_call_arguments.delta",
            "output_index": 1,
            "item_id": "fc_apply_patch_1",
            "delta": delta
        });

        let metadata = downstream_sse_trace_metadata(&parsed, DownstreamTraceAction::Forwarded);

        assert_eq!(
            metadata.event_type,
            "response.function_call_arguments.delta"
        );
        assert_eq!(
            metadata.translation_action,
            DownstreamTraceAction::Forwarded.as_str()
        );
        assert_eq!(metadata.delta_length, Some(delta.len()));
        assert_eq!(metadata.output_index, Some(1));
        assert_eq!(metadata.item_id.as_deref(), Some("fc_apply_patch_1"));
        assert_eq!(metadata.arguments_length, None);
        assert_eq!(metadata.item_name, None);
    }

    #[test]
    fn upstream_event_trace_metadata_reports_compaction_without_encrypted_content() {
        let encrypted_content = "opaque-compaction-payload";
        let parsed = json!({
            "type": "response.output_item.done",
            "output_index": 4,
            "item": {
                "type": "compaction",
                "id": "compaction-visible",
                "encrypted_content": encrypted_content
            }
        });

        let metadata = UpstreamEventTraceMetadata::from_event(&parsed);
        let metadata_debug = format!("{metadata:?}");

        assert_eq!(metadata.event_type, "response.output_item.done");
        assert_eq!(metadata.item_type.as_deref(), Some("compaction"));
        assert!(metadata.is_compaction);
        assert_eq!(
            metadata.compaction_id.as_deref(),
            Some("compaction-visible")
        );
        assert_eq!(metadata.output_index, Some(4));
        assert_eq!(metadata.has_encrypted_content, Some(true));
        assert!(!metadata_debug.contains(encrypted_content));
    }

    #[test]
    fn upstream_event_trace_metadata_reports_compaction_without_blob_when_missing() {
        let parsed = json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "type": "compaction",
                "id": "compaction-empty"
            }
        });

        let metadata = UpstreamEventTraceMetadata::from_event(&parsed);

        assert!(metadata.is_compaction);
        assert_eq!(metadata.compaction_id.as_deref(), Some("compaction-empty"));
        assert_eq!(metadata.has_encrypted_content, Some(false));
    }

    #[test]
    fn translation_trace_event_names_remain_stable() {
        assert_eq!(
            RESPONSES_TRANSLATION_UPSTREAM_EVENT,
            "responses_translation_upstream_event"
        );
        assert_eq!(
            RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT,
            "responses_translation_downstream_sse_event"
        );
        assert_eq!(
            RESPONSES_TRANSLATION_EVENT_SUPPRESSED,
            "responses_translation_event_suppressed"
        );
    }
}
