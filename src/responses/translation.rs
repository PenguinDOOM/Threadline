use std::collections::{HashSet, VecDeque};
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
    sse_terminal_response_failed_chunk, sse_terminal_response_incomplete_chunk,
};
use super::upstream::{ThreadlineServices, send_followup_tool_outputs};

fn response_id_from_event(event: &Value) -> Option<&str> {
    event
        .get("response_id")
        .and_then(Value::as_str)
        .or_else(|| {
            event
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
        })
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
const RESPONSES_TRANSLATION_NO_OBSERVABLE_OUTPUT_GUARD: &str =
    "responses_translation_no_observable_output_guard";

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
    response_id: Option<String>,
    item_type: Option<String>,
    item_name: Option<String>,
    call_id: Option<String>,
    arguments_length: Option<usize>,
    delta_length: Option<usize>,
    output_index: Option<u64>,
    content_index: Option<u64>,
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
            response_id: response_id_from_event(event).map(ToString::to_string),
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
            content_index: event.get("content_index").and_then(Value::as_u64),
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

#[derive(Debug, Default, PartialEq, Eq)]
struct DownstreamTraceDiagnostics {
    response_id: Option<String>,
    synthetic_delta_source: Option<&'static str>,
    visible_text_delta_count: Option<usize>,
    visible_text_length: Option<usize>,
    sanitized_internal_function_call_count: Option<usize>,
    sanitized_compaction_count: Option<usize>,
    completed_visible_message_count: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct DownstreamSseTraceMetadata {
    translation_action: &'static str,
    event_type: String,
    response_id: Option<String>,
    item_type: Option<String>,
    item_name: Option<String>,
    call_id: Option<String>,
    arguments_length: Option<usize>,
    delta_length: Option<usize>,
    output_index: Option<u64>,
    content_index: Option<u64>,
    item_id: Option<String>,
    is_compaction: bool,
    compaction_id: Option<String>,
    has_encrypted_content: Option<bool>,
    synthetic_delta_source: Option<&'static str>,
    visible_text_delta_count: Option<usize>,
    visible_text_length: Option<usize>,
    sanitized_internal_function_call_count: Option<usize>,
    sanitized_compaction_count: Option<usize>,
    completed_visible_message_count: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct NoObservableOutputGuardDiagnostics {
    response_id: Option<String>,
    pending_internal_outputs_count: usize,
    suppressed_internal_tool_call_count: usize,
    forwarded_external_tool_call_count: usize,
    forwarded_compaction_or_marker_count: usize,
    visible_assistant_text_len: usize,
    completed_output_item_types: Vec<String>,
    upstream_last_event_type: Option<String>,
    is_intermediate_completed: bool,
}

fn downstream_sse_trace_metadata(
    event: &Value,
    action: DownstreamTraceAction,
    diagnostics: Option<&DownstreamTraceDiagnostics>,
) -> DownstreamSseTraceMetadata {
    let metadata = UpstreamEventTraceMetadata::from_event(event);
    DownstreamSseTraceMetadata {
        translation_action: action.as_str(),
        event_type: metadata.event_type,
        response_id: diagnostics
            .and_then(|value| value.response_id.clone())
            .or(metadata.response_id),
        item_type: metadata.item_type,
        item_name: metadata.item_name,
        call_id: metadata.call_id,
        arguments_length: metadata.arguments_length,
        delta_length: metadata.delta_length,
        output_index: metadata.output_index,
        content_index: metadata.content_index,
        item_id: metadata.item_id,
        is_compaction: metadata.is_compaction,
        compaction_id: metadata.compaction_id,
        has_encrypted_content: metadata.has_encrypted_content,
        synthetic_delta_source: diagnostics.and_then(|value| value.synthetic_delta_source),
        visible_text_delta_count: diagnostics.and_then(|value| value.visible_text_delta_count),
        visible_text_length: diagnostics.and_then(|value| value.visible_text_length),
        sanitized_internal_function_call_count: diagnostics
            .and_then(|value| value.sanitized_internal_function_call_count),
        sanitized_compaction_count: diagnostics.and_then(|value| value.sanitized_compaction_count),
        completed_visible_message_count: diagnostics
            .and_then(|value| value.completed_visible_message_count),
    }
}

fn trace_upstream_event(metadata: &UpstreamEventTraceMetadata) {
    trace!(
        event_type = %metadata.event_type,
        response_id = ?metadata.response_id,
        item_type = ?metadata.item_type,
        item_name = ?metadata.item_name,
        call_id = ?metadata.call_id,
        arguments_length = ?metadata.arguments_length,
        delta_length = ?metadata.delta_length,
        output_index = ?metadata.output_index,
        content_index = ?metadata.content_index,
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
        response_id = ?metadata.response_id,
        item_type = ?metadata.item_type,
        item_name = ?metadata.item_name,
        call_id = ?metadata.call_id,
        arguments_length = ?metadata.arguments_length,
        delta_length = ?metadata.delta_length,
        output_index = ?metadata.output_index,
        content_index = ?metadata.content_index,
        item_id = ?metadata.item_id,
        is_compaction = metadata.is_compaction,
        compaction_id = ?metadata.compaction_id,
        has_encrypted_content = ?metadata.has_encrypted_content,
        synthetic_delta_source = ?metadata.synthetic_delta_source,
        visible_text_delta_count = ?metadata.visible_text_delta_count,
        visible_text_length = ?metadata.visible_text_length,
        sanitized_internal_function_call_count =
            ?metadata.sanitized_internal_function_call_count,
        sanitized_compaction_count = ?metadata.sanitized_compaction_count,
        completed_visible_message_count = ?metadata.completed_visible_message_count,
        "{RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT}"
    );
}

fn trace_suppressed_event(metadata: &UpstreamEventTraceMetadata) {
    trace!(
        translation_action = DownstreamTraceAction::Suppressed.as_str(),
        event_type = %metadata.event_type,
        response_id = ?metadata.response_id,
        item_type = ?metadata.item_type,
        item_name = ?metadata.item_name,
        call_id = ?metadata.call_id,
        arguments_length = ?metadata.arguments_length,
        delta_length = ?metadata.delta_length,
        output_index = ?metadata.output_index,
        content_index = ?metadata.content_index,
        item_id = ?metadata.item_id,
        is_compaction = metadata.is_compaction,
        compaction_id = ?metadata.compaction_id,
        has_encrypted_content = ?metadata.has_encrypted_content,
        "{RESPONSES_TRANSLATION_EVENT_SUPPRESSED}"
    );
}

fn trace_no_observable_output_guard(metadata: &NoObservableOutputGuardDiagnostics) {
    debug!(
        response_id = ?metadata.response_id,
        pending_internal_outputs_count = metadata.pending_internal_outputs_count,
        suppressed_internal_tool_call_count = metadata.suppressed_internal_tool_call_count,
        forwarded_external_tool_call_count = metadata.forwarded_external_tool_call_count,
        forwarded_compaction_or_marker_count = metadata.forwarded_compaction_or_marker_count,
        visible_assistant_text_len = metadata.visible_assistant_text_len,
        completed_output_item_types = ?metadata.completed_output_item_types,
        upstream_last_event_type = ?metadata.upstream_last_event_type,
        is_intermediate_completed = metadata.is_intermediate_completed,
        "{RESPONSES_TRANSLATION_NO_OBSERVABLE_OUTPUT_GUARD}"
    );
}

fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(ToString::to_string)
}

fn string_length_field(value: Option<&Value>) -> Option<usize> {
    value.and_then(Value::as_str).map(str::len)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct VisibleTextSourceKey {
    item_id: Option<String>,
    output_index: Option<u64>,
    content_index: Option<u64>,
}

impl VisibleTextSourceKey {
    fn new(item_id: Option<String>, output_index: Option<u64>, content_index: Option<u64>) -> Self {
        Self {
            item_id,
            output_index,
            content_index,
        }
    }

    fn dedupe_identity(&self) -> Option<VisibleTextDedupeIdentity> {
        if let Some(item_id) = self.item_id.as_ref() {
            return Some(VisibleTextDedupeIdentity::ItemId {
                item_id: item_id.clone(),
                content_index: self.content_index,
            });
        }

        self.output_index
            .map(|output_index| VisibleTextDedupeIdentity::OutputIndex {
                output_index,
                content_index: self.content_index,
            })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum VisibleTextDedupeIdentity {
    ItemId {
        item_id: String,
        content_index: Option<u64>,
    },
    OutputIndex {
        output_index: u64,
        content_index: Option<u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct VisibleAssistantText {
    key: VisibleTextSourceKey,
    text: String,
}

fn response_output_text_delta_payload(key: &VisibleTextSourceKey, delta: &str) -> Option<Value> {
    if delta.trim().is_empty() {
        return None;
    }

    let mut payload = serde_json::Map::new();
    payload.insert(
        "type".to_string(),
        Value::String("response.output_text.delta".to_string()),
    );
    payload.insert("delta".to_string(), Value::String(delta.to_string()));

    if let Some(item_id) = key.item_id.as_ref() {
        payload.insert("item_id".to_string(), Value::String(item_id.clone()));
    }
    if let Some(output_index) = key.output_index {
        payload.insert("output_index".to_string(), Value::from(output_index));
    }
    if let Some(content_index) = key.content_index {
        payload.insert("content_index".to_string(), Value::from(content_index));
    }

    Some(Value::Object(payload))
}

fn visible_text_delta_source_key(event: &Value) -> VisibleTextSourceKey {
    VisibleTextSourceKey::new(
        string_field(event.get("item_id")),
        event.get("output_index").and_then(Value::as_u64),
        event.get("content_index").and_then(Value::as_u64),
    )
}

fn synthesized_output_text_done_delta(event: &Value) -> Option<(VisibleTextSourceKey, Value)> {
    let key = visible_text_delta_source_key(event);
    let text = event.get("text").and_then(Value::as_str)?;
    let payload = response_output_text_delta_payload(&key, text)?;
    Some((key, payload))
}

fn message_output_text_delta_payloads(
    item: &Value,
    output_index: Option<u64>,
) -> Vec<(VisibleTextSourceKey, Value)> {
    if item.get("type").and_then(Value::as_str) != Some("message")
        || item.get("role").and_then(Value::as_str) != Some("assistant")
    {
        return Vec::new();
    }

    let Some(content) = item.get("content").and_then(Value::as_array) else {
        return Vec::new();
    };

    let item_id = string_field(item.get("id"));
    let mut payloads = Vec::new();
    for (content_index, part) in content.iter().enumerate() {
        if part.get("type").and_then(Value::as_str) != Some("output_text") {
            continue;
        }

        let Some(text) = part.get("text").and_then(Value::as_str) else {
            continue;
        };

        let key =
            VisibleTextSourceKey::new(item_id.clone(), output_index, Some(content_index as u64));
        let Some(payload) = response_output_text_delta_payload(&key, text) else {
            continue;
        };
        payloads.push((key, payload));
    }

    payloads
}

fn synthesized_output_item_done_text_delta(event: &Value) -> Vec<(VisibleTextSourceKey, Value)> {
    let Some(item) = event.get("item") else {
        return Vec::new();
    };

    message_output_text_delta_payloads(item, output_index_from_event(event))
}

fn synthesized_completed_output_text_delta(event: &Value) -> Vec<(VisibleTextSourceKey, Value)> {
    let Some(output) = event
        .get("response")
        .and_then(|response| response.get("output"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };

    let mut first_key: Option<VisibleTextSourceKey> = None;
    let mut combined_text = String::new();

    for (output_index, item) in output.iter().enumerate() {
        if item.get("type").and_then(Value::as_str) != Some("message")
            || item.get("role").and_then(Value::as_str) != Some("assistant")
        {
            continue;
        }

        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };

        let item_id = string_field(item.get("id"));
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

            if first_key.is_none() {
                first_key = Some(VisibleTextSourceKey::new(
                    item_id.clone(),
                    Some(output_index as u64),
                    Some(content_index as u64),
                ));
            }

            combined_text.push_str(text);
        }
    }

    let Some(key) = first_key else {
        return Vec::new();
    };
    let Some(payload) = response_output_text_delta_payload(&key, &combined_text) else {
        return Vec::new();
    };

    vec![(key, payload)]
}

fn record_visible_assistant_text(
    visible_text: &mut Vec<VisibleAssistantText>,
    key: &VisibleTextSourceKey,
    text: &str,
) {
    if text.trim().is_empty() {
        return;
    }

    if let Some(existing) = visible_text.iter_mut().find(|entry| entry.key == *key) {
        existing.text.push_str(text);
        return;
    }

    visible_text.push(VisibleAssistantText {
        key: key.clone(),
        text: text.to_string(),
    });
}

fn track_visible_text_identity(
    state: &mut ResponseStreamState,
    key: &VisibleTextSourceKey,
    text: &str,
) -> bool {
    if let Some(identity) = key.dedupe_identity() {
        state.downstream_visible_text_sources.insert(identity);
        return true;
    }

    if state.last_unidentified_visible_text.as_deref() == Some(text) {
        return false;
    }

    state.last_unidentified_visible_text = Some(text.to_string());
    true
}

fn record_forwarded_visible_text_delta(
    state: &mut ResponseStreamState,
    key: VisibleTextSourceKey,
    delta: &str,
) {
    if !track_visible_text_identity(state, &key, delta) {
        return;
    }

    record_visible_assistant_text(&mut state.visible_assistant_text, &key, delta);
}

fn queue_visible_text_delta(
    state: &mut ResponseStreamState,
    key: VisibleTextSourceKey,
    payload: Value,
    synthetic_delta_source: &'static str,
    response_id: Option<&str>,
) -> bool {
    let Some(delta) = payload.get("delta").and_then(Value::as_str) else {
        return false;
    };

    if !key
        .dedupe_identity()
        .map(|identity| state.downstream_visible_text_sources.insert(identity))
        .unwrap_or_else(|| {
            if state.last_unidentified_visible_text.as_deref() == Some(delta) {
                false
            } else {
                state.last_unidentified_visible_text = Some(delta.to_string());
                true
            }
        })
    {
        return false;
    }

    record_visible_assistant_text(&mut state.visible_assistant_text, &key, delta);
    state
        .queued_synthetic_output_text_deltas
        .push_back(QueuedSyntheticOutputTextDelta {
            payload,
            response_id: response_id.map(ToString::to_string),
            synthetic_delta_source,
        });
    true
}

fn queue_visible_text_deltas(
    state: &mut ResponseStreamState,
    payloads: Vec<(VisibleTextSourceKey, Value)>,
    synthetic_delta_source: &'static str,
    response_id: Option<&str>,
) -> bool {
    let mut queued = false;
    for (key, payload) in payloads {
        if queue_visible_text_delta(state, key, payload, synthetic_delta_source, response_id) {
            queued = true;
        }
    }

    queued
}

fn assistant_message_has_visible_output_text(item: &Value) -> bool {
    if item.get("type").and_then(Value::as_str) != Some("message")
        || item.get("role").and_then(Value::as_str) != Some("assistant")
    {
        return false;
    }

    item.get("content")
        .and_then(Value::as_array)
        .is_some_and(|content| {
            content.iter().any(|part| {
                part.get("type").and_then(Value::as_str) == Some("output_text")
                    && part
                        .get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.trim().is_empty())
            })
        })
}

fn synthetic_assistant_message_id(response_id: Option<&str>) -> String {
    match response_id {
        Some(response_id) if !response_id.is_empty() => {
            format!("synthetic_assistant_{response_id}")
        }
        _ => "synthetic_assistant".to_string(),
    }
}

fn synthetic_assistant_message(
    response_id: Option<&str>,
    visible_text: &[VisibleAssistantText],
) -> Option<Value> {
    let content: Vec<Value> = visible_text
        .iter()
        .filter(|entry| !entry.text.trim().is_empty())
        .map(|entry| {
            Value::Object(serde_json::Map::from_iter([
                ("type".to_string(), Value::String("output_text".to_string())),
                ("text".to_string(), Value::String(entry.text.clone())),
                ("annotations".to_string(), Value::Array(Vec::new())),
            ]))
        })
        .collect();

    if content.is_empty() {
        return None;
    }

    let message_id = visible_text
        .iter()
        .find_map(|entry| entry.key.item_id.clone())
        .unwrap_or_else(|| synthetic_assistant_message_id(response_id));

    Some(Value::Object(serde_json::Map::from_iter([
        ("id".to_string(), Value::String(message_id)),
        ("type".to_string(), Value::String("message".to_string())),
        ("role".to_string(), Value::String("assistant".to_string())),
        ("content".to_string(), Value::Array(content)),
    ])))
}

fn has_accumulated_visible_assistant_text(visible_text: &[VisibleAssistantText]) -> bool {
    visible_text
        .iter()
        .any(|entry| !entry.text.trim().is_empty())
}

fn is_forwarded_external_tool_call_item(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("function_call")
        && item
            .get("name")
            .or_else(|| item.get("tool_name"))
            .and_then(Value::as_str)
            .is_some_and(|name| !is_internal_tool_name(name))
}

fn has_image_generation_result(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("image_generation_call")
        && item
            .get("result")
            .and_then(Value::as_str)
            .is_some_and(|result| !result.trim().is_empty())
}

fn is_forwarded_marker_like_item(item: &Value) -> bool {
    matches!(
        item.get("type").and_then(Value::as_str),
        Some("compaction") | Some("context")
    )
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct DownstreamObservableOutputState {
    forwarded_visible_text_delta_count: usize,
    forwarded_external_tool_call_count: usize,
    forwarded_image_generation_count: usize,
    forwarded_marker_like_output_count: usize,
    final_visible_message_count: usize,
    final_external_tool_call_count: usize,
    final_image_generation_count: usize,
    final_marker_like_output_count: usize,
    last_upstream_event_type: Option<String>,
}

impl DownstreamObservableOutputState {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn has_observable_output(&self) -> bool {
        self.forwarded_visible_text_delta_count > 0
            || self.forwarded_external_tool_call_count > 0
            || self.forwarded_image_generation_count > 0
            || self.forwarded_marker_like_output_count > 0
            || self.final_visible_message_count > 0
            || self.final_external_tool_call_count > 0
            || self.final_image_generation_count > 0
            || self.final_marker_like_output_count > 0
    }
}

fn record_forwarded_observable_output(
    observable_output: &mut DownstreamObservableOutputState,
    event_type: &str,
    event: &Value,
) {
    match event_type {
        "response.output_text.delta" => {
            observable_output.forwarded_visible_text_delta_count += 1;
        }
        "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
            observable_output.forwarded_external_tool_call_count += 1;
        }
        "response.output_item.added" | "response.output_item.done" => {
            let Some(item) = event.get("item") else {
                return;
            };

            if is_forwarded_external_tool_call_item(item) {
                observable_output.forwarded_external_tool_call_count += 1;
            } else if has_image_generation_result(item) {
                observable_output.forwarded_image_generation_count += 1;
            } else if is_forwarded_marker_like_item(item) {
                observable_output.forwarded_marker_like_output_count += 1;
            }
        }
        _ => {}
    }
}

fn record_completed_observable_output(
    observable_output: &mut DownstreamObservableOutputState,
    event: &Value,
    diagnostics: &CompletedSanitizationDiagnostics,
) {
    observable_output.final_visible_message_count = diagnostics.completed_visible_message_count;
    observable_output.final_external_tool_call_count = 0;
    observable_output.final_image_generation_count = 0;
    observable_output.final_marker_like_output_count = 0;

    let Some(output) = event
        .get("response")
        .and_then(|response| response.get("output"))
        .and_then(Value::as_array)
    else {
        return;
    };

    for item in output {
        if is_forwarded_external_tool_call_item(item) {
            observable_output.final_external_tool_call_count += 1;
        } else if has_image_generation_result(item) {
            observable_output.final_image_generation_count += 1;
        } else if is_forwarded_marker_like_item(item) {
            observable_output.final_marker_like_output_count += 1;
        }
    }
}

fn has_downstream_observable_output(state: &ResponseStreamState) -> bool {
    state.observable_output.has_observable_output()
}

fn no_observable_output_guard_diagnostics(
    state: &ResponseStreamState,
    completed_event: &Value,
    diagnostics: &CompletedSanitizationDiagnostics,
) -> NoObservableOutputGuardDiagnostics {
    NoObservableOutputGuardDiagnostics {
        response_id: response_id_from_event(completed_event).map(ToString::to_string),
        pending_internal_outputs_count: state.pending_internal_outputs.len(),
        suppressed_internal_tool_call_count: state.suppressed_internal_output_indexes.len()
            + diagnostics.sanitized_internal_function_call_count,
        forwarded_external_tool_call_count: state
            .observable_output
            .forwarded_external_tool_call_count
            + state.observable_output.final_external_tool_call_count,
        forwarded_compaction_or_marker_count: state
            .observable_output
            .forwarded_marker_like_output_count
            + state.observable_output.final_marker_like_output_count,
        visible_assistant_text_len: state
            .visible_assistant_text
            .iter()
            .map(|entry| entry.text.len())
            .sum(),
        completed_output_item_types: completed_event
            .get("response")
            .and_then(|response| response.get("output"))
            .and_then(Value::as_array)
            .map(|output| {
                output
                    .iter()
                    .filter_map(|item| item.get("type").and_then(Value::as_str))
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        upstream_last_event_type: state.observable_output.last_upstream_event_type.clone(),
        is_intermediate_completed: !state.pending_internal_outputs.is_empty(),
    }
}

fn no_observable_output_failed_payload(response_id: Option<&str>) -> Value {
    let mut response = serde_json::Map::new();
    if let Some(response_id) = response_id.filter(|value| !value.is_empty()) {
        response.insert("id".to_string(), Value::String(response_id.to_string()));
    }

    Value::Object(serde_json::Map::from_iter([
        (
            "type".to_string(),
            Value::String("response.failed".to_string()),
        ),
        ("response".to_string(), Value::Object(response)),
        (
            "error".to_string(),
            Value::Object(serde_json::Map::from_iter([
                (
                    "code".to_string(),
                    Value::String("threadline_no_observable_output".to_string()),
                ),
                (
                    "message".to_string(),
                    Value::String("Response contained no observable output.".to_string()),
                ),
            ])),
        ),
    ]))
}

fn terminal_failed_payload(
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

fn terminal_failed_payload_from_error(
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CompletedSanitizationDiagnostics {
    sanitized_internal_function_call_count: usize,
    sanitized_compaction_count: usize,
    completed_visible_message_count: usize,
}

fn sanitized_completed_event_with_diagnostics(
    event: &Value,
    visible_text: &[VisibleAssistantText],
) -> (Value, CompletedSanitizationDiagnostics) {
    let mut sanitized = event.clone();
    let response_id = response_id_from_event(event);
    let mut diagnostics = CompletedSanitizationDiagnostics::default();

    let Some(response) = sanitized.get_mut("response").and_then(Value::as_object_mut) else {
        return (sanitized, diagnostics);
    };

    let Some(original_output) = response.get("output").and_then(Value::as_array) else {
        if let Some(message) = synthetic_assistant_message(response_id, visible_text) {
            diagnostics.completed_visible_message_count = 1;
            response.insert("output".to_string(), Value::Array(vec![message]));
        }
        return (sanitized, diagnostics);
    };

    let mut final_output = original_output
        .iter()
        .filter_map(|item| match item.get("type").and_then(Value::as_str) {
            Some("compaction") => {
                diagnostics.sanitized_compaction_count += 1;
                None
            }
            Some("function_call")
                if item
                    .get("name")
                    .or_else(|| item.get("tool_name"))
                    .and_then(Value::as_str)
                    .is_some_and(is_internal_tool_name) =>
            {
                diagnostics.sanitized_internal_function_call_count += 1;
                None
            }
            _ => Some(item.clone()),
        })
        .collect::<Vec<_>>();

    let has_visible_assistant_message = final_output
        .iter()
        .any(assistant_message_has_visible_output_text);
    let mut output_changed = diagnostics.sanitized_internal_function_call_count > 0
        || diagnostics.sanitized_compaction_count > 0;

    if !has_visible_assistant_message
        && let Some(message) = synthetic_assistant_message(response_id, visible_text)
    {
        final_output.push(message);
        output_changed = true;
    }

    diagnostics.completed_visible_message_count = final_output
        .iter()
        .filter(|item| assistant_message_has_visible_output_text(item))
        .count();

    if output_changed {
        response.insert("output".to_string(), Value::Array(final_output));
    }

    (sanitized, diagnostics)
}

#[derive(Clone, Debug)]
pub(super) struct QueuedSyntheticOutputTextDelta {
    payload: Value,
    response_id: Option<String>,
    synthetic_delta_source: &'static str,
}

#[derive(Clone, Debug)]
pub(super) struct QueuedCompletedEvent {
    payload: Value,
    diagnostics: CompletedSanitizationDiagnostics,
}

#[derive(Clone, Debug)]
pub(super) struct QueuedForwardedEvent {
    payload: Value,
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
    pub(super) observable_output: DownstreamObservableOutputState,
    pub(super) downstream_visible_text_sources: HashSet<VisibleTextDedupeIdentity>,
    pub(super) downstream_visible_text_delta_count: usize,
    pub(super) visible_assistant_text: Vec<VisibleAssistantText>,
    pub(super) last_unidentified_visible_text: Option<String>,
    pub(super) queued_synthetic_output_text_deltas: VecDeque<QueuedSyntheticOutputTextDelta>,
    pub(super) queued_forwarded_event: Option<QueuedForwardedEvent>,
    pub(super) queued_final_completed: Option<QueuedCompletedEvent>,
    pub(super) final_done_pending: bool,
    pub(super) apply_no_observable_output_failure: bool,
    pub(super) done: bool,
}

pub(super) fn response_stream(
    state: ResponseStreamState,
) -> impl futures_util::Stream<Item = Result<Bytes, Infallible>> {
    stream::unfold(state, |mut state| async move {
        loop {
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
                return Some((
                    Ok::<Bytes, Infallible>(sse_json_chunk(&event_type, &synthetic_delta.payload)),
                    state,
                ));
            }

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
                return Some((
                    Ok::<Bytes, Infallible>(sse_json_chunk(&event_type, &forwarded_event.payload)),
                    state,
                ));
            }

            if let Some(completed) = state.queued_final_completed.take() {
                let response_id = response_id_from_event(&completed.payload);
                if let Some(response_id) = response_id {
                    state.lease.record_completed_marker(response_id).await;
                }
                let trace_diagnostics = DownstreamTraceDiagnostics {
                    response_id: response_id.map(ToString::to_string),
                    visible_text_delta_count: Some(state.downstream_visible_text_delta_count),
                    sanitized_internal_function_call_count: Some(
                        completed.diagnostics.sanitized_internal_function_call_count,
                    ),
                    sanitized_compaction_count: Some(
                        completed.diagnostics.sanitized_compaction_count,
                    ),
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
                return Some((
                    Ok::<Bytes, Infallible>(sse_json_chunk(
                        "response.completed",
                        &completed.payload,
                    )),
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
                        let failed_payload = terminal_failed_payload_from_error(
                            None,
                            None,
                            &ThreadlineError::UpstreamWebSocketClosed,
                        );
                        trace_downstream_sse_event(&downstream_sse_trace_metadata(
                            &failed_payload,
                            DownstreamTraceAction::Terminal,
                            None,
                        ));
                        state.lease.mark_upstream_recoverable().await;
                        state.lease.release();
                        state.final_done_pending = true;
                        return Some((
                            Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                                &failed_payload,
                            )),
                            state,
                        ));
                    }
                    Err(error) => {
                        let failed_payload = terminal_failed_payload_from_error(None, None, &error);
                        trace_downstream_sse_event(&downstream_sse_trace_metadata(
                            &failed_payload,
                            DownstreamTraceAction::Terminal,
                            None,
                        ));
                        state.lease.mark_upstream_terminal().await;
                        state.lease.release();
                        state.final_done_pending = true;
                        return Some((
                            Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                                &failed_payload,
                            )),
                            state,
                        ));
                    }
                },
            };

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
                state.lease.mark_upstream_terminal().await;
                state.lease.release();
                state.final_done_pending = true;
                return Some((
                    Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(&failed_payload)),
                    state,
                ));
            }

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
            state.observable_output.last_upstream_event_type =
                Some(trace_metadata.event_type.clone());

            if state.execute_internal_tools {
                let internal_tool_call = match InternalToolCall::from_event(&parsed) {
                    Ok(call) => call,
                    Err(error) => {
                        trace_downstream_sse_event(&downstream_sse_trace_metadata(
                            &parsed,
                            DownstreamTraceAction::ErrorTranslated,
                            None,
                        ));
                        let failed_payload = terminal_failed_payload_from_error(
                            parsed.get("response"),
                            response_id_from_event(&parsed),
                            &error,
                        );
                        state.lease.mark_upstream_terminal().await;
                        state.lease.release();
                        state.final_done_pending = true;
                        return Some((
                            Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                                &failed_payload,
                            )),
                            state,
                        ));
                    }
                };

                if let Some(call) = internal_tool_call {
                    match call.execute() {
                        Ok(output) => {
                            if state
                                .pending_internal_outputs
                                .iter()
                                .any(|pending| pending.call_id() == output.call_id())
                            {
                                let failed_payload = terminal_failed_payload_from_error(
                                    parsed.get("response"),
                                    response_id_from_event(&parsed),
                                    &ThreadlineError::InternalToolFailed,
                                );
                                trace_downstream_sse_event(&downstream_sse_trace_metadata(
                                    &failed_payload,
                                    DownstreamTraceAction::Terminal,
                                    None,
                                ));
                                state.lease.mark_upstream_terminal().await;
                                state.lease.release();
                                state.final_done_pending = true;
                                return Some((
                                    Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                                        &failed_payload,
                                    )),
                                    state,
                                ));
                            }
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
                                None,
                            ));
                            let failed_payload = terminal_failed_payload_from_error(
                                parsed.get("response"),
                                response_id_from_event(&parsed),
                                &error,
                            );
                            state.lease.mark_upstream_terminal().await;
                            state.lease.release();
                            state.final_done_pending = true;
                            return Some((
                                Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                                    &failed_payload,
                                )),
                                state,
                            ));
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

            if !state.pending_internal_outputs.is_empty()
                && (matches!(
                    event_type.as_str(),
                    "response.output_text.delta" | "response.output_text.done"
                ) || (event_type == "response.output_item.done"
                    && !synthesized_output_item_done_text_delta(&parsed).is_empty()))
            {
                trace_suppressed_event(&trace_metadata);
                debug!(event_type, "translation_event_suppressed_internal_tool");
                continue;
            }

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

                    if !state.pending_internal_outputs.is_empty() {
                        let Some(response_id) = response_id.as_deref() else {
                            let error = ThreadlineError::InternalToolFailed;
                            trace_downstream_sse_event(&downstream_sse_trace_metadata(
                                &parsed,
                                DownstreamTraceAction::ErrorTranslated,
                                None,
                            ));
                            let failed_payload = terminal_failed_payload_from_error(
                                parsed.get("response"),
                                response_id_from_event(&parsed),
                                &error,
                            );
                            state.lease.mark_upstream_terminal().await;
                            state.lease.release();
                            state.final_done_pending = true;
                            return Some((
                                Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                                    &failed_payload,
                                )),
                                state,
                            ));
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
                            let failed_payload = terminal_failed_payload_from_error(
                                parsed.get("response"),
                                Some(response_id),
                                &error,
                            );
                            trace_downstream_sse_event(&downstream_sse_trace_metadata(
                                &failed_payload,
                                DownstreamTraceAction::Terminal,
                                None,
                            ));
                            state.lease.mark_upstream_terminal().await;
                            state.lease.release();
                            state.final_done_pending = true;
                            return Some((
                                Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                                    &failed_payload,
                                )),
                                state,
                            ));
                        }
                        state.downstream_visible_text_sources.clear();
                        state.downstream_visible_text_delta_count = 0;
                        state.visible_assistant_text.clear();
                        state.last_unidentified_visible_text = None;
                        state.queued_synthetic_output_text_deltas.clear();
                        state.observable_output.reset();
                        debug!(
                            response_id,
                            output_count,
                            previous_response_id = state.previous_response_id.as_deref(),
                            "internal_tool_followup_sent"
                        );
                        continue;
                    }

                    if !has_accumulated_visible_assistant_text(&state.visible_assistant_text) {
                        queue_visible_text_deltas(
                            &mut state,
                            synthesized_completed_output_text_delta(&parsed),
                            "response.completed",
                            response_id.as_deref(),
                        );
                    }

                    let (sanitized_completed, diagnostics) =
                        sanitized_completed_event_with_diagnostics(
                            &parsed,
                            &state.visible_assistant_text,
                        );
                    record_completed_observable_output(
                        &mut state.observable_output,
                        &sanitized_completed,
                        &diagnostics,
                    );

                    if state.apply_no_observable_output_failure
                        && !has_downstream_observable_output(&state)
                    {
                        let guard_diagnostics =
                            no_observable_output_guard_diagnostics(&state, &parsed, &diagnostics);
                        trace_no_observable_output_guard(&guard_diagnostics);
                        let failed_payload =
                            no_observable_output_failed_payload(response_id.as_deref());
                        trace_downstream_sse_event(&downstream_sse_trace_metadata(
                            &failed_payload,
                            DownstreamTraceAction::Terminal,
                            None,
                        ));
                        state.lease.mark_upstream_terminal().await;
                        state.lease.release();
                        state.final_done_pending = true;
                        debug!(response_id, event_type, "translation_event_forwarded");
                        debug!(response_id, "terminal_response_forwarded");
                        debug!(response_id, "final_done_queued");
                        return Some((
                            Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                                &failed_payload,
                            )),
                            state,
                        ));
                    }

                    state.queued_final_completed = Some(QueuedCompletedEvent {
                        payload: sanitized_completed,
                        diagnostics,
                    });
                    continue;
                }
                "response.failed" => {
                    trace_downstream_sse_event(&downstream_sse_trace_metadata(
                        &parsed,
                        DownstreamTraceAction::Terminal,
                        None,
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
                "response.incomplete" => {
                    trace_downstream_sse_event(&downstream_sse_trace_metadata(
                        &parsed,
                        DownstreamTraceAction::Terminal,
                        None,
                    ));
                    state.lease.mark_upstream_terminal().await;
                    state.lease.release();
                    state.final_done_pending = true;
                    debug!(event_type, "terminal_response_forwarded");
                    debug!(event_type, "final_done_queued");
                    return Some((
                        Ok::<Bytes, Infallible>(sse_terminal_response_incomplete_chunk(&parsed)),
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
                        None,
                    ));
                    let public_error = ThreadlineError::UpstreamErrorEvent.public_error();
                    let failed_payload = terminal_failed_payload(
                        parsed.get("response"),
                        response_id_from_event(&parsed),
                        public_error.code.into_owned(),
                        error_message.unwrap_or_else(|| public_error.message.into_owned()),
                    );
                    state.lease.mark_upstream_terminal().await;
                    state.lease.release();
                    state.final_done_pending = true;
                    return Some((
                        Ok::<Bytes, Infallible>(sse_terminal_response_failed_chunk(
                            &failed_payload,
                        )),
                        state,
                    ));
                }
                _ => {
                    let mut trace_diagnostics = DownstreamTraceDiagnostics::default();
                    if event_type == "response.output_text.delta" {
                        let key = visible_text_delta_source_key(&parsed);
                        if let Some(delta) = parsed.get("delta").and_then(Value::as_str) {
                            record_forwarded_visible_text_delta(&mut state, key, delta);
                            trace_diagnostics.visible_text_length = Some(delta.len());
                        }
                        record_forwarded_observable_output(
                            &mut state.observable_output,
                            &event_type,
                            &parsed,
                        );
                        state.downstream_visible_text_delta_count += 1;
                        trace_diagnostics.response_id =
                            response_id_from_event(&parsed).map(ToString::to_string);
                        trace_diagnostics.visible_text_delta_count = Some(1);
                    } else if event_type == "response.output_text.done" {
                        if let Some((key, payload)) = synthesized_output_text_done_delta(&parsed)
                            && queue_visible_text_delta(
                                &mut state,
                                key,
                                payload,
                                "response.output_text.done",
                                response_id_from_event(&parsed),
                            )
                        {
                            state.queued_forwarded_event =
                                Some(QueuedForwardedEvent { payload: parsed });
                            continue;
                        }
                    } else if event_type == "response.output_item.done"
                        && queue_visible_text_deltas(
                            &mut state,
                            synthesized_output_item_done_text_delta(&parsed),
                            "response.output_item.done",
                            response_id_from_event(&parsed),
                        )
                    {
                        state.queued_forwarded_event =
                            Some(QueuedForwardedEvent { payload: parsed });
                        continue;
                    }
                    record_forwarded_observable_output(
                        &mut state.observable_output,
                        &event_type,
                        &parsed,
                    );
                    trace_downstream_sse_event(&downstream_sse_trace_metadata(
                        &parsed,
                        DownstreamTraceAction::Forwarded,
                        Some(&trace_diagnostics),
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
        CompletedSanitizationDiagnostics, DownstreamTraceAction, DownstreamTraceDiagnostics,
        RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT, RESPONSES_TRANSLATION_EVENT_SUPPRESSED,
        RESPONSES_TRANSLATION_NO_OBSERVABLE_OUTPUT_GUARD, RESPONSES_TRANSLATION_UPSTREAM_EVENT,
        UpstreamEventTraceMetadata, VisibleAssistantText, VisibleTextSourceKey,
        downstream_sse_trace_metadata, sanitized_completed_event_with_diagnostics,
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

        let metadata =
            downstream_sse_trace_metadata(&parsed, DownstreamTraceAction::Forwarded, None);

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
    fn synthetic_delta_emission_records_source_and_lengths_without_text() {
        let synthetic_text = "synthesized visible assistant text";
        let parsed = json!({
            "type": "response.output_text.delta",
            "item_id": "msg-visible",
            "output_index": 3,
            "content_index": 1,
            "delta": synthetic_text
        });

        let metadata = downstream_sse_trace_metadata(
            &parsed,
            DownstreamTraceAction::Forwarded,
            Some(&DownstreamTraceDiagnostics {
                response_id: Some("response-visible".to_string()),
                synthetic_delta_source: Some("response.output_text.done"),
                visible_text_delta_count: Some(1),
                visible_text_length: Some(synthetic_text.len()),
                ..Default::default()
            }),
        );
        let metadata_debug = format!("{metadata:?}");

        assert_eq!(metadata.translation_action, "forwarded");
        assert_eq!(metadata.event_type, "response.output_text.delta");
        assert_eq!(metadata.response_id.as_deref(), Some("response-visible"));
        assert_eq!(metadata.item_id.as_deref(), Some("msg-visible"));
        assert_eq!(metadata.output_index, Some(3));
        assert_eq!(metadata.content_index, Some(1));
        assert_eq!(
            metadata.synthetic_delta_source,
            Some("response.output_text.done")
        );
        assert_eq!(metadata.visible_text_delta_count, Some(1));
        assert_eq!(metadata.visible_text_length, Some(synthetic_text.len()));
        assert!(!metadata_debug.contains(synthetic_text));
    }

    #[test]
    fn sanitized_completed_event_preserves_compaction_output() {
        let parsed = json!({
            "type": "response.completed",
            "response": {
                "id": "response-compaction-only",
                "output": [
                    {
                        "type": "compaction",
                        "id": "compaction-1",
                        "encrypted_content": "opaque-compaction-payload"
                    }
                ]
            }
        });

        let (sanitized, diagnostics) = sanitized_completed_event_with_diagnostics(&parsed, &[]);
        let sanitized_output = sanitized["response"]["output"]
            .as_array()
            .expect("sanitized output array");

        assert_eq!(diagnostics, CompletedSanitizationDiagnostics::default());
        assert_eq!(sanitized_output.len(), 1);
        assert_eq!(sanitized_output[0]["type"], "compaction");
        assert_eq!(sanitized_output[0]["id"], "compaction-1");
        assert_eq!(sanitized_output[0]["encrypted_content"], "opaque-compaction-payload");
    }

    #[test]
    fn sanitized_completed_event_removes_internal_function_calls_but_preserves_compaction() {
        let encrypted_content = "opaque-compaction-payload";
        let internal_arguments = "{\"token\":\"secret\"}";
        let visible_text = vec![VisibleAssistantText {
            key: VisibleTextSourceKey::new(Some("message-visible".to_string()), Some(2), Some(0)),
            text: "visible assistant answer".to_string(),
        }];
        let parsed = json!({
            "type": "response.completed",
            "response": {
                "id": "response-sanitized",
                "output": [
                    {
                        "type": "function_call",
                        "id": "fc-internal",
                        "name": "threadline_echo",
                        "arguments": internal_arguments
                    },
                    {
                        "type": "compaction",
                        "id": "compaction-1",
                        "encrypted_content": encrypted_content
                    }
                ]
            }
        });

        let (sanitized, diagnostics) =
            sanitized_completed_event_with_diagnostics(&parsed, &visible_text);
        let diagnostics_debug = format!("{diagnostics:?}");
        let sanitized_output = sanitized["response"]["output"]
            .as_array()
            .expect("sanitized output array");

        assert_eq!(
            diagnostics,
            CompletedSanitizationDiagnostics {
                sanitized_internal_function_call_count: 1,
                sanitized_compaction_count: 0,
                completed_visible_message_count: 1,
            }
        );
        assert_eq!(sanitized_output.len(), 2);
        assert_eq!(sanitized_output[0]["type"], "compaction");
        assert_eq!(sanitized_output[0]["id"], "compaction-1");
        assert_eq!(sanitized_output[0]["encrypted_content"], encrypted_content);
        assert_eq!(sanitized_output[1]["type"], "message");
        assert!(!diagnostics_debug.contains(encrypted_content));
        assert!(!diagnostics_debug.contains(internal_arguments));
    }

    #[test]
    fn completed_sanitization_preserves_concrete_non_text_observable_output() {
        let parsed = json!({
            "type": "response.completed",
            "response": {
                "id": "response-non-text-observable",
                "output": [
                    {
                        "type": "function_call",
                        "id": "fc-internal",
                        "name": "threadline_echo",
                        "arguments": "{\"value\":\"hidden\"}"
                    },
                    {
                        "type": "function_call",
                        "id": "fc-external",
                        "call_id": "call-visible",
                        "name": "apply_patch",
                        "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"
                    },
                    {
                        "type": "image_generation_call",
                        "id": "img-1",
                        "result": "image-asset-1"
                    },
                    {
                        "type": "context",
                        "id": "ctx-1",
                        "summary": "context snapshot"
                    },
                    {
                        "type": "compaction",
                        "id": "cmp-1",
                        "encrypted_content": "opaque"
                    }
                ]
            }
        });

        let (sanitized, diagnostics) = sanitized_completed_event_with_diagnostics(&parsed, &[]);
        let sanitized_output = sanitized["response"]["output"]
            .as_array()
            .expect("sanitized output array");

        assert_eq!(
            diagnostics,
            CompletedSanitizationDiagnostics {
                sanitized_internal_function_call_count: 1,
                sanitized_compaction_count: 0,
                completed_visible_message_count: 0,
            }
        );
        assert_eq!(sanitized_output.len(), 4);
        assert_eq!(sanitized_output[0]["id"], "fc-external");
        assert_eq!(sanitized_output[0]["name"], "apply_patch");
        assert_eq!(sanitized_output[1]["id"], "img-1");
        assert_eq!(sanitized_output[1]["type"], "image_generation_call");
        assert_eq!(sanitized_output[2]["id"], "ctx-1");
        assert_eq!(sanitized_output[2]["type"], "context");
        assert_eq!(sanitized_output[3]["id"], "cmp-1");
        assert_eq!(sanitized_output[3]["type"], "compaction");
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
        assert_eq!(
            RESPONSES_TRANSLATION_NO_OBSERVABLE_OUTPUT_GUARD,
            "responses_translation_no_observable_output_guard"
        );
    }
}
