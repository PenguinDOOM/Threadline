use axum::body::Bytes;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::errors::ThreadlineError;

const AUTO_CONTEXT_TOO_LARGE_PROMPT: &str =
    "The conversation has grown too large for the context window and must be compacted now";
const AUTO_SUMMARY_TAGS_INSTRUCTION: &str =
    "Output your summary wrapped in <summary> and </summary> tags";
const AUTO_ONLY_TASK_INSTRUCTION: &str =
    "Your ONLY task right now is to produce a comprehensive summary";
const NEW_AUTO_DETAILED_SUMMARY_INSTRUCTION: &str = "Your task is to create a comprehensive, detailed summary of the entire conversation that captures all essential information needed to seamlessly continue the work without any loss of context";
const MANUAL_SUMMARY_PROMPT: &str = "Summarize the conversation history so far, paying special attention to the most recent agent commands and tool results";
const MANUAL_STRUCTURE_INSTRUCTION: &str =
    "Structure your summary using the enhanced format provided in the system message";
const MANUAL_TOOL_RESULTS_INSTRUCTION: &str = "Include all important tool calls and their results";
const SIMPLE_HISTORY_CONTEXT_OBSERVED: &str =
    "The following is a compressed version of the preceeding history in the current conversation";
const SIMPLE_HISTORY_CONTEXT_CORRECTED: &str =
    "The following is a compressed version of the preceding history in the current conversation";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum DownstreamRequestClassification {
    #[default]
    Normal,
    AuxiliarySummary,
}

#[derive(Debug, Deserialize)]
pub(super) struct DownstreamResponsesRequest {
    #[serde(default)]
    pub(super) previous_response_id: Option<String>,
    #[serde(skip)]
    pub(super) classification: DownstreamRequestClassification,
    #[serde(skip)]
    routing_diagnostics: DownstreamRequestRoutingDiagnostics,
    #[serde(flatten)]
    pub(super) payload: serde_json::Map<String, Value>,
}

impl DownstreamResponsesRequest {
    pub(super) fn routing_diagnostics(&self) -> &DownstreamRequestRoutingDiagnostics {
        &self.routing_diagnostics
    }
}

pub(super) fn parse_downstream_request(
    payload: Value,
) -> Result<DownstreamResponsesRequest, ThreadlineError> {
    let mut request = serde_json::from_value::<DownstreamResponsesRequest>(payload)
        .map_err(|_| ThreadlineError::InvalidResponsesRequest)?;
    let routing_diagnostics = collect_request_routing_diagnostics(&request.payload);
    request.classification = classify_request(&routing_diagnostics);
    request.routing_diagnostics = routing_diagnostics;
    Ok(request)
}

fn classify_request(
    routing_diagnostics: &DownstreamRequestRoutingDiagnostics,
) -> DownstreamRequestClassification {
    if is_auxiliary_summary_request(&routing_diagnostics.summary_hits) {
        DownstreamRequestClassification::AuxiliarySummary
    } else {
        DownstreamRequestClassification::Normal
    }
}

fn is_auxiliary_summary_request(summary_hits: &SummaryFingerprintHits) -> bool {
    summary_hits.matches_auxiliary_summary()
}

pub(super) fn looks_like_auxiliary_summary_conflict_fallback(
    payload: &serde_json::Map<String, Value>,
) -> bool {
    if !payload.contains_key("context_management") {
        return false;
    }

    let Some(input) = payload.get("input") else {
        return false;
    };

    collect_conflict_fallback_summary_fingerprints(input).matches_auxiliary_summary()
}

pub(super) fn wants_reasoning_all_turns(payload: &serde_json::Map<String, Value>) -> bool {
    payload
        .get("reasoning")
        .and_then(Value::as_object)
        .and_then(|reasoning| reasoning.get("context"))
        .and_then(Value::as_str)
        == Some("all_turns")
}

#[derive(Debug, Clone, Default)]
pub(super) struct DownstreamRequestRoutingDiagnostics {
    pub(super) summary_hits: SummaryFingerprintHits,
    pub(super) tool_choice: Option<String>,
    pub(super) tools_count: usize,
    pub(super) input_item_count: usize,
    pub(super) last_input_role: Option<String>,
    pub(super) last_input_type: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct SummaryFingerprintHits {
    pub(super) manual_summary_prompt_hit: bool,
    pub(super) manual_structure_instruction_hit: bool,
    pub(super) manual_tool_results_instruction_hit: bool,
    pub(super) auto_context_too_large_hit: bool,
    pub(super) auto_summary_tags_hit: bool,
    pub(super) auto_only_task_hit: bool,
    pub(super) simple_history_context_hit: bool,
    pub(super) new_auto_detailed_summary_hit: bool,
    pub(super) new_auto_user_history_hit: bool,
    pub(super) new_auto_user_final_summary_prompt_hit: bool,
    pub(super) summary_instruction_like_hit: bool,
    manual_summary_prompt_instruction_like: bool,
    manual_structure_instruction_instruction_like: bool,
    manual_tool_results_instruction_instruction_like: bool,
    auto_context_too_large_instruction_like: bool,
    auto_summary_tags_instruction_like: bool,
    auto_only_task_instruction_like: bool,
    simple_history_context_instruction_like: bool,
    new_auto_detailed_summary_instruction_like: bool,
}

impl SummaryFingerprintHits {
    fn matches_auxiliary_summary(&self) -> bool {
        let manual_primary = self.manual_summary_prompt_instruction_like;
        let manual_secondary = self.manual_structure_instruction_instruction_like
            || self.manual_tool_results_instruction_instruction_like
            || self.simple_history_context_instruction_like;
        let auto_primary = self.auto_context_too_large_instruction_like;
        let auto_secondary = self.auto_summary_tags_instruction_like
            || self.auto_only_task_instruction_like
            || self.simple_history_context_instruction_like;
        let new_auto = self.new_auto_detailed_summary_instruction_like
            && self.new_auto_user_history_hit
            && self.new_auto_user_final_summary_prompt_hit;

        (manual_primary && manual_secondary) || (auto_primary && auto_secondary) || new_auto
    }

    fn record_text(&mut self, text: &str, context: SummaryObservationContext<'_>) {
        self.record_text_with_instruction_like(text, context.is_summary_instruction_like());

        if text.contains(NEW_AUTO_DETAILED_SUMMARY_INSTRUCTION) {
            self.new_auto_detailed_summary_hit = true;
            self.new_auto_detailed_summary_instruction_like |=
                context.is_summary_instruction_like();
        }

        if context.is_user_input_text()
            && (text.contains(SIMPLE_HISTORY_CONTEXT_OBSERVED)
                || text.contains(SIMPLE_HISTORY_CONTEXT_CORRECTED))
        {
            self.new_auto_user_history_hit = true;
        }

        if context.is_user_input_text()
            && text.contains(MANUAL_SUMMARY_PROMPT)
            && text.contains(MANUAL_STRUCTURE_INSTRUCTION)
            && text.contains(MANUAL_TOOL_RESULTS_INSTRUCTION)
        {
            self.new_auto_user_final_summary_prompt_hit = true;
        }
    }

    fn record_text_with_instruction_like(&mut self, text: &str, instruction_like: bool) {
        let had_instruction_like_hit = self.manual_summary_prompt_instruction_like
            || self.manual_structure_instruction_instruction_like
            || self.manual_tool_results_instruction_instruction_like
            || self.auto_context_too_large_instruction_like
            || self.auto_summary_tags_instruction_like
            || self.auto_only_task_instruction_like
            || self.simple_history_context_instruction_like;

        if text.contains(MANUAL_SUMMARY_PROMPT) {
            self.manual_summary_prompt_hit = true;
            self.manual_summary_prompt_instruction_like |= instruction_like;
        }
        if text.contains(MANUAL_STRUCTURE_INSTRUCTION) {
            self.manual_structure_instruction_hit = true;
            self.manual_structure_instruction_instruction_like |= instruction_like;
        }
        if text.contains(MANUAL_TOOL_RESULTS_INSTRUCTION) {
            self.manual_tool_results_instruction_hit = true;
            self.manual_tool_results_instruction_instruction_like |= instruction_like;
        }
        if text.contains(AUTO_CONTEXT_TOO_LARGE_PROMPT) {
            self.auto_context_too_large_hit = true;
            self.auto_context_too_large_instruction_like |= instruction_like;
        }
        if text.contains(AUTO_SUMMARY_TAGS_INSTRUCTION) {
            self.auto_summary_tags_hit = true;
            self.auto_summary_tags_instruction_like |= instruction_like;
        }
        if text.contains(AUTO_ONLY_TASK_INSTRUCTION) {
            self.auto_only_task_hit = true;
            self.auto_only_task_instruction_like |= instruction_like;
        }
        if text.contains(SIMPLE_HISTORY_CONTEXT_OBSERVED)
            || text.contains(SIMPLE_HISTORY_CONTEXT_CORRECTED)
        {
            self.simple_history_context_hit = true;
            self.simple_history_context_instruction_like |= instruction_like;
        }

        let has_instruction_like_hit = self.manual_summary_prompt_instruction_like
            || self.manual_structure_instruction_instruction_like
            || self.manual_tool_results_instruction_instruction_like
            || self.auto_context_too_large_instruction_like
            || self.auto_summary_tags_instruction_like
            || self.auto_only_task_instruction_like
            || self.simple_history_context_instruction_like;

        self.summary_instruction_like_hit |=
            instruction_like && (had_instruction_like_hit || has_instruction_like_hit);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum InputSourceCategory {
    SummaryInstructionLike,
    OrdinaryUserContent,
    #[default]
    UnknownInputContent,
}

impl InputSourceCategory {
    fn from_role(role: Option<&str>) -> Self {
        match role {
            Some("system" | "developer") => Self::SummaryInstructionLike,
            Some("user") => Self::OrdinaryUserContent,
            Some(_) | None => Self::UnknownInputContent,
        }
    }

    fn is_summary_instruction_like(self) -> bool {
        matches!(self, Self::SummaryInstructionLike)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SummaryObservationContext<'a> {
    message_role: Option<&'a str>,
    content_item_type: Option<&'a str>,
    under_content_array: bool,
    _final_input_item: bool,
    source_category: InputSourceCategory,
}

impl SummaryObservationContext<'_> {
    fn is_summary_instruction_like(self) -> bool {
        self.under_content_array
            && self.content_item_type == Some("input_text")
            && self.source_category.is_summary_instruction_like()
    }

    fn is_user_input_text(self) -> bool {
        self.under_content_array
            && self.content_item_type == Some("input_text")
            && self.source_category == InputSourceCategory::OrdinaryUserContent
    }
}

fn collect_request_routing_diagnostics(
    payload: &serde_json::Map<String, Value>,
) -> DownstreamRequestRoutingDiagnostics {
    let input = payload.get("input");

    DownstreamRequestRoutingDiagnostics {
        summary_hits: collect_summary_fingerprints(input),
        tool_choice: safe_value_type_label(payload.get("tool_choice")),
        tools_count: payload
            .get("tools")
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
        input_item_count: input.map_or(0, input_item_count),
        last_input_role: input.and_then(last_input_role),
        last_input_type: input.and_then(last_input_type),
    }
}

fn collect_summary_fingerprints(input: Option<&Value>) -> SummaryFingerprintHits {
    let Some(input) = input else {
        return SummaryFingerprintHits::default();
    };

    let mut fingerprints = SummaryFingerprintHits::default();
    collect_summary_fingerprints_into_input(input, &mut fingerprints);
    fingerprints
}

fn collect_conflict_fallback_summary_fingerprints(input: &Value) -> SummaryFingerprintHits {
    let mut fingerprints = SummaryFingerprintHits::default();

    match input {
        Value::Array(items) => {
            for item in items {
                collect_conflict_fallback_summary_from_input_item(item, &mut fingerprints);
            }
        }
        _ => collect_conflict_fallback_summary_from_input_item(input, &mut fingerprints),
    }

    fingerprints
}

fn collect_conflict_fallback_summary_from_input_item(
    value: &Value,
    fingerprints: &mut SummaryFingerprintHits,
) {
    let Some(item) = value.as_object() else {
        return;
    };

    if item.get("type").and_then(Value::as_str) != Some("input_text") {
        return;
    }

    let Some(text) = item.get("text").and_then(Value::as_str) else {
        return;
    };

    // Conflict fallback intentionally accepts only direct top-level input_text items.
    fingerprints.record_text_with_instruction_like(text, true);
}

fn collect_summary_fingerprints_into_input(
    value: &Value,
    fingerprints: &mut SummaryFingerprintHits,
) {
    match value {
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let context = SummaryObservationContext {
                    _final_input_item: index + 1 == items.len(),
                    ..SummaryObservationContext::default()
                };
                collect_summary_fingerprints_from_input_item(item, context, fingerprints);
            }
        }
        _ => collect_summary_fingerprints_into(
            value,
            SummaryObservationContext::default(),
            fingerprints,
        ),
    }
}

fn collect_summary_fingerprints_from_input_item<'a>(
    value: &'a Value,
    mut context: SummaryObservationContext<'a>,
    fingerprints: &mut SummaryFingerprintHits,
) {
    if let Some(item) = value.as_object() {
        context.content_item_type = item.get("type").and_then(Value::as_str);
        if context.content_item_type == Some("message") {
            context.message_role = item.get("role").and_then(Value::as_str);
            context.source_category = InputSourceCategory::from_role(context.message_role);

            if let Some(content) = item.get("content").and_then(Value::as_array) {
                for content_item in content {
                    let mut content_context = context;
                    content_context.under_content_array = true;
                    content_context.content_item_type = content_item
                        .as_object()
                        .and_then(|object| object.get("type"))
                        .and_then(Value::as_str);
                    collect_summary_fingerprints_into(content_item, content_context, fingerprints);
                }
                return;
            }
        }
    }

    collect_summary_fingerprints_into(value, context, fingerprints);
}

fn collect_summary_fingerprints_into<'a>(
    value: &'a Value,
    context: SummaryObservationContext<'a>,
    fingerprints: &mut SummaryFingerprintHits,
) {
    match value {
        Value::String(text) => fingerprints.record_text(text, context),
        Value::Array(values) => {
            for value in values {
                collect_summary_fingerprints_into(value, context, fingerprints);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_summary_fingerprints_into(value, context, fingerprints);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn input_item_count(value: &Value) -> usize {
    match value {
        Value::Array(items) => items.len(),
        Value::Null => 0,
        _ => 1,
    }
}

fn last_input_role(value: &Value) -> Option<String> {
    let value = match value {
        Value::Array(items) => items.last()?,
        _ => value,
    };

    value
        .as_object()
        .and_then(|object| object.get("role"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn last_input_type(value: &Value) -> Option<String> {
    let value = match value {
        Value::Array(items) => items.last()?,
        _ => value,
    };

    safe_value_type_label(Some(value))
}

fn safe_value_type_label(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(_) => Some("string".to_string()),
        Value::Array(_) => Some("array".to_string()),
        Value::Object(object) => object
            .get("type")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| Some("object".to_string())),
        Value::Bool(_) => Some("bool".to_string()),
        Value::Number(_) => Some("number".to_string()),
        Value::Null => Some("null".to_string()),
    }
}

pub(super) fn sse_payload_chunk(event: &str, payload: &str) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {payload}\n\n"))
}

pub(super) fn sse_json_chunk(event: &str, payload: &Value) -> Bytes {
    let payload = serde_json::to_string(payload).expect("serialize downstream sse payload");
    sse_payload_chunk(event, &payload)
}

pub(super) fn sse_done_chunk() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}

fn safe_object_clone(value: Option<&Value>) -> Option<Value> {
    value.and_then(Value::as_object).cloned().map(Value::Object)
}

fn sanitized_terminal_response(payload: &Value, status: &str) -> Map<String, Value> {
    let source = payload.get("response").and_then(Value::as_object);
    let mut response = Map::new();

    if let Some(response_id) = source
        .and_then(|value| value.get("id"))
        .and_then(safe_scalar_field)
    {
        response.insert("id".to_string(), Value::String(response_id));
    }

    if let Some(model) = source
        .and_then(|value| value.get("model"))
        .and_then(safe_scalar_field)
    {
        response.insert("model".to_string(), Value::String(model));
    }

    if let Some(usage) = safe_object_clone(source.and_then(|value| value.get("usage"))) {
        response.insert("usage".to_string(), usage);
    }

    if let Some(output) = payload
        .get("response")
        .and_then(|value| value.get("output"))
        .and_then(Value::as_array)
        .cloned()
    {
        response.insert("output".to_string(), Value::Array(output));
    }

    response.insert("status".to_string(), Value::String(status.to_string()));
    response
}

pub(super) fn sse_terminal_response_failed_chunk(payload: &Value) -> Bytes {
    let fallback = ThreadlineError::UpstreamResponseFailed.public_error();
    let error = payload.get("error");
    let mut response = sanitized_terminal_response(payload, "failed");
    response.insert(
        "error".to_string(),
        Value::Object(Map::from_iter([
            (
                "code".to_string(),
                Value::String(
                    error
                        .and_then(|value| value.get("code"))
                        .and_then(safe_scalar_field)
                        .unwrap_or_else(|| fallback.code.into_owned()),
                ),
            ),
            (
                "message".to_string(),
                Value::String(
                    error
                        .and_then(|value| value.get("message"))
                        .and_then(safe_scalar_field)
                        .unwrap_or_else(|| fallback.message.into_owned()),
                ),
            ),
        ])),
    );

    sse_json_chunk(
        "response.failed",
        &Value::Object(Map::from_iter([
            (
                "type".to_string(),
                Value::String("response.failed".to_string()),
            ),
            ("response".to_string(), Value::Object(response)),
        ])),
    )
}

pub(super) fn sse_terminal_response_incomplete_chunk(payload: &Value) -> Bytes {
    let mut response = sanitized_terminal_response(payload, "incomplete");

    if let Some(reason) = payload
        .get("response")
        .and_then(|value| value.get("incomplete_details"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("reason"))
        .and_then(safe_scalar_field)
    {
        response.insert(
            "incomplete_details".to_string(),
            Value::Object(Map::from_iter([(
                "reason".to_string(),
                Value::String(reason),
            )])),
        );
    }

    sse_json_chunk(
        "response.incomplete",
        &Value::Object(Map::from_iter([
            (
                "type".to_string(),
                Value::String("response.incomplete".to_string()),
            ),
            ("response".to_string(), Value::Object(response)),
        ])),
    )
}

pub(super) fn safe_scalar_field(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

pub(super) fn sse_error_chunk(error: &ThreadlineError) -> Bytes {
    let payload = serde_json::to_value(error.public_error_document())
        .expect("convert threadline error payload to json value");
    sse_json_chunk("error", &payload)
}

#[cfg(test)]
mod tests {
    use super::{
        DownstreamRequestClassification, parse_downstream_request, safe_scalar_field,
        sse_done_chunk, sse_error_chunk, sse_json_chunk, sse_payload_chunk,
        sse_terminal_response_failed_chunk, sse_terminal_response_incomplete_chunk,
        wants_reasoning_all_turns,
    };
    use crate::errors::ThreadlineError;
    use serde_json::{Value, json};

    fn auxiliary_summary_text() -> &'static str {
        concat!(
            "The conversation has grown too large for the context window and must be compacted now",
            "\n\n",
            "Your ONLY task right now is to produce a comprehensive summary",
            "\n",
            "Output your summary wrapped in <summary> and </summary> tags"
        )
    }

    fn auxiliary_summary_input_item() -> Value {
        json!({
            "type": "message",
            "role": "system",
            "content": [
                {
                    "type": "input_text",
                    "text": auxiliary_summary_text()
                }
            ]
        })
    }

    fn manual_summary_text() -> &'static str {
        concat!(
            "Summarize the conversation history so far, paying special attention to the most recent agent commands and tool results",
            "\n\n",
            "Structure your summary using the enhanced format provided in the system message",
            "\n",
            "Include all important tool calls and their results"
        )
    }

    fn manual_summary_input_item() -> Value {
        json!({
            "type": "message",
            "role": "system",
            "content": [
                {
                    "type": "input_text",
                    "text": manual_summary_text()
                }
            ]
        })
    }

    fn manual_simple_summary_text() -> &'static str {
        concat!(
            "Summarize the conversation history so far, paying special attention to the most recent agent commands and tool results",
            "\n\n",
            "Include all important tool calls and their results"
        )
    }

    fn manual_simple_summary_input_item() -> Value {
        json!({
            "type": "message",
            "role": "system",
            "content": [
                {
                    "type": "input_text",
                    "text": manual_simple_summary_text()
                }
            ]
        })
    }

    fn simple_history_context_text() -> &'static str {
        "The following is a compressed version of the preceeding history in the current conversation"
    }

    fn simple_history_context_input_item() -> Value {
        json!({
            "type": "message",
            "role": "system",
            "content": [
                {
                    "type": "input_text",
                    "text": simple_history_context_text()
                }
            ]
        })
    }

    fn new_auto_system_summary_text() -> &'static str {
        "Your task is to create a comprehensive, detailed summary of the entire conversation that captures all essential information needed to seamlessly continue the work without any loss of context"
    }

    fn new_auto_compressed_history_text() -> &'static str {
        "The following is a compressed version of the preceeding history in the current conversation"
    }

    fn new_auto_compressed_history_text_corrected() -> &'static str {
        "The following is a compressed version of the preceding history in the current conversation"
    }

    fn new_auto_final_summary_prompt_text() -> &'static str {
        concat!(
            "Summarize the conversation history so far, paying special attention to the most recent agent commands and tool results that triggered this summarization.",
            " Structure your summary using the enhanced format provided in the system message.\n",
            "Focus particularly on:\n",
            "- The specific agent commands/tools that were just executed\n",
            "- The results returned from these recent tool calls (truncate if very long but preserve key information)\n",
            "- What the agent was actively working on when the token budget was exceeded\n",
            "- How these recent operations connect to the overall user goals\n",
            "Include all important tool calls and their results as part of the appropriate sections, with special emphasis on the most recent operations."
        )
    }

    fn input_text_message(role: &str, text: &str) -> Value {
        json!({
            "type": "message",
            "role": role,
            "content": [
                {
                    "type": "input_text",
                    "text": text
                }
            ]
        })
    }

    #[test]
    fn wants_reasoning_all_turns_matches_only_exact_string_value() {
        assert!(wants_reasoning_all_turns(
            json!({
                "reasoning": {
                    "context": "all_turns"
                }
            })
            .as_object()
            .expect("object payload")
        ));

        assert!(!wants_reasoning_all_turns(
            json!({
                "reasoning": {
                    "context": "last_turn"
                }
            })
            .as_object()
            .expect("object payload")
        ));
    }

    #[test]
    fn wants_reasoning_all_turns_returns_false_for_missing_or_non_object_reasoning() {
        assert!(!wants_reasoning_all_turns(
            json!({ "input": "no reasoning" })
                .as_object()
                .expect("object payload")
        ));

        assert!(!wants_reasoning_all_turns(
            json!({ "reasoning": "all_turns" })
                .as_object()
                .expect("object payload")
        ));
    }

    #[test]
    fn wants_reasoning_all_turns_returns_false_for_non_string_context() {
        assert!(!wants_reasoning_all_turns(
            json!({
                "reasoning": {
                    "context": true
                }
            })
            .as_object()
            .expect("object payload")
        ));
    }

    fn new_auto_system_summary_input_item() -> Value {
        input_text_message("system", new_auto_system_summary_text())
    }

    fn new_auto_compressed_history_input_item() -> Value {
        input_text_message("user", new_auto_compressed_history_text())
    }

    fn new_auto_compressed_history_input_item_corrected() -> Value {
        input_text_message("user", new_auto_compressed_history_text_corrected())
    }

    fn new_auto_final_summary_prompt_input_item() -> Value {
        input_text_message("user", new_auto_final_summary_prompt_text())
    }

    fn classify_input(input: Vec<Value>) -> DownstreamRequestClassification {
        parse_downstream_request(json!({
            "previous_response_id": "resp_123",
            "input": input
        }))
        .expect("parse request")
        .classification
    }

    fn sanitized_observed_auxiliary_summary_request() -> Value {
        json!({
            "model": "gpt-5.4",
            "previous_response_id": "resp_123",
            "context_management": {
                "type": "compaction",
                "compact_threshold": 12345
            },
            "tools": [
                {
                    "type": "function",
                    "name": "user_tool",
                    "description": "User-defined tool",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false
                    }
                },
                {
                    "type": "function",
                    "name": "threadline_echo",
                    "description": "Threadline internal tool",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "value": {
                                "type": "string"
                            }
                        },
                        "required": ["value"],
                        "additionalProperties": false
                    }
                }
            ],
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Continue from the earlier answer."
                        }
                    ]
                },
                auxiliary_summary_input_item()
            ]
        })
    }

    #[test]
    fn parse_downstream_request_extracts_previous_response_id_and_payload() {
        let request = parse_downstream_request(json!({
            "previous_response_id": "resp_123",
            "model": "gpt-5.4",
            "stream": true
        }))
        .expect("parse request");

        assert_eq!(request.previous_response_id.as_deref(), Some("resp_123"));
        assert_eq!(request.payload.get("model"), Some(&json!("gpt-5.4")));
        assert_eq!(request.payload.get("stream"), Some(&json!(true)));
        assert!(!request.payload.contains_key("previous_response_id"));
    }

    #[test]
    fn parse_downstream_request_identifies_auxiliary_summary_request() {
        let request = parse_downstream_request(sanitized_observed_auxiliary_summary_request())
            .expect("parse request");

        assert_eq!(
            request.classification,
            DownstreamRequestClassification::AuxiliarySummary
        );
    }

    #[test]
    fn parse_downstream_request_does_not_classify_context_management_only() {
        let request = parse_downstream_request(json!({
            "previous_response_id": "resp_123",
            "context_management": {
                "type": "auto"
            },
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Please continue the earlier task."
                        }
                    ]
                }
            ]
        }))
        .expect("parse request");

        assert_eq!(
            request.classification,
            DownstreamRequestClassification::Normal
        );
    }

    #[test]
    fn parse_downstream_request_does_not_classify_fingerprints_outside_input() {
        let request = parse_downstream_request(json!({
            "previous_response_id": "resp_123",
            "metadata": {
                "summary_prompt": auxiliary_summary_text()
            },
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Please continue the earlier task."
                        }
                    ]
                }
            ]
        }))
        .expect("parse request");

        assert_eq!(
            request.classification,
            DownstreamRequestClassification::Normal
        );
    }

    #[test]
    fn parse_downstream_request_does_not_classify_partial_summary_quote() {
        let request = parse_downstream_request(json!({
            "previous_response_id": "resp_123",
            "input": [
                {
                    "type": "message",
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "The conversation has grown too large for the context window and must be compacted now"
                        }
                    ]
                }
            ]
        }))
        .expect("parse request");

        assert_eq!(
            request.classification,
            DownstreamRequestClassification::Normal
        );
    }

    #[test]
    fn parse_downstream_request_does_not_classify_user_role_full_prompt_quote() {
        let request = parse_downstream_request(json!({
            "previous_response_id": "resp_123",
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": auxiliary_summary_text()
                        }
                    ]
                }
            ]
        }))
        .expect("parse request");

        assert_eq!(
            request.classification,
            DownstreamRequestClassification::Normal
        );
    }

    #[test]
    fn parse_downstream_request_classifies_manual_full_summary_prompt_fingerprints() {
        assert_eq!(
            classify_input(vec![
                json!({
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Continue from the earlier answer."
                        }
                    ]
                }),
                manual_summary_input_item(),
            ]),
            DownstreamRequestClassification::AuxiliarySummary
        );
    }

    #[test]
    fn parse_downstream_request_classifies_manual_simple_summary_prompt_fingerprints() {
        assert_eq!(
            classify_input(vec![
                manual_simple_summary_input_item(),
                json!({
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Acknowledge the compaction request."
                        }
                    ]
                }),
            ]),
            DownstreamRequestClassification::AuxiliarySummary
        );
    }

    #[test]
    fn parse_downstream_request_does_not_classify_user_role_manual_summary_quote_only() {
        assert_eq!(
            classify_input(vec![json!({
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": format!("Quoted prompt: {}", manual_summary_text())
                    }
                ]
            })]),
            DownstreamRequestClassification::Normal
        );
    }

    #[test]
    fn parse_downstream_request_classifies_new_auto_compaction_prompt_fingerprints() {
        assert_eq!(
            classify_input(vec![
                new_auto_system_summary_input_item(),
                new_auto_compressed_history_input_item(),
                new_auto_final_summary_prompt_input_item(),
            ]),
            DownstreamRequestClassification::AuxiliarySummary
        );
    }

    #[test]
    fn parse_downstream_request_does_not_classify_new_auto_user_quote_only() {
        assert_eq!(
            classify_input(vec![
                new_auto_compressed_history_input_item(),
                new_auto_final_summary_prompt_input_item(),
            ]),
            DownstreamRequestClassification::Normal
        );
    }

    #[test]
    fn parse_downstream_request_does_not_classify_new_auto_partial_fingerprints() {
        for (name, input) in [
            (
                "system_plus_history_only",
                vec![
                    new_auto_system_summary_input_item(),
                    new_auto_compressed_history_input_item(),
                ],
            ),
            (
                "system_plus_final_prompt_only",
                vec![
                    new_auto_system_summary_input_item(),
                    new_auto_final_summary_prompt_input_item(),
                ],
            ),
            (
                "history_plus_final_prompt_only",
                vec![
                    new_auto_compressed_history_input_item(),
                    new_auto_final_summary_prompt_input_item(),
                ],
            ),
            ("system_only", vec![new_auto_system_summary_input_item()]),
            (
                "history_only",
                vec![new_auto_compressed_history_input_item()],
            ),
            (
                "final_prompt_only",
                vec![new_auto_final_summary_prompt_input_item()],
            ),
        ] {
            assert_eq!(
                classify_input(input),
                DownstreamRequestClassification::Normal,
                "fixture should remain normal: {name}"
            );
        }
    }

    #[test]
    fn parse_downstream_request_does_not_classify_new_auto_fingerprints_outside_input_text() {
        let request = parse_downstream_request(json!({
            "previous_response_id": "resp_123",
            "metadata": {
                "system_prompt": new_auto_system_summary_text(),
                "history": new_auto_compressed_history_text(),
                "final_prompt": new_auto_final_summary_prompt_text()
            },
            "tools": [
                {
                    "type": "function",
                    "name": "echo",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "summary": {
                                "type": "string",
                                "description": new_auto_final_summary_prompt_text()
                            }
                        }
                    }
                }
            ],
            "input": [
                {
                    "type": "message",
                    "role": "system",
                    "content": [
                        {
                            "type": "input_image",
                            "image_url": new_auto_system_summary_text()
                        }
                    ]
                },
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Please continue the earlier task."
                        }
                    ]
                }
            ]
        }))
        .expect("parse request");

        assert_eq!(
            request.classification,
            DownstreamRequestClassification::Normal
        );
    }

    #[test]
    fn parse_downstream_request_classifies_new_auto_compaction_prompt_with_corrected_history_spelling()
     {
        assert_eq!(
            classify_input(vec![
                new_auto_system_summary_input_item(),
                new_auto_compressed_history_input_item_corrected(),
                new_auto_final_summary_prompt_input_item(),
            ]),
            DownstreamRequestClassification::AuxiliarySummary
        );
    }

    #[test]
    fn parse_downstream_request_classifies_auto_background_compaction_in_non_final_shapes() {
        for (name, input) in [
            (
                "auto_summary_followed_by_user_message",
                vec![
                    auxiliary_summary_input_item(),
                    json!({
                        "type": "message",
                        "role": "user",
                        "content": [
                            {
                                "type": "input_text",
                                "text": "Please keep this request moving."
                            }
                        ]
                    }),
                ],
            ),
            (
                "auto_summary_before_non_message_item",
                vec![
                    auxiliary_summary_input_item(),
                    json!({
                        "type": "input_text",
                        "text": "Resume after compaction."
                    }),
                ],
            ),
        ] {
            assert_eq!(
                classify_input(input),
                DownstreamRequestClassification::AuxiliarySummary,
                "fixture should classify as auxiliary summary: {name}"
            );
        }
    }

    #[test]
    fn parse_downstream_request_classifies_simple_history_context_only_with_summary_prompt() {
        for (name, input) in [
            (
                "simple_history_plus_manual_summary_prompt",
                vec![
                    simple_history_context_input_item(),
                    manual_summary_input_item(),
                ],
            ),
            (
                "simple_history_plus_auto_summary_prompt",
                vec![
                    simple_history_context_input_item(),
                    auxiliary_summary_input_item(),
                ],
            ),
        ] {
            assert_eq!(
                classify_input(input),
                DownstreamRequestClassification::AuxiliarySummary,
                "fixture should classify as auxiliary summary: {name}"
            );
        }
    }

    #[test]
    fn parse_downstream_request_keeps_ordinary_and_quoted_summary_shapes_normal() {
        for (name, input) in [
            (
                "ordinary_user_prompt",
                vec![json!({
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Please continue the earlier task."
                        }
                    ]
                })],
            ),
            (
                "simple_history_only_context",
                vec![simple_history_context_input_item()],
            ),
            (
                "user_role_full_prompt_quote_with_simple_history_context",
                vec![
                    simple_history_context_input_item(),
                    json!({
                        "type": "message",
                        "role": "user",
                        "content": [
                            {
                                "type": "input_text",
                                "text": concat!(
                                    "Quoted prompt: ",
                                    "Summarize the conversation history so far, paying special attention to the most recent agent commands and tool results",
                                    "\n\n",
                                    "Structure your summary using the enhanced format provided in the system message",
                                    "\n",
                                    "Include all important tool calls and their results"
                                )
                            }
                        ]
                    }),
                ],
            ),
        ] {
            assert_eq!(
                classify_input(input),
                DownstreamRequestClassification::Normal,
                "fixture should remain normal: {name}"
            );
        }
    }

    #[test]
    fn sse_payload_chunk_keeps_single_line_frame() {
        let chunk = sse_payload_chunk("response.output_text.delta", "{\"delta\":\"hi\"}");

        assert_eq!(
            std::str::from_utf8(&chunk).expect("utf8"),
            "event: response.output_text.delta\ndata: {\"delta\":\"hi\"}\n\n"
        );
    }

    #[test]
    fn sse_json_chunk_serializes_compact_json() {
        let chunk = sse_json_chunk("response.completed", &json!({"id":"resp_1","ok":true}));

        assert_eq!(
            std::str::from_utf8(&chunk).expect("utf8"),
            "event: response.completed\ndata: {\"id\":\"resp_1\",\"ok\":true}\n\n"
        );
    }

    #[test]
    fn sse_done_chunk_keeps_bare_done_payload() {
        let chunk = sse_done_chunk();

        assert_eq!(
            std::str::from_utf8(&chunk).expect("utf8"),
            "data: [DONE]\n\n"
        );
    }

    #[test]
    fn sse_terminal_response_failed_chunk_preserves_public_shape() {
        let chunk = sse_terminal_response_failed_chunk(&json!({
            "type": "response.failed",
            "response": { "id": 42 },
            "error": {
                "code": "tool_timeout",
                "message": "tool timed out"
            }
        }));

        assert_eq!(
            std::str::from_utf8(&chunk).expect("utf8"),
            concat!(
                "event: response.failed\n",
                "data: {\"response\":{\"error\":{\"code\":\"tool_timeout\",\"message\":\"tool timed out\"},\"id\":\"42\",\"status\":\"failed\"},\"type\":\"response.failed\"}\n\n"
            )
        );
    }

    #[test]
    fn sse_terminal_response_failed_chunk_uses_fallback_error_fields() {
        let chunk = sse_terminal_response_failed_chunk(&json!({
            "type": "response.failed",
            "response": {},
            "error": {}
        }));

        assert_eq!(
            std::str::from_utf8(&chunk).expect("utf8"),
            concat!(
                "event: response.failed\n",
                "data: {\"response\":{\"error\":{\"code\":\"upstream_response_failed\",\"message\":\"The upstream response.failed event cannot be streamed as a successful downstream response.\"},\"status\":\"failed\"},\"type\":\"response.failed\"}\n\n"
            )
        );
    }

    #[test]
    fn sse_terminal_response_incomplete_chunk_preserves_safe_terminal_fields() {
        let chunk = sse_terminal_response_incomplete_chunk(&json!({
            "response": {
                "id": "response-1",
                "model": "gpt-5.4",
                "usage": {
                    "input_tokens": 4,
                    "output_tokens": 2,
                    "total_tokens": 6
                },
                "output": [
                    {
                        "id": "assistant-1",
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "partial"
                            }
                        ]
                    }
                ],
                "incomplete_details": {
                    "reason": "max_output_tokens",
                    "ignored": {"nested": true}
                }
            }
        }));

        let payload = std::str::from_utf8(&chunk)
            .expect("utf8 chunk")
            .strip_prefix("event: response.incomplete\ndata: ")
            .and_then(|text| text.strip_suffix("\n\n"))
            .map(|text| serde_json::from_str::<Value>(text).expect("payload json"))
            .expect("incomplete payload");

        assert_eq!(payload["type"], "response.incomplete");
        assert_eq!(payload["response"]["id"], "response-1");
        assert_eq!(payload["response"]["model"], "gpt-5.4");
        assert_eq!(payload["response"]["usage"]["total_tokens"], 6);
        assert_eq!(
            payload["response"]["output"][0]["content"][0]["text"],
            "partial"
        );
        assert_eq!(
            payload["response"]["incomplete_details"]["reason"],
            "max_output_tokens"
        );
    }

    #[test]
    fn safe_scalar_field_accepts_only_scalar_values() {
        assert_eq!(
            safe_scalar_field(&json!("hello")),
            Some("hello".to_string())
        );
        assert_eq!(safe_scalar_field(&json!(7)), Some("7".to_string()));
        assert_eq!(safe_scalar_field(&json!(false)), Some("false".to_string()));
        assert_eq!(safe_scalar_field(&Value::Null), None);
        assert_eq!(safe_scalar_field(&json!([1, 2, 3])), None);
        assert_eq!(safe_scalar_field(&json!({"a": 1})), None);
    }

    #[test]
    fn sse_error_chunk_preserves_public_error_shape() {
        let chunk = sse_error_chunk(&ThreadlineError::UpstreamWebSocketClosed);

        assert_eq!(
            std::str::from_utf8(&chunk).expect("utf8"),
            concat!(
                "event: error\n",
                "data: {\"error\":{\"code\":\"upstream_websocket_closed\",\"message\":\"The upstream Codex websocket closed before Threadline finished streaming the response.\",\"type\":\"bad_gateway_error\"}}\n\n"
            )
        );
    }
}
