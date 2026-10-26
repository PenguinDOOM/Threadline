mod fingerprints;
mod sse;
use fingerprints::*;
#[cfg(test)]
use sse::sse_payload_chunk;
pub(super) use sse::{
    safe_scalar_field, sse_done_chunk, sse_error_chunk, sse_json_chunk,
    sse_terminal_response_failed_chunk, sse_terminal_response_incomplete_chunk,
};

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
const MAX_ALLOWLISTED_INTERACTION_TYPE_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum DownstreamRequestClassification {
    #[default]
    Normal,
    AuxiliarySummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DownstreamInteractionType {
    #[default]
    None,
    ConversationCompaction,
    Other,
}

impl DownstreamInteractionType {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ConversationCompaction => "conversation_compaction",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DownstreamRequestMetadata {
    interaction_type: DownstreamInteractionType,
}

impl DownstreamRequestMetadata {
    #[cfg(test)]
    pub(crate) fn from_interaction_type_header_value(value: Option<&str>) -> Self {
        Self {
            interaction_type: normalize_interaction_type(value),
        }
    }

    pub(crate) fn from_interaction_type_header_bytes(value: Option<&[u8]>) -> Self {
        let interaction_type = match value {
            Some(raw) => match std::str::from_utf8(raw) {
                Ok(text) => normalize_interaction_type(Some(text)),
                Err(_) => DownstreamInteractionType::Other,
            },
            None => DownstreamInteractionType::None,
        };

        Self { interaction_type }
    }

    #[cfg(test)]
    pub(crate) fn interaction_type(self) -> DownstreamInteractionType {
        self.interaction_type
    }
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

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn parse_downstream_request(
    payload: Value,
) -> Result<DownstreamResponsesRequest, ThreadlineError> {
    parse_downstream_request_with_metadata(payload, DownstreamRequestMetadata::default())
}

pub(super) fn parse_downstream_request_with_metadata(
    payload: Value,
    metadata: DownstreamRequestMetadata,
) -> Result<DownstreamResponsesRequest, ThreadlineError> {
    let mut request = serde_json::from_value::<DownstreamResponsesRequest>(payload)
        .map_err(|_| ThreadlineError::InvalidResponsesRequest)?;
    let routing_diagnostics = collect_request_routing_diagnostics(&request.payload, metadata);
    request.classification = classify_request(&routing_diagnostics);
    request.routing_diagnostics = routing_diagnostics;
    Ok(request)
}

fn normalize_interaction_type(value: Option<&str>) -> DownstreamInteractionType {
    let Some(value) = value.map(str::trim) else {
        return DownstreamInteractionType::None;
    };

    if value.is_empty() {
        return DownstreamInteractionType::None;
    }

    if value.len() > MAX_ALLOWLISTED_INTERACTION_TYPE_LEN {
        return DownstreamInteractionType::Other;
    }

    if value.eq_ignore_ascii_case("conversation-compaction") {
        DownstreamInteractionType::ConversationCompaction
    } else {
        DownstreamInteractionType::Other
    }
}

fn classify_request(
    routing_diagnostics: &DownstreamRequestRoutingDiagnostics,
) -> DownstreamRequestClassification {
    if routing_diagnostics.interaction_type_compaction_hit {
        return DownstreamRequestClassification::AuxiliarySummary;
    }

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
    let Some(input) = payload.get("input") else {
        return false;
    };

    collect_conflict_fallback_summary_fingerprints(input).matches_auxiliary_summary()
}

#[derive(Debug, Clone, Default)]
pub(super) struct DownstreamRequestRoutingDiagnostics {
    pub(super) summary_hits: SummaryFingerprintHits,
    pub(super) interaction_type: DownstreamInteractionType,
    pub(super) interaction_type_compaction_hit: bool,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum InputSourceCategory {
    SummaryInstructionLike,
    OrdinaryUserContent,
    #[default]
    UnknownInputContent,
}

#[derive(Clone, Copy, Debug, Default)]
struct SummaryObservationContext<'a> {
    message_role: Option<&'a str>,
    content_item_type: Option<&'a str>,
    under_content_array: bool,
    final_input_item: bool,
    source_category: InputSourceCategory,
}

fn collect_request_routing_diagnostics(
    payload: &serde_json::Map<String, Value>,
    metadata: DownstreamRequestMetadata,
) -> DownstreamRequestRoutingDiagnostics {
    let input = payload.get("input");

    DownstreamRequestRoutingDiagnostics {
        summary_hits: collect_summary_fingerprints(input),
        interaction_type: metadata.interaction_type,
        interaction_type_compaction_hit: matches!(
            metadata.interaction_type,
            DownstreamInteractionType::ConversationCompaction
        ),
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

#[cfg(test)]
mod tests;
