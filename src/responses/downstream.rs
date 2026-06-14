use axum::body::Bytes;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::errors::ThreadlineError;

const SUMMARY_PROMPT_PREFIX: &str =
    "The conversation has grown too large for the context window and must be compacted now";
const SUMMARY_TAGS_INSTRUCTION: &str =
    "Output your summary wrapped in <summary> and </summary> tags";
const SUMMARY_ONLY_TASK_INSTRUCTION: &str =
    "Your ONLY task right now is to produce a comprehensive summary";

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
    #[serde(flatten)]
    pub(super) payload: serde_json::Map<String, Value>,
}

pub(super) fn parse_downstream_request(
    payload: Value,
) -> Result<DownstreamResponsesRequest, ThreadlineError> {
    let mut request = serde_json::from_value::<DownstreamResponsesRequest>(payload)
        .map_err(|_| ThreadlineError::InvalidResponsesRequest)?;
    request.classification = classify_request(&request.payload);
    Ok(request)
}

fn classify_request(payload: &serde_json::Map<String, Value>) -> DownstreamRequestClassification {
    if is_auxiliary_summary_request(payload.get("input")) {
        DownstreamRequestClassification::AuxiliarySummary
    } else {
        DownstreamRequestClassification::Normal
    }
}

fn is_auxiliary_summary_request(input: Option<&Value>) -> bool {
    let Some(input) = input else {
        return false;
    };

    let Some(summary_text) = final_summary_instruction_text(input) else {
        return false;
    };

    let fingerprints = collect_summary_fingerprints(input);
    fingerprints.all_present() && text_matches_summary_fingerprints(summary_text)
}

fn final_summary_instruction_text(input: &Value) -> Option<&str> {
    let final_item = input.as_array()?.last()?.as_object()?;

    if final_item.get("type")?.as_str()? != "message" {
        return None;
    }

    if final_item.get("role")?.as_str()? != "system" {
        return None;
    }

    let content = final_item.get("content")?.as_array()?;
    if content.len() != 1 {
        return None;
    }

    let content_item = content.first()?.as_object()?;
    if content_item.get("type")?.as_str()? != "input_text" {
        return None;
    }

    content_item.get("text")?.as_str()
}

#[derive(Default)]
struct SummaryFingerprints {
    has_prompt_prefix: bool,
    has_summary_tags_instruction: bool,
    has_summary_only_task_instruction: bool,
}

impl SummaryFingerprints {
    fn all_present(&self) -> bool {
        self.has_prompt_prefix
            && self.has_summary_tags_instruction
            && self.has_summary_only_task_instruction
    }

    fn record_text(&mut self, text: &str) {
        self.has_prompt_prefix |= text.starts_with(SUMMARY_PROMPT_PREFIX);
        self.has_summary_tags_instruction |= text.contains(SUMMARY_TAGS_INSTRUCTION);
        self.has_summary_only_task_instruction |= text.contains(SUMMARY_ONLY_TASK_INSTRUCTION);
    }
}

fn collect_summary_fingerprints(value: &Value) -> SummaryFingerprints {
    let mut fingerprints = SummaryFingerprints::default();
    collect_summary_fingerprints_into(value, &mut fingerprints);
    fingerprints
}

fn collect_summary_fingerprints_into(value: &Value, fingerprints: &mut SummaryFingerprints) {
    match value {
        Value::String(text) => fingerprints.record_text(text),
        Value::Array(values) => {
            for value in values {
                collect_summary_fingerprints_into(value, fingerprints);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_summary_fingerprints_into(value, fingerprints);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn text_matches_summary_fingerprints(text: &str) -> bool {
    text.starts_with(SUMMARY_PROMPT_PREFIX)
        && text.contains(SUMMARY_TAGS_INSTRUCTION)
        && text.contains(SUMMARY_ONLY_TASK_INSTRUCTION)
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
        let request = parse_downstream_request(json!({
            "previous_response_id": "resp_123",
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
        }))
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
