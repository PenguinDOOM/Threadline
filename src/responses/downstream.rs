use axum::body::Bytes;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::errors::ThreadlineError;

#[derive(Debug, Deserialize)]
pub(super) struct DownstreamResponsesRequest {
    #[serde(default)]
    pub(super) previous_response_id: Option<String>,
    #[serde(flatten)]
    pub(super) payload: serde_json::Map<String, Value>,
}

pub(super) fn parse_downstream_request(
    payload: Value,
) -> Result<DownstreamResponsesRequest, ThreadlineError> {
    serde_json::from_value::<DownstreamResponsesRequest>(payload)
        .map_err(|_| ThreadlineError::InvalidResponsesRequest)
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

pub(super) fn sse_terminal_response_failed_chunk(payload: &Value) -> Bytes {
    let fallback = ThreadlineError::UpstreamResponseFailed.public_error();
    let error = payload.get("error");
    let mut response = Map::new();

    if let Some(response_id) = payload
        .get("response")
        .and_then(|value| value.get("id"))
        .and_then(safe_scalar_field)
    {
        response.insert("id".to_string(), Value::String(response_id));
    }

    response.insert("status".to_string(), Value::String("failed".to_string()));
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
        parse_downstream_request, safe_scalar_field, sse_done_chunk, sse_error_chunk,
        sse_json_chunk, sse_payload_chunk, sse_terminal_response_failed_chunk,
    };
    use crate::errors::ThreadlineError;
    use serde_json::{Value, json};

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
