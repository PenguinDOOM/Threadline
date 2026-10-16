use super::*;

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
            "model": "gpt-6-sol",
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
    assert_eq!(payload["response"]["model"], "gpt-6-sol");
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
