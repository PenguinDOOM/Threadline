use super::*;

pub(in crate::responses) fn sse_payload_chunk(event: &str, payload: &str) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {payload}\n\n"))
}

pub(in crate::responses) fn sse_json_chunk(event: &str, payload: &Value) -> Bytes {
    let payload = serde_json::to_string(payload).expect("serialize downstream sse payload");
    sse_payload_chunk(event, &payload)
}

pub(in crate::responses) fn sse_done_chunk() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}

pub(in crate::responses) fn safe_object_clone(value: Option<&Value>) -> Option<Value> {
    value.and_then(Value::as_object).cloned().map(Value::Object)
}

pub(in crate::responses) fn sanitized_terminal_response(
    payload: &Value,
    status: &str,
) -> Map<String, Value> {
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

pub(in crate::responses) fn sse_terminal_response_failed_chunk(payload: &Value) -> Bytes {
    let fallback = ThreadlineError::UpstreamResponseFailed.public_error();
    let error = payload
        .get("error")
        .filter(|error| error.is_object())
        .or_else(|| payload.pointer("/response/error"));
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

pub(in crate::responses) fn sse_terminal_response_incomplete_chunk(payload: &Value) -> Bytes {
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

pub(in crate::responses) fn safe_scalar_field(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

pub(in crate::responses) fn sse_error_chunk(error: &ThreadlineError) -> Bytes {
    let payload = serde_json::to_value(error.public_error_document())
        .expect("convert threadline error payload to json value");
    sse_json_chunk("error", &payload)
}
