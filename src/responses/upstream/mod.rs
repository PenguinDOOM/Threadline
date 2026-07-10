mod original;

use serde_json::{Map, Value};

use crate::errors::ThreadlineError;
use crate::ws_pump::LiveUpstreamWebSocket;

pub use original::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};

const REASONING_ALL_TURNS_CONTEXT: &str = "all_turns";

fn inject_reasoning_all_turns(payload: &mut Map<String, Value>) {
    let Some(model) = payload.get("model").and_then(Value::as_str) else {
        return;
    };

    if !matches!(
        model,
        "gpt-5.6-sol" | "gpt-5.6-terra" | "gpt-5.6-luna"
    ) {
        return;
    }

    let Some(reasoning) = payload.get_mut("reasoning").and_then(Value::as_object_mut) else {
        return;
    };

    reasoning
        .entry("context".to_string())
        .or_insert_with(|| Value::String(REASONING_ALL_TURNS_CONTEXT.to_string()));
}

pub(super) fn build_response_create_payload(request: Value) -> Result<Value, ThreadlineError> {
    let mut payload = match request {
        Value::Object(payload) => payload,
        other => return original::build_response_create_payload(other),
    };
    inject_reasoning_all_turns(&mut payload);
    original::build_response_create_payload(Value::Object(payload))
}

pub(super) async fn send_response_create(
    upstream: &LiveUpstreamWebSocket,
    request_payload: &Map<String, Value>,
) -> Result<(), ThreadlineError> {
    let payload = build_response_create_payload(Value::Object(request_payload.clone()))?;
    let text = serde_json::to_string(&payload).expect("serialize response.create payload");
    upstream
        .send_text(text)
        .await
        .map_err(|_| ThreadlineError::UpstreamWebSocketClosed)
}

pub(super) fn build_followup_tool_outputs_payload(
    request: Value,
    previous_response_id: &str,
    input: Value,
) -> Result<Value, ThreadlineError> {
    let mut payload = match request {
        Value::Object(payload) => payload,
        other => {
            return original::build_followup_tool_outputs_payload(
                other,
                previous_response_id,
                input,
            );
        }
    };
    inject_reasoning_all_turns(&mut payload);
    original::build_followup_tool_outputs_payload(
        Value::Object(payload),
        previous_response_id,
        input,
    )
}

pub(super) async fn send_followup_tool_outputs(
    upstream: &LiveUpstreamWebSocket,
    request_payload: &Map<String, Value>,
    previous_response_id: &str,
    input: Value,
) -> Result<(), ThreadlineError> {
    let payload = build_followup_tool_outputs_payload(
        Value::Object(request_payload.clone()),
        previous_response_id,
        input,
    )?;
    let text = serde_json::to_string(&payload).expect("serialize followup response.create payload");
    upstream
        .send_text(text)
        .await
        .map_err(|_| ThreadlineError::UpstreamWebSocketClosed)
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::build_response_create_payload;

    #[test]
    fn build_response_create_payload_injects_all_turns_for_gpt_5_6() {
        let payload = build_response_create_payload(json!({
            "model": "gpt-5.6-sol",
            "reasoning": {
                "effort": "high"
            }
        }))
        .expect("response.create payload");

        assert_eq!(payload["reasoning"]["effort"], "high");
        assert_eq!(payload["reasoning"]["context"], "all_turns");
        assert_eq!(payload["store"], Value::Bool(false));
    }
}
