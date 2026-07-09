use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_json::{Map, Value};

use crate::auth::LoadedUpstreamAuth;
use crate::codex_ws::UpstreamSessionDescriptor;
use crate::errors::ThreadlineError;
use crate::ws_pump::LiveUpstreamWebSocket;

const UNSUPPORTED_RESPONSE_FIELDS: &[&str] = &[
    "max_output_tokens",
    "max_tokens",
    "max_completion_tokens",
    "truncation",
];

pub trait UpstreamAuthProvider: Send + Sync {
    fn load(&self) -> Result<LoadedUpstreamAuth, ThreadlineError>;
}

pub trait UpstreamConnector: Send + Sync {
    fn connect(
        &self,
        auth: LoadedUpstreamAuth,
        session: Option<UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>>;
}

#[derive(Clone)]
pub struct ThreadlineServices {
    auth_provider: Arc<dyn UpstreamAuthProvider>,
    connector: Arc<dyn UpstreamConnector>,
}

pub struct ConnectedUpstream {
    pub websocket: Arc<LiveUpstreamWebSocket>,
    pub session: UpstreamSessionDescriptor,
    pub turn_state: Option<String>,
}

impl ThreadlineServices {
    pub fn new(
        auth_provider: Arc<dyn UpstreamAuthProvider>,
        connector: Arc<dyn UpstreamConnector>,
    ) -> Self {
        Self {
            auth_provider,
            connector,
        }
    }

    pub fn auth_provider(&self) -> &Arc<dyn UpstreamAuthProvider> {
        &self.auth_provider
    }

    pub fn connector(&self) -> &Arc<dyn UpstreamConnector> {
        &self.connector
    }
}

pub(super) fn build_response_create_payload(request: Value) -> Result<Value, ThreadlineError> {
    let mut payload = require_payload_object(request)?;
    payload.insert(
        "type".to_string(),
        Value::String("response.create".to_string()),
    );
    payload.insert("store".to_string(), Value::Bool(false));

    if matches!(payload.get("instructions"), None | Some(Value::Null)) {
        payload.insert("instructions".to_string(), Value::String(String::new()));
    }

    normalize_system_input_roles_for_codex(&mut payload);
    remove_codex_unsupported_response_fields(&mut payload);
    normalize_codex_reasoning_fields(&mut payload);
    Ok(Value::Object(payload))
}

pub(super) fn remove_codex_unsupported_response_fields(payload: &mut Map<String, Value>) {
    for field in UNSUPPORTED_RESPONSE_FIELDS {
        payload.remove(*field);
    }
}

pub(super) fn normalize_codex_reasoning_fields(payload: &mut Map<String, Value>) {
    let remove_reasoning = match payload.get_mut("reasoning").and_then(Value::as_object_mut) {
        Some(reasoning) => {
            // VS Code briefly sent `reasoning.summary = "off"` to disable reasoning summaries.
            // Codex/Responses API does not accept "off"; disabling summaries means omitting
            // the `summary` field entirely. Preserve valid values and only strip this known
            // compatibility value.
            if reasoning.get("summary").and_then(Value::as_str) == Some("off") {
                reasoning.remove("summary");
            }

            reasoning.is_empty()
        }
        None => false,
    };

    if remove_reasoning {
        payload.remove("reasoning");
    }
}

pub(super) fn normalize_system_input_roles_for_codex(payload: &mut Map<String, Value>) {
    let Some(input) = payload.get_mut("input").and_then(Value::as_array_mut) else {
        return;
    };

    for item in input {
        let Some(object) = item.as_object_mut() else {
            continue;
        };

        if object.get("role").and_then(Value::as_str) == Some("system") {
            object.insert("role".to_string(), Value::String("developer".to_string()));
        }
    }
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
    let mut payload = require_payload_object(request)?;
    payload.insert(
        "previous_response_id".to_string(),
        Value::String(previous_response_id.to_string()),
    );
    payload.insert("input".to_string(), input);
    build_response_create_payload(Value::Object(payload))
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

fn require_payload_object(payload: Value) -> Result<Map<String, Value>, ThreadlineError> {
    match payload {
        Value::Object(object) => Ok(object),
        _ => Err(ThreadlineError::InvalidResponsesRequest),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{build_followup_tool_outputs_payload, build_response_create_payload};

    #[test]
    fn build_response_create_payload_sets_required_defaults_and_filters_unsupported_fields() {
        let payload = build_response_create_payload(json!({
            "model": "gpt-test",
            "instructions": null,
            "max_output_tokens": 32,
            "max_tokens": 64,
            "max_completion_tokens": 96,
            "truncation": "auto"
        }))
        .expect("response.create payload");

        assert_eq!(payload["type"], "response.create");
        assert_eq!(payload["store"], false);
        assert_eq!(payload["instructions"], "");
        assert!(payload.get("max_output_tokens").is_none());
        assert!(payload.get("max_tokens").is_none());
        assert!(payload.get("max_completion_tokens").is_none());
        assert!(payload.get("truncation").is_none());
    }

    #[test]
    fn build_response_create_payload_omits_off_reasoning_summary() {
        let payload = build_response_create_payload(json!({
            "model": "gpt-test",
            "instructions": "keep",
            "reasoning": {
                "effort": "medium",
                "summary": "off"
            }
        }))
        .expect("response.create payload");

        assert_eq!(
            payload["reasoning"],
            json!({
                "effort": "medium"
            })
        );
    }

    #[test]
    fn build_response_create_payload_removes_empty_reasoning_after_off_summary() {
        let payload = build_response_create_payload(json!({
            "model": "gpt-test",
            "instructions": "keep",
            "reasoning": {
                "summary": "off"
            }
        }))
        .expect("response.create payload");

        assert!(payload.get("reasoning").is_none());
    }

    #[test]
    fn build_response_create_payload_preserves_context_management_and_previous_response_id() {
        let payload = build_response_create_payload(json!({
            "model": "gpt-test",
            "instructions": "keep",
            "previous_response_id": "resp_123",
            "context_management": {
                "type": "compaction",
                "compact_threshold": 12345
            },
            "reasoning": {
                "effort": "high",
                "summary": "auto"
            },
            "include": ["reasoning.encrypted_content"],
            "max_output_tokens": 32,
            "max_tokens": 64,
            "max_completion_tokens": 96,
            "truncation": "auto"
        }))
        .expect("response.create payload");

        assert_eq!(payload["type"], "response.create");
        assert_eq!(payload["store"], false);
        assert_eq!(payload["instructions"], "keep");
        assert_eq!(payload["previous_response_id"], "resp_123");
        assert_eq!(
            payload["context_management"],
            json!({
                "type": "compaction",
                "compact_threshold": 12345
            })
        );
        assert_eq!(
            payload["reasoning"],
            json!({
                "effort": "high",
                "summary": "auto"
            })
        );
        assert_eq!(payload["include"], json!(["reasoning.encrypted_content"]));
        assert!(payload.get("max_output_tokens").is_none());
        assert!(payload.get("max_tokens").is_none());
        assert!(payload.get("max_completion_tokens").is_none());
        assert!(payload.get("truncation").is_none());
    }

    #[test]
    fn build_response_create_payload_converts_system_input_roles_to_developer() {
        let payload = build_response_create_payload(json!({
            "model": "gpt-test",
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Follow project instructions."
                        }
                    ],
                    "custom_field": {
                        "preserve": true
                    }
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Hello"
                        }
                    ]
                }
            ]
        }))
        .expect("response.create payload");

        assert_eq!(payload["type"], "response.create");
        assert_eq!(payload["store"], false);
        assert_eq!(payload["input"][0]["role"], "developer");
        assert_eq!(
            payload["input"][0]["content"],
            json!([
                {
                    "type": "input_text",
                    "text": "Follow project instructions."
                }
            ])
        );
        assert_eq!(
            payload["input"][0]["custom_field"],
            json!({ "preserve": true })
        );
        assert_eq!(payload["input"][1]["role"], "user");
        assert!(
            payload["input"]
                .as_array()
                .expect("input array")
                .iter()
                .all(|item| item.get("role").and_then(Value::as_str) != Some("system")),
            "upstream response.create payload must not forward system input roles"
        );
    }

    #[test]
    fn build_response_create_payload_leaves_non_system_input_shapes_unchanged() {
        let non_array_input = json!({
            "unexpected": true,
            "role": "system"
        });
        let payload = build_response_create_payload(json!({
            "model": "gpt-test",
            "input": non_array_input.clone()
        }))
        .expect("response.create payload");

        assert_eq!(payload["type"], "response.create");
        assert_eq!(payload["store"], false);
        assert_eq!(payload["input"], non_array_input);

        let mixed_input = json!([
            "raw text item",
            17,
            {
                "role": 99,
                "content": [
                    {
                        "type": "input_text",
                        "text": "numeric roles must stay untouched"
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "user stays user"
                    }
                ],
                "custom_field": {
                    "preserve": true
                }
            },
            {
                "role": "assistant",
                "content": []
            },
            {
                "role": "developer",
                "content": []
            }
        ]);
        let payload = build_response_create_payload(json!({
            "model": "gpt-test",
            "input": mixed_input.clone()
        }))
        .expect("response.create payload");

        assert_eq!(payload["type"], "response.create");
        assert_eq!(payload["store"], false);
        assert_eq!(payload["input"], mixed_input);
    }

    #[test]
    fn build_followup_tool_outputs_payload_preserves_previous_response_id_and_output_shape() {
        let payload = build_followup_tool_outputs_payload(
            json!({
                "model": "gpt-test",
                "instructions": "keep",
                "truncation": "auto"
            }),
            "resp_intermediate",
            json!([
                {
                    "type": "function_call_output",
                    "call_id": "call_123",
                    "output": "done"
                }
            ]),
        )
        .expect("followup payload");

        assert_eq!(payload["type"], "response.create");
        assert_eq!(payload["store"], false);
        assert_eq!(payload["previous_response_id"], "resp_intermediate");
        assert_eq!(payload["instructions"], "keep");
        assert_eq!(payload["input"][0]["type"], "function_call_output");
        assert_eq!(payload["input"][0]["call_id"], "call_123");
        assert_eq!(payload["input"][0]["output"], "done");
        assert!(payload.get("truncation").is_none());
    }
}
