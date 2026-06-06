use serde_json::{Map, Value, json};
use tracing::debug;

use crate::errors::ThreadlineError;

pub const INTERNAL_TOOL_PREFIX: &str = "threadline_";
const ECHO_TOOL_NAME: &str = "threadline_echo";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingInternalToolOutput {
    call_id: String,
    output: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InternalToolCall {
    call_id: String,
    name: String,
    arguments: Value,
}

impl PendingInternalToolOutput {
    pub fn new(call_id: impl Into<String>, output: impl Into<String>) -> Self {
        Self {
            call_id: call_id.into(),
            output: output.into(),
        }
    }

    pub fn into_followup_input(self) -> Value {
        json!({
            "type": "function_call_output",
            "call_id": self.call_id,
            "output": self.output,
        })
    }
}

impl InternalToolCall {
    pub fn from_event(event: &Value) -> Result<Option<Self>, ThreadlineError> {
        let Some(event_type) = event.get("type").and_then(Value::as_str) else {
            return Ok(None);
        };
        if event_type != "response.output_item.done" {
            return Ok(None);
        }

        let Some(item) = event.get("item") else {
            return Ok(None);
        };
        if item.get("type").and_then(Value::as_str) != Some("function_call") {
            return Ok(None);
        }

        let Some(name) = item.get("name").and_then(Value::as_str) else {
            return Ok(None);
        };
        if !is_internal_tool_name(name) {
            return Ok(None);
        }

        let call_id = item
            .get("call_id")
            .and_then(Value::as_str)
            .ok_or(ThreadlineError::InternalToolFailed)?;
        let arguments = parse_arguments(item.get("arguments"))?;

        debug!(call_id = %call_id, tool_name = name, "internal_tool_detected");

        Ok(Some(Self {
            call_id: call_id.to_string(),
            name: name.to_string(),
            arguments,
        }))
    }

    pub fn execute(self) -> Result<PendingInternalToolOutput, ThreadlineError> {
        match self.name.as_str() {
            ECHO_TOOL_NAME => Ok(PendingInternalToolOutput::new(
                self.call_id,
                extract_echo_output(&self.arguments),
            )),
            _ => Err(ThreadlineError::InternalToolFailed),
        }
    }
}

pub fn inject_internal_tools(payload: &mut Map<String, Value>) {
    let internal_tools = internal_tool_definitions();

    match payload.get_mut("tools") {
        Some(Value::Array(existing_tools)) => {
            for tool in internal_tools {
                let tool_name = tool.get("name").and_then(Value::as_str);
                let already_present = tool_name.is_some_and(|name| {
                    existing_tools
                        .iter()
                        .any(|existing| existing.get("name").and_then(Value::as_str) == Some(name))
                });
                if !already_present {
                    existing_tools.push(tool);
                }
            }
        }
        Some(Value::Null) | None => {
            payload.insert("tools".to_string(), Value::Array(internal_tools));
        }
        Some(_) => {}
    }
}

pub fn build_followup_input(outputs: Vec<PendingInternalToolOutput>) -> Value {
    Value::Array(
        outputs
            .into_iter()
            .map(PendingInternalToolOutput::into_followup_input)
            .collect(),
    )
}

pub fn is_internal_tool_name(name: &str) -> bool {
    name.starts_with(INTERNAL_TOOL_PREFIX)
}

pub fn event_contains_internal_tool_name(event: &Value) -> bool {
    value_contains_internal_tool_name(event)
}

fn parse_arguments(arguments: Option<&Value>) -> Result<Value, ThreadlineError> {
    match arguments {
        Some(Value::String(text)) => {
            serde_json::from_str(text).map_err(|_| ThreadlineError::InternalToolFailed)
        }
        Some(Value::Null) | None => Ok(Value::Object(Map::new())),
        Some(value) => Ok(value.clone()),
    }
}

fn extract_echo_output(arguments: &Value) -> String {
    arguments
        .get("value")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| arguments.to_string())
}

fn value_contains_internal_tool_name(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, nested)| {
            ((key == "name" || key == "tool_name")
                && nested.as_str().is_some_and(is_internal_tool_name))
                || value_contains_internal_tool_name(nested)
        }),
        Value::Array(items) => items.iter().any(value_contains_internal_tool_name),
        _ => false,
    }
}

fn internal_tool_definitions() -> Vec<Value> {
    vec![json!({
        "type": "function",
        "name": ECHO_TOOL_NAME,
        "description": "Return the provided value so Threadline can satisfy local tool loops without involving downstream clients.",
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
    })]
}
