use std::sync::OnceLock;

use serde_json::{Map, Value, json};
use tracing::debug;

use crate::config::active_job_manager_config;
use crate::errors::ThreadlineError;
use crate::jobs::ThreadlineJobManager;

pub const INTERNAL_TOOL_PREFIX: &str = "threadline_";
const ECHO_TOOL_NAME: &str = "threadline_echo";
const START_JOB_TOOL_NAME: &str = "threadline_start_job";
const POLL_JOB_TOOL_NAME: &str = "threadline_poll_job";
const READ_JOB_OUTPUT_TOOL_NAME: &str = "threadline_read_job_output";
const GET_JOB_RESULT_TOOL_NAME: &str = "threadline_get_job_result";
const CANCEL_JOB_TOOL_NAME: &str = "threadline_cancel_job";

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
        self.execute_with_job_manager(&global_job_manager())
    }

    pub fn execute_with_job_manager(
        self,
        job_manager: &ThreadlineJobManager,
    ) -> Result<PendingInternalToolOutput, ThreadlineError> {
        match self.name.as_str() {
            ECHO_TOOL_NAME => Ok(PendingInternalToolOutput::new(
                self.call_id,
                extract_echo_output(&self.arguments),
            )),
            START_JOB_TOOL_NAME => Ok(PendingInternalToolOutput::new(
                self.call_id,
                start_job_output(job_manager, &self.arguments).to_string(),
            )),
            POLL_JOB_TOOL_NAME => Ok(PendingInternalToolOutput::new(
                self.call_id,
                poll_job_output(job_manager, &self.arguments).to_string(),
            )),
            READ_JOB_OUTPUT_TOOL_NAME => Ok(PendingInternalToolOutput::new(
                self.call_id,
                read_job_output(job_manager, &self.arguments).to_string(),
            )),
            GET_JOB_RESULT_TOOL_NAME => Ok(PendingInternalToolOutput::new(
                self.call_id,
                get_job_result_output(job_manager, &self.arguments).to_string(),
            )),
            CANCEL_JOB_TOOL_NAME => Ok(PendingInternalToolOutput::new(
                self.call_id,
                cancel_job_output(job_manager, &self.arguments).to_string(),
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
    vec![
        json!({
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
        }),
        json!({
            "type": "function",
            "name": START_JOB_TOOL_NAME,
            "description": "Start a background Threadline job for an allowed local command, return immediately with a job id, and avoid busy-polling when independent work is still available.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1
                    }
                },
                "required": ["command"],
                "additionalProperties": false
            }
        }),
        json!({
            "type": "function",
            "name": POLL_JOB_TOOL_NAME,
            "description": "Check a previously started Threadline job at a natural checkpoint for status updates, not in a tight loop.",
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"}
                },
                "required": ["job_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "type": "function",
            "name": READ_JOB_OUTPUT_TOOL_NAME,
            "description": "Read incremental output from a Threadline job using a previous output offset; preserve next_offset for the next read and notice truncated_before if older buffered output was dropped.",
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "offset": {"type": "integer", "minimum": 0}
                },
                "required": ["job_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "type": "function",
            "name": GET_JOB_RESULT_TOOL_NAME,
            "description": "Get the current or terminal result payload for a Threadline job after a terminal poll state or before final claims that depend on success or failure.",
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"}
                },
                "required": ["job_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "type": "function",
            "name": CANCEL_JOB_TOOL_NAME,
            "description": "Cancel a stuck or no-longer-useful Threadline job, then poll or get the result to confirm the terminal state.",
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"}
                },
                "required": ["job_id"],
                "additionalProperties": false
            }
        }),
    ]
}

fn global_job_manager() -> ThreadlineJobManager {
    static JOB_MANAGER: OnceLock<ThreadlineJobManager> = OnceLock::new();

    JOB_MANAGER
        .get_or_init(|| ThreadlineJobManager::new(active_job_manager_config()))
        .clone()
}

fn start_job_output(job_manager: &ThreadlineJobManager, arguments: &Value) -> Value {
    match extract_string_list(arguments.get("command")) {
        Some(command) => job_manager.start_command_json(command),
        None => invalid_job_request("threadline_start_job requires a command array of strings."),
    }
}

fn poll_job_output(job_manager: &ThreadlineJobManager, arguments: &Value) -> Value {
    match extract_job_id(arguments) {
        Some(job_id) => job_manager.poll_json(&job_id),
        None => invalid_job_request("threadline_poll_job requires a job_id string."),
    }
}

fn read_job_output(job_manager: &ThreadlineJobManager, arguments: &Value) -> Value {
    match extract_job_id(arguments) {
        Some(job_id) => job_manager.read_output_json(&job_id, extract_offset(arguments)),
        None => invalid_job_request("threadline_read_job_output requires a job_id string."),
    }
}

fn get_job_result_output(job_manager: &ThreadlineJobManager, arguments: &Value) -> Value {
    match extract_job_id(arguments) {
        Some(job_id) => job_manager.get_result_json(&job_id),
        None => invalid_job_request("threadline_get_job_result requires a job_id string."),
    }
}

fn cancel_job_output(job_manager: &ThreadlineJobManager, arguments: &Value) -> Value {
    match extract_job_id(arguments) {
        Some(job_id) => job_manager.cancel_json(&job_id),
        None => invalid_job_request("threadline_cancel_job requires a job_id string."),
    }
}

fn extract_string_list(value: Option<&Value>) -> Option<Vec<String>> {
    let items = value?.as_array()?;
    items
        .iter()
        .map(|item| item.as_str().map(ToString::to_string))
        .collect()
}

fn extract_job_id(arguments: &Value) -> Option<String> {
    arguments
        .get("job_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn extract_offset(arguments: &Value) -> u64 {
    arguments.get("offset").and_then(Value::as_u64).unwrap_or(0)
}

fn invalid_job_request(message: &'static str) -> Value {
    json!({
        "ok": false,
        "code": "invalid_job_request",
        "message": message,
    })
}
