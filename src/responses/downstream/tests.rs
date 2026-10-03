use super::{
    DownstreamInteractionType, DownstreamRequestClassification, DownstreamRequestMetadata,
    looks_like_auxiliary_summary_conflict_fallback, parse_downstream_request,
    parse_downstream_request_with_metadata, safe_scalar_field, sse_done_chunk, sse_error_chunk,
    sse_json_chunk, sse_payload_chunk, sse_terminal_response_failed_chunk,
    sse_terminal_response_incomplete_chunk,
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

fn parse_input_with_metadata(
    input: Vec<Value>,
    metadata: DownstreamRequestMetadata,
) -> super::DownstreamResponsesRequest {
    parse_downstream_request_with_metadata(
        json!({
            "previous_response_id": "resp_123",
            "input": input
        }),
        metadata,
    )
    .expect("parse request")
}

fn sanitized_observed_auxiliary_summary_request() -> Value {
    json!({
        "model": "gpt-6-sol",
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

mod auto_compaction;
mod classification;
mod conflict;
mod foreground;
mod metadata;
mod sse;
