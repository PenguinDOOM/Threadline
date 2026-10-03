use super::*;

#[test]
fn parse_downstream_request_extracts_previous_response_id_and_payload() {
    let request = parse_downstream_request(json!({
        "previous_response_id": "resp_123",
        "model": "gpt-6-sol",
        "stream": true
    }))
    .expect("parse request");

    assert_eq!(request.previous_response_id.as_deref(), Some("resp_123"));
    assert_eq!(request.payload.get("model"), Some(&json!("gpt-6-sol")));
    assert_eq!(request.payload.get("stream"), Some(&json!(true)));
    assert!(!request.payload.contains_key("previous_response_id"));
}

#[test]
fn parse_downstream_request_identifies_auxiliary_summary_request() {
    let request = parse_downstream_request(sanitized_observed_auxiliary_summary_request())
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
fn parse_downstream_request_classifies_conversation_compaction_with_context_management() {
    let request = parse_downstream_request_with_metadata(
        json!({
            "previous_response_id": "resp_123",
            "context_management": {
                "type": "compaction",
                "compact_threshold": 12345
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
        }),
        DownstreamRequestMetadata::from_interaction_type_header_value(Some(
            "conversation-compaction",
        )),
    )
    .expect("parse request");

    assert_eq!(
        request.routing_diagnostics().interaction_type,
        DownstreamInteractionType::ConversationCompaction
    );
    assert!(
        request
            .routing_diagnostics()
            .interaction_type_compaction_hit
    );
    assert_eq!(
        request.classification,
        DownstreamRequestClassification::AuxiliarySummary
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
fn parse_downstream_request_classifies_manual_full_summary_prompt_fingerprints() {
    assert_eq!(
        classify_input(vec![
            json!({
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Continue from the earlier answer."
                    }
                ]
            }),
            manual_summary_input_item(),
        ]),
        DownstreamRequestClassification::AuxiliarySummary
    );
}

#[test]
fn parse_downstream_request_classifies_manual_simple_summary_prompt_fingerprints() {
    assert_eq!(
        classify_input(vec![
            manual_simple_summary_input_item(),
            json!({
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Acknowledge the compaction request."
                    }
                ]
            }),
        ]),
        DownstreamRequestClassification::AuxiliarySummary
    );
}

#[test]
fn parse_downstream_request_does_not_classify_user_role_manual_summary_quote_only() {
    assert_eq!(
        classify_input(vec![json!({
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": format!("Quoted prompt: {}", manual_summary_text())
                }
            ]
        })]),
        DownstreamRequestClassification::Normal
    );
}

#[test]
fn parse_downstream_request_does_not_classify_user_role_foreground_prompt_quote_only() {
    assert_eq!(
        classify_input(vec![json!({
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": format!("Quoted prompt: {}", new_auto_final_summary_prompt_text())
                }
            ]
        })]),
        DownstreamRequestClassification::Normal
    );
}

#[test]
fn parse_downstream_request_does_not_classify_foreground_prompt_when_not_final_user_input() {
    assert_eq!(
        classify_input(vec![
            new_auto_system_summary_input_item(),
            new_auto_final_summary_prompt_input_item(),
            input_text_message("user", "Please continue the earlier task."),
        ]),
        DownstreamRequestClassification::Normal
    );
}

#[test]
fn parse_downstream_request_does_not_classify_split_ordinary_conversation_quotes() {
    assert_eq!(
        classify_input(vec![
            input_text_message(
                "user",
                &format!(
                    "The user quoted this instruction earlier: {}",
                    new_auto_system_summary_text()
                ),
            ),
            input_text_message(
                "user",
                &format!(
                    "The user later quoted this prompt too: {}",
                    new_auto_final_summary_prompt_text()
                ),
            ),
        ]),
        DownstreamRequestClassification::Normal
    );
}

#[test]
fn parse_downstream_request_classifies_simple_history_context_only_with_summary_prompt() {
    for (name, input) in [
        (
            "simple_history_plus_manual_summary_prompt",
            vec![
                simple_history_context_input_item(),
                manual_summary_input_item(),
            ],
        ),
        (
            "simple_history_plus_auto_summary_prompt",
            vec![
                simple_history_context_input_item(),
                auxiliary_summary_input_item(),
            ],
        ),
    ] {
        assert_eq!(
            classify_input(input),
            DownstreamRequestClassification::AuxiliarySummary,
            "fixture should classify as auxiliary summary: {name}"
        );
    }
}

#[test]
fn parse_downstream_request_keeps_ordinary_and_quoted_summary_shapes_normal() {
    for (name, input) in [
        (
            "ordinary_user_prompt",
            vec![json!({
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Please continue the earlier task."
                    }
                ]
            })],
        ),
        (
            "simple_history_only_context",
            vec![simple_history_context_input_item()],
        ),
        (
            "user_role_full_prompt_quote_with_simple_history_context",
            vec![
                simple_history_context_input_item(),
                json!({
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": concat!(
                                "Quoted prompt: ",
                                "Summarize the conversation history so far, paying special attention to the most recent agent commands and tool results",
                                "\n\n",
                                "Structure your summary using the enhanced format provided in the system message",
                                "\n",
                                "Include all important tool calls and their results"
                            )
                        }
                    ]
                }),
            ],
        ),
    ] {
        assert_eq!(
            classify_input(input),
            DownstreamRequestClassification::Normal,
            "fixture should remain normal: {name}"
        );
    }
}
