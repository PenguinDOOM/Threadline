use super::*;

#[test]
fn parse_downstream_request_classifies_new_auto_compaction_prompt_fingerprints() {
    assert_eq!(
        classify_input(vec![
            new_auto_system_summary_input_item(),
            new_auto_compressed_history_input_item(),
            new_auto_final_summary_prompt_input_item(),
        ]),
        DownstreamRequestClassification::AuxiliarySummary
    );
}

#[test]
fn parse_downstream_request_does_not_classify_new_auto_user_quote_only() {
    assert_eq!(
        classify_input(vec![
            new_auto_compressed_history_input_item(),
            new_auto_final_summary_prompt_input_item(),
        ]),
        DownstreamRequestClassification::Normal
    );
}

#[test]
fn parse_downstream_request_does_not_classify_new_auto_partial_fingerprints() {
    for (name, input) in [
        (
            "system_plus_history_only",
            vec![
                new_auto_system_summary_input_item(),
                new_auto_compressed_history_input_item(),
            ],
        ),
        (
            "history_plus_final_prompt_only",
            vec![
                new_auto_compressed_history_input_item(),
                new_auto_final_summary_prompt_input_item(),
            ],
        ),
        ("system_only", vec![new_auto_system_summary_input_item()]),
        (
            "history_only",
            vec![new_auto_compressed_history_input_item()],
        ),
        (
            "final_prompt_only",
            vec![new_auto_final_summary_prompt_input_item()],
        ),
    ] {
        assert_eq!(
            classify_input(input),
            DownstreamRequestClassification::Normal,
            "fixture should remain normal: {name}"
        );
    }
}

#[test]
fn parse_downstream_request_does_not_classify_new_auto_fingerprints_outside_input_text() {
    let request = auto_fingerprints_outside_text();

    assert_eq!(
        request.classification,
        DownstreamRequestClassification::Normal
    );
}

#[test]
fn parse_downstream_request_classifies_new_auto_compaction_prompt_with_corrected_history_spelling()
{
    assert_eq!(
        classify_input(vec![
            new_auto_system_summary_input_item(),
            new_auto_compressed_history_input_item_corrected(),
            new_auto_final_summary_prompt_input_item(),
        ]),
        DownstreamRequestClassification::AuxiliarySummary
    );
}

#[test]
fn parse_downstream_request_classifies_auto_background_compaction_in_non_final_shapes() {
    for (name, input) in [
        (
            "auto_summary_followed_by_user_message",
            vec![
                auxiliary_summary_input_item(),
                json!({
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "Please keep this request moving."
                        }
                    ]
                }),
            ],
        ),
        (
            "auto_summary_before_non_message_item",
            vec![
                auxiliary_summary_input_item(),
                json!({
                    "type": "input_text",
                    "text": "Resume after compaction."
                }),
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

fn auto_fingerprints_outside_text() -> super::super::DownstreamResponsesRequest {
    parse_downstream_request(json!({
        "previous_response_id": "resp_123",
        "metadata": {
            "system_prompt": new_auto_system_summary_text(),
            "history": new_auto_compressed_history_text(),
            "final_prompt": new_auto_final_summary_prompt_text()
        },
        "tools": [
            {
                "type": "function",
                "name": "echo",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "summary": {
                            "type": "string",
                            "description": new_auto_final_summary_prompt_text()
                        }
                    }
                }
            }
        ],
        "input": [
            {
                "type": "message",
                "role": "system",
                "content": [
                    {
                        "type": "input_image",
                        "image_url": new_auto_system_summary_text()
                    }
                ]
            },
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
    .expect("parse request")
}
