use super::*;

#[test]
fn parse_downstream_request_classifies_new_foreground_summary_prompt_without_compressed_history() {
    assert_eq!(
        classify_input(vec![
            new_auto_system_summary_input_item(),
            new_auto_final_summary_prompt_input_item(),
        ]),
        DownstreamRequestClassification::AuxiliarySummary
    );
}

#[test]
fn parse_downstream_request_does_not_classify_new_foreground_partial_fingerprints() {
    for (name, input) in [
        ("system_only", vec![new_auto_system_summary_input_item()]),
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
fn parse_downstream_request_does_not_classify_new_foreground_fingerprints_outside_input_text() {
    let request = foreground_fingerprints_outside_text();

    assert_eq!(
        request.classification,
        DownstreamRequestClassification::Normal
    );
}

fn foreground_fingerprints_outside_text() -> super::super::DownstreamResponsesRequest {
    parse_downstream_request(json!({
        "previous_response_id": "resp_123",
        "metadata": {
            "system_prompt": new_auto_system_summary_text(),
            "final_prompt": new_auto_final_summary_prompt_text()
        },
        "tools": [
            {
                "type": "function",
                "name": "echo",
                "description": new_auto_final_summary_prompt_text(),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "summary": {
                            "type": "string"
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
