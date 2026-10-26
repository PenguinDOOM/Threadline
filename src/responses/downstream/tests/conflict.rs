use super::*;

#[test]
fn looks_like_auxiliary_summary_conflict_fallback_detects_nested_vscode_summary_shape_without_context_management()
 {
    let payload = json!({
        "input": [
            {
                "type": "message",
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": manual_summary_text()
                    },
                    {
                        "type": "input_text",
                        "text": simple_history_context_text()
                    }
                ]
            }
        ]
    });

    let payload = payload
        .as_object()
        .expect("payload object for conflict fallback test");

    assert!(looks_like_auxiliary_summary_conflict_fallback(payload));
}

#[test]
fn looks_like_auxiliary_summary_conflict_fallback_rejects_nested_ordinary_quoted_text() {
    let payload = json!({
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": format!(
                            "Quoted prompt: {}",
                            manual_summary_text()
                        )
                    }
                ]
            }
        ]
    });

    let payload = payload
        .as_object()
        .expect("payload object for conflict fallback test");

    assert!(!looks_like_auxiliary_summary_conflict_fallback(payload));
}
