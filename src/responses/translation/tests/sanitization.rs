use super::*;

#[test]
fn sanitized_completed_event_preserves_compaction_output() {
    let parsed = json!({
        "type": "response.completed",
        "response": {
            "id": "response-compaction-only",
            "output": [
                {
                    "type": "compaction",
                    "id": "compaction-1",
                    "encrypted_content": "opaque-compaction-payload"
                }
            ]
        }
    });

    let (sanitized, diagnostics) = sanitized_completed_event_with_diagnostics(&parsed, &[]);
    let sanitized_output = sanitized["response"]["output"]
        .as_array()
        .expect("sanitized output array");

    assert_eq!(diagnostics, CompletedSanitizationDiagnostics::default());
    assert_eq!(sanitized_output.len(), 1);
    assert_eq!(sanitized_output[0]["type"], "compaction");
    assert_eq!(sanitized_output[0]["id"], "compaction-1");
    assert_eq!(
        sanitized_output[0]["encrypted_content"],
        "opaque-compaction-payload"
    );
}

#[test]
fn sanitized_completed_event_removes_internal_function_calls_but_preserves_compaction() {
    let encrypted_content = "opaque-compaction-payload";
    let internal_arguments = "{\"token\":\"secret\"}";
    let visible_text = vec![VisibleAssistantText {
        key: VisibleTextSourceKey::new(Some("message-visible".to_string()), Some(2), Some(0)),
        text: "visible assistant answer".to_string(),
    }];
    let parsed = json!({
        "type": "response.completed",
        "response": {
            "id": "response-sanitized",
            "output": [
                {
                    "type": "function_call",
                    "id": "fc-internal",
                    "name": "threadline_echo",
                    "arguments": internal_arguments
                },
                {
                    "type": "compaction",
                    "id": "compaction-1",
                    "encrypted_content": encrypted_content
                }
            ]
        }
    });

    let (sanitized, diagnostics) =
        sanitized_completed_event_with_diagnostics(&parsed, &visible_text);
    let diagnostics_debug = format!("{diagnostics:?}");
    let sanitized_output = sanitized["response"]["output"]
        .as_array()
        .expect("sanitized output array");

    assert_eq!(
        diagnostics,
        CompletedSanitizationDiagnostics {
            sanitized_internal_function_call_count: 1,
            sanitized_compaction_count: 0,
            completed_visible_message_count: 1,
        }
    );
    assert_eq!(sanitized_output.len(), 2);
    assert_eq!(sanitized_output[0]["type"], "compaction");
    assert_eq!(sanitized_output[0]["id"], "compaction-1");
    assert_eq!(sanitized_output[0]["encrypted_content"], encrypted_content);
    assert_eq!(sanitized_output[1]["type"], "message");
    assert!(!diagnostics_debug.contains(encrypted_content));
    assert!(!diagnostics_debug.contains(internal_arguments));
}

#[test]
fn completed_sanitization_preserves_concrete_non_text_observable_output() {
    let parsed = non_text_observable_completion();

    let (sanitized, diagnostics) = sanitized_completed_event_with_diagnostics(&parsed, &[]);
    let sanitized_output = sanitized["response"]["output"]
        .as_array()
        .expect("sanitized output array");

    assert_eq!(
        diagnostics,
        CompletedSanitizationDiagnostics {
            sanitized_internal_function_call_count: 1,
            sanitized_compaction_count: 0,
            completed_visible_message_count: 0,
        }
    );
    assert_eq!(sanitized_output.len(), 4);
    assert_eq!(sanitized_output[0]["id"], "fc-external");
    assert_eq!(sanitized_output[0]["name"], "apply_patch");
    assert_eq!(sanitized_output[1]["id"], "img-1");
    assert_eq!(sanitized_output[1]["type"], "image_generation_call");
    assert_eq!(sanitized_output[2]["id"], "ctx-1");
    assert_eq!(sanitized_output[2]["type"], "context");
    assert_eq!(sanitized_output[3]["id"], "cmp-1");
    assert_eq!(sanitized_output[3]["type"], "compaction");
}

fn non_text_observable_completion() -> serde_json::Value {
    json!({
        "type": "response.completed",
        "response": {
            "id": "response-non-text-observable",
            "output": [
                {
                    "type": "function_call",
                    "id": "fc-internal",
                    "name": "threadline_echo",
                    "arguments": "{\"value\":\"hidden\"}"
                },
                {
                    "type": "function_call",
                    "id": "fc-external",
                    "call_id": "call-visible",
                    "name": "apply_patch",
                    "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"
                },
                {
                    "type": "image_generation_call",
                    "id": "img-1",
                    "result": "image-asset-1"
                },
                {
                    "type": "context",
                    "id": "ctx-1",
                    "summary": "context snapshot"
                },
                {
                    "type": "compaction",
                    "id": "cmp-1",
                    "encrypted_content": "opaque"
                }
            ]
        }
    })
}
