use super::*;

#[test]
fn upstream_event_trace_metadata_redacts_argument_bodies_and_keeps_lengths() {
    let arguments = "{\"input\":\"*** Begin Patch\\nsecret\\n*** End Patch\"}";
    let parsed = json!({
        "type": "response.output_item.added",
        "output_index": 2,
        "item_id": "item-visible",
        "item": {
            "type": "function_call",
            "call_id": "call-visible",
            "name": "apply_patch",
            "arguments": arguments
        }
    });

    let metadata = UpstreamEventTraceMetadata::from_event(&parsed);

    assert_eq!(metadata.event_type, "response.output_item.added");
    assert_eq!(metadata.item_type.as_deref(), Some("function_call"));
    assert_eq!(metadata.item_name.as_deref(), Some("apply_patch"));
    assert_eq!(metadata.call_id.as_deref(), Some("call-visible"));
    assert_eq!(metadata.arguments_length, Some(arguments.len()));
    assert_eq!(metadata.output_index, Some(2));
    assert_eq!(metadata.item_id.as_deref(), Some("item-visible"));
}

#[test]
fn downstream_sse_trace_metadata_reports_action_without_delta_body() {
    let delta = "{\"input\":\"*** Begin Patch";
    let parsed = json!({
        "type": "response.function_call_arguments.delta",
        "output_index": 1,
        "item_id": "fc_apply_patch_1",
        "delta": delta
    });

    let metadata = downstream_sse_trace_metadata(&parsed, DownstreamTraceAction::Forwarded, None);

    assert_eq!(
        metadata.event_type,
        "response.function_call_arguments.delta"
    );
    assert_eq!(
        metadata.translation_action,
        DownstreamTraceAction::Forwarded.as_str()
    );
    assert_eq!(metadata.delta_length, Some(delta.len()));
    assert_eq!(metadata.output_index, Some(1));
    assert_eq!(metadata.item_id.as_deref(), Some("fc_apply_patch_1"));
    assert_eq!(metadata.arguments_length, None);
    assert_eq!(metadata.item_name, None);
}

#[test]
fn synthetic_delta_emission_records_source_and_lengths_without_text() {
    let synthetic_text = "synthesized visible assistant text";
    let parsed = json!({
        "type": "response.output_text.delta",
        "item_id": "msg-visible",
        "output_index": 3,
        "content_index": 1,
        "delta": synthetic_text
    });

    let metadata = downstream_sse_trace_metadata(
        &parsed,
        DownstreamTraceAction::Forwarded,
        Some(&DownstreamTraceDiagnostics {
            response_id: Some("response-visible".to_string()),
            synthetic_delta_source: Some("response.output_text.done"),
            visible_text_delta_count: Some(1),
            visible_text_length: Some(synthetic_text.len()),
            ..Default::default()
        }),
    );
    let metadata_debug = format!("{metadata:?}");

    assert_eq!(metadata.translation_action, "forwarded");
    assert_eq!(metadata.event_type, "response.output_text.delta");
    assert_eq!(metadata.response_id.as_deref(), Some("response-visible"));
    assert_eq!(metadata.item_id.as_deref(), Some("msg-visible"));
    assert_eq!(metadata.output_index, Some(3));
    assert_eq!(metadata.content_index, Some(1));
    assert_eq!(
        metadata.synthetic_delta_source,
        Some("response.output_text.done")
    );
    assert_eq!(metadata.visible_text_delta_count, Some(1));
    assert_eq!(metadata.visible_text_length, Some(synthetic_text.len()));
    assert!(!metadata_debug.contains(synthetic_text));
}

#[test]
fn upstream_event_trace_metadata_reports_compaction_without_encrypted_content() {
    let encrypted_content = "opaque-compaction-payload";
    let parsed = json!({
        "type": "response.output_item.done",
        "output_index": 4,
        "item": {
            "type": "compaction",
            "id": "compaction-visible",
            "encrypted_content": encrypted_content
        }
    });

    let metadata = UpstreamEventTraceMetadata::from_event(&parsed);
    let metadata_debug = format!("{metadata:?}");

    assert_eq!(metadata.event_type, "response.output_item.done");
    assert_eq!(metadata.item_type.as_deref(), Some("compaction"));
    assert!(metadata.is_compaction);
    assert_eq!(
        metadata.compaction_id.as_deref(),
        Some("compaction-visible")
    );
    assert_eq!(metadata.output_index, Some(4));
    assert_eq!(metadata.has_encrypted_content, Some(true));
    assert!(!metadata_debug.contains(encrypted_content));
}

#[test]
fn upstream_event_trace_metadata_reports_compaction_without_blob_when_missing() {
    let parsed = json!({
        "type": "response.output_item.added",
        "output_index": 0,
        "item": {
            "type": "compaction",
            "id": "compaction-empty"
        }
    });

    let metadata = UpstreamEventTraceMetadata::from_event(&parsed);

    assert!(metadata.is_compaction);
    assert_eq!(metadata.compaction_id.as_deref(), Some("compaction-empty"));
    assert_eq!(metadata.has_encrypted_content, Some(false));
}

#[test]
fn translation_trace_event_names_remain_stable() {
    assert_eq!(
        RESPONSES_TRANSLATION_UPSTREAM_EVENT,
        "responses_translation_upstream_event"
    );
    assert_eq!(
        RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT,
        "responses_translation_downstream_sse_event"
    );
    assert_eq!(
        RESPONSES_TRANSLATION_EVENT_SUPPRESSED,
        "responses_translation_event_suppressed"
    );
    assert_eq!(
        RESPONSES_TRANSLATION_NO_OBSERVABLE_OUTPUT_GUARD,
        "responses_translation_no_observable_output_guard"
    );
}
