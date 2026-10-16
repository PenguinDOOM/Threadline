use super::*;

pub(super) fn downstream_sse_trace_metadata(
    event: &Value,
    action: DownstreamTraceAction,
    diagnostics: Option<&DownstreamTraceDiagnostics>,
) -> DownstreamSseTraceMetadata {
    let metadata = UpstreamEventTraceMetadata::from_event(event);
    DownstreamSseTraceMetadata {
        translation_action: action.as_str(),
        event_type: metadata.event_type,
        response_id: diagnostics
            .and_then(|value| value.response_id.clone())
            .or(metadata.response_id),
        item_type: metadata.item_type,
        item_name: metadata.item_name,
        call_id: metadata.call_id,
        arguments_length: metadata.arguments_length,
        delta_length: metadata.delta_length,
        output_index: metadata.output_index,
        content_index: metadata.content_index,
        item_id: metadata.item_id,
        is_compaction: metadata.is_compaction,
        compaction_id: metadata.compaction_id,
        has_encrypted_content: metadata.has_encrypted_content,
        synthetic_delta_source: diagnostics.and_then(|value| value.synthetic_delta_source),
        visible_text_delta_count: diagnostics.and_then(|value| value.visible_text_delta_count),
        visible_text_length: diagnostics.and_then(|value| value.visible_text_length),
        sanitized_internal_function_call_count: diagnostics
            .and_then(|value| value.sanitized_internal_function_call_count),
        sanitized_compaction_count: diagnostics.and_then(|value| value.sanitized_compaction_count),
        completed_visible_message_count: diagnostics
            .and_then(|value| value.completed_visible_message_count),
    }
}

pub(super) fn trace_upstream_event(metadata: &UpstreamEventTraceMetadata) {
    trace!(
        event_type = %metadata.event_type,
        response_id = tracing::field::debug(&metadata.response_id),
        item_type = tracing::field::debug(&metadata.item_type),
        item_name = tracing::field::debug(&metadata.item_name),
        call_id = tracing::field::debug(&metadata.call_id),
        arguments_length = tracing::field::debug(&metadata.arguments_length),
        delta_length = tracing::field::debug(&metadata.delta_length),
        output_index = tracing::field::debug(&metadata.output_index),
        content_index = tracing::field::debug(&metadata.content_index),
        item_id = tracing::field::debug(&metadata.item_id),
        is_compaction = metadata.is_compaction,
        compaction_id = tracing::field::debug(&metadata.compaction_id),
        has_encrypted_content = tracing::field::debug(&metadata.has_encrypted_content),
        "{RESPONSES_TRANSLATION_UPSTREAM_EVENT}"
    );
}

pub(super) fn trace_downstream_sse_event(metadata: &DownstreamSseTraceMetadata) {
    trace!(
        translation_action = metadata.translation_action,
        event_type = %metadata.event_type,
        response_id = tracing::field::debug(&metadata.response_id),
        item_type = tracing::field::debug(&metadata.item_type),
        item_name = tracing::field::debug(&metadata.item_name),
        call_id = tracing::field::debug(&metadata.call_id),
        arguments_length = tracing::field::debug(&metadata.arguments_length),
        delta_length = tracing::field::debug(&metadata.delta_length),
        output_index = tracing::field::debug(&metadata.output_index),
        content_index = tracing::field::debug(&metadata.content_index),
        item_id = tracing::field::debug(&metadata.item_id),
        is_compaction = metadata.is_compaction,
        compaction_id = tracing::field::debug(&metadata.compaction_id),
        has_encrypted_content = tracing::field::debug(&metadata.has_encrypted_content),
        synthetic_delta_source = tracing::field::debug(&metadata.synthetic_delta_source),
        visible_text_delta_count = tracing::field::debug(&metadata.visible_text_delta_count),
        visible_text_length = tracing::field::debug(&metadata.visible_text_length),
        sanitized_internal_function_call_count =
            tracing::field::debug(&metadata.sanitized_internal_function_call_count),
        sanitized_compaction_count = tracing::field::debug(&metadata.sanitized_compaction_count),
        completed_visible_message_count = tracing::field::debug(&metadata.completed_visible_message_count),
        "{RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT}"
    );
}

pub(super) fn trace_suppressed_event(metadata: &UpstreamEventTraceMetadata) {
    trace!(
        translation_action = DownstreamTraceAction::Suppressed.as_str(),
        event_type = %metadata.event_type,
        response_id = tracing::field::debug(&metadata.response_id),
        item_type = tracing::field::debug(&metadata.item_type),
        item_name = tracing::field::debug(&metadata.item_name),
        call_id = tracing::field::debug(&metadata.call_id),
        arguments_length = tracing::field::debug(&metadata.arguments_length),
        delta_length = tracing::field::debug(&metadata.delta_length),
        output_index = tracing::field::debug(&metadata.output_index),
        content_index = tracing::field::debug(&metadata.content_index),
        item_id = tracing::field::debug(&metadata.item_id),
        is_compaction = metadata.is_compaction,
        compaction_id = tracing::field::debug(&metadata.compaction_id),
        has_encrypted_content = tracing::field::debug(&metadata.has_encrypted_content),
        "{RESPONSES_TRANSLATION_EVENT_SUPPRESSED}"
    );
}

pub(super) fn trace_no_observable_output_guard(metadata: &NoObservableOutputGuardDiagnostics) {
    debug!(
        response_id = ?metadata.response_id,
        pending_internal_outputs_count = metadata.pending_internal_outputs_count,
        suppressed_internal_tool_call_count = metadata.suppressed_internal_tool_call_count,
        forwarded_external_tool_call_count = metadata.forwarded_external_tool_call_count,
        forwarded_compaction_or_marker_count = metadata.forwarded_compaction_or_marker_count,
        visible_assistant_text_len = metadata.visible_assistant_text_len,
        completed_output_item_types = ?metadata.completed_output_item_types,
        upstream_last_event_type = ?metadata.upstream_last_event_type,
        is_intermediate_completed = metadata.is_intermediate_completed,
        "{RESPONSES_TRANSLATION_NO_OBSERVABLE_OUTPUT_GUARD}"
    );
}

pub(super) fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(ToString::to_string)
}

pub(super) fn string_length_field(value: Option<&Value>) -> Option<usize> {
    value.and_then(Value::as_str).map(str::len)
}
