use super::*;

pub(super) fn is_forwarded_external_tool_call_item(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("function_call")
        && item
            .get("name")
            .or_else(|| item.get("tool_name"))
            .and_then(Value::as_str)
            .is_some_and(|name| !is_internal_tool_name(name))
}

pub(super) fn has_image_generation_result(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("image_generation_call")
        && item
            .get("result")
            .and_then(Value::as_str)
            .is_some_and(|result| !result.trim().is_empty())
}

pub(super) fn is_forwarded_marker_like_item(item: &Value) -> bool {
    matches!(
        item.get("type").and_then(Value::as_str),
        Some("compaction") | Some("context")
    )
}

pub(super) fn record_forwarded_observable_output(
    observable_output: &mut DownstreamObservableOutputState,
    event_type: &str,
    event: &Value,
) {
    match event_type {
        "response.output_text.delta" => {
            observable_output.forwarded_visible_text_delta_count += 1;
        }
        "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
            observable_output.forwarded_external_tool_call_count += 1;
        }
        "response.output_item.added" | "response.output_item.done" => {
            let Some(item) = event.get("item") else {
                return;
            };

            if is_forwarded_external_tool_call_item(item) {
                observable_output.forwarded_external_tool_call_count += 1;
            } else if has_image_generation_result(item) {
                observable_output.forwarded_image_generation_count += 1;
            } else if is_forwarded_marker_like_item(item) {
                observable_output.forwarded_marker_like_output_count += 1;
            }
        }
        _ => {}
    }
}

pub(super) fn record_completed_observable_output(
    observable_output: &mut DownstreamObservableOutputState,
    event: &Value,
    diagnostics: &CompletedSanitizationDiagnostics,
) {
    observable_output.final_visible_message_count = diagnostics.completed_visible_message_count;
    observable_output.final_external_tool_call_count = 0;
    observable_output.final_image_generation_count = 0;
    observable_output.final_marker_like_output_count = 0;

    let Some(output) = event
        .get("response")
        .and_then(|response| response.get("output"))
        .and_then(Value::as_array)
    else {
        return;
    };

    for item in output {
        if is_forwarded_external_tool_call_item(item) {
            observable_output.final_external_tool_call_count += 1;
        } else if has_image_generation_result(item) {
            observable_output.final_image_generation_count += 1;
        } else if is_forwarded_marker_like_item(item) {
            observable_output.final_marker_like_output_count += 1;
        }
    }
}

pub(super) fn has_downstream_observable_output(state: &ResponseStreamState) -> bool {
    state.observable_output.has_observable_output()
}

pub(super) fn no_observable_output_guard_diagnostics(
    state: &ResponseStreamState,
    completed_event: &Value,
    diagnostics: &CompletedSanitizationDiagnostics,
) -> NoObservableOutputGuardDiagnostics {
    NoObservableOutputGuardDiagnostics {
        response_id: response_id_from_event(completed_event).map(ToString::to_string),
        pending_internal_outputs_count: state.pending_internal_outputs.len(),
        suppressed_internal_tool_call_count: state.suppressed_internal_output_indexes.len()
            + diagnostics.sanitized_internal_function_call_count,
        forwarded_external_tool_call_count: state
            .observable_output
            .forwarded_external_tool_call_count
            + state.observable_output.final_external_tool_call_count,
        forwarded_compaction_or_marker_count: state
            .observable_output
            .forwarded_marker_like_output_count
            + state.observable_output.final_marker_like_output_count,
        visible_assistant_text_len: state
            .visible_assistant_text
            .iter()
            .map(|entry| entry.text.len())
            .sum(),
        completed_output_item_types: completed_event
            .get("response")
            .and_then(|response| response.get("output"))
            .and_then(Value::as_array)
            .map(|output| {
                output
                    .iter()
                    .filter_map(|item| item.get("type").and_then(Value::as_str))
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        upstream_last_event_type: state.observable_output.last_upstream_event_type.clone(),
        is_intermediate_completed: !state.pending_internal_outputs.is_empty(),
    }
}

pub(super) fn no_observable_output_failed_payload(response_id: Option<&str>) -> Value {
    let mut response = serde_json::Map::new();
    if let Some(response_id) = response_id.filter(|value| !value.is_empty()) {
        response.insert("id".to_string(), Value::String(response_id.to_string()));
    }

    Value::Object(serde_json::Map::from_iter([
        (
            "type".to_string(),
            Value::String("response.failed".to_string()),
        ),
        ("response".to_string(), Value::Object(response)),
        (
            "error".to_string(),
            Value::Object(serde_json::Map::from_iter([
                (
                    "code".to_string(),
                    Value::String("threadline_no_observable_output".to_string()),
                ),
                (
                    "message".to_string(),
                    Value::String("Response contained no observable output.".to_string()),
                ),
            ])),
        ),
    ]))
}

pub(super) fn sanitized_completed_event_with_diagnostics(
    event: &Value,
    visible_text: &[VisibleAssistantText],
) -> (Value, CompletedSanitizationDiagnostics) {
    let mut sanitized = event.clone();
    let response_id = response_id_from_event(event);
    let mut diagnostics = CompletedSanitizationDiagnostics::default();

    let Some(response) = sanitized.get_mut("response").and_then(Value::as_object_mut) else {
        return (sanitized, diagnostics);
    };

    let Some(original_output) = response.get("output").and_then(Value::as_array) else {
        if let Some(message) = synthetic_assistant_message(response_id, visible_text) {
            diagnostics.completed_visible_message_count = 1;
            response.insert("output".to_string(), Value::Array(vec![message]));
        }
        return (sanitized, diagnostics);
    };

    let mut final_output = filter_completed_output(original_output, &mut diagnostics);

    let has_visible_assistant_message = final_output
        .iter()
        .any(assistant_message_has_visible_output_text);
    let mut output_changed = diagnostics.sanitized_internal_function_call_count > 0;

    if !has_visible_assistant_message
        && let Some(message) = synthetic_assistant_message(response_id, visible_text)
    {
        final_output.push(message);
        output_changed = true;
    }

    diagnostics.completed_visible_message_count = final_output
        .iter()
        .filter(|item| assistant_message_has_visible_output_text(item))
        .count();

    if output_changed {
        response.insert("output".to_string(), Value::Array(final_output));
    }

    (sanitized, diagnostics)
}

pub(super) fn filter_completed_output(
    original_output: &[Value],
    diagnostics: &mut CompletedSanitizationDiagnostics,
) -> Vec<Value> {
    original_output
        .iter()
        .filter_map(|item| match item.get("type").and_then(Value::as_str) {
            Some("function_call")
                if item
                    .get("name")
                    .or_else(|| item.get("tool_name"))
                    .and_then(Value::as_str)
                    .is_some_and(is_internal_tool_name) =>
            {
                diagnostics.sanitized_internal_function_call_count += 1;
                None
            }
            _ => Some(item.clone()),
        })
        .collect::<Vec<_>>()
}
