use super::*;

pub(super) fn response_output_text_delta_payload(
    key: &VisibleTextSourceKey,
    delta: &str,
) -> Option<Value> {
    if delta.trim().is_empty() {
        return None;
    }

    let mut payload = serde_json::Map::new();
    payload.insert(
        "type".to_string(),
        Value::String("response.output_text.delta".to_string()),
    );
    payload.insert("delta".to_string(), Value::String(delta.to_string()));

    if let Some(item_id) = key.item_id.as_ref() {
        payload.insert("item_id".to_string(), Value::String(item_id.clone()));
    }
    if let Some(output_index) = key.output_index {
        payload.insert("output_index".to_string(), Value::from(output_index));
    }
    if let Some(content_index) = key.content_index {
        payload.insert("content_index".to_string(), Value::from(content_index));
    }

    Some(Value::Object(payload))
}

pub(super) fn visible_text_delta_source_key(event: &Value) -> VisibleTextSourceKey {
    VisibleTextSourceKey::new(
        string_field(event.get("item_id")),
        event.get("output_index").and_then(Value::as_u64),
        event.get("content_index").and_then(Value::as_u64),
    )
}

pub(super) fn synthesized_output_text_done_delta(
    event: &Value,
) -> Option<(VisibleTextSourceKey, Value)> {
    let key = visible_text_delta_source_key(event);
    let text = event.get("text").and_then(Value::as_str)?;
    let payload = response_output_text_delta_payload(&key, text)?;
    Some((key, payload))
}

pub(super) fn message_output_text_delta_payloads(
    item: &Value,
    output_index: Option<u64>,
) -> Vec<(VisibleTextSourceKey, Value)> {
    if item.get("type").and_then(Value::as_str) != Some("message")
        || item.get("role").and_then(Value::as_str) != Some("assistant")
    {
        return Vec::new();
    }

    let Some(content) = item.get("content").and_then(Value::as_array) else {
        return Vec::new();
    };

    let item_id = string_field(item.get("id"));
    let mut payloads = Vec::new();
    for (content_index, part) in content.iter().enumerate() {
        if part.get("type").and_then(Value::as_str) != Some("output_text") {
            continue;
        }

        let Some(text) = part.get("text").and_then(Value::as_str) else {
            continue;
        };

        let key =
            VisibleTextSourceKey::new(item_id.clone(), output_index, Some(content_index as u64));
        let Some(payload) = response_output_text_delta_payload(&key, text) else {
            continue;
        };
        payloads.push((key, payload));
    }

    payloads
}

pub(super) fn synthesized_output_item_done_text_delta(
    event: &Value,
) -> Vec<(VisibleTextSourceKey, Value)> {
    let Some(item) = event.get("item") else {
        return Vec::new();
    };

    message_output_text_delta_payloads(item, output_index_from_event(event))
}

pub(super) fn synthesized_completed_output_text_delta(
    event: &Value,
) -> Vec<(VisibleTextSourceKey, Value)> {
    let Some(output) = event
        .get("response")
        .and_then(|response| response.get("output"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };

    let mut first_key: Option<VisibleTextSourceKey> = None;
    let mut combined_text = String::new();

    for (output_index, item) in output.iter().enumerate() {
        if item.get("type").and_then(Value::as_str) != Some("message")
            || item.get("role").and_then(Value::as_str) != Some("assistant")
        {
            continue;
        }

        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };

        let item_id = string_field(item.get("id"));
        for (content_index, part) in content.iter().enumerate() {
            if part.get("type").and_then(Value::as_str) != Some("output_text") {
                continue;
            }

            let Some(text) = part.get("text").and_then(Value::as_str) else {
                continue;
            };

            if text.trim().is_empty() {
                continue;
            }

            if first_key.is_none() {
                first_key = Some(VisibleTextSourceKey::new(
                    item_id.clone(),
                    Some(output_index as u64),
                    Some(content_index as u64),
                ));
            }

            combined_text.push_str(text);
        }
    }

    let Some(key) = first_key else {
        return Vec::new();
    };
    let Some(payload) = response_output_text_delta_payload(&key, &combined_text) else {
        return Vec::new();
    };

    vec![(key, payload)]
}

pub(super) fn record_visible_assistant_text(
    visible_text: &mut Vec<VisibleAssistantText>,
    key: &VisibleTextSourceKey,
    text: &str,
) {
    if text.trim().is_empty() {
        return;
    }

    if let Some(existing) = visible_text.iter_mut().find(|entry| entry.key == *key) {
        existing.text.push_str(text);
        return;
    }

    visible_text.push(VisibleAssistantText {
        key: key.clone(),
        text: text.to_string(),
    });
}

pub(super) fn track_visible_text_identity(
    state: &mut ResponseStreamState,
    key: &VisibleTextSourceKey,
    text: &str,
) -> bool {
    if let Some(identity) = key.dedupe_identity() {
        state.downstream_visible_text_sources.insert(identity);
        return true;
    }

    if state.last_unidentified_visible_text.as_deref() == Some(text) {
        return false;
    }

    state.last_unidentified_visible_text = Some(text.to_string());
    true
}

pub(super) fn record_forwarded_visible_text_delta(
    state: &mut ResponseStreamState,
    key: VisibleTextSourceKey,
    delta: &str,
) {
    if !track_visible_text_identity(state, &key, delta) {
        return;
    }

    record_visible_assistant_text(&mut state.visible_assistant_text, &key, delta);
}

pub(super) fn queue_visible_text_delta(
    state: &mut ResponseStreamState,
    key: VisibleTextSourceKey,
    payload: Value,
    synthetic_delta_source: &'static str,
    response_id: Option<&str>,
) -> bool {
    let Some(delta) = payload.get("delta").and_then(Value::as_str) else {
        return false;
    };

    if !key
        .dedupe_identity()
        .map(|identity| state.downstream_visible_text_sources.insert(identity))
        .unwrap_or_else(|| {
            if state.last_unidentified_visible_text.as_deref() == Some(delta) {
                false
            } else {
                state.last_unidentified_visible_text = Some(delta.to_string());
                true
            }
        })
    {
        return false;
    }

    record_visible_assistant_text(&mut state.visible_assistant_text, &key, delta);
    state
        .queued_synthetic_output_text_deltas
        .push_back(QueuedSyntheticOutputTextDelta {
            payload,
            response_id: response_id.map(ToString::to_string),
            synthetic_delta_source,
        });
    true
}

pub(super) fn queue_visible_text_deltas(
    state: &mut ResponseStreamState,
    payloads: Vec<(VisibleTextSourceKey, Value)>,
    synthetic_delta_source: &'static str,
    response_id: Option<&str>,
) -> bool {
    let mut queued = false;
    for (key, payload) in payloads {
        if queue_visible_text_delta(state, key, payload, synthetic_delta_source, response_id) {
            queued = true;
        }
    }

    queued
}

pub(super) fn assistant_message_has_visible_output_text(item: &Value) -> bool {
    if item.get("type").and_then(Value::as_str) != Some("message")
        || item.get("role").and_then(Value::as_str) != Some("assistant")
    {
        return false;
    }

    item.get("content")
        .and_then(Value::as_array)
        .is_some_and(|content| {
            content.iter().any(|part| {
                part.get("type").and_then(Value::as_str) == Some("output_text")
                    && part
                        .get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.trim().is_empty())
            })
        })
}

pub(super) fn synthetic_assistant_message_id(response_id: Option<&str>) -> String {
    match response_id {
        Some(response_id) if !response_id.is_empty() => {
            format!("synthetic_assistant_{response_id}")
        }
        _ => "synthetic_assistant".to_string(),
    }
}

pub(super) fn synthetic_assistant_message(
    response_id: Option<&str>,
    visible_text: &[VisibleAssistantText],
) -> Option<Value> {
    let content: Vec<Value> = visible_text
        .iter()
        .filter(|entry| !entry.text.trim().is_empty())
        .map(|entry| {
            Value::Object(serde_json::Map::from_iter([
                ("type".to_string(), Value::String("output_text".to_string())),
                ("text".to_string(), Value::String(entry.text.clone())),
                ("annotations".to_string(), Value::Array(Vec::new())),
            ]))
        })
        .collect();

    if content.is_empty() {
        return None;
    }

    let message_id = visible_text
        .iter()
        .find_map(|entry| entry.key.item_id.clone())
        .unwrap_or_else(|| synthetic_assistant_message_id(response_id));

    Some(Value::Object(serde_json::Map::from_iter([
        ("id".to_string(), Value::String(message_id)),
        ("type".to_string(), Value::String("message".to_string())),
        ("role".to_string(), Value::String("assistant".to_string())),
        ("content".to_string(), Value::Array(content)),
    ])))
}

pub(super) fn has_accumulated_visible_assistant_text(
    visible_text: &[VisibleAssistantText],
) -> bool {
    visible_text
        .iter()
        .any(|entry| !entry.text.trim().is_empty())
}

pub(super) fn record_visible_delta_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
) -> DownstreamTraceDiagnostics {
    let mut trace_diagnostics = DownstreamTraceDiagnostics::default();

    let key = visible_text_delta_source_key(parsed);
    if let Some(delta) = parsed.get("delta").and_then(Value::as_str) {
        record_forwarded_visible_text_delta(state, key, delta);
        trace_diagnostics.visible_text_length = Some(delta.len());
    }
    record_forwarded_observable_output(
        &mut state.observable_output,
        "response.output_text.delta",
        &parsed,
    );
    state.downstream_visible_text_delta_count += 1;
    trace_diagnostics.response_id = response_id_from_event(&parsed).map(ToString::to_string);
    trace_diagnostics.visible_text_delta_count = Some(1);

    trace_diagnostics
}

pub(super) fn queue_visible_done_event(
    state: &mut ResponseStreamState,
    parsed: &Value,
    event_type: &str,
) -> bool {
    if event_type == "response.output_text.done" {
        if let Some((key, payload)) = synthesized_output_text_done_delta(parsed)
            && queue_visible_text_delta(
                state,
                key,
                payload,
                "response.output_text.done",
                response_id_from_event(parsed),
            )
        {
            return true;
        }
    } else if event_type == "response.output_item.done"
        && queue_visible_text_deltas(
            state,
            synthesized_output_item_done_text_delta(parsed),
            "response.output_item.done",
            response_id_from_event(parsed),
        )
    {
        return true;
    }

    false
}
