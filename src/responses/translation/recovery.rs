use serde_json::{Map, Value};

use super::ResponseStreamState;
use crate::errors::ThreadlineError;
use crate::ws_pump::{UpstreamTerminalState, UpstreamWebSocketError};

const PRELUDE_MAX_EVENTS: usize = 8;
const PRELUDE_MAX_BYTES: usize = 64 * 1024;

pub(super) fn replay_allowed(state: &ResponseStreamState, failure: RecoveryFailure) -> bool {
    state.replay_stale_marker_on_pre_first_event_close
        && state.recovery_local_tools_only
        && !state.headers_committed
        && !state.replay_prohibited
        && match failure {
            RecoveryFailure::ConnectionLimit
            | RecoveryFailure::MarkerMissing
            | RecoveryFailure::TransportLost => true,
            RecoveryFailure::LivenessTimeout => !state.upstream_event_seen,
            RecoveryFailure::Other => false,
        }
}

pub(in crate::responses) async fn preflight(
    state: &mut ResponseStreamState,
) -> Result<(), ThreadlineError> {
    if !state.replay_stale_marker_on_pre_first_event_close || !state.recovery_local_tools_only {
        state.headers_committed = true;
        return Ok(());
    }

    let mut bytes = 0;
    loop {
        let text = match receive_preflight(state).await {
            Ok(text) => text,
            Err(error) => return finish_preflight_failure(state, error),
        };
        state.upstream_event_seen = true;

        if let Ok(event) = serde_json::from_str::<Value>(&text) {
            if let Some(error) = preflight_event_error(state, &event) {
                return finish_preflight_failure(state, error);
            }
            if lifecycle_only(&event)
                && state.pending_upstream_events.len() < PRELUDE_MAX_EVENTS
                && text.len() <= PRELUDE_MAX_BYTES - bytes
            {
                bytes += text.len();
                state.pending_upstream_events.push_back(text);
                continue;
            }
        }

        state.replay_prohibited = true;
        state.pending_upstream_events.push_back(text);
        state.headers_committed = true;
        return Ok(());
    }
}

async fn receive_preflight(state: &ResponseStreamState) -> Result<String, ThreadlineError> {
    let Some(upstream) = state.upstream.as_ref() else {
        return Err(ThreadlineError::UpstreamWebSocketClosed);
    };
    if let Some(error) = super::queue_transport_error(upstream.terminal_state())
        .filter(|error| !matches!(error, ThreadlineError::UpstreamWebSocketClosed))
    {
        return Err(error);
    }
    let received = upstream.recv_text().await;
    if let Some(error) = super::final_completion_acceptance_error(upstream.terminal_state()) {
        return Err(error);
    }
    match received {
        Ok(Some(text)) => Ok(text),
        Ok(None) | Err(UpstreamWebSocketError::OutboundQueueClosed) => {
            Err(ThreadlineError::UpstreamWebSocketClosed)
        }
        Err(UpstreamWebSocketError::InboundBufferOverflow) => {
            Err(ThreadlineError::UpstreamInboundBufferOverflow)
        }
        Err(UpstreamWebSocketError::LivenessTimeout) => {
            Err(ThreadlineError::UpstreamLivenessTimeout)
        }
    }
}

fn preflight_event_error(
    state: &mut ResponseStreamState,
    event: &Value,
) -> Option<ThreadlineError> {
    let failure = failure_kind(event);
    if !matches!(
        failure,
        RecoveryFailure::ConnectionLimit | RecoveryFailure::MarkerMissing
    ) {
        return None;
    }
    if !failure_without_progress(event) {
        state.replay_prohibited = true;
    }
    if replay_allowed(state, failure) {
        Some(ThreadlineError::PreviousResponseNotFound)
    } else if failure == RecoveryFailure::ConnectionLimit {
        Some(ThreadlineError::UpstreamWebSocketConnectionLimit)
    } else {
        None
    }
}

fn finish_preflight_failure(
    state: &mut ResponseStreamState,
    error: ThreadlineError,
) -> Result<(), ThreadlineError> {
    let failure = match error {
        ThreadlineError::UpstreamWebSocketClosed => RecoveryFailure::TransportLost,
        ThreadlineError::UpstreamLivenessTimeout => RecoveryFailure::LivenessTimeout,
        _ => RecoveryFailure::Other,
    };
    let error = if replay_allowed(state, failure) {
        ThreadlineError::PreviousResponseNotFound
    } else {
        error
    };
    state.pending_upstream_events.clear();
    state.upstream = None;
    if matches!(error, ThreadlineError::UpstreamLivenessTimeout) && state.upstream_event_seen {
        state.lease.finalize_liveness_timeout_turn();
    }
    state.lease.release();
    Err(error)
}

pub(in crate::responses) fn queue_transport_error(
    terminal: UpstreamTerminalState,
) -> Option<ThreadlineError> {
    match terminal {
        UpstreamTerminalState::Open => None,
        UpstreamTerminalState::Closed(_) => Some(ThreadlineError::UpstreamWebSocketClosed),
        UpstreamTerminalState::InboundBufferOverflow(_) => {
            Some(ThreadlineError::UpstreamInboundBufferOverflow)
        }
        UpstreamTerminalState::LivenessTimeout(_) => Some(ThreadlineError::UpstreamLivenessTimeout),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryFailure {
    ConnectionLimit,
    MarkerMissing,
    TransportLost,
    LivenessTimeout,
    Other,
}

pub(crate) fn local_tools_only(request: &Map<String, Value>) -> bool {
    match request.get("tools") {
        None => true,
        Some(Value::Array(tools)) => tools.iter().all(|tool| {
            tool.as_object().is_some_and(|tool| {
                tool.get("type").and_then(Value::as_str) == Some("function")
                    && tool
                        .get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| !name.is_empty())
                    && tool.get("parameters").is_none_or(Value::is_object)
                    && tool.get("description").is_none_or(Value::is_string)
                    && tool.get("strict").is_none_or(Value::is_boolean)
                    && tool.keys().all(|key| {
                        matches!(
                            key.as_str(),
                            "type" | "name" | "parameters" | "description" | "strict"
                        )
                    })
            })
        }),
        Some(_) => false,
    }
}

pub(crate) fn failure_kind(event: &Value) -> RecoveryFailure {
    if !matches!(
        event.get("type").and_then(Value::as_str),
        Some("error" | "response.failed")
    ) {
        return RecoveryFailure::Other;
    }
    let envelopes = [
        event.get("error"),
        event.pointer("/response/error"),
        event.pointer("/error/error"),
    ];
    let mut code = None;
    for envelope in envelopes
        .into_iter()
        .flatten()
        .filter(|value| !value.is_null())
    {
        if envelope.get("error").is_some() && envelope.get("code").is_none() {
            continue;
        }
        let Some(candidate) = envelope.get("code").and_then(Value::as_str) else {
            return RecoveryFailure::Other;
        };
        if code.is_some_and(|code| code != candidate) {
            return RecoveryFailure::Other;
        }
        code = Some(candidate);
    }
    match code {
        Some("websocket_connection_limit_reached") => RecoveryFailure::ConnectionLimit,
        Some("previous_response_not_found") => RecoveryFailure::MarkerMissing,
        _ => RecoveryFailure::Other,
    }
}

fn metadata_response(response: &Value, allow_error: bool) -> bool {
    response.as_object().is_some_and(|response| {
        response.iter().all(|(key, value)| match key.as_str() {
            "output" => value.as_array().is_some_and(Vec::is_empty),
            "error" => value.is_null() || (allow_error && known_error(value)),
            "id" | "object" | "model" => value.is_string(),
            "status" => value.as_str().is_some_and(|status| {
                matches!(status, "queued" | "in_progress") || (allow_error && status == "failed")
            }),
            "created_at" => value.is_number(),
            _ => false,
        })
    })
}

pub(crate) fn lifecycle_only(event: &Value) -> bool {
    matches!(
        event.get("type").and_then(Value::as_str),
        Some("response.created" | "response.in_progress")
    ) && metadata_event(event, false)
}

pub(crate) fn failure_without_progress(event: &Value) -> bool {
    metadata_event(event, true)
}

fn known_error(error: &Value) -> bool {
    error.is_null()
        || error.as_object().is_some_and(|error| {
            error.iter().all(|(key, value)| match key.as_str() {
                "code" | "message" | "type" | "param" => value.is_string() || value.is_null(),
                "error" => known_error(value),
                "status" | "status_code" => value.is_number(),
                _ => false,
            })
        })
}

fn metadata_event(event: &Value, allow_error: bool) -> bool {
    event.as_object().is_some_and(|event| {
        event.iter().all(|(key, value)| match key.as_str() {
            "type" | "response_id" | "event_id" => value.is_string(),
            "sequence_number" | "status" | "status_code" => value.is_number(),
            "response" => metadata_response(value, allow_error),
            "error" => value.is_null() || (allow_error && known_error(value)),
            _ => false,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recovery_exact_classifiers_reject_conflicts_and_message_heuristics() {
        assert_eq!(
            failure_kind(
                &json!({"type":"error","error":{"code":"websocket_connection_limit_reached"}})
            ),
            RecoveryFailure::ConnectionLimit
        );
        assert_eq!(
            failure_kind(
                &json!({"type":"response.failed","response":{"error":{"code":"previous_response_not_found"}}})
            ),
            RecoveryFailure::MarkerMissing
        );
        assert_eq!(
            failure_kind(
                &json!({"type":"response.failed","error":{"code":"previous_response_not_found"},"response":{"error":{"code":"rate_limit_exceeded"}}})
            ),
            RecoveryFailure::Other
        );
        assert_eq!(
            failure_kind(
                &json!({"type":"error","error":{"message":"Previous response with id x not found"}})
            ),
            RecoveryFailure::Other
        );
    }

    #[test]
    fn recovery_tool_safety_excludes_hosted_unknown_and_malformed_tools() {
        for tools in [
            json!([{"type":"web_search"}]),
            json!([{"type":"function"}]),
            json!(null),
            json!([{"type":"function","name":"external","server":true}]),
        ] {
            assert!(!local_tools_only(
                json!({"tools":tools,"tool_choice":"none"})
                    .as_object()
                    .unwrap()
            ));
        }
        assert!(local_tools_only(
            json!({"tools":[{"type":"function","name":"external","parameters":{}}]})
                .as_object()
                .unwrap()
        ));
        assert!(local_tools_only(json!({}).as_object().unwrap()));
    }

    #[test]
    fn recovery_preflight_accepts_only_empty_lifecycle_metadata() {
        assert!(lifecycle_only(
            &json!({"type":"response.created","response":{"id":"r","output":[],"error":null}})
        ));
        for event in [
            json!({"type":"response.output_text.delta","delta":""}),
            json!({"type":"response.created","response":{"output":[{"type":"message","content":[]}]}}),
            json!({"type":"response.created","response":{"state":"opaque"}}),
            json!({"type":"response.created","response":{"status":"completed","output":[]}}),
        ] {
            assert!(!lifecycle_only(&event));
        }
        assert!(!failure_without_progress(
            &json!({"type":"response.failed","response":{"output":[{"type":"reasoning"}],"error":{"code":"previous_response_not_found"}}})
        ));
    }
}
