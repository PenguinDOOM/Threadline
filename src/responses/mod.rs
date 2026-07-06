use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::response::IntoResponse;
use serde_json::Value;
use tracing::debug;

use crate::auth::LoadedUpstreamAuth;
use crate::errors::ThreadlineError;
use crate::models::{RouteProfile, resolve_request_model_for_profile};
use crate::registry::{RegistryAcquireError, RetainedSessionLease, RetainedSessionRegistry};
use crate::tools::{inject_internal_tools, is_internal_tool_name};
use crate::ws_pump::LiveUpstreamWebSocket;

mod downstream;
mod translation;
mod upstream;
mod virtual_tools;

use self::downstream::{
    DownstreamRequestClassification, looks_like_auxiliary_summary_conflict_fallback,
    parse_downstream_request_with_metadata, wants_reasoning_all_turns,
};
use self::translation::{ResponseStreamLease, ResponseStreamState, response_stream};
use self::upstream::send_response_create;
use self::virtual_tools::{
    detect_virtual_tool_summarizer_request, inject_virtual_tool_summarizer_instruction,
};

#[cfg(test)]
pub(crate) use self::downstream::DownstreamInteractionType;
pub(crate) use self::downstream::DownstreamRequestMetadata;

pub use self::upstream::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};

pub const TURN_STATE_HEADER: &str = "x-codex-turn-state";

#[derive(Clone)]
pub struct ResponsesRouteState {
    pub profile: RouteProfile,
    pub registry: Arc<RetainedSessionRegistry>,
    pub services: ThreadlineServices,
}

struct PreparedResponseRoute {
    upstream: Arc<LiveUpstreamWebSocket>,
    lease: ResponseStreamLease,
    previous_response_id: Option<String>,
    replay_stale_marker_on_pre_first_event_close: bool,
    reconnect_attempted: bool,
    upstream_request: serde_json::Map<String, Value>,
    execute_internal_tools: bool,
    apply_no_observable_output_failure: bool,
}

#[derive(Clone, Copy)]
enum TransientRouteKind {
    AuxiliarySummary,
    Utility,
}

impl TransientRouteKind {
    fn label(self) -> &'static str {
        match self {
            Self::AuxiliarySummary => "auxiliary_summary",
            Self::Utility => "utility",
        }
    }
}

pub(crate) async fn responses_handler(
    State(state): State<ResponsesRouteState>,
    axum::Json(payload): axum::Json<Value>,
    request_metadata: DownstreamRequestMetadata,
) -> Result<impl IntoResponse, ThreadlineError> {
    let mut request = parse_downstream_request_with_metadata(payload, request_metadata)?;
    let model_alias = resolve_request_model_for_profile(&request.payload, state.profile)?;
    if wants_reasoning_all_turns(&request.payload) && !model_alias.supports_reasoning_all_turns {
        return Err(ThreadlineError::UnsupportedReasoningContext);
    }
    request.payload.insert(
        "model".to_string(),
        Value::String(model_alias.upstream_model_id.to_string()),
    );
    let classification = request.classification;
    let routing_diagnostics = request.routing_diagnostics().clone();
    let previous_response_id_present = request.previous_response_id.is_some();
    let context_management_present = request.payload.contains_key("context_management");
    debug!(
        request_class = request_class_label(classification),
        interaction_type = routing_diagnostics.interaction_type.label(),
        interaction_type_compaction_hit = routing_diagnostics.interaction_type_compaction_hit,
        previous_response_id_present,
        context_management_present,
        manual_summary_prompt_hit = routing_diagnostics.summary_hits.manual_summary_prompt_hit,
        manual_structure_instruction_hit = routing_diagnostics
            .summary_hits
            .manual_structure_instruction_hit,
        manual_tool_results_instruction_hit = routing_diagnostics
            .summary_hits
            .manual_tool_results_instruction_hit,
        auto_context_too_large_hit = routing_diagnostics.summary_hits.auto_context_too_large_hit,
        auto_summary_tags_hit = routing_diagnostics.summary_hits.auto_summary_tags_hit,
        auto_only_task_hit = routing_diagnostics.summary_hits.auto_only_task_hit,
        simple_history_context_hit = routing_diagnostics.summary_hits.simple_history_context_hit,
        new_auto_detailed_summary_hit = routing_diagnostics
            .summary_hits
            .new_auto_detailed_summary_hit,
        new_auto_user_history_hit = routing_diagnostics.summary_hits.new_auto_user_history_hit,
        new_auto_user_final_summary_prompt_hit = routing_diagnostics
            .summary_hits
            .new_auto_user_final_summary_prompt_hit,
        summary_instruction_like_hit = routing_diagnostics
            .summary_hits
            .summary_instruction_like_hit,
        tool_choice = routing_diagnostics.tool_choice.as_deref().unwrap_or("none"),
        tools_count = routing_diagnostics.tools_count,
        input_item_count = routing_diagnostics.input_item_count,
        last_input_role = routing_diagnostics
            .last_input_role
            .as_deref()
            .unwrap_or("none"),
        last_input_type = routing_diagnostics
            .last_input_type
            .as_deref()
            .unwrap_or("none"),
        "responses_request_routed"
    );
    let mut base_request = request.payload;
    let previous_response_id = request.previous_response_id;
    let is_continuation_request = previous_response_id.is_some();
    let prepared = if state.profile == RouteProfile::Utility {
        maybe_inject_virtual_tool_summarizer_instruction(
            model_alias.upstream_model_id,
            &routing_diagnostics,
            &mut base_request,
        );
        start_transient_route(
            &state.services,
            base_request,
            classification,
            TransientRouteKind::Utility,
        )
        .await?
    } else {
        match classification {
            DownstreamRequestClassification::Normal => {
                match acquire_lease(&state.registry, previous_response_id.as_deref()).await {
                    Ok(mut lease) => {
                        let mut upstream_request = base_request.clone();
                        inject_internal_tools(&mut upstream_request);
                        strip_context_management_for_upstream(
                            &mut upstream_request,
                            "normal",
                            classification,
                        );
                        let mut reconnect_attempted = false;
                        let upstream = if let Some(previous_response_id) = &previous_response_id {
                            if !lease.has_open_upstream() {
                                debug!(
                                    previous_response_id,
                                    session_id = %lease.session().session_id,
                                    thread_id = %lease.session().thread_id,
                                    window_id = %lease.session().window_id,
                                    stale_reason = "missing_or_closed_upstream",
                                    "stale_previous_response_requires_client_replay"
                                );
                                lease.release();
                                return Err(ThreadlineError::PreviousResponseNotFound);
                            }

                            upstream_request.insert(
                                "previous_response_id".to_string(),
                                Value::String(previous_response_id.clone()),
                            );

                            let upstream = lease.upstream().expect(
                                "open retained upstream must exist for continuation preflight",
                            );
                            if let Err(error) =
                                send_response_create(&upstream, &upstream_request).await
                            {
                                let error = rewrite_stale_continuation_first_send_error(error);
                                if matches!(error, ThreadlineError::PreviousResponseNotFound) {
                                    debug!(
                                        previous_response_id,
                                        session_id = %lease.session().session_id,
                                        thread_id = %lease.session().thread_id,
                                        window_id = %lease.session().window_id,
                                        stale_reason = "first_send_closed",
                                        "stale_previous_response_requires_client_replay"
                                    );
                                    lease.release();
                                    return Err(ThreadlineError::PreviousResponseNotFound);
                                }

                                return Err(error);
                            }

                            tokio::task::yield_now().await;
                            if upstream.is_closed() {
                                debug!(
                                    previous_response_id,
                                    session_id = %lease.session().session_id,
                                    thread_id = %lease.session().thread_id,
                                    window_id = %lease.session().window_id,
                                    stale_reason = "first_send_closed_after_enqueue",
                                    "stale_previous_response_requires_client_replay"
                                );
                                lease.release();
                                return Err(ThreadlineError::PreviousResponseNotFound);
                            }

                            upstream
                        } else {
                            let auth = state.services.auth_provider().load()?;
                            let mut upstream =
                                ensure_upstream(&state.services, &mut lease, auth).await?;
                            if let Err(error) =
                                send_response_create(&upstream, &upstream_request).await
                            {
                                if let Some(reconnected) = attempt_pre_first_event_reconnect(
                                    &state.services,
                                    &mut lease,
                                    &upstream_request,
                                    previous_response_id.as_deref(),
                                    false,
                                    &mut reconnect_attempted,
                                )
                                .await?
                                {
                                    upstream = reconnected;
                                } else {
                                    return Err(error);
                                }
                            }

                            upstream
                        };

                        PreparedResponseRoute {
                            upstream,
                            lease: ResponseStreamLease::Retained(lease),
                            previous_response_id,
                            replay_stale_marker_on_pre_first_event_close: is_continuation_request,
                            reconnect_attempted,
                            upstream_request,
                            execute_internal_tools: true,
                            apply_no_observable_output_failure: true,
                        }
                    }
                    Err(ThreadlineError::RetainedSessionConflict) => {
                        let reroute_reason = retained_session_conflict_reroute_reason(
                            &routing_diagnostics,
                            &base_request,
                        );
                        if reroute_reason.is_none() {
                            return Err(ThreadlineError::RetainedSessionConflict);
                        }
                        let reroute_reason = reroute_reason.expect("reroute reason present");

                        debug!(
                            reroute_reason,
                            request_class = request_class_label(classification),
                            interaction_type = routing_diagnostics.interaction_type.label(),
                            interaction_type_compaction_hit =
                                routing_diagnostics.interaction_type_compaction_hit,
                            previous_response_id_present,
                            context_management_present,
                            manual_summary_prompt_hit =
                                routing_diagnostics.summary_hits.manual_summary_prompt_hit,
                            manual_structure_instruction_hit = routing_diagnostics
                                .summary_hits
                                .manual_structure_instruction_hit,
                            manual_tool_results_instruction_hit = routing_diagnostics
                                .summary_hits
                                .manual_tool_results_instruction_hit,
                            auto_context_too_large_hit =
                                routing_diagnostics.summary_hits.auto_context_too_large_hit,
                            auto_summary_tags_hit =
                                routing_diagnostics.summary_hits.auto_summary_tags_hit,
                            auto_only_task_hit =
                                routing_diagnostics.summary_hits.auto_only_task_hit,
                            simple_history_context_hit =
                                routing_diagnostics.summary_hits.simple_history_context_hit,
                            new_auto_detailed_summary_hit = routing_diagnostics
                                .summary_hits
                                .new_auto_detailed_summary_hit,
                            new_auto_user_history_hit =
                                routing_diagnostics.summary_hits.new_auto_user_history_hit,
                            new_auto_user_final_summary_prompt_hit = routing_diagnostics
                                .summary_hits
                                .new_auto_user_final_summary_prompt_hit,
                            summary_instruction_like_hit = routing_diagnostics
                                .summary_hits
                                .summary_instruction_like_hit,
                            fallback_summary_input_hit = reroute_reason == "fallback_summary_input",
                            tool_choice =
                                routing_diagnostics.tool_choice.as_deref().unwrap_or("none"),
                            tools_count = routing_diagnostics.tools_count,
                            input_item_count = routing_diagnostics.input_item_count,
                            last_input_role = routing_diagnostics
                                .last_input_role
                                .as_deref()
                                .unwrap_or("none"),
                            last_input_type = routing_diagnostics
                                .last_input_type
                                .as_deref()
                                .unwrap_or("none"),
                            "retained_session_conflict_rerouted"
                        );

                        start_transient_route(
                            &state.services,
                            base_request,
                            classification,
                            TransientRouteKind::AuxiliarySummary,
                        )
                        .await?
                    }
                    Err(error) => return Err(error),
                }
            }
            DownstreamRequestClassification::AuxiliarySummary => {
                start_transient_route(
                    &state.services,
                    base_request,
                    classification,
                    TransientRouteKind::AuxiliarySummary,
                )
                .await?
            }
        }
    };

    let stream = response_stream(ResponseStreamState {
        services: state.services.clone(),
        upstream: prepared.upstream,
        lease: prepared.lease,
        base_request: prepared.upstream_request,
        pending_internal_outputs: Vec::new(),
        previous_response_id: prepared.previous_response_id,
        execute_internal_tools: prepared.execute_internal_tools,
        suppressed_internal_output_indexes: std::collections::HashSet::new(),
        upstream_event_seen: false,
        replay_stale_marker_on_pre_first_event_close: prepared
            .replay_stale_marker_on_pre_first_event_close,
        reconnect_attempted: prepared.reconnect_attempted,
        observable_output: Default::default(),
        downstream_visible_text_sources: std::collections::HashSet::new(),
        downstream_visible_text_delta_count: 0,
        visible_assistant_text: Vec::new(),
        last_unidentified_visible_text: None,
        queued_synthetic_output_text_deltas: std::collections::VecDeque::new(),
        queued_forwarded_event: None,
        queued_final_completed: None,
        final_done_pending: false,
        apply_no_observable_output_failure: prepared.apply_no_observable_output_failure,
        done: false,
    });

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        )
        .header(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"))
        .body(Body::from_stream(stream))
        .expect("build sse response");
    Ok(response)
}

fn strip_threadline_tools(payload: &mut serde_json::Map<String, Value>) {
    let Some(Value::Array(tools)) = payload.get_mut("tools") else {
        return;
    };

    tools.retain(|tool| {
        !tool
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(is_internal_tool_name)
    });
}

fn strip_context_management_for_upstream(
    payload: &mut serde_json::Map<String, Value>,
    route_kind: &'static str,
    classification: DownstreamRequestClassification,
) -> bool {
    let stripped = payload.remove("context_management").is_some();
    if stripped {
        debug!(
            route_kind,
            request_class = request_class_label(classification),
            client_compaction_only = true,
            "context_management_stripped"
        );
    }
    stripped
}

fn request_class_label(classification: DownstreamRequestClassification) -> &'static str {
    match classification {
        DownstreamRequestClassification::Normal => "normal",
        DownstreamRequestClassification::AuxiliarySummary => "auxiliary_summary",
    }
}

fn retained_session_conflict_reroute_reason(
    routing_diagnostics: &self::downstream::DownstreamRequestRoutingDiagnostics,
    payload: &serde_json::Map<String, Value>,
) -> Option<&'static str> {
    if routing_diagnostics.interaction_type_compaction_hit {
        return Some("interaction_type_compaction");
    }

    if routing_diagnostics.summary_hits.matches_auxiliary_summary() {
        return Some("summary_fingerprint");
    }

    if looks_like_auxiliary_summary_conflict_fallback(payload) {
        return Some("fallback_summary_input");
    }

    None
}

fn rewrite_stale_continuation_first_send_error(error: ThreadlineError) -> ThreadlineError {
    match error {
        ThreadlineError::UpstreamWebSocketClosed => ThreadlineError::PreviousResponseNotFound,
        other => other,
    }
}

async fn attempt_pre_first_event_reconnect(
    services: &ThreadlineServices,
    lease: &mut RetainedSessionLease,
    request_payload: &serde_json::Map<String, Value>,
    previous_response_id: Option<&str>,
    upstream_event_seen: bool,
    reconnect_attempted: &mut bool,
) -> Result<Option<Arc<LiveUpstreamWebSocket>>, ThreadlineError> {
    let Some(previous_response_id) = previous_response_id else {
        return Ok(None);
    };

    if upstream_event_seen || *reconnect_attempted {
        return Ok(None);
    }

    *reconnect_attempted = true;
    lease.mark_upstream_recoverable().await;
    debug!(
        previous_response_id,
        session_id = %lease.session().session_id,
        thread_id = %lease.session().thread_id,
        window_id = %lease.session().window_id,
        "reconnect_continuation_attempt"
    );

    let auth = services.auth_provider().load()?;
    let upstream = match ensure_upstream(services, lease, auth).await {
        Ok(upstream) => upstream,
        Err(error) => {
            debug!(
                previous_response_id,
                session_id = %lease.session().session_id,
                thread_id = %lease.session().thread_id,
                "reconnect_continuation_failed"
            );
            return Err(error);
        }
    };

    if let Err(error) = send_response_create(&upstream, request_payload).await {
        debug!(
            previous_response_id,
            session_id = %lease.session().session_id,
            thread_id = %lease.session().thread_id,
            "reconnect_continuation_failed"
        );
        return Err(error);
    }

    Ok(Some(upstream))
}

async fn acquire_lease(
    registry: &RetainedSessionRegistry,
    previous_response_id: Option<&str>,
) -> Result<RetainedSessionLease, ThreadlineError> {
    match previous_response_id {
        Some(previous_response_id) => registry
            .acquire_previous(previous_response_id)
            .await
            .map_err(map_registry_error),
        None => registry.acquire_new().await.map_err(map_registry_error),
    }
}

async fn start_transient_route(
    services: &ThreadlineServices,
    mut upstream_request: serde_json::Map<String, Value>,
    classification: DownstreamRequestClassification,
    kind: TransientRouteKind,
) -> Result<PreparedResponseRoute, ThreadlineError> {
    strip_threadline_tools(&mut upstream_request);
    strip_context_management_for_upstream(&mut upstream_request, kind.label(), classification);

    upstream_request.remove("previous_response_id");

    let auth = services.auth_provider().load()?;
    let connected = services.connector().connect(auth, None).await?;
    send_response_create(&connected.websocket, &upstream_request).await?;

    Ok(PreparedResponseRoute {
        upstream: connected.websocket,
        lease: ResponseStreamLease::TransientAuxiliary,
        previous_response_id: None,
        replay_stale_marker_on_pre_first_event_close: false,
        reconnect_attempted: false,
        upstream_request,
        execute_internal_tools: false,
        apply_no_observable_output_failure: false,
    })
}

async fn ensure_upstream(
    services: &ThreadlineServices,
    lease: &mut RetainedSessionLease,
    auth: LoadedUpstreamAuth,
) -> Result<Arc<LiveUpstreamWebSocket>, ThreadlineError> {
    if let Some(upstream) = lease.upstream() {
        if !upstream.is_closed() {
            return Ok(upstream);
        }

        lease.mark_upstream_recoverable().await;
    }

    let connected = services
        .connector()
        .connect(auth, Some(lease.session().clone()))
        .await?;
    let turn_state = connected
        .turn_state
        .clone()
        .or_else(|| lease.session().turn_state.clone());
    lease.update_turn_state(turn_state).await;
    lease
        .replace_upstream(Some(Arc::clone(&connected.websocket)))
        .await;
    Ok(connected.websocket)
}

fn map_registry_error(error: RegistryAcquireError) -> ThreadlineError {
    match error {
        RegistryAcquireError::PreviousResponseNotFound => ThreadlineError::PreviousResponseNotFound,
        RegistryAcquireError::RetainedSessionConflict => ThreadlineError::RetainedSessionConflict,
        RegistryAcquireError::RetainedSessionCapacityExceeded => {
            ThreadlineError::RetainedSessionCapacityExceeded
        }
    }
}

fn maybe_inject_virtual_tool_summarizer_instruction(
    model: &str,
    routing_diagnostics: &downstream::DownstreamRequestRoutingDiagnostics,
    request: &mut serde_json::Map<String, Value>,
) {
    let detection = detect_virtual_tool_summarizer_request(request);
    if !detection.is_match() {
        return;
    }

    let instructions_existed = request.contains_key("instructions");
    debug!(
        profile = "utility",
        model,
        input_item_count = routing_diagnostics.input_item_count,
        tools_count = routing_diagnostics.tools_count,
        instructions_existed,
        semantic_similarity_hit = detection.semantic_similarity_hit,
        group_index_tag_hit = detection.group_index_tag_hit,
        group_index_field_hit = detection.group_index_field_hit,
        group_name_field_hit = detection.group_name_field_hit,
        summary_field_hit = detection.summary_field_hit,
        required_hit_count = detection.required_hit_count(),
        "virtual_tools_summarizer_request_detected"
    );

    let mutation = inject_virtual_tool_summarizer_instruction(request);
    if mutation.injected() {
        debug!(
            profile = "utility",
            model,
            instructions_existed,
            mutation = mutation.outcome_label(),
            "virtual_tools_summarizer_instruction_injected"
        );
        return;
    }

    if let Some(skip_reason) = mutation.skip_reason() {
        debug!(
            profile = "utility",
            model,
            instructions_existed,
            skip_reason,
            mutation = mutation.outcome_label(),
            "virtual_tools_summarizer_instruction_skipped"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::rewrite_stale_continuation_first_send_error;
    use crate::errors::ThreadlineError;

    #[test]
    fn stale_continuation_first_send_rewrites_closed_upstream_to_previous_response_not_found() {
        let rewritten =
            rewrite_stale_continuation_first_send_error(ThreadlineError::UpstreamWebSocketClosed);

        assert!(matches!(
            rewritten,
            ThreadlineError::PreviousResponseNotFound
        ));
    }

    #[test]
    fn stale_continuation_first_send_preserves_non_transport_errors() {
        let preserved =
            rewrite_stale_continuation_first_send_error(ThreadlineError::InvalidResponsesRequest);

        assert!(matches!(
            preserved,
            ThreadlineError::InvalidResponsesRequest
        ));
    }
}
