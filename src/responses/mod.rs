use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::response::IntoResponse;
use serde_json::Value;
use tracing::debug;

use crate::auth::LoadedUpstreamAuth;
use crate::errors::ThreadlineError;
use crate::models::validate_request_model;
use crate::registry::{RegistryAcquireError, RetainedSessionLease, RetainedSessionRegistry};
use crate::tools::{inject_internal_tools, is_internal_tool_name};
use crate::ws_pump::LiveUpstreamWebSocket;

mod downstream;
mod translation;
mod upstream;

use self::downstream::{
    DownstreamRequestClassification, looks_like_auxiliary_summary_conflict_fallback,
    parse_downstream_request,
};
use self::translation::{ResponseStreamLease, ResponseStreamState, response_stream};
use self::upstream::send_response_create;

pub use self::upstream::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};

pub const TURN_STATE_HEADER: &str = "x-codex-turn-state";

#[derive(Clone)]
pub struct ResponsesRouteState {
    pub registry: Arc<RetainedSessionRegistry>,
    pub services: ThreadlineServices,
}

struct PreparedResponseRoute {
    upstream: Arc<LiveUpstreamWebSocket>,
    lease: ResponseStreamLease,
    previous_response_id: Option<String>,
    reconnect_attempted: bool,
    upstream_request: serde_json::Map<String, Value>,
    execute_internal_tools: bool,
    apply_no_observable_output_failure: bool,
}

pub async fn responses_handler(
    State(state): State<ResponsesRouteState>,
    axum::Json(payload): axum::Json<Value>,
) -> Result<impl IntoResponse, ThreadlineError> {
    let request = parse_downstream_request(payload)?;
    validate_request_model(&request.payload)?;
    let classification = request.classification;
    let routing_diagnostics = request.routing_diagnostics().clone();
    let previous_response_id_present = request.previous_response_id.is_some();
    let context_management_present = request.payload.contains_key("context_management");
    debug!(
        request_class = request_class_label(classification),
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
    let previous_response_id = request.previous_response_id;
    let base_request = request.payload;
    let prepared = match classification {
        DownstreamRequestClassification::Normal => {
            match acquire_lease(&state.registry, previous_response_id.as_deref()).await {
                Ok(mut lease) => {
                    let auth = state.services.auth_provider().load()?;
                    let mut upstream_request = base_request.clone();
                    inject_internal_tools(&mut upstream_request);
                    let mut upstream = ensure_upstream(&state.services, &mut lease, auth).await?;

                    if let Some(previous_response_id) = &previous_response_id {
                        upstream_request.insert(
                            "previous_response_id".to_string(),
                            Value::String(previous_response_id.clone()),
                        );
                    }

                    let mut reconnect_attempted = false;
                    if let Err(error) = send_response_create(&upstream, &upstream_request).await {
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

                    PreparedResponseRoute {
                        upstream,
                        lease: ResponseStreamLease::Retained(lease),
                        previous_response_id,
                        reconnect_attempted,
                        upstream_request,
                        execute_internal_tools: true,
                        apply_no_observable_output_failure: true,
                    }
                }
                Err(ThreadlineError::RetainedSessionConflict) => {
                    let fallback_rerouted =
                        looks_like_auxiliary_summary_conflict_fallback(&base_request);
                    if !fallback_rerouted {
                        return Err(ThreadlineError::RetainedSessionConflict);
                    }

                    debug!(
                        request_class = request_class_label(classification),
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
                        auto_only_task_hit = routing_diagnostics.summary_hits.auto_only_task_hit,
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
                        fallback_summary_input_hit = fallback_rerouted,
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
                        "retained_session_conflict_rerouted"
                    );

                    start_transient_auxiliary_route(&state.services, base_request).await?
                }
                Err(error) => return Err(error),
            }
        }
        DownstreamRequestClassification::AuxiliarySummary => {
            start_transient_auxiliary_route(&state.services, base_request).await?
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

fn request_class_label(classification: DownstreamRequestClassification) -> &'static str {
    match classification {
        DownstreamRequestClassification::Normal => "normal",
        DownstreamRequestClassification::AuxiliarySummary => "auxiliary_summary",
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

async fn start_transient_auxiliary_route(
    services: &ThreadlineServices,
    mut upstream_request: serde_json::Map<String, Value>,
) -> Result<PreparedResponseRoute, ThreadlineError> {
    strip_threadline_tools(&mut upstream_request);

    let auth = services.auth_provider().load()?;
    let connected = services.connector().connect(auth, None).await?;
    send_response_create(&connected.websocket, &upstream_request).await?;

    Ok(PreparedResponseRoute {
        upstream: connected.websocket,
        lease: ResponseStreamLease::TransientAuxiliary,
        previous_response_id: None,
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
