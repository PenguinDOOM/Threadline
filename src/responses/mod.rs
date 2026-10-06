use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::response::IntoResponse;
use serde_json::Value;
use tracing::debug;
use uuid::Uuid;

use crate::auth::LoadedUpstreamAuth;
use crate::codex_ws::apply_session_metadata;
use crate::errors::ThreadlineError;
use crate::models::{ModelAlias, RouteProfile, resolve_request_model_for_profile};
use crate::registry::{RegistryAcquireError, RetainedSessionLease, RetainedSessionRegistry};
use crate::tools::{inject_internal_tools, is_internal_tool_name};
use crate::ws_pump::LiveUpstreamWebSocket;

mod continuation;
mod downstream;
mod preparation;

use continuation::*;
use preparation::*;
mod translation;
mod upstream;
mod virtual_tools;

use self::downstream::{
    DownstreamRequestClassification, looks_like_auxiliary_summary_conflict_fallback,
    parse_downstream_request_with_metadata,
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
    ConnectedUpstream, InternalToolExecutor, ThreadlineServices, UpstreamAuthProvider,
    UpstreamConnector,
};

pub const TURN_STATE_HEADER: &str = "x-codex-turn-state";

#[derive(Clone)]
pub struct ResponsesRouteState {
    pub profile: RouteProfile,
    pub persistent_reasoning_enabled: bool,
    pub registry: Arc<RetainedSessionRegistry>,
    pub services: ThreadlineServices,
}

pub(crate) async fn responses_handler(
    State(state): State<ResponsesRouteState>,
    axum::Json(payload): axum::Json<Value>,
    request_metadata: DownstreamRequestMetadata,
) -> Result<impl IntoResponse, ThreadlineError> {
    let mut request = parse_downstream_request_with_metadata(payload, request_metadata)?;
    let model_alias = resolve_request_model_for_profile(&request.payload, state.profile)?;
    let persistent_reasoning_applied = normalize_persistent_reasoning_context(
        &mut request.payload,
        state.persistent_reasoning_enabled,
        model_alias,
    );
    request.payload.insert(
        "model".to_string(),
        Value::String(model_alias.upstream_model_id.to_string()),
    );
    let classification = request.classification;
    let routing_diagnostics = request.routing_diagnostics().clone();
    let previous_response_id_present = request.previous_response_id.is_some();
    let context_management_present = request.payload.contains_key("context_management");
    trace_request_routed(
        classification,
        &routing_diagnostics,
        previous_response_id_present,
        context_management_present,
        model_alias,
        persistent_reasoning_applied,
    );
    let prepared = prepare_response_route(
        &state,
        request.payload,
        classification,
        &routing_diagnostics,
        request.previous_response_id,
        model_alias,
    )
    .await?;

    let mut stream_state = prepared.into_stream_state(state.services.clone());
    translation::recovery::preflight(&mut stream_state).await?;
    let stream = response_stream(stream_state);

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

fn trace_request_routed(
    classification: DownstreamRequestClassification,
    routing_diagnostics: &downstream::DownstreamRequestRoutingDiagnostics,
    previous_response_id_present: bool,
    context_management_present: bool,
    model_alias: &ModelAlias,
    persistent_reasoning_applied: bool,
) {
    let summary_hits = &routing_diagnostics.summary_hits;
    debug!(
        request_class = request_class_label(classification),
        interaction_type = routing_diagnostics.interaction_type.label(),
        interaction_type_compaction_hit = routing_diagnostics.interaction_type_compaction_hit,
        previous_response_id_present,
        context_management_present,
        model_alias = model_alias.alias_id,
        persistent_reasoning_applied,
        manual_summary_prompt_hit = summary_hits.manual_summary_prompt_hit,
        manual_structure_instruction_hit = summary_hits.manual_structure_instruction_hit,
        manual_tool_results_instruction_hit = summary_hits.manual_tool_results_instruction_hit,
        auto_context_too_large_hit = summary_hits.auto_context_too_large_hit,
        auto_summary_tags_hit = summary_hits.auto_summary_tags_hit,
        auto_only_task_hit = summary_hits.auto_only_task_hit,
        simple_history_context_hit = summary_hits.simple_history_context_hit,
        new_auto_detailed_summary_hit = summary_hits.new_auto_detailed_summary_hit,
        new_auto_user_history_hit = summary_hits.new_auto_user_history_hit,
        new_auto_user_final_summary_prompt_hit =
            summary_hits.new_auto_user_final_summary_prompt_hit,
        summary_instruction_like_hit = summary_hits.summary_instruction_like_hit,
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
}

fn normalize_persistent_reasoning_context(
    payload: &mut serde_json::Map<String, Value>,
    persistent_reasoning_enabled: bool,
    model_alias: &ModelAlias,
) -> bool {
    if !persistent_reasoning_enabled || model_alias.profile != RouteProfile::Main {
        return false;
    }

    match payload.get_mut("reasoning") {
        None | Some(Value::Null) => {
            payload.insert(
                "reasoning".to_string(),
                serde_json::json!({ "context": "all_turns" }),
            );
            true
        }
        Some(Value::Object(reasoning)) => {
            if reasoning.contains_key("context") {
                false
            } else {
                reasoning.insert(
                    "context".to_string(),
                    Value::String("all_turns".to_string()),
                );
                true
            }
        }
        Some(_) => false,
    }
}

fn thread_id_from_prompt_cache_key(payload: &serde_json::Map<String, Value>) -> Option<String> {
    let prompt_cache_key = payload.get("prompt_cache_key")?.as_str()?;
    let (conversation_id, _) = prompt_cache_key.rsplit_once(':')?;
    Uuid::parse_str(conversation_id.trim())
        .ok()
        .map(|id| id.to_string())
}

fn request_class_label(classification: DownstreamRequestClassification) -> &'static str {
    match classification {
        DownstreamRequestClassification::Normal => "normal",
        DownstreamRequestClassification::AuxiliarySummary => "auxiliary_summary",
    }
}

#[cfg(test)]
mod tests;
