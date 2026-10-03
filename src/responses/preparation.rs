use super::*;

pub(super) struct PreparedResponseRoute {
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

impl PreparedResponseRoute {
    pub(super) fn into_stream_state(self, services: ThreadlineServices) -> ResponseStreamState {
        ResponseStreamState {
            services,
            upstream: Some(self.upstream),
            lease: self.lease,
            base_request: self.upstream_request,
            pending_internal_outputs: Vec::new(),
            followup_send_started: false,
            previous_response_id: self.previous_response_id,
            execute_internal_tools: self.execute_internal_tools,
            suppressed_internal_output_indexes: std::collections::HashSet::new(),
            upstream_event_seen: false,
            replay_stale_marker_on_pre_first_event_close: self
                .replay_stale_marker_on_pre_first_event_close,
            reconnect_attempted: self.reconnect_attempted,
            observable_output: Default::default(),
            downstream_visible_text_sources: std::collections::HashSet::new(),
            downstream_visible_text_delta_count: 0,
            visible_assistant_text: Vec::new(),
            last_unidentified_visible_text: None,
            queued_synthetic_output_text_deltas: std::collections::VecDeque::new(),
            queued_forwarded_event: None,
            queued_final_completed: None,
            final_done_pending: false,
            apply_no_observable_output_failure: self.apply_no_observable_output_failure,
            done: false,
        }
    }
}

pub(super) async fn prepare_response_route(
    state: &ResponsesRouteState,
    mut base_request: serde_json::Map<String, Value>,
    classification: DownstreamRequestClassification,
    routing_diagnostics: &downstream::DownstreamRequestRoutingDiagnostics,
    previous_response_id: Option<String>,
    model_alias: &ModelAlias,
) -> Result<PreparedResponseRoute, ThreadlineError> {
    if state.profile == RouteProfile::Utility {
        maybe_inject_virtual_tool_summarizer_instruction(
            model_alias.upstream_model_id,
            routing_diagnostics,
            &mut base_request,
        );
        start_transient_route(
            &state.services,
            base_request,
            classification,
            TransientRouteKind::Utility,
        )
        .await
    } else if classification == DownstreamRequestClassification::AuxiliarySummary {
        start_transient_route(
            &state.services,
            base_request,
            classification,
            TransientRouteKind::AuxiliarySummary,
        )
        .await
    } else {
        prepare_normal_route(
            state,
            base_request,
            routing_diagnostics,
            previous_response_id,
        )
        .await
    }
}

async fn prepare_normal_route(
    state: &ResponsesRouteState,
    base_request: serde_json::Map<String, Value>,
    routing_diagnostics: &downstream::DownstreamRequestRoutingDiagnostics,
    previous_response_id: Option<String>,
) -> Result<PreparedResponseRoute, ThreadlineError> {
    let downstream_thread_id = thread_id_from_prompt_cache_key(&base_request);
    let classification = DownstreamRequestClassification::Normal;
    match acquire_lease(
        &state.registry,
        previous_response_id.as_deref(),
        downstream_thread_id.as_deref(),
    )
    .await
    {
        Ok(lease) => {
            start_retained_route(
                state,
                lease,
                &base_request,
                classification,
                previous_response_id,
            )
            .await
        }
        Err(ThreadlineError::RetainedSessionConflict) => {
            let previous_response_id_present = previous_response_id.is_some();
            let context_management_present = base_request.contains_key("context_management");
            start_conflict_fallback(
                state,
                base_request,
                classification,
                routing_diagnostics,
                previous_response_id_present,
                context_management_present,
            )
            .await
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn start_retained_route(
    state: &ResponsesRouteState,
    mut lease: RetainedSessionLease,
    base_request: &serde_json::Map<String, Value>,
    classification: DownstreamRequestClassification,
    previous_response_id: Option<String>,
) -> Result<PreparedResponseRoute, ThreadlineError> {
    let is_continuation_request = previous_response_id.is_some();
    let mut upstream_request = base_request.clone();
    inject_internal_tools(&mut upstream_request);
    strip_context_management_for_upstream(&mut upstream_request, "normal", classification);
    apply_session_metadata(&mut upstream_request, lease.session());
    let mut reconnect_attempted = false;
    let upstream = if let Some(previous_response_id) = &previous_response_id {
        start_continuation_upstream(&mut lease, &mut upstream_request, previous_response_id).await?
    } else {
        start_new_upstream(
            &state.services,
            &mut lease,
            &upstream_request,
            &mut reconnect_attempted,
        )
        .await?
    };

    Ok(PreparedResponseRoute {
        upstream,
        lease: ResponseStreamLease::Retained(lease),
        previous_response_id,
        replay_stale_marker_on_pre_first_event_close: is_continuation_request,
        reconnect_attempted,
        upstream_request,
        execute_internal_tools: true,
        apply_no_observable_output_failure: true,
    })
}

pub(super) async fn start_conflict_fallback(
    state: &ResponsesRouteState,
    base_request: serde_json::Map<String, Value>,
    classification: DownstreamRequestClassification,
    routing_diagnostics: &downstream::DownstreamRequestRoutingDiagnostics,
    previous_response_id_present: bool,
    context_management_present: bool,
) -> Result<PreparedResponseRoute, ThreadlineError> {
    let reroute_reason =
        retained_session_conflict_reroute_reason(routing_diagnostics, &base_request);
    if reroute_reason.is_none() {
        return Err(ThreadlineError::RetainedSessionConflict);
    }
    let reroute_reason = reroute_reason.expect("reroute reason present");

    trace_conflict_rerouted(
        reroute_reason,
        classification,
        routing_diagnostics,
        previous_response_id_present,
        context_management_present,
    );

    start_transient_route(
        &state.services,
        base_request,
        classification,
        TransientRouteKind::AuxiliarySummary,
    )
    .await
}

pub(super) fn trace_conflict_rerouted(
    reroute_reason: &str,
    classification: DownstreamRequestClassification,
    routing_diagnostics: &downstream::DownstreamRequestRoutingDiagnostics,
    previous_response_id_present: bool,
    context_management_present: bool,
) {
    debug!(
        reroute_reason,
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
        fallback_summary_input_hit = reroute_reason == "fallback_summary_input",
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
    apply_session_metadata(&mut upstream_request, &connected.session);
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

pub(super) fn strip_threadline_tools(payload: &mut serde_json::Map<String, Value>) {
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

pub(super) fn strip_context_management_for_upstream(
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

pub(super) fn retained_session_conflict_reroute_reason(
    routing_diagnostics: &downstream::DownstreamRequestRoutingDiagnostics,
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

pub(super) fn maybe_inject_virtual_tool_summarizer_instruction(
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
