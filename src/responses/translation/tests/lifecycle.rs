use super::*;

struct DispatchCounter(Arc<std::sync::atomic::AtomicUsize>);

impl crate::responses::InternalToolExecutor for DispatchCounter {
    fn execute(
        &self,
        call: crate::tools::InternalToolCall,
    ) -> BoxFuture<'static, Result<crate::tools::PendingInternalToolOutput, ThreadlineError>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move { call.execute() })
    }
}

#[tokio::test]
async fn internal_tool_ledger_capacity_rejects_before_dispatch_without_eviction() {
    use crate::responses::translation::internal_tools::*;
    for byte_limit in [false, true] {
        let (upstream, _pending_send) =
            LiveUpstreamWebSocket::test_followup_send_pending_for_liveness_timeout();
        let upstream = Arc::new(upstream);
        let registry = Arc::new(RetainedSessionRegistry::new(1));
        let lease = armed_followup_lease(&registry, Arc::clone(&upstream)).await;
        let mut state = followup_stream_state(upstream, lease);
        let dispatches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        state.services = ThreadlineServices::with_internal_tool_executor(
            Arc::new(UnusedAuthProvider),
            Arc::new(UnusedConnector),
            Arc::new(DispatchCounter(Arc::clone(&dispatches))),
        );
        let mut ledger = InternalToolLedger::default();
        let first = if byte_limit {
            "a".repeat(1024 * 1024)
        } else {
            "reserved-0".to_string()
        };
        ledger.reserve(&first).expect("exact first reservation");
        if !byte_limit {
            for index in 1..4096 {
                ledger
                    .reserve(&format!("reserved-{index}"))
                    .expect("within count limit");
            }
        }
        let event = json!({"type":"response.output_item.done","item":{"type":"function_call","name":"threadline_echo","call_id":"rejected-call","arguments":"{}"}});
        let call = crate::tools::InternalToolCall::from_event(&event)
            .unwrap()
            .unwrap();
        let metadata = UpstreamEventTraceMetadata::from_event(&event);
        let progress =
            execute_internal_tool_event(&mut state, &event, call, &metadata, &mut ledger).await;
        let crate::responses::translation::StreamProgress::Yield(chunk) = progress else {
            panic!("capacity must fail");
        };
        let mut body = String::from_utf8(chunk.to_vec()).unwrap();
        for chunk in response_stream(state).collect::<Vec<_>>().await {
            body.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
        }
        assert_eq!(dispatches.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(body.matches("event: response.failed").count(), 1);
        assert_eq!(body.matches("data: [DONE]").count(), 1);
        assert!(body.contains("internal_tool_failed"));
        assert!(ledger.reserve(&first).is_err());
    }
}

#[test]
fn final_completion_acceptance_snapshot_rejects_overflow_but_allows_closed() {
    let overflow = UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
        cause: InboundBufferOverflowCause::MessageCount,
        queued_messages: 1,
        queued_bytes: 1,
        incoming_bytes: 1,
        max_messages: 1,
        max_bytes: 1,
    });

    assert!(matches!(
        final_completion_acceptance_error(overflow),
        Some(ThreadlineError::UpstreamInboundBufferOverflow)
    ));
    assert!(
        final_completion_acceptance_error(UpstreamTerminalState::Closed(UpstreamCloseMetadata {
            code: None,
            reason: None,
            error: None,
        },))
        .is_none()
    );
}

#[tokio::test]
async fn pending_internal_tool_followup_send_liveness_timeout_invalidates_retained_aliases() {
    let (upstream, pending_send) =
        LiveUpstreamWebSocket::test_followup_send_pending_for_liveness_timeout();
    let upstream = Arc::new(upstream);
    pending_send
        .send_inbound_text(
            r#"{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"threadline_echo","arguments":"{\"value\":\"alpha\"}"}}"#,
        )
        .await;
    pending_send
        .send_inbound_text(
            r#"{"type":"response.completed","response":{"id":"response-intermediate"}}"#,
        )
        .await;

    let registry = Arc::new(RetainedSessionRegistry::new(1));
    let active_lease = armed_followup_lease(&registry, Arc::clone(&upstream)).await;
    let state = followup_stream_state(Arc::clone(&upstream), active_lease);
    let stream_task = tokio::spawn(async move { response_stream(state).collect::<Vec<_>>().await });

    pending_send.wait_for_send_pending().await;
    assert_eq!(pending_send.text_send_attempts(), 1);
    pending_send.trigger_liveness_timeout().await;

    let chunks = stream_task.await.expect("response stream task");
    let body = chunks
        .into_iter()
        .map(|chunk| String::from_utf8(chunk.expect("SSE chunk").to_vec()).expect("SSE utf8"))
        .collect::<String>();
    assert_eq!(body.matches("event: response.failed").count(), 1);
    assert_eq!(body.matches("data: [DONE]").count(), 1);
    assert!(body.contains("upstream_liveness_timeout"));
    assert_eq!(pending_send.text_send_attempts(), 1);

    for marker in [
        "response-accepted",
        "response-alias",
        "response-intermediate",
    ] {
        assert_eq!(
            registry
                .acquire_previous(marker)
                .await
                .expect_err("started follow-up timeout must invalidate every retained alias"),
            RegistryAcquireError::PreviousResponseNotFound,
            "marker {marker}"
        );
    }
    registry
        .acquire_new()
        .await
        .expect("invalidation should reclaim retained capacity");
}

pub(super) async fn armed_followup_lease(
    registry: &Arc<RetainedSessionRegistry>,
    upstream: Arc<LiveUpstreamWebSocket>,
) -> crate::registry::RetainedSessionLease {
    let mut seeded_lease = registry.acquire_new().await.expect("seed retained lease");
    seeded_lease
        .replace_upstream(Some(Arc::clone(&upstream)))
        .await;
    seeded_lease
        .record_completed_marker("response-accepted")
        .await;
    seeded_lease.record_completed_marker("response-alias").await;
    seeded_lease.release();

    let mut active_lease = registry
        .acquire_previous("response-accepted")
        .await
        .expect("acquire retained lease");
    active_lease.arm_active_turn();
    active_lease
}

pub(super) fn followup_stream_state(
    upstream: Arc<LiveUpstreamWebSocket>,
    active_lease: crate::registry::RetainedSessionLease,
) -> ResponseStreamState {
    ResponseStreamState {
        services: ThreadlineServices::new(Arc::new(UnusedAuthProvider), Arc::new(UnusedConnector)),
        upstream: Some(Arc::clone(&upstream)),
        lease: ResponseStreamLease::Retained(active_lease),
        base_request: serde_json::Map::new(),
        pending_internal_outputs: Vec::new(),
        followup_send_started: false,
        previous_response_id: Some("response-accepted".to_string()),
        execute_internal_tools: true,
        suppressed_internal_output_indexes: HashSet::new(),
        upstream_event_seen: false,
        headers_committed: true,
        replay_prohibited: true,
        recovery_local_tools_only: true,
        pending_upstream_events: VecDeque::new(),
        replay_stale_marker_on_pre_first_event_close: false,
        observable_output: DownstreamObservableOutputState::default(),
        downstream_visible_text_sources: HashSet::new(),
        downstream_visible_text_delta_count: 0,
        visible_assistant_text: Vec::new(),
        last_unidentified_visible_text: None,
        queued_synthetic_output_text_deltas: VecDeque::new(),
        queued_forwarded_event: None,
        queued_final_completed: None,
        final_done_pending: false,
        apply_no_observable_output_failure: true,
        done: false,
    }
}
