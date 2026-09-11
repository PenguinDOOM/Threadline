use threadline::registry::{RegistryAcquireError, RetainedSessionRegistry};

#[tokio::test]
async fn previous_response_marker_reuses_the_same_retained_session_after_release() {
    let registry = RetainedSessionRegistry::new(2);
    let mut first = registry.acquire_new().await.expect("create session");
    let original_session = first.session().clone();

    first
        .update_turn_state(Some("turn-state-1".to_string()))
        .await;
    first.record_completed_marker("response-1").await;

    drop(first);

    let followup = registry
        .acquire_previous("response-1")
        .await
        .expect("reuse previous response session");

    assert_eq!(followup.session().session_id, original_session.session_id);
    assert_eq!(followup.session().thread_id, original_session.thread_id);
    assert_eq!(followup.session().window_id, original_session.window_id);
    assert_eq!(
        followup.session().turn_state.as_deref(),
        Some("turn-state-1")
    );
}

#[tokio::test]
async fn released_lease_can_be_reacquired_before_drop() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    let original_session = lease.session().clone();

    lease.record_completed_marker("response-1").await;
    lease.release();

    let reacquired = registry
        .acquire_previous("response-1")
        .await
        .expect("released marker should be reacquired before drop");

    assert_eq!(reacquired.session().session_id, original_session.session_id);
    assert_eq!(reacquired.session().thread_id, original_session.thread_id);
    assert_eq!(reacquired.session().window_id, original_session.window_id);
}

#[tokio::test]
async fn active_lease_still_conflicts() {
    let registry = RetainedSessionRegistry::new(2);
    let mut first = registry.acquire_new().await.expect("create session");

    first.record_completed_marker("response-1").await;

    let error = registry
        .acquire_previous("response-1")
        .await
        .expect_err("leased marker should conflict");

    assert_eq!(error, RegistryAcquireError::RetainedSessionConflict);
}

#[tokio::test]
async fn armed_lease_drop_invalidates_all_markers_and_releases_capacity() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    lease.record_completed_marker("response-1").await;
    lease.record_completed_marker("response-2").await;
    lease.arm_active_turn();

    drop(lease);

    for marker in ["response-1", "response-2"] {
        let error = registry
            .acquire_previous(marker)
            .await
            .expect_err("abandoned marker must be removed");
        assert_eq!(error, RegistryAcquireError::PreviousResponseNotFound);
    }
    registry
        .acquire_new()
        .await
        .expect("abandoned entry must release capacity");
}

#[tokio::test]
async fn armed_lease_release_invalidates_all_markers_and_later_drop_is_inert() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    lease.record_completed_marker("response-1").await;
    lease.record_completed_marker("response-2").await;
    lease.arm_active_turn();
    lease.release();

    for marker in ["response-1", "response-2"] {
        assert_eq!(
            registry
                .acquire_previous(marker)
                .await
                .expect_err("abandoned marker must be removed"),
            RegistryAcquireError::PreviousResponseNotFound
        );
    }

    let replacement = registry.acquire_new().await.expect("capacity released");
    drop(lease);
    drop(replacement);
}

#[tokio::test]
async fn armed_lease_detach_then_drop_still_invalidates_markers() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    lease.record_completed_marker("response-1").await;
    lease.arm_active_turn();
    lease.detach_upstream_recoverably();

    drop(lease);

    assert_eq!(
        registry
            .acquire_previous("response-1")
            .await
            .expect_err("detached active turn must remain non-reusable"),
        RegistryAcquireError::PreviousResponseNotFound
    );
}

#[tokio::test]
async fn finalized_recoverable_turn_preserves_marker_without_a_live_upstream() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    lease.record_completed_marker("response-1").await;
    lease.arm_active_turn();
    lease.finalize_recoverable_turn();
    lease.release();

    let recovered = registry
        .acquire_previous("response-1")
        .await
        .expect("known terminal failure must preserve the prior marker");
    assert!(!recovered.has_live_upstream());
}

#[tokio::test]
async fn missing_previous_response_marker_returns_not_found() {
    let registry = RetainedSessionRegistry::new(2);

    let error = registry
        .acquire_previous("missing-response")
        .await
        .expect_err("unknown marker should fail");

    assert_eq!(error, RegistryAcquireError::PreviousResponseNotFound);
}

#[tokio::test]
async fn capacity_exhaustion_returns_a_stable_error_while_all_sessions_are_leased() {
    let registry = RetainedSessionRegistry::new(1);
    let _lease = registry.acquire_new().await.expect("first session");

    let error = registry
        .acquire_new()
        .await
        .expect_err("leased capacity should fail");

    assert_eq!(error, RegistryAcquireError::RetainedSessionCapacityExceeded);
}

#[tokio::test]
async fn recoverable_close_preserves_marker_continuity_without_a_live_socket() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    let original_session = lease.session().clone();

    lease.record_completed_marker("response-2").await;
    lease.mark_upstream_recoverable().await;
    drop(lease);

    let reacquired = registry
        .acquire_previous("response-2")
        .await
        .expect("recoverable marker should survive");

    assert_eq!(reacquired.session().session_id, original_session.session_id);
    assert!(!reacquired.has_live_upstream());
    assert!(reacquired.upstream().is_none());
}
