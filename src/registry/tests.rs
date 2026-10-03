use super::*;
use std::time::Duration;
use tokio::io::duplex;
use tokio::time::advance;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;

use crate::ws_pump::{UpstreamLivenessTimeout, UpstreamWatchdogPolicy};

fn explicit_session() -> UpstreamSessionDescriptor {
    UpstreamSessionDescriptor {
        session_id: "session-known".to_string(),
        thread_id: "thread-known".to_string(),
        window_id: "thread-known:7".to_string(),
        turn_state: Some("turn-state-known".to_string()),
    }
}

fn set_entry_metadata(
    registry: &RetainedSessionRegistry,
    entry_id: u64,
    session: UpstreamSessionDescriptor,
) {
    let mut state = registry.inner.lock().expect("registry mutex poisoned");
    let entry = state
        .entries
        .get_mut(&entry_id)
        .expect("entry should exist");
    entry.session = session;
    entry.window_generation = 7;
}

async fn liveness_timed_out_upstream() -> Arc<LiveUpstreamWebSocket> {
    let (client_io, _server_io) = duplex(1024);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let policy = UpstreamWatchdogPolicy::new(Duration::from_millis(1), Duration::from_secs(1))
        .expect("valid watchdog policy");
    let upstream = Arc::new(
        LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::ZERO,
            policy,
        ),
    );

    tokio::task::yield_now().await;
    advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        upstream.terminal_state(),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout { .. })
    ));

    upstream
}

#[tokio::test]
async fn recording_a_completed_marker_refreshes_last_used() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");

    let initial_last_used = {
        let state = registry.inner.lock().expect("registry mutex poisoned");
        state
            .entries
            .get(&lease.entry_id)
            .expect("entry should exist")
            .last_used
    };

    std::thread::sleep(Duration::from_millis(5));
    lease.record_completed_marker("response-1").await;

    let refreshed_last_used = {
        let state = registry.inner.lock().expect("registry mutex poisoned");
        state
            .entries
            .get(&lease.entry_id)
            .expect("entry should exist")
            .last_used
    };

    assert!(refreshed_last_used > initial_last_used);
}

#[tokio::test]
async fn new_session_can_use_downstream_thread_identity() {
    let registry = RetainedSessionRegistry::new(1);
    let thread_id = "48a65359-981b-47c2-9612-e1c64ae07e22".to_string();

    let lease = registry
        .acquire_new_with_thread_id(Some(thread_id.clone()))
        .await
        .expect("create session");

    assert_eq!(lease.session().thread_id, thread_id);
    assert_eq!(
        lease.session().window_id,
        format!("{}:0", lease.session().thread_id)
    );
}

#[tokio::test]
async fn recoverable_close_preserves_window_generation() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    let original_window_id = lease.session().window_id.clone();

    lease.mark_upstream_recoverable().await;

    assert_eq!(lease.session().window_id, original_window_id);
}

#[tokio::test]
async fn explicit_window_generation_advance_updates_window_id() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    let thread_id = lease.session().thread_id.clone();

    lease.advance_window_generation().await;

    assert_eq!(lease.session().window_id, format!("{thread_id}:1"));
}

#[tokio::test]
async fn detached_nonrecoverable_entry_repeats_inbound_overflow_for_its_marker() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    lease.record_completed_marker("response-overflow").await;
    lease.release();

    {
        let mut state = registry.inner.lock().expect("registry mutex poisoned");
        let entry_id = *state
            .markers
            .get("response-overflow")
            .expect("marker should exist");
        let entry = state
            .entries
            .get_mut(&entry_id)
            .expect("entry should exist");
        entry.upstream = None;
        entry.recoverable = false;
    }

    for _ in 0..2 {
        assert!(matches!(
            registry.acquire_previous("response-overflow").await,
            Err(RegistryAcquireError::UpstreamInboundBufferOverflow)
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn idle_liveness_timeout_preserves_completed_aliases_and_metadata_after_repeated_acquire() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    let session = explicit_session();
    set_entry_metadata(&registry, lease.entry_id, session.clone());
    lease.record_completed_marker("response-accepted").await;
    lease.record_completed_marker("response-alias").await;
    lease
        .replace_upstream(Some(liveness_timed_out_upstream().await))
        .await;
    lease.release();

    let entry_id = lease.entry_id;
    for _ in 0..2 {
        assert!(matches!(
            registry.acquire_previous("response-accepted").await,
            Err(RegistryAcquireError::PreviousResponseNotFound)
        ));
    }

    let state = registry.inner.lock().expect("registry mutex poisoned");
    assert_timed_out_metadata(&state, entry_id, &session, false);
}

fn assert_timed_out_metadata(
    state: &RegistryState,
    entry_id: u64,
    session: &UpstreamSessionDescriptor,
    in_use: bool,
) {
    let entry = state.entries.get(&entry_id).expect("entry should remain");
    assert_eq!(state.markers.get("response-accepted"), Some(&entry_id));
    assert_eq!(state.markers.get("response-alias"), Some(&entry_id));
    assert_eq!(entry.markers, vec!["response-accepted", "response-alias"]);
    assert_eq!(&entry.session, session);
    assert_eq!(entry.window_generation, 7);
    assert!(entry.upstream.is_none());
    assert!(entry.recoverable);
    assert!(entry.liveness_timed_out);
    assert_eq!(entry.in_use, in_use);
}

#[tokio::test]
async fn finalize_liveness_timeout_preserves_completed_metadata_and_reclaims_markerless_entry() {
    let registry = RetainedSessionRegistry::new(1);
    let mut lease = registry.acquire_new().await.expect("create session");
    let session = explicit_session();
    set_entry_metadata(&registry, lease.entry_id, session.clone());
    lease.record_completed_marker("response-accepted").await;
    lease.record_completed_marker("response-alias").await;
    lease.arm_active_turn();
    lease.finalize_liveness_timeout_turn();

    let entry_id = lease.entry_id;
    {
        let state = registry.inner.lock().expect("registry mutex poisoned");
        assert_timed_out_metadata(&state, entry_id, &session, true);
        assert!(!state.markers.contains_key("response-unaccepted-final"));
    }

    lease.release();
    {
        let state = registry.inner.lock().expect("registry mutex poisoned");
        assert!(
            !state
                .entries
                .get(&entry_id)
                .expect("entry should remain")
                .in_use
        );
    }
    assert!(matches!(
        registry.acquire_previous("response-unaccepted-final").await,
        Err(RegistryAcquireError::PreviousResponseNotFound)
    ));

    let markerless_registry = RetainedSessionRegistry::new(1);
    let mut markerless_lease = markerless_registry
        .acquire_new()
        .await
        .expect("create markerless session");
    markerless_lease.arm_active_turn();
    markerless_lease.finalize_liveness_timeout_turn();

    {
        let state = markerless_registry
            .inner
            .lock()
            .expect("registry mutex poisoned");
        assert!(state.entries.is_empty());
        assert!(state.markers.is_empty());
    }
    markerless_registry
        .acquire_new()
        .await
        .expect("markerless timeout should reclaim capacity");
}
