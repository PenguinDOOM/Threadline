use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::codex_ws::UpstreamSessionDescriptor;
use crate::ws_pump::{LiveUpstreamWebSocket, UpstreamTerminalState};
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryAcquireError {
    PreviousResponseNotFound,
    RetainedSessionConflict,
    RetainedSessionCapacityExceeded,
    UpstreamInboundBufferOverflow,
}

pub struct RetainedSessionRegistry {
    inner: Arc<Mutex<RegistryState>>,
}

pub struct RetainedSessionLease {
    entry_id: u64,
    registry: Arc<Mutex<RegistryState>>,
    session: UpstreamSessionDescriptor,
    upstream: Option<Arc<LiveUpstreamWebSocket>>,
    armed: bool,
    removed: bool,
    released: bool,
}

impl std::fmt::Debug for RetainedSessionLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedSessionLease")
            .field("entry_id", &self.entry_id)
            .field("session", &self.session)
            .field("has_live_upstream", &self.upstream.is_some())
            .field("armed", &self.armed)
            .field("removed", &self.removed)
            .field("released", &self.released)
            .finish()
    }
}

struct RegistryState {
    capacity: usize,
    next_entry_id: u64,
    entries: HashMap<u64, RegistryEntry>,
    markers: HashMap<String, u64>,
}

struct RegistryEntry {
    session: UpstreamSessionDescriptor,
    window_generation: u64,
    upstream: Option<Arc<LiveUpstreamWebSocket>>,
    in_use: bool,
    recoverable: bool,
    liveness_timed_out: bool,
    last_used: Instant,
    markers: Vec<String>,
}

impl RetainedSessionRegistry {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryState {
                capacity,
                next_entry_id: 1,
                entries: HashMap::new(),
                markers: HashMap::new(),
            })),
        }
    }

    pub async fn acquire_new(&self) -> Result<RetainedSessionLease, RegistryAcquireError> {
        self.acquire_new_with_thread_id(None).await
    }

    pub async fn acquire_new_with_thread_id(
        &self,
        thread_id: Option<String>,
    ) -> Result<RetainedSessionLease, RegistryAcquireError> {
        let mut state = self.inner.lock().expect("registry mutex poisoned");
        if state.capacity == 0 {
            return Err(RegistryAcquireError::RetainedSessionCapacityExceeded);
        }

        if state.entries.len() >= state.capacity {
            if let Some(entry_id) = state
                .entries
                .iter()
                .filter(|(_, entry)| !entry.in_use)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(entry_id, _)| *entry_id)
            {
                remove_entry(&mut state, entry_id);
            } else {
                return Err(RegistryAcquireError::RetainedSessionCapacityExceeded);
            }
        }

        let entry_id = state.next_entry_id;
        state.next_entry_id += 1;
        let session = UpstreamSessionDescriptor::new(thread_id);
        state.entries.insert(
            entry_id,
            RegistryEntry {
                session: session.clone(),
                window_generation: 0,
                upstream: None,
                in_use: true,
                recoverable: true,
                liveness_timed_out: false,
                last_used: Instant::now(),
                markers: Vec::new(),
            },
        );

        debug!(
            session_id = %session.session_id,
            thread_id = %session.thread_id,
            window_id = %session.window_id,
            "retained_session_acquired"
        );

        Ok(RetainedSessionLease {
            entry_id,
            registry: Arc::clone(&self.inner),
            session,
            upstream: None,
            armed: false,
            removed: false,
            released: false,
        })
    }

    pub async fn acquire_previous(
        &self,
        response_marker: &str,
    ) -> Result<RetainedSessionLease, RegistryAcquireError> {
        let mut state = self.inner.lock().expect("registry mutex poisoned");
        let Some(entry_id) = state.markers.get(response_marker).copied() else {
            return Err(RegistryAcquireError::PreviousResponseNotFound);
        };
        let Some(entry) = state.entries.get_mut(&entry_id) else {
            state.markers.remove(response_marker);
            return Err(RegistryAcquireError::PreviousResponseNotFound);
        };

        if entry.in_use {
            return Err(RegistryAcquireError::RetainedSessionConflict);
        }

        if !entry.recoverable {
            return Err(RegistryAcquireError::UpstreamInboundBufferOverflow);
        }

        if entry.liveness_timed_out {
            return Err(RegistryAcquireError::PreviousResponseNotFound);
        }

        if let Some(upstream) = entry.upstream.as_ref() {
            match upstream.terminal_state() {
                UpstreamTerminalState::InboundBufferOverflow(_) => {
                    entry.upstream = None;
                    entry.recoverable = false;
                    return Err(RegistryAcquireError::UpstreamInboundBufferOverflow);
                }
                UpstreamTerminalState::Closed(_) => {
                    entry.upstream = None;
                    entry.recoverable = true;
                }
                UpstreamTerminalState::LivenessTimeout(_) => {
                    entry.upstream = None;
                    entry.recoverable = true;
                    entry.liveness_timed_out = true;
                    return Err(RegistryAcquireError::PreviousResponseNotFound);
                }
                UpstreamTerminalState::Open => {}
            }
        }

        entry.in_use = true;
        entry.last_used = Instant::now();

        debug!(
            response_marker,
            session_id = %entry.session.session_id,
            thread_id = %entry.session.thread_id,
            window_id = %entry.session.window_id,
            "retained_session_acquired"
        );

        Ok(RetainedSessionLease {
            entry_id,
            registry: Arc::clone(&self.inner),
            session: entry.session.clone(),
            upstream: entry.upstream.clone(),
            armed: false,
            removed: false,
            released: false,
        })
    }
}

impl RetainedSessionLease {
    pub fn session(&self) -> &UpstreamSessionDescriptor {
        &self.session
    }

    pub fn has_live_upstream(&self) -> bool {
        self.upstream.is_some()
    }

    pub fn has_open_upstream(&self) -> bool {
        self.upstream
            .as_ref()
            .is_some_and(|upstream| !upstream.is_closed())
    }

    pub fn upstream(&self) -> Option<Arc<LiveUpstreamWebSocket>> {
        self.upstream.clone()
    }

    pub fn arm_active_turn(&mut self) {
        if !self.removed && !self.released {
            self.armed = true;
        }
    }

    pub fn disarm_active_turn(&mut self) {
        if !self.removed {
            self.armed = false;
        }
    }

    pub fn record_completed_marker_and_disarm(&mut self, response_marker: impl Into<String>) {
        let response_marker = response_marker.into();
        let mut state = match self.registry.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let Some(entry) = state.entries.get_mut(&self.entry_id) else {
            return;
        };

        if !entry
            .markers
            .iter()
            .any(|marker| marker == &response_marker)
        {
            entry.markers.push(response_marker.clone());
        }
        entry.last_used = Instant::now();
        state.markers.insert(response_marker, self.entry_id);
        self.armed = false;
    }

    pub fn finalize_recoverable_turn(&mut self) {
        self.detach_upstream_recoverably();
        self.disarm_active_turn();
        self.remove_markerless_entry();
    }

    pub fn finalize_liveness_timeout_turn(&mut self) {
        self.detach_upstream_recoverably();
        self.disarm_active_turn();
        if let Ok(mut state) = self.registry.lock()
            && let Some(entry) = state.entries.get_mut(&self.entry_id)
        {
            entry.liveness_timed_out = true;
        }
        self.remove_markerless_entry();
    }

    pub fn detach_upstream_recoverably(&mut self) {
        self.upstream = None;
        if let Ok(mut state) = self.registry.lock()
            && let Some(entry) = state.entries.get_mut(&self.entry_id)
        {
            entry.upstream = None;
            entry.recoverable = true;
            self.session = entry.session.clone();
            entry.last_used = Instant::now();
        }
    }

    pub fn release(&mut self) {
        if self.removed || self.released {
            return;
        }

        if self.armed {
            self.remove_entry();
            return;
        }

        self.released = true;
        self.upstream = None;

        if let Ok(mut state) = self.registry.lock()
            && let Some(entry) = state.entries.get_mut(&self.entry_id)
        {
            entry.in_use = false;
            entry.last_used = Instant::now();
            debug!(
                session_id = %entry.session.session_id,
                thread_id = %entry.session.thread_id,
                window_id = %entry.session.window_id,
                "retained_session_released"
            );
        }
    }

    pub async fn record_completed_marker(&mut self, response_marker: impl Into<String>) {
        self.record_completed_marker_and_disarm(response_marker);
    }

    pub async fn update_turn_state(&mut self, turn_state: Option<String>) {
        self.session.turn_state = turn_state.clone();
        let mut state = self.registry.lock().expect("registry mutex poisoned");
        if let Some(entry) = state.entries.get_mut(&self.entry_id) {
            entry.session.turn_state = turn_state;
            entry.last_used = Instant::now();
        }
    }

    pub async fn replace_upstream(&mut self, upstream: Option<Arc<LiveUpstreamWebSocket>>) {
        self.upstream = upstream.clone();
        let mut state = self.registry.lock().expect("registry mutex poisoned");
        if let Some(entry) = state.entries.get_mut(&self.entry_id) {
            entry.upstream = upstream;
            entry.recoverable = true;
            entry.liveness_timed_out = false;
            entry.last_used = Instant::now();
        }
    }

    pub async fn mark_upstream_recoverable(&mut self) {
        self.finalize_recoverable_turn();
    }

    pub async fn advance_window_generation(&mut self) {
        let mut state = self.registry.lock().expect("registry mutex poisoned");
        if let Some(entry) = state.entries.get_mut(&self.entry_id) {
            entry.window_generation += 1;
            entry.session.set_window_generation(entry.window_generation);
            self.session = entry.session.clone();
            entry.last_used = Instant::now();
        }
    }

    pub async fn mark_upstream_terminal(&mut self) {
        self.remove_entry();
    }

    fn remove_entry(&mut self) {
        if self.removed {
            return;
        }

        if let Ok(mut state) = self.registry.lock() {
            remove_entry(&mut state, self.entry_id);
        }
        self.upstream = None;
        self.armed = false;
        self.removed = true;
        self.released = true;
    }

    fn remove_markerless_entry(&mut self) {
        let is_markerless = self
            .registry
            .lock()
            .ok()
            .and_then(|state| {
                state
                    .entries
                    .get(&self.entry_id)
                    .map(|entry| entry.markers.is_empty())
            })
            .unwrap_or(false);
        if is_markerless {
            self.remove_entry();
        }
    }
}

impl Drop for RetainedSessionLease {
    fn drop(&mut self) {
        self.release();
    }
}

fn remove_entry(state: &mut RegistryState, entry_id: u64) {
    let Some(entry) = state.entries.remove(&entry_id) else {
        return;
    };
    for marker in entry.markers {
        if state.markers.get(&marker).copied() == Some(entry_id) {
            state.markers.remove(&marker);
        }
    }
}

#[cfg(test)]
mod tests {
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
    async fn idle_liveness_timeout_preserves_completed_aliases_and_metadata_after_repeated_acquire()
    {
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
        let entry = state.entries.get(&entry_id).expect("entry should remain");
        assert_eq!(state.markers.get("response-accepted"), Some(&entry_id));
        assert_eq!(state.markers.get("response-alias"), Some(&entry_id));
        assert_eq!(entry.markers, vec!["response-accepted", "response-alias"]);
        assert_eq!(entry.session, session);
        assert_eq!(entry.window_generation, 7);
        assert!(entry.upstream.is_none());
        assert!(entry.recoverable);
        assert!(entry.liveness_timed_out);
        assert!(!entry.in_use);
    }

    #[tokio::test]
    async fn finalize_liveness_timeout_preserves_completed_metadata_and_reclaims_markerless_entry()
    {
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
            let entry = state.entries.get(&entry_id).expect("entry should remain");
            assert_eq!(state.markers.get("response-accepted"), Some(&entry_id));
            assert_eq!(state.markers.get("response-alias"), Some(&entry_id));
            assert!(!state.markers.contains_key("response-unaccepted-final"));
            assert_eq!(entry.markers, vec!["response-accepted", "response-alias"]);
            assert_eq!(entry.session, session);
            assert_eq!(entry.window_generation, 7);
            assert!(entry.upstream.is_none());
            assert!(entry.recoverable);
            assert!(entry.liveness_timed_out);
            assert!(entry.in_use);
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
}
