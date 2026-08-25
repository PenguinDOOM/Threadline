use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::codex_ws::UpstreamSessionDescriptor;
use crate::ws_pump::LiveUpstreamWebSocket;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryAcquireError {
    PreviousResponseNotFound,
    RetainedSessionConflict,
    RetainedSessionCapacityExceeded,
}

pub struct RetainedSessionRegistry {
    inner: Arc<Mutex<RegistryState>>,
}

pub struct RetainedSessionLease {
    entry_id: u64,
    registry: Arc<Mutex<RegistryState>>,
    session: UpstreamSessionDescriptor,
    upstream: Option<Arc<LiveUpstreamWebSocket>>,
    removed: bool,
    released: bool,
}

impl std::fmt::Debug for RetainedSessionLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedSessionLease")
            .field("entry_id", &self.entry_id)
            .field("session", &self.session)
            .field("has_live_upstream", &self.upstream.is_some())
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

        if entry
            .upstream
            .as_ref()
            .is_some_and(|upstream| upstream.is_closed())
        {
            entry.upstream = None;
            entry.recoverable = true;
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

    pub fn release(&mut self) {
        if self.removed || self.released {
            return;
        }

        self.released = true;

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
        let response_marker = response_marker.into();
        let mut state = self.registry.lock().expect("registry mutex poisoned");
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
            entry.last_used = Instant::now();
        }
    }

    pub async fn mark_upstream_recoverable(&mut self) {
        self.upstream = None;
        let mut state = self.registry.lock().expect("registry mutex poisoned");
        if let Some(entry) = state.entries.get_mut(&self.entry_id) {
            entry.upstream = None;
            entry.recoverable = true;
            self.session = entry.session.clone();
            entry.last_used = Instant::now();
        }
    }

    pub async fn advance_window_generation(&mut self) {
        let mut state = self.registry.lock().expect("registry mutex poisoned");
        if let Some(entry) = state.entries.get_mut(&self.entry_id) {
            entry.window_generation += 1;
            entry
                .session
                .set_window_generation(entry.window_generation);
            self.session = entry.session.clone();
            entry.last_used = Instant::now();
        }
    }

    pub async fn mark_upstream_terminal(&mut self) {
        let mut state = self.registry.lock().expect("registry mutex poisoned");
        remove_entry(&mut state, self.entry_id);
        self.upstream = None;
        self.removed = true;
        self.released = true;
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
}