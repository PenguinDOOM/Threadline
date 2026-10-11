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
    UpstreamWebSocketPolicyViolation,
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
    prohibition: Option<RegistryProhibition>,
    liveness_timed_out: bool,
    last_used: Instant,
    markers: Vec<String>,
}

#[derive(Clone, Copy)]
enum RegistryProhibition {
    InboundOverflow,
    PolicyViolation,
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
        state.ensure_new_capacity()?;

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
                prohibition: None,
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

        entry.prepare_acquire()?;

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

impl RegistryState {
    fn ensure_new_capacity(&mut self) -> Result<(), RegistryAcquireError> {
        if self.capacity == 0 {
            return Err(RegistryAcquireError::RetainedSessionCapacityExceeded);
        }
        if self.entries.len() >= self.capacity {
            let entry_id = self
                .entries
                .iter()
                .filter(|(_, entry)| !entry.in_use)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(entry_id, _)| *entry_id)
                .ok_or(RegistryAcquireError::RetainedSessionCapacityExceeded)?;
            remove_entry(self, entry_id);
        }
        Ok(())
    }
}

impl RegistryEntry {
    fn prepare_acquire(&mut self) -> Result<(), RegistryAcquireError> {
        if self.in_use {
            return Err(RegistryAcquireError::RetainedSessionConflict);
        }
        if let Some(prohibition) = self.prohibition {
            return Err(match prohibition {
                RegistryProhibition::InboundOverflow => {
                    RegistryAcquireError::UpstreamInboundBufferOverflow
                }
                RegistryProhibition::PolicyViolation => {
                    RegistryAcquireError::UpstreamWebSocketPolicyViolation
                }
            });
        }
        if self.liveness_timed_out {
            return Err(RegistryAcquireError::PreviousResponseNotFound);
        }
        if let Some(upstream) = self.upstream.as_ref() {
            let terminal = upstream.terminal_state();
            if terminal.is_policy_violation() {
                self.prohibition = Some(RegistryProhibition::PolicyViolation);
                self.upstream = None;
                return Err(RegistryAcquireError::UpstreamWebSocketPolicyViolation);
            }
            match terminal {
                UpstreamTerminalState::InboundBufferOverflow(_) => {
                    self.upstream = None;
                    self.prohibition = Some(RegistryProhibition::InboundOverflow);
                    return Err(RegistryAcquireError::UpstreamInboundBufferOverflow);
                }
                UpstreamTerminalState::Closed(_)
                | UpstreamTerminalState::TransportClosed { .. } => {
                    self.upstream = None;
                }
                UpstreamTerminalState::LivenessTimeout(_) => {
                    self.upstream = None;
                    self.liveness_timed_out = true;
                    return Err(RegistryAcquireError::PreviousResponseNotFound);
                }
                UpstreamTerminalState::Open => {}
            }
        }
        self.in_use = true;
        self.last_used = Instant::now();
        Ok(())
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

    pub fn finalize_policy_violation_turn(&mut self) {
        if self.removed || self.released {
            return;
        }
        if let Ok(mut state) = self.registry.lock()
            && let Some(entry) = state.entries.get_mut(&self.entry_id)
        {
            entry.prohibition = Some(RegistryProhibition::PolicyViolation);
            entry.upstream = None;
            entry.last_used = Instant::now();
            let markerless = entry.markers.is_empty();
            self.upstream = None;
            self.armed = false;
            if markerless {
                remove_entry(&mut state, self.entry_id);
                self.removed = true;
                self.released = true;
            }
        }
    }

    pub fn detach_upstream_recoverably(&mut self) {
        self.upstream = None;
        if let Ok(mut state) = self.registry.lock()
            && let Some(entry) = state.entries.get_mut(&self.entry_id)
        {
            entry.upstream = None;
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
            entry.prohibition = None;
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
mod tests;
