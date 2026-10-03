use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::{BufReader, Read};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
use std::io::Write;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::mpsc;

use serde_json::{Value, json};
use tracing::{debug, warn};
use uuid::Uuid;

const JOB_START_NEXT_ACTION_HINT: &str = "This job is running in the background. Continue other useful work if available, then poll status or read output later when needed.";
pub const DEFAULT_MAX_ACTIVE_JOBS: usize = 16;
pub const DEFAULT_MAX_RETAINED_JOBS: usize = 128;

#[cfg(test)]
static OUTPUT_READER_SPAWN_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static OUTPUT_READER_FAIL_ON_ATTEMPT: AtomicUsize = AtomicUsize::new(usize::MAX);
#[cfg(test)]
static COMMAND_CHILD_KILL_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static COMMAND_CHILD_REAPED: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static COMMAND_READER_COMPLETIONS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static COMMAND_READER_JOINS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static COMMAND_WORKER_COMPLETIONS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static FORCE_COMMAND_CLEANUP_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_COMMAND_OBSERVATION_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static COMMAND_RESOURCE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
#[cfg(test)]
static FORCE_COMMAND_WORKER_SPAWN_FAILURE: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
#[derive(Debug)]
struct CancellationInterleaveHook {
    acquired: mpsc::Sender<()>,
    proceed: mpsc::Receiver<()>,
}

#[derive(Debug, Clone)]
pub struct ThreadlineJobManager {
    inner: Arc<ThreadlineJobManagerInner>,
}

#[derive(Debug)]
struct ThreadlineJobManagerInner {
    config: ThreadlineJobManagerConfig,
    entries: Mutex<HashMap<String, Arc<Mutex<JobEntry>>>>,
    #[cfg(test)]
    cancel_after_entry_acquired: Mutex<Option<CancellationInterleaveHook>>,
}

#[derive(Debug, Clone)]
pub struct ThreadlineJobManagerConfig {
    pub jobs_enabled: bool,
    pub output_buffer_limit_bytes: usize,
    pub retention_ttl: Duration,
    pub max_active_jobs: usize,
    pub max_retained_jobs: usize,
    pub allowed_commands: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobState {
    Starting,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobTerminalState {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug)]
struct JobEntry {
    job_id: String,
    name: String,
    state: JobState,
    output: JobOutputRingBuffer,
    result: Option<Value>,
    error: Option<JobFailurePayload>,
    cancel_requested: bool,
    child: Option<Arc<Mutex<Child>>>,
    finished_at: Option<Instant>,
    execution_finished: bool,
}

#[derive(Debug, Clone)]
struct JobFailurePayload {
    code: &'static str,
    message: String,
}

#[derive(Debug, Clone)]
pub struct ManagedJobContext {
    entry: Arc<Mutex<JobEntry>>,
}

#[derive(Debug)]
struct ExecutionReservation {
    entry: Arc<Mutex<JobEntry>>,
    release_on_drop: bool,
}

#[derive(Debug)]
struct PendingJob {
    context: ManagedJobContext,
    reservation: ExecutionReservation,
}

struct ExecutingFuture<Fut>
where
    Fut: Future<Output = ()>,
{
    future: Option<Pin<Box<Fut>>>,
    context: ManagedJobContext,
    reservation: Option<ExecutionReservation>,
}

#[derive(Debug)]
struct JobOutputRingBuffer {
    limit: usize,
    next_offset: u64,
    truncated_before: u64,
    buffered_bytes: usize,
    segments: VecDeque<JobOutputSegment>,
}

#[derive(Debug)]
struct JobOutputSegment {
    offset: u64,
    stream: &'static str,
    text: String,
}

impl Default for ThreadlineJobManagerConfig {
    fn default() -> Self {
        Self {
            jobs_enabled: false,
            output_buffer_limit_bytes: 32 * 1024,
            retention_ttl: Duration::from_secs(300),
            max_active_jobs: DEFAULT_MAX_ACTIVE_JOBS,
            max_retained_jobs: DEFAULT_MAX_RETAINED_JOBS,
            allowed_commands: Vec::new(),
        }
    }
}

impl ThreadlineJobManager {
    pub fn new(config: ThreadlineJobManagerConfig) -> Self {
        Self {
            inner: Arc::new(ThreadlineJobManagerInner {
                config,
                entries: Mutex::new(HashMap::new()),
                #[cfg(test)]
                cancel_after_entry_acquired: Mutex::new(None),
            }),
        }
    }

    pub fn spawn_job<F, Fut>(&self, name: &str, task: F) -> Value
    where
        F: FnOnce(ManagedJobContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let PendingJob {
            context,
            reservation,
        } = match self.admit_job(name) {
            Ok(pending) => pending,
            Err(error) => return error,
        };
        let job_id = context.job_id();
        let spawned_context = context.clone();

        tokio::spawn(ExecutingFuture {
            future: Some(Box::pin(async move {
                task(spawned_context).await;
            })),
            context,
            reservation: Some(reservation),
        });

        job_started_json(&job_id)
    }

    pub fn start_command_json(&self, command: Vec<String>) -> Value {
        self.prune_expired();
        if !self.inner.config.jobs_enabled {
            return stable_error("jobs_disabled", "Threadline jobs are disabled.");
        }
        if command.is_empty() {
            return stable_error(
                "invalid_job_request",
                "threadline_start_job requires a non-empty command array.",
            );
        }

        let program = command[0].clone();
        if !self.command_allowed(&program) {
            return stable_error(
                "job_command_not_allowed",
                "The requested command is not allowed by the configured Threadline job policy.",
            );
        }

        let PendingJob {
            context,
            reservation,
        } = match self.admit_job("command") {
            Ok(pending) => pending,
            Err(error) => return error,
        };
        let job_id = context.job_id();
        if spawn_command_worker(move || {
            let mut reservation = reservation;
            let cleanup_confirmed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_command_job(context.clone(), command)
            }))
            .unwrap_or_else(|_| {
                context.fail(
                    "job_worker_cleanup_incomplete",
                    "Threadline could not confirm job worker cleanup.",
                );
                mark_cleanup_incomplete(&context);
                false
            });
            if !cleanup_confirmed {
                reservation.release_on_drop = false;
            }
            drop(reservation);
            #[cfg(test)]
            COMMAND_WORKER_COMPLETIONS.fetch_add(1, Ordering::SeqCst);
        })
        .is_err()
        {
            self.remove_entry(&job_id);
            return stable_error(
                "job_worker_spawn_failed",
                "Threadline could not start the job worker.",
            );
        }

        job_started_json(&job_id)
    }

    pub fn poll_json(&self, job_id: &str) -> Value {
        match self.entry(job_id) {
            Some(entry) => entry_snapshot(&entry),
            None => job_not_found(job_id),
        }
    }

    pub fn read_output_json(&self, job_id: &str, offset: u64) -> Value {
        let Some(entry) = self.entry(job_id) else {
            return job_not_found(job_id);
        };

        let entry = lock_job_entry(&entry);
        let effective_offset = offset.max(entry.output.truncated_before);
        let output = entry.output.read_from(offset);
        debug!(
            job_id = %entry.job_id,
            requested_offset = offset,
            served_from_offset = effective_offset,
            item_count = output.len(),
            next_offset = entry.output.next_offset,
            truncated_before = entry.output.truncated_before,
            "job_output_offset_served"
        );
        json!({
            "ok": true,
            "job_id": entry.job_id,
            "status": entry.state.as_str(),
            "items": output,
            "next_offset": entry.output.next_offset,
            "truncated_before": entry.output.truncated_before,
        })
    }

    pub fn get_result_json(&self, job_id: &str) -> Value {
        let Some(entry) = self.entry(job_id) else {
            return job_not_found(job_id);
        };

        let entry = lock_job_entry(&entry);
        let error = entry.error.as_ref().map(|payload| {
            json!({
                "code": payload.code,
                "message": payload.message,
            })
        });

        json!({
            "ok": true,
            "job_id": entry.job_id,
            "status": entry.state.as_str(),
            "result": entry.result,
            "error": error,
        })
    }

    pub fn cancel_json(&self, job_id: &str) -> Value {
        let Some(entry) = self.entry(job_id) else {
            return job_not_found(job_id);
        };

        #[cfg(test)]
        self.pause_after_cancel_entry_acquired();

        cancel_acquired_entry(entry)
    }
}

fn cancel_acquired_entry(entry: Arc<Mutex<JobEntry>>) -> Value {
    let child = {
        let mut entry = lock_job_entry(&entry);
        entry.cancel_requested = true;
        if !entry.state.is_terminal() {
            entry.state = JobState::Cancelled;
            entry.result = None;
            entry.error = Some(JobFailurePayload {
                code: "job_cancelled",
                message: "The Threadline job was cancelled.".to_string(),
            });
            entry.finished_at = Some(Instant::now());
            debug!(
                job_id = %entry.job_id,
                terminal_state = JobTerminalState::Cancelled.as_str(),
                "job_terminal_state_changed"
            );
        }
        entry.child.clone()
    };

    if let Some(child) = child {
        let _ = child.lock().expect("child lock").kill();
    }

    entry_snapshot(&entry)
}

impl ThreadlineJobManager {
    pub fn prune_expired(&self) -> usize {
        self.prune_expired_at(Instant::now())
    }

    fn admit_job(&self, name: &str) -> Result<PendingJob, Value> {
        let now = Instant::now();
        let job_id = Uuid::now_v7().to_string();
        let entry = Arc::new(Mutex::new(JobEntry {
            job_id: job_id.clone(),
            name: name.to_string(),
            state: JobState::Starting,
            output: JobOutputRingBuffer::new(self.inner.config.output_buffer_limit_bytes),
            result: None,
            error: None,
            cancel_requested: false,
            child: None,
            finished_at: None,
            execution_finished: false,
        }));
        let mut entries = lock_entries(&self.inner.entries);
        let removed = prune_expired_entries(&mut entries, now, self.inner.config.retention_ttl);
        log_pruned(removed, entries.len(), self.inner.config.retention_ttl);

        let active_count = entries
            .values()
            .filter(|entry| !lock_job_entry(entry).execution_finished)
            .count();
        if active_count >= self.inner.config.max_active_jobs {
            log_capacity_rejected("active", active_count, entries.len(), &self.inner.config);
            return Err(job_capacity_exceeded());
        }
        if self.inner.config.max_retained_jobs == 0 {
            log_capacity_rejected("retained", active_count, entries.len(), &self.inner.config);
            return Err(job_capacity_exceeded());
        }

        while entries.len() >= self.inner.config.max_retained_jobs {
            let Some(victim_id) = oldest_removable_entry(&entries) else {
                log_capacity_rejected("retained", active_count, entries.len(), &self.inner.config);
                return Err(job_capacity_exceeded());
            };
            let victim = entries
                .remove(&victim_id)
                .expect("selected job entry exists");
            let victim = lock_job_entry(&victim);
            debug!(
                job_id = %victim.job_id,
                terminal_state = ?victim.state.terminal_state(),
                age_secs = victim.finished_at.map(|finished| now.saturating_duration_since(finished).as_secs()),
                entry_count = entries.len(),
                retained_limit = self.inner.config.max_retained_jobs,
                "job_retention_evicted"
            );
        }

        entries.insert(job_id, Arc::clone(&entry));
        Ok(PendingJob {
            context: ManagedJobContext {
                entry: Arc::clone(&entry),
            },
            reservation: ExecutionReservation {
                entry,
                release_on_drop: true,
            },
        })
    }

    fn entry(&self, job_id: &str) -> Option<Arc<Mutex<JobEntry>>> {
        self.prune_expired();
        lock_entries(&self.inner.entries).get(job_id).cloned()
    }

    fn prune_expired_at(&self, now: Instant) -> usize {
        let mut entries = lock_entries(&self.inner.entries);
        let removed = prune_expired_entries(&mut entries, now, self.inner.config.retention_ttl);
        log_pruned(removed, entries.len(), self.inner.config.retention_ttl);
        removed
    }

    fn remove_entry(&self, job_id: &str) {
        lock_entries(&self.inner.entries).remove(job_id);
    }

    fn command_allowed(&self, program: &str) -> bool {
        self.inner
            .config
            .allowed_commands
            .iter()
            .any(|allowed| allowed == program)
    }

    #[cfg(test)]
    fn pause_after_cancel_entry_acquired(&self) {
        let hook = self
            .inner
            .cancel_after_entry_acquired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(hook) = hook {
            let _ = hook.acquired.send(());
            let _ = hook.proceed.recv();
        }
    }
}

impl Drop for ExecutionReservation {
    fn drop(&mut self) {
        if !self.release_on_drop {
            return;
        }

        let mut entry = lock_job_entry(&self.entry);
        if !entry.state.is_terminal() {
            entry.state = JobState::Failed;
            entry.result = None;
            entry.error = Some(JobFailurePayload {
                code: "job_did_not_finalize",
                message: "The Threadline job ended without reporting a terminal state.".to_string(),
            });
            entry.child = None;
            entry.finished_at = Some(Instant::now());
        }
        entry.execution_finished = true;
    }
}

fn lock_entries(
    entries: &Mutex<HashMap<String, Arc<Mutex<JobEntry>>>>,
) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Mutex<JobEntry>>>> {
    entries
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_job_entry(entry: &Arc<Mutex<JobEntry>>) -> std::sync::MutexGuard<'_, JobEntry> {
    entry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn entry_is_removable(entry: &JobEntry) -> bool {
    entry.state.is_terminal() && entry.execution_finished && entry.finished_at.is_some()
}

fn prune_expired_entries(
    entries: &mut HashMap<String, Arc<Mutex<JobEntry>>>,
    now: Instant,
    ttl: Duration,
) -> usize {
    let original_count = entries.len();
    entries.retain(|_, entry| {
        let entry = lock_job_entry(entry);
        !entry_is_removable(&entry)
            || now.saturating_duration_since(entry.finished_at.expect("removable timestamp")) < ttl
    });
    original_count - entries.len()
}

fn oldest_removable_entry(entries: &HashMap<String, Arc<Mutex<JobEntry>>>) -> Option<String> {
    entries
        .iter()
        .filter_map(|(job_id, entry)| {
            let entry = lock_job_entry(entry);
            entry_is_removable(&entry).then(|| {
                (
                    entry.finished_at.expect("removable timestamp"),
                    job_id.clone(),
                )
            })
        })
        .min()
        .map(|(_, job_id)| job_id)
}

fn log_pruned(removed: usize, remaining: usize, ttl: Duration) {
    if removed > 0 {
        debug!(
            removed_count = removed,
            remaining_count = remaining,
            retention_ttl_secs = ttl.as_secs(),
            "job_retention_pruned"
        );
    }
}

fn log_capacity_rejected(
    reason: &'static str,
    active_count: usize,
    entry_count: usize,
    config: &ThreadlineJobManagerConfig,
) {
    warn!(
        reason,
        active_count,
        entry_count,
        max_active_jobs = config.max_active_jobs,
        max_retained_jobs = config.max_retained_jobs,
        "job_capacity_rejected"
    );
}

fn job_capacity_exceeded() -> Value {
    stable_error(
        "job_capacity_exceeded",
        "Threadline job capacity is exhausted.",
    )
}

impl JobTerminalState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl ManagedJobContext {
    pub fn mark_running(&self) {
        let mut entry = lock_job_entry(&self.entry);
        if entry.state == JobState::Starting {
            entry.state = JobState::Running;
        }
    }

    pub fn push_stdout(&self, text: &str) {
        self.push_output("stdout", text);
    }

    pub fn push_stderr(&self, text: &str) {
        self.push_output("stderr", text);
    }

    pub fn complete(&self, result: Value) {
        let mut entry = lock_job_entry(&self.entry);
        if entry.state.is_terminal() {
            return;
        }

        entry.state = JobState::Completed;
        entry.result = Some(result);
        entry.error = None;
        entry.child = None;
        entry.finished_at = Some(Instant::now());
        debug!(
            job_id = %entry.job_id,
            terminal_state = JobTerminalState::Completed.as_str(),
            "job_terminal_state_changed"
        );
    }

    pub fn fail(&self, code: &'static str, message: impl Into<String>) {
        let message = message.into();
        let mut entry = lock_job_entry(&self.entry);
        if entry.state.is_terminal() {
            return;
        }

        entry.state = JobState::Failed;
        entry.result = None;
        entry.error = Some(JobFailurePayload { code, message });
        entry.child = None;
        entry.finished_at = Some(Instant::now());
        debug!(
            job_id = %entry.job_id,
            terminal_state = JobTerminalState::Failed.as_str(),
            error_code = code,
            "job_terminal_state_changed"
        );
    }

    pub fn is_cancelled(&self) -> bool {
        lock_job_entry(&self.entry).cancel_requested
    }

    pub fn job_id(&self) -> String {
        lock_job_entry(&self.entry).job_id.clone()
    }

    fn push_output(&self, stream: &'static str, text: &str) {
        let mut entry = lock_job_entry(&self.entry);
        if entry.state.is_terminal() || text.is_empty() {
            return;
        }

        let start_offset = entry.output.next_offset;
        let truncated_before = entry.output.truncated_before;
        entry.output.append(stream, text);
        debug!(
            job_id = %entry.job_id,
            stream,
            byte_count = text.len(),
            start_offset,
            next_offset = entry.output.next_offset,
            truncated_before = entry.output.truncated_before,
            "job_output_chunk_appended"
        );
        if entry.output.truncated_before != truncated_before {
            debug!(
                job_id = %entry.job_id,
                stream,
                previous_truncated_before = truncated_before,
                truncated_before = entry.output.truncated_before,
                next_offset = entry.output.next_offset,
                buffered_bytes = entry.output.buffered_bytes,
                "job_output_truncation_advanced"
            );
        }
    }

    fn attach_child(&self, child: Arc<Mutex<Child>>) {
        let mut entry = lock_job_entry(&self.entry);
        if entry.state.is_terminal() {
            return;
        }
        entry.child = Some(child);
    }

    fn clear_child(&self) {
        lock_job_entry(&self.entry).child = None;
    }

    fn fail_if_unresolved(&self) {
        let mut entry = lock_job_entry(&self.entry);
        if entry.state.is_terminal() {
            return;
        }

        entry.state = JobState::Failed;
        entry.result = None;
        entry.error = Some(JobFailurePayload {
            code: "job_did_not_finalize",
            message: "The Threadline job ended without reporting a terminal state.".to_string(),
        });
        entry.child = None;
        entry.finished_at = Some(Instant::now());
        debug!(
            job_id = %entry.job_id,
            terminal_state = JobTerminalState::Failed.as_str(),
            error_code = "job_did_not_finalize",
            "job_terminal_state_changed"
        );
    }
}

impl JobOutputRingBuffer {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            next_offset: 0,
            truncated_before: 0,
            buffered_bytes: 0,
            segments: VecDeque::new(),
        }
    }

    fn append(&mut self, stream: &'static str, text: &str) {
        let added = text.len();
        if added == 0 {
            return;
        }

        let offset = self.next_offset;
        self.next_offset += added as u64;

        if self.limit == 0 {
            self.truncated_before = self.next_offset;
            return;
        }

        self.buffered_bytes += added;
        self.segments.push_back(JobOutputSegment {
            offset,
            stream,
            text: text.to_string(),
        });
        self.trim_to_limit();
    }

    fn read_from(&self, offset: u64) -> Vec<Value> {
        let effective_offset = offset.max(self.truncated_before);

        self.segments
            .iter()
            .filter_map(|segment| {
                let segment_end = segment.offset + segment.text.len() as u64;
                if segment_end <= effective_offset {
                    return None;
                }

                if effective_offset <= segment.offset {
                    return Some(json!({
                        "offset": segment.offset,
                        "stream": segment.stream,
                        "text": segment.text,
                    }));
                }

                let skip = (effective_offset - segment.offset) as usize;
                let (skipped_bytes, trimmed_text) = trim_front_bytes(&segment.text, skip);
                if trimmed_text.is_empty() {
                    return None;
                }

                Some(json!({
                    "offset": segment.offset + skipped_bytes as u64,
                    "stream": segment.stream,
                    "text": trimmed_text,
                }))
            })
            .collect()
    }

    fn trim_to_limit(&mut self) {
        while self.buffered_bytes > self.limit {
            let overflow = self.buffered_bytes - self.limit;
            let Some(front) = self.segments.front_mut() else {
                break;
            };

            let available = front.text.len();
            let trim = overflow.min(available);
            let (trimmed_bytes, trimmed_text) = trim_front_bytes(&front.text, trim);
            front.text = trimmed_text;
            front.offset += trimmed_bytes as u64;
            self.buffered_bytes -= trimmed_bytes;
            self.truncated_before += trimmed_bytes as u64;

            if front.text.is_empty() {
                self.segments.pop_front();
            }
        }
    }
}

impl JobState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    fn terminal_state(self) -> Option<JobTerminalState> {
        match self {
            Self::Completed => Some(JobTerminalState::Completed),
            Self::Failed => Some(JobTerminalState::Failed),
            Self::Cancelled => Some(JobTerminalState::Cancelled),
            Self::Starting | Self::Running => None,
        }
    }
}

fn entry_snapshot(entry: &Arc<Mutex<JobEntry>>) -> Value {
    let entry = lock_job_entry(entry);
    json!({
        "ok": true,
        "job_id": entry.job_id,
        "name": entry.name,
        "status": entry.state.as_str(),
        "finished": entry.state.is_terminal(),
        "cancel_requested": entry.cancel_requested,
        "terminal_state": entry.state.terminal_state().map(JobTerminalState::as_str),
    })
}

fn job_not_found(job_id: &str) -> Value {
    json!({
        "ok": false,
        "code": "job_not_found",
        "message": "Threadline could not find a job with that job_id.",
        "job_id": job_id,
    })
}

fn stable_error(code: &'static str, message: &'static str) -> Value {
    json!({
        "ok": false,
        "code": code,
        "message": message,
    })
}

fn job_started_json(job_id: &str) -> Value {
    json!({
        "ok": true,
        "job_id": job_id,
        "status": JobState::Starting.as_str(),
        "next_action_hint": JOB_START_NEXT_ACTION_HINT,
    })
}

fn run_command_job(context: ManagedJobContext, command: Vec<String>) -> bool {
    context.mark_running();

    let mut child = match spawn_command(&command) {
        Ok(child) => child,
        Err(error) => {
            context.fail(
                "job_command_spawn_failed",
                format!("Threadline could not start the requested command: {error}"),
            );
            return true;
        }
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let child = Arc::new(Mutex::new(child));
    context.attach_child(Arc::clone(&child));

    let stdout_reader =
        match stdout.map(|stdout| spawn_output_reader(stdout, context.clone(), "stdout")) {
            Some(Ok(reader)) => Some(reader),
            Some(Err(_)) => {
                let cleanup_confirmed = cleanup_command_resources(&child, None, None);
                context.clear_child();
                context.fail(
                    "job_output_reader_spawn_failed",
                    "Threadline could not start a job output reader.",
                );
                if !cleanup_confirmed {
                    mark_cleanup_incomplete(&context);
                }
                return cleanup_confirmed;
            }
            None => None,
        };
    let stderr_reader =
        match stderr.map(|stderr| spawn_output_reader(stderr, context.clone(), "stderr")) {
            Some(Ok(reader)) => Some(reader),
            Some(Err(_)) => {
                let cleanup_confirmed = cleanup_command_resources(&child, stdout_reader, None);
                context.clear_child();
                context.fail(
                    "job_output_reader_spawn_failed",
                    "Threadline could not start a job output reader.",
                );
                if !cleanup_confirmed {
                    mark_cleanup_incomplete(&context);
                }
                return cleanup_confirmed;
            }
            None => None,
        };

    let status = loop {
        if context.is_cancelled() {
            let _ = child.lock().expect("child lock").kill();
        }

        let observation = {
            let mut child = child.lock().expect("child lock");
            child.try_wait().and_then(inject_command_observation_error)
        };
        match observation {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(10)),
            Err(error) => {
                let cleanup_confirmed =
                    cleanup_command_resources(&child, stdout_reader, stderr_reader);
                context.clear_child();
                context.fail(
                    "job_command_failed",
                    format!("Threadline could not observe the command status: {error}"),
                );
                if !cleanup_confirmed {
                    mark_cleanup_incomplete(&context);
                }
                return cleanup_confirmed;
            }
        }
    };

    let cleanup_confirmed = cleanup_command_resources(&child, stdout_reader, stderr_reader);
    context.clear_child();
    if !cleanup_confirmed {
        context.fail(
            "job_worker_cleanup_incomplete",
            "Threadline could not confirm job worker cleanup.",
        );
        mark_cleanup_incomplete(&context);
        return false;
    }

    if context.is_cancelled() {
        return true;
    }

    if status.success() {
        context.complete(json!({
            "kind": "command",
            "command": command,
            "exit_code": status.code(),
            "success": true,
        }));
    } else {
        context.fail(
            "job_command_failed",
            format!(
                "The Threadline job command exited unsuccessfully with code {:?}.",
                status.code()
            ),
        );
    }
    true
}

fn spawn_command(command: &[String]) -> Result<Child, std::io::Error> {
    let mut child = Command::new(&command[0]);
    if command.len() > 1 {
        child.args(&command[1..]);
    }

    child.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()
}

fn inject_command_observation_error(
    status: Option<std::process::ExitStatus>,
) -> Result<Option<std::process::ExitStatus>, std::io::Error> {
    #[cfg(test)]
    if FORCE_COMMAND_OBSERVATION_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(std::io::Error::other(
            "injected command observation failure",
        ));
    }

    Ok(status)
}

fn spawn_command_worker(
    task: impl FnOnce() + Send + 'static,
) -> Result<thread::JoinHandle<()>, std::io::Error> {
    #[cfg(test)]
    if FORCE_COMMAND_WORKER_SPAWN_FAILURE.load(Ordering::SeqCst) {
        return Err(std::io::Error::other(
            "injected command worker spawn failure",
        ));
    }
    thread::Builder::new()
        .name("threadline-job-command".to_string())
        .spawn(task)
}

fn spawn_output_reader<R>(
    reader: R,
    context: ManagedJobContext,
    stream: &'static str,
) -> Result<thread::JoinHandle<()>, std::io::Error>
where
    R: std::io::Read + Send + 'static,
{
    #[cfg(test)]
    {
        let attempt = OUTPUT_READER_SPAWN_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        if attempt == OUTPUT_READER_FAIL_ON_ATTEMPT.load(Ordering::SeqCst) {
            return Err(std::io::Error::other(
                "injected output reader spawn failure",
            ));
        }
    }
    thread::Builder::new()
        .name(format!("threadline-job-{stream}"))
        .spawn(move || {
            let mut reader = BufReader::new(reader);
            let mut pending = Vec::new();
            let mut buffer = [0u8; 1024];

            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read_bytes) => {
                        pending.extend_from_slice(&buffer[..read_bytes]);
                        flush_output_chunks(&context, stream, &mut pending);
                    }
                    Err(_) => break,
                }
            }

            if !pending.is_empty() {
                let text = String::from_utf8_lossy(&pending).to_string();
                push_stream_output(&context, stream, &text);
            }
            #[cfg(test)]
            COMMAND_READER_COMPLETIONS.fetch_add(1, Ordering::SeqCst);
        })
}

fn cleanup_command_resources(
    child: &Arc<Mutex<Child>>,
    stdout_reader: Option<thread::JoinHandle<()>>,
    stderr_reader: Option<thread::JoinHandle<()>>,
) -> bool {
    let child_reaped = {
        let mut child = child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        #[cfg(test)]
        COMMAND_CHILD_KILL_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        let _ = child.kill();
        let child_reaped = child.wait().is_ok();
        #[cfg(test)]
        COMMAND_CHILD_REAPED.store(child_reaped, Ordering::SeqCst);
        child_reaped
    };
    let stdout_joined = join_reader(stdout_reader);
    let stderr_joined = join_reader(stderr_reader);
    let cleanup_confirmed = child_reaped && stdout_joined && stderr_joined;
    #[cfg(test)]
    if FORCE_COMMAND_CLEANUP_FAILURE.load(Ordering::SeqCst) {
        return false;
    }
    cleanup_confirmed
}

fn mark_cleanup_incomplete(context: &ManagedJobContext) {
    let entry = lock_job_entry(&context.entry);
    warn!(job_id = %entry.job_id, reason = "cleanup_unconfirmed", "job_worker_cleanup_incomplete");
}

fn flush_output_chunks(context: &ManagedJobContext, stream: &'static str, pending: &mut Vec<u8>) {
    loop {
        if let Some(newline_index) = pending.iter().position(|byte| *byte == b'\n') {
            let chunk: Vec<u8> = pending.drain(..=newline_index).collect();
            let text = String::from_utf8_lossy(&chunk).to_string();
            push_stream_output(context, stream, &text);
            continue;
        }

        match std::str::from_utf8(pending) {
            Ok(text) => {
                if !text.is_empty() {
                    push_stream_output(context, stream, text);
                    pending.clear();
                }
                break;
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                if valid_up_to > 0 {
                    let text =
                        std::str::from_utf8(&pending[..valid_up_to]).expect("valid utf-8 prefix");
                    push_stream_output(context, stream, text);
                    pending.drain(..valid_up_to);
                    continue;
                }

                if error.error_len().is_none() {
                    break;
                }

                let text = String::from_utf8_lossy(pending).to_string();
                push_stream_output(context, stream, &text);
                pending.clear();
                break;
            }
        }
    }
}

fn push_stream_output(context: &ManagedJobContext, stream: &'static str, text: &str) {
    if text.is_empty() {
        return;
    }

    match stream {
        "stdout" => context.push_stdout(text),
        "stderr" => context.push_stderr(text),
        _ => {}
    }
}

fn join_reader(reader: Option<thread::JoinHandle<()>>) -> bool {
    if let Some(reader) = reader {
        let joined = reader.join().is_ok();
        #[cfg(test)]
        COMMAND_READER_JOINS.fetch_add(1, Ordering::SeqCst);
        return joined;
    }
    true
}

impl<Fut> Future for ExecutingFuture<Fut>
where
    Fut: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let poll = this
            .future
            .as_mut()
            .expect("executing future must be present")
            .as_mut()
            .poll(cx);
        if poll.is_ready() {
            drop(this.future.take());
            this.context.fail_if_unresolved();
            drop(this.reservation.take());
        }
        poll
    }
}

fn trim_front_bytes(text: &str, count: usize) -> (usize, String) {
    if count == 0 {
        return (0, text.to_string());
    }

    if count >= text.len() {
        return (text.len(), String::new());
    }

    let mut start = count;
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }

    (start, text[start..].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;
    use std::sync::mpsc;

    fn manager(max_active_jobs: usize) -> ThreadlineJobManager {
        ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            jobs_enabled: true,
            output_buffer_limit_bytes: 1024,
            retention_ttl: Duration::from_secs(60),
            max_active_jobs,
            max_retained_jobs: 4,
            allowed_commands: Vec::new(),
        })
    }

    fn command_manager(max_active_jobs: usize) -> ThreadlineJobManager {
        let command = test_command();
        ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            jobs_enabled: true,
            output_buffer_limit_bytes: 1024,
            retention_ttl: Duration::from_secs(60),
            max_active_jobs,
            max_retained_jobs: 4,
            allowed_commands: vec![command[0].clone()],
        })
    }

    fn ttl_zero_manager(allowed_commands: Vec<String>) -> ThreadlineJobManager {
        ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            jobs_enabled: true,
            output_buffer_limit_bytes: 1024,
            retention_ttl: Duration::ZERO,
            max_active_jobs: 4,
            max_retained_jobs: 16,
            allowed_commands,
        })
    }

    fn seed_entry(
        manager: &ThreadlineJobManager,
        state: JobState,
        execution_finished: bool,
    ) -> String {
        let job_id = format!("seeded-{}", Uuid::now_v7());
        let finished_at = state.is_terminal().then(Instant::now);
        let entry = Arc::new(Mutex::new(JobEntry {
            job_id: job_id.clone(),
            name: "seeded".to_string(),
            state,
            output: JobOutputRingBuffer::new(1024),
            result: None,
            error: None,
            cancel_requested: false,
            child: None,
            finished_at,
            execution_finished,
        }));
        lock_entries(&manager.inner.entries).insert(job_id.clone(), entry);
        job_id
    }

    fn registry_contains(manager: &ThreadlineJobManager, job_id: &str) -> bool {
        lock_entries(&manager.inner.entries).contains_key(job_id)
    }

    async fn wait_for_execution_to_finish(manager: &ThreadlineJobManager, job_id: &str) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let finished = lock_entries(&manager.inner.entries)
                .get(job_id)
                .is_some_and(|entry| lock_job_entry(entry).execution_finished);
            if finished {
                return;
            }
            assert!(Instant::now() < deadline, "job worker did not finish");
            tokio::task::yield_now().await;
        }
    }

    struct CommandTestSeamReset;

    impl Drop for CommandTestSeamReset {
        fn drop(&mut self) {
            OUTPUT_READER_SPAWN_ATTEMPTS.store(0, Ordering::SeqCst);
            OUTPUT_READER_FAIL_ON_ATTEMPT.store(usize::MAX, Ordering::SeqCst);
            COMMAND_CHILD_KILL_ATTEMPTS.store(0, Ordering::SeqCst);
            COMMAND_CHILD_REAPED.store(false, Ordering::SeqCst);
            COMMAND_READER_COMPLETIONS.store(0, Ordering::SeqCst);
            COMMAND_READER_JOINS.store(0, Ordering::SeqCst);
            COMMAND_WORKER_COMPLETIONS.store(0, Ordering::SeqCst);
            FORCE_COMMAND_CLEANUP_FAILURE.store(false, Ordering::SeqCst);
            FORCE_COMMAND_OBSERVATION_FAILURE.store(false, Ordering::SeqCst);
            FORCE_COMMAND_WORKER_SPAWN_FAILURE.store(false, Ordering::SeqCst);
        }
    }

    fn reset_command_observation_test_seam(cleanup_confirmed: bool) {
        COMMAND_CHILD_KILL_ATTEMPTS.store(0, Ordering::SeqCst);
        COMMAND_CHILD_REAPED.store(false, Ordering::SeqCst);
        COMMAND_READER_COMPLETIONS.store(0, Ordering::SeqCst);
        COMMAND_READER_JOINS.store(0, Ordering::SeqCst);
        COMMAND_WORKER_COMPLETIONS.store(0, Ordering::SeqCst);
        FORCE_COMMAND_CLEANUP_FAILURE.store(!cleanup_confirmed, Ordering::SeqCst);
        FORCE_COMMAND_OBSERVATION_FAILURE.store(true, Ordering::SeqCst);
    }

    fn test_command() -> Vec<String> {
        if cfg!(windows) {
            vec![
                "pwsh".to_string(),
                "-NoProfile".to_string(),
                "-Command".to_string(),
                "Write-Output stdout; [Console]::Error.WriteLine('stderr')".to_string(),
            ]
        } else {
            vec![
                "sh".to_string(),
                "-lc".to_string(),
                "printf stdout; printf stderr >&2".to_string(),
            ]
        }
    }

    struct DestructorGateFuture {
        ready: bool,
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Future for DestructorGateFuture {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            if self.ready {
                Poll::Ready(())
            } else {
                self.ready = true;
                Poll::Pending
            }
        }
    }

    impl Drop for DestructorGateFuture {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self.release.recv();
        }
    }

    struct DestructorGateRelease(Option<mpsc::Sender<()>>);

    impl DestructorGateRelease {
        fn release(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    impl Drop for DestructorGateRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    fn run_gated_destructor_case(initially_ready: bool) {
        let manager = manager(1);
        let PendingJob {
            context,
            reservation,
        } = manager.admit_job("destructor-gate").expect("admitted job");
        let job_id = context.job_id();
        let result = json!({"summary": "early terminal"});
        context.complete(result.clone());
        let finished_at = lock_job_entry(&context.entry)
            .finished_at
            .expect("early terminal timestamp");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let worker_context = context.clone();
        let worker = thread::spawn(move || {
            let mut future = Box::pin(ExecutingFuture {
                future: Some(Box::pin(DestructorGateFuture {
                    ready: initially_ready,
                    entered: entered_tx,
                    release: release_rx,
                })),
                context: worker_context,
                reservation: Some(reservation),
            });
            let mut task_context = Context::from_waker(std::task::Waker::noop());
            if initially_ready {
                assert!(future.as_mut().poll(&mut task_context).is_ready());
            } else {
                assert!(future.as_mut().poll(&mut task_context).is_pending());
                drop(future);
            }
            let _ = completed_tx.send(());
        });

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("future destructor entered");
        let mut release_guard = DestructorGateRelease(Some(release_tx));
        assert_eq!(
            manager.admit_job("must-wait-for-destructor").unwrap_err()["code"],
            "job_capacity_exceeded"
        );
        assert_eq!(
            manager.prune_expired_at(finished_at + Duration::from_secs(60)),
            0
        );
        assert!(registry_contains(&manager, &job_id));
        release_guard.release();
        completed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("destructor worker completion");
        worker.join().expect("destructor worker");
        let entry = lock_job_entry(&context.entry);
        assert!(entry.execution_finished);
        assert_eq!(entry.result.as_ref(), Some(&result));
        assert_eq!(entry.finished_at, Some(finished_at));
        drop(entry);
        assert!(manager.admit_job("reused-after-destructor").is_ok());
        assert_eq!(
            manager.prune_expired_at(finished_at + Duration::from_secs(60)),
            1
        );
        assert!(!registry_contains(&manager, &job_id));
    }

    #[test]
    fn never_polled_execution_future_releases_its_reservation_after_dropping_user_future() {
        let manager = manager(1);
        let PendingJob {
            context,
            reservation,
        } = manager.admit_job("never-polled").expect("admitted job");
        let entry = Arc::clone(&context.entry);
        let future = ExecutingFuture {
            future: Some(Box::pin(pending())),
            context,
            reservation: Some(reservation),
        };

        drop(future);

        let entry = lock_job_entry(&entry);
        assert!(entry.execution_finished);
        assert_eq!(entry.state, JobState::Failed);
        assert_eq!(
            entry.error.as_ref().map(|error| error.code),
            Some("job_did_not_finalize")
        );
    }

    #[test]
    fn unresolved_ready_execution_future_finalizes_before_releasing_its_reservation() {
        let manager = manager(1);
        let PendingJob {
            context,
            reservation,
        } = manager.admit_job("unresolved-ready").expect("admitted job");
        let entry = Arc::clone(&context.entry);
        let mut future = Box::pin(ExecutingFuture {
            future: Some(Box::pin(async {})),
            context,
            reservation: Some(reservation),
        });
        let mut task_context = Context::from_waker(std::task::Waker::noop());

        assert!(future.as_mut().poll(&mut task_context).is_ready());
        let entry = lock_job_entry(&entry);
        assert!(entry.execution_finished);
        assert_eq!(
            entry.error.as_ref().map(|error| error.code),
            Some("job_did_not_finalize")
        );
    }

    #[test]
    fn panicking_user_future_finalizes_when_its_execution_wrapper_is_dropped() {
        let manager = manager(1);
        let PendingJob {
            context,
            reservation,
        } = manager.admit_job("panicking-future").expect("admitted job");
        let job_id = context.job_id();
        let mut future = Box::pin(ExecutingFuture {
            future: Some(Box::pin(async {
                panic!("user future panic");
            })),
            context,
            reservation: Some(reservation),
        });
        let mut task_context = Context::from_waker(std::task::Waker::noop());

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = future.as_mut().poll(&mut task_context);
        }));
        assert!(panic.is_err());
        drop(future);

        assert_eq!(
            manager.get_result_json(&job_id)["error"]["code"],
            "job_did_not_finalize"
        );
        assert!(manager.admit_job("reused-after-panic").is_ok());
    }

    #[test]
    fn ready_future_destructor_holds_capacity_until_drop_completes() {
        run_gated_destructor_case(true);
    }

    #[test]
    fn pending_future_destructor_holds_capacity_until_drop_completes() {
        run_gated_destructor_case(false);
    }

    #[test]
    fn panicking_message_conversion_leaves_entry_recoverable_after_worker_drop() {
        struct PanicMessage;
        impl From<PanicMessage> for String {
            fn from(_: PanicMessage) -> Self {
                panic!("message conversion panic");
            }
        }

        let manager = manager(1);
        let PendingJob {
            context,
            reservation,
        } = manager.admit_job("conversion-panic").expect("admitted job");
        let job_id = context.job_id();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            context.fail("failure", PanicMessage)
        }));
        assert!(panic.is_err());
        drop(reservation);

        assert_eq!(
            manager.get_result_json(&job_id)["error"]["code"],
            "job_did_not_finalize"
        );
        assert!(lock_job_entry(&context.entry).finished_at.is_some());
        assert!(manager.admit_job("post-panic").is_ok());
        assert_eq!(
            manager.prune_expired_at(Instant::now() + Duration::from_secs(61)),
            2
        );
    }

    #[test]
    fn poisoned_entry_is_recovered_for_result_admission_and_expiry() {
        let manager = manager(1);
        let PendingJob {
            context,
            reservation,
        } = manager.admit_job("poisoned-entry").expect("admitted job");
        let job_id = context.job_id();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _entry = context.entry.lock().expect("entry lock");
            panic!("poison entry");
        }));
        drop(reservation);

        assert_eq!(
            manager.get_result_json(&job_id)["error"]["code"],
            "job_did_not_finalize"
        );
        assert!(lock_job_entry(&context.entry).finished_at.is_some());
        assert!(manager.admit_job("post-poison").is_ok());
        assert_eq!(
            manager.prune_expired_at(Instant::now() + Duration::from_secs(61)),
            2
        );
    }

    #[test]
    fn output_reader_failures_cleanup_before_reservation_reuse() {
        let _serial = COMMAND_RESOURCE_TEST_LOCK.blocking_lock();
        let _reset = CommandTestSeamReset;
        for fail_attempt in [0, 1] {
            OUTPUT_READER_SPAWN_ATTEMPTS.store(0, Ordering::SeqCst);
            OUTPUT_READER_FAIL_ON_ATTEMPT.store(fail_attempt, Ordering::SeqCst);
            COMMAND_CHILD_KILL_ATTEMPTS.store(0, Ordering::SeqCst);
            COMMAND_CHILD_REAPED.store(false, Ordering::SeqCst);
            COMMAND_READER_COMPLETIONS.store(0, Ordering::SeqCst);
            COMMAND_READER_JOINS.store(0, Ordering::SeqCst);
            FORCE_COMMAND_CLEANUP_FAILURE.store(false, Ordering::SeqCst);

            let manager = manager(1);
            let PendingJob {
                context,
                reservation,
            } = manager.admit_job("reader-failure").expect("admitted job");
            assert!(run_command_job(context.clone(), test_command()));
            drop(reservation);

            assert_eq!(
                lock_job_entry(&context.entry)
                    .error
                    .as_ref()
                    .map(|error| error.code),
                Some("job_output_reader_spawn_failed")
            );
            assert_eq!(COMMAND_CHILD_KILL_ATTEMPTS.load(Ordering::SeqCst), 1);
            assert!(COMMAND_CHILD_REAPED.load(Ordering::SeqCst));
            assert_eq!(
                COMMAND_READER_COMPLETIONS.load(Ordering::SeqCst),
                fail_attempt
            );
            assert_eq!(COMMAND_READER_JOINS.load(Ordering::SeqCst), fail_attempt);
            assert!(manager.admit_job("reused-after-cleanup").is_ok());
        }
    }

    #[test]
    fn unconfirmed_command_cleanup_keeps_execution_reservation_fail_closed() {
        #[derive(Clone)]
        struct TestLogCapture(Arc<Mutex<Vec<u8>>>);

        struct TestLogWriter(Arc<Mutex<Vec<u8>>>);

        impl Write for TestLogWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                lock_job_entry_log_buffer(&self.0).extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for TestLogCapture {
            type Writer = TestLogWriter;

            fn make_writer(&'writer self) -> Self::Writer {
                TestLogWriter(Arc::clone(&self.0))
            }
        }

        let _serial = COMMAND_RESOURCE_TEST_LOCK.blocking_lock();
        let _reset = CommandTestSeamReset;
        OUTPUT_READER_SPAWN_ATTEMPTS.store(0, Ordering::SeqCst);
        OUTPUT_READER_FAIL_ON_ATTEMPT.store(0, Ordering::SeqCst);
        FORCE_COMMAND_CLEANUP_FAILURE.store(true, Ordering::SeqCst);

        let manager = manager(1);
        let PendingJob {
            context,
            mut reservation,
        } = manager
            .admit_job("cleanup-unconfirmed")
            .expect("admitted job");
        let log_buffer = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .without_time()
            .with_ansi(false)
            .with_writer(TestLogCapture(Arc::clone(&log_buffer)))
            .finish();
        let cleanup_confirmed = tracing::subscriber::with_default(subscriber, || {
            run_command_job(context.clone(), test_command())
        });
        assert!(!cleanup_confirmed);
        reservation.release_on_drop = cleanup_confirmed;
        drop(reservation);

        assert!(!lock_job_entry(&context.entry).execution_finished);
        assert_eq!(
            manager.admit_job("must-remain-rejected").unwrap_err()["code"],
            "job_capacity_exceeded"
        );
        let logs = String::from_utf8(lock_job_entry_log_buffer(&log_buffer).clone())
            .expect("cleanup trace is UTF-8");
        assert_eq!(logs.matches("job_worker_cleanup_incomplete").count(), 1);
        assert!(
            logs.contains("reason=\"cleanup_unconfirmed\""),
            "cleanup reason was missing: {logs}"
        );
        assert!(
            logs.contains(&format!("job_id={}", context.job_id())),
            "cleanup job id was missing: {logs}"
        );
        assert!(!logs.contains("injected output reader spawn failure"));
    }

    #[test]
    fn injected_command_observation_error_cleans_resources_before_releasing_or_fail_closing_reservation()
     {
        let _serial = COMMAND_RESOURCE_TEST_LOCK.blocking_lock();
        let _reset = CommandTestSeamReset;

        for cleanup_confirmed in [true, false] {
            reset_command_observation_test_seam(cleanup_confirmed);

            let manager = command_manager(1);
            let started = manager.start_command_json(test_command());
            let job_id = started["job_id"]
                .as_str()
                .expect("command job id")
                .to_string();
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let result = manager.get_result_json(&job_id);
                if result["status"] == "failed" {
                    assert_eq!(result["error"]["code"], "job_command_failed");
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "command observation worker did not report its failure"
                );
                thread::sleep(Duration::from_millis(10));
            }

            let deadline = Instant::now() + Duration::from_secs(2);
            while COMMAND_WORKER_COMPLETIONS.load(Ordering::SeqCst) == 0 {
                assert!(
                    Instant::now() < deadline,
                    "command observation worker did not finish postprocessing"
                );
                thread::yield_now();
            }

            assert_eq!(COMMAND_CHILD_KILL_ATTEMPTS.load(Ordering::SeqCst), 1);
            assert!(COMMAND_CHILD_REAPED.load(Ordering::SeqCst));
            assert_eq!(COMMAND_READER_COMPLETIONS.load(Ordering::SeqCst), 2);
            assert_eq!(COMMAND_READER_JOINS.load(Ordering::SeqCst), 2);

            let execution_finished = lock_job_entry(
                &lock_entries(&manager.inner.entries)
                    .get(&job_id)
                    .expect("command entry retained")
                    .clone(),
            )
            .execution_finished;
            assert_eq!(execution_finished, cleanup_confirmed);
            if cleanup_confirmed {
                assert!(
                    manager
                        .admit_job("reused-after-observation-cleanup")
                        .is_ok()
                );
            } else {
                assert_eq!(
                    manager
                        .admit_job("blocked-after-unconfirmed-observation-cleanup")
                        .unwrap_err()["code"],
                    "job_capacity_exceeded"
                );
            }
        }
    }

    fn lock_job_entry_log_buffer(
        buffer: &Arc<Mutex<Vec<u8>>>,
    ) -> std::sync::MutexGuard<'_, Vec<u8>> {
        buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn command_worker_launch_failure_rolls_back_entry_and_reservation() {
        let _serial = COMMAND_RESOURCE_TEST_LOCK.blocking_lock();
        let _reset = CommandTestSeamReset;
        FORCE_COMMAND_WORKER_SPAWN_FAILURE.store(true, Ordering::SeqCst);
        let manager = command_manager(1);

        let error = manager.start_command_json(test_command());

        assert_eq!(error["code"], "job_worker_spawn_failed");
        assert_eq!(
            error["message"],
            "Threadline could not start the job worker."
        );
        assert!(lock_entries(&manager.inner.entries).is_empty());
        assert!(manager.admit_job("admitted-after-rollback").is_ok());
    }

    #[test]
    fn expiry_uses_exact_ttl_boundary_and_tolerates_future_terminal_timestamps() {
        let manager = manager(3);
        let scan_time = Instant::now();

        let exact = manager.admit_job("exact-boundary").expect("exact job");
        exact.context.complete(json!({"summary": "exact"}));
        drop(exact.reservation);

        let just_before = manager.admit_job("just-before").expect("near job");
        just_before.context.complete(json!({"summary": "near"}));
        drop(just_before.reservation);

        let future = manager.admit_job("future").expect("future job");
        future.context.complete(json!({"summary": "future"}));
        drop(future.reservation);
        lock_job_entry(&exact.context.entry).finished_at =
            Some(scan_time - Duration::from_secs(60));
        lock_job_entry(&just_before.context.entry).finished_at =
            Some(scan_time - Duration::from_secs(59));
        lock_job_entry(&future.context.entry).finished_at =
            Some(scan_time + Duration::from_secs(60));

        assert_eq!(manager.prune_expired_at(scan_time), 1);
        assert_eq!(lock_entries(&manager.inner.entries).len(), 2);
        assert!(lock_entries(&manager.inner.entries).contains_key(&just_before.context.job_id()));
        assert!(lock_entries(&manager.inner.entries).contains_key(&future.context.job_id()));
    }

    #[tokio::test]
    async fn lazy_expiry_removes_eligible_entries_at_each_public_boundary() {
        let _serial = COMMAND_RESOURCE_TEST_LOCK.lock().await;

        #[derive(Debug)]
        enum Boundary {
            SpawnJob,
            StartCommand,
            DisabledStart,
            InvalidStart,
            DisallowedStart,
            PollMissing,
            ReadMissing,
            ResultMissing,
            CancelMissing,
        }

        for boundary in [
            Boundary::SpawnJob,
            Boundary::StartCommand,
            Boundary::DisabledStart,
            Boundary::InvalidStart,
            Boundary::DisallowedStart,
            Boundary::PollMissing,
            Boundary::ReadMissing,
            Boundary::ResultMissing,
            Boundary::CancelMissing,
        ] {
            let command = test_command();
            let manager = match boundary {
                Boundary::StartCommand => ttl_zero_manager(vec![command[0].clone()]),
                Boundary::DisabledStart => ThreadlineJobManager::new(ThreadlineJobManagerConfig {
                    retention_ttl: Duration::ZERO,
                    ..ThreadlineJobManagerConfig::default()
                }),
                _ => ttl_zero_manager(Vec::new()),
            };
            let expired_id = seed_entry(&manager, JobState::Completed, true);

            let response = match boundary {
                Boundary::SpawnJob => manager.spawn_job("test", |_| async {}),
                Boundary::StartCommand => manager.start_command_json(command),
                Boundary::DisabledStart => manager.start_command_json(command),
                Boundary::InvalidStart => manager.start_command_json(Vec::new()),
                Boundary::DisallowedStart => manager.start_command_json(command),
                Boundary::PollMissing => manager.poll_json("missing"),
                Boundary::ReadMissing => manager.read_output_json("missing", 0),
                Boundary::ResultMissing => manager.get_result_json("missing"),
                Boundary::CancelMissing => manager.cancel_json("missing"),
            };

            assert!(
                !registry_contains(&manager, &expired_id),
                "expired entry survived {boundary:?}"
            );
            match boundary {
                Boundary::SpawnJob | Boundary::StartCommand => {
                    assert_eq!(response["ok"], true);
                    wait_for_execution_to_finish(&manager, response["job_id"].as_str().unwrap())
                        .await;
                }
                Boundary::DisabledStart => assert_eq!(response["code"], "jobs_disabled"),
                Boundary::InvalidStart => assert_eq!(response["code"], "invalid_job_request"),
                Boundary::DisallowedStart => {
                    assert_eq!(response["code"], "job_command_not_allowed")
                }
                Boundary::PollMissing
                | Boundary::ReadMissing
                | Boundary::ResultMissing
                | Boundary::CancelMissing => assert_eq!(response["code"], "job_not_found"),
            }
        }
    }

    #[test]
    fn terminal_lookups_and_repeat_cancellation_preserve_finished_at() {
        let manager = manager(4);
        for state in [JobState::Completed, JobState::Failed, JobState::Cancelled] {
            let job_id = seed_entry(&manager, state, true);
            let finished_at = lock_job_entry(
                lock_entries(&manager.inner.entries)
                    .get(&job_id)
                    .expect("seeded entry"),
            )
            .finished_at
            .expect("terminal timestamp");

            let _ = manager.poll_json(&job_id);
            let _ = manager.poll_json(&job_id);
            let _ = manager.read_output_json(&job_id, 0);
            let _ = manager.read_output_json(&job_id, 0);
            let _ = manager.get_result_json(&job_id);
            let _ = manager.get_result_json(&job_id);
            let _ = manager.cancel_json(&job_id);
            let _ = manager.cancel_json(&job_id);

            let entries = lock_entries(&manager.inner.entries);
            let entry = lock_job_entry(entries.get(&job_id).expect("terminal entry retained"));
            assert_eq!(entry.finished_at, Some(finished_at));
        }
    }

    #[test]
    fn ttl_zero_preserves_unfinished_entries_regardless_of_public_state() {
        let manager = ttl_zero_manager(Vec::new());
        for state in [
            JobState::Starting,
            JobState::Running,
            JobState::Cancelled,
            JobState::Completed,
        ] {
            let job_id = seed_entry(&manager, state, false);
            let _ = manager.poll_json(&job_id);
            assert!(
                registry_contains(&manager, &job_id),
                "unfinished {state:?} entry was removed"
            );
        }
    }

    #[test]
    fn explicit_prune_returns_the_eligible_removal_count_without_prior_cleanup() {
        let manager = ttl_zero_manager(Vec::new());
        for state in [JobState::Completed, JobState::Failed, JobState::Cancelled] {
            seed_entry(&manager, state, true);
        }

        assert_eq!(manager.prune_expired(), 3);
        assert!(lock_entries(&manager.inner.entries).is_empty());
    }

    #[test]
    fn retained_eviction_breaks_equal_completion_times_by_job_id() {
        let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            jobs_enabled: true,
            max_active_jobs: 3,
            max_retained_jobs: 2,
            ..ThreadlineJobManagerConfig::default()
        });
        let first = manager.admit_job("first").expect("first admitted");
        let second = manager.admit_job("second").expect("second admitted");
        first.context.complete(Value::Null);
        second.context.complete(Value::Null);
        drop(first.reservation);
        drop(second.reservation);
        let timestamp = Instant::now();
        lock_job_entry(&first.context.entry).finished_at = Some(timestamp);
        lock_job_entry(&second.context.entry).finished_at = Some(timestamp);
        let expected_evicted = first.context.job_id().min(second.context.job_id());

        let admitted = manager.admit_job("third").expect("third admitted");
        drop(admitted.reservation);

        assert!(!lock_entries(&manager.inner.entries).contains_key(&expected_evicted));
    }

    #[test]
    fn active_capacity_rejection_prunes_expired_history_without_evicting_fresh_history() {
        let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            max_active_jobs: 1,
            max_retained_jobs: 3,
            retention_ttl: Duration::from_secs(60),
            ..ThreadlineJobManagerConfig::default()
        });
        let expired_id = seed_entry(&manager, JobState::Completed, true);
        let fresh_id = seed_entry(&manager, JobState::Completed, true);
        let active = manager.admit_job("active").expect("active job admitted");
        let scan_time = Instant::now();
        lock_job_entry(
            lock_entries(&manager.inner.entries)
                .get(&expired_id)
                .expect("expired entry"),
        )
        .finished_at = Some(scan_time - Duration::from_secs(60));
        lock_job_entry(
            lock_entries(&manager.inner.entries)
                .get(&fresh_id)
                .expect("fresh entry"),
        )
        .finished_at = Some(scan_time - Duration::from_secs(59));

        assert_eq!(
            manager.admit_job("rejected").unwrap_err()["code"],
            "job_capacity_exceeded"
        );
        assert!(!registry_contains(&manager, &expired_id));
        assert!(registry_contains(&manager, &fresh_id));
        drop(active.reservation);
    }

    #[test]
    fn retained_admission_prunes_expired_history_without_removing_unfinished_or_fresh_entries() {
        let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            max_active_jobs: 3,
            max_retained_jobs: 2,
            retention_ttl: Duration::from_secs(60),
            ..ThreadlineJobManagerConfig::default()
        });
        let expired_id = seed_entry(&manager, JobState::Completed, true);
        let fresh_id = seed_entry(&manager, JobState::Completed, true);
        let scan_time = Instant::now();
        lock_job_entry(
            lock_entries(&manager.inner.entries)
                .get(&expired_id)
                .expect("expired entry"),
        )
        .finished_at = Some(scan_time - Duration::from_secs(60));
        lock_job_entry(
            lock_entries(&manager.inner.entries)
                .get(&fresh_id)
                .expect("fresh entry"),
        )
        .finished_at = Some(scan_time - Duration::from_secs(59));

        let admitted = manager
            .admit_job("after-expiry")
            .expect("admitted after pruning");
        assert!(!registry_contains(&manager, &expired_id));
        assert!(registry_contains(&manager, &fresh_id));
        drop(admitted.reservation);

        let unfinished_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            max_active_jobs: 3,
            max_retained_jobs: 2,
            ..ThreadlineJobManagerConfig::default()
        });
        let running_id = seed_entry(&unfinished_manager, JobState::Running, false);
        let cancelled_id = seed_entry(&unfinished_manager, JobState::Cancelled, false);

        assert_eq!(
            unfinished_manager
                .admit_job("retained-rejected")
                .unwrap_err()["code"],
            "job_capacity_exceeded"
        );
        assert!(registry_contains(&unfinished_manager, &running_id));
        assert!(registry_contains(&unfinished_manager, &cancelled_id));

        let zero_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            max_active_jobs: 1,
            max_retained_jobs: 0,
            ..ThreadlineJobManagerConfig::default()
        });
        let fresh_id = seed_entry(&zero_manager, JobState::Completed, true);

        assert_eq!(
            zero_manager
                .admit_job("zero-retained-rejected")
                .unwrap_err()["code"],
            "job_capacity_exceeded"
        );
        assert!(registry_contains(&zero_manager, &fresh_id));
    }

    #[test]
    fn allowed_command_capacity_rejection_does_not_reach_the_worker_executor() {
        let _serial = COMMAND_RESOURCE_TEST_LOCK.blocking_lock();
        let _reset = CommandTestSeamReset;
        FORCE_COMMAND_WORKER_SPAWN_FAILURE.store(true, Ordering::SeqCst);
        let manager = command_manager(1);
        let active = manager.admit_job("active").expect("active job admitted");

        let rejected = manager.start_command_json(test_command());

        assert_eq!(rejected["code"], "job_capacity_exceeded");
        assert_eq!(lock_entries(&manager.inner.entries).len(), 1);
        drop(active.reservation);
    }

    #[test]
    fn cancellation_returns_acquired_entry_snapshot_after_registry_removal() {
        let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            max_active_jobs: 1,
            max_retained_jobs: 1,
            retention_ttl: Duration::ZERO,
            ..ThreadlineJobManagerConfig::default()
        });
        let PendingJob {
            context,
            reservation,
        } = manager.admit_job("cancelled").expect("job admitted");
        let job_id = context.job_id();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (proceed_tx, proceed_rx) = mpsc::channel();
        *manager
            .inner
            .cancel_after_entry_acquired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(CancellationInterleaveHook {
            acquired: acquired_tx,
            proceed: proceed_rx,
        });

        let cancelling_manager = manager.clone();
        let cancelling_job_id = job_id.clone();
        let cancellation =
            thread::spawn(move || cancelling_manager.cancel_json(&cancelling_job_id));
        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("cancel acquired entry");
        manager.remove_entry(&job_id);
        proceed_tx.send(()).expect("resume cancellation");

        let snapshot = cancellation.join().expect("cancellation worker");
        assert_eq!(snapshot["status"], "cancelled");
        assert_eq!(snapshot["terminal_state"], "cancelled");
        assert_eq!(manager.poll_json(&job_id)["code"], "job_not_found");
        drop(reservation);
    }
}
