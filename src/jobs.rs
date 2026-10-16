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

mod admission;
mod command;
mod execution;
mod state;

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
        prepare_admission_capacity(&mut entries, now, &self.inner.config)?;

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

fn prepare_admission_capacity(
    entries: &mut HashMap<String, Arc<Mutex<JobEntry>>>,
    now: Instant,
    config: &ThreadlineJobManagerConfig,
) -> Result<(), Value> {
    admission::prepare_capacity(entries, now, config)
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

impl ManagedJobContext {
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
    command::execute_command_job(context, command)
}

fn mark_cleanup_incomplete(context: &ManagedJobContext) {
    let entry = lock_job_entry(&context.entry);
    warn!(job_id = %entry.job_id, reason = "cleanup_unconfirmed", "job_worker_cleanup_incomplete");
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
mod tests;
