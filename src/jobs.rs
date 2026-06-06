use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ThreadlineJobManager {
    inner: Arc<ThreadlineJobManagerInner>,
}

#[derive(Debug)]
struct ThreadlineJobManagerInner {
    config: ThreadlineJobManagerConfig,
    entries: Mutex<HashMap<String, Arc<Mutex<JobEntry>>>>,
}

#[derive(Debug, Clone)]
pub struct ThreadlineJobManagerConfig {
    pub jobs_enabled: bool,
    pub output_buffer_limit_bytes: usize,
    pub retention_ttl: Duration,
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
            }),
        }
    }

    pub fn spawn_job<F, Fut>(&self, name: &str, task: F) -> Value
    where
        F: FnOnce(ManagedJobContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let context = self.insert_job(name);
        let job_id = context.job_id();
        let spawned_context = context.clone();

        tokio::spawn(async move {
            task(spawned_context.clone()).await;
            spawned_context.fail_if_unresolved();
        });

        json!({
            "ok": true,
            "job_id": job_id,
            "status": JobState::Starting.as_str(),
        })
    }

    pub fn start_command_json(&self, command: Vec<String>) -> Value {
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

        let context = self.insert_job("command");
        let job_id = context.job_id();
        thread::spawn(move || run_command_job(context, command));

        json!({
            "ok": true,
            "job_id": job_id,
            "status": JobState::Starting.as_str(),
        })
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

        let entry = entry.lock().expect("job entry lock");
        let output = entry.output.read_from(offset);
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

        let entry = entry.lock().expect("job entry lock");
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

        let child = {
            let mut entry = entry.lock().expect("job entry lock");
            entry.cancel_requested = true;
            if !entry.state.is_terminal() {
                entry.state = JobState::Cancelled;
                entry.result = None;
                entry.error = Some(JobFailurePayload {
                    code: "job_cancelled",
                    message: "The Threadline job was cancelled.".to_string(),
                });
                entry.finished_at = Some(Instant::now());
            }
            entry.child.clone()
        };

        if let Some(child) = child {
            let _ = child.lock().expect("child lock").kill();
        }

        self.poll_json(job_id)
    }

    pub fn prune_expired(&self) -> usize {
        let now = Instant::now();
        let ttl = self.inner.config.retention_ttl;
        let mut removed = 0usize;

        self.inner
            .entries
            .lock()
            .expect("entries lock")
            .retain(|_, entry| {
                let keep = {
                    let entry = entry.lock().expect("job entry lock");
                    match entry.finished_at {
                        Some(finished_at) => now.duration_since(finished_at) < ttl,
                        None => true,
                    }
                };
                if !keep {
                    removed += 1;
                }
                keep
            });

        removed
    }

    fn insert_job(&self, name: &str) -> ManagedJobContext {
        let job_id = Uuid::now_v7().to_string();
        let entry = Arc::new(Mutex::new(JobEntry {
            job_id,
            name: name.to_string(),
            state: JobState::Starting,
            output: JobOutputRingBuffer::new(self.inner.config.output_buffer_limit_bytes),
            result: None,
            error: None,
            cancel_requested: false,
            child: None,
            finished_at: None,
        }));

        let job_id = entry.lock().expect("job entry lock").job_id.clone();
        self.inner
            .entries
            .lock()
            .expect("entries lock")
            .insert(job_id, Arc::clone(&entry));

        ManagedJobContext { entry }
    }

    fn entry(&self, job_id: &str) -> Option<Arc<Mutex<JobEntry>>> {
        self.inner
            .entries
            .lock()
            .expect("entries lock")
            .get(job_id)
            .cloned()
    }

    fn command_allowed(&self, program: &str) -> bool {
        self.inner
            .config
            .allowed_commands
            .iter()
            .any(|allowed| allowed == program)
    }
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
        let mut entry = self.entry.lock().expect("job entry lock");
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
        let mut entry = self.entry.lock().expect("job entry lock");
        if entry.state.is_terminal() {
            return;
        }

        entry.state = JobState::Completed;
        entry.result = Some(result);
        entry.error = None;
        entry.child = None;
        entry.finished_at = Some(Instant::now());
    }

    pub fn fail(&self, code: &'static str, message: impl Into<String>) {
        let mut entry = self.entry.lock().expect("job entry lock");
        if entry.state.is_terminal() {
            return;
        }

        entry.state = JobState::Failed;
        entry.result = None;
        entry.error = Some(JobFailurePayload {
            code,
            message: message.into(),
        });
        entry.child = None;
        entry.finished_at = Some(Instant::now());
    }

    pub fn is_cancelled(&self) -> bool {
        self.entry.lock().expect("job entry lock").cancel_requested
    }

    pub fn job_id(&self) -> String {
        self.entry.lock().expect("job entry lock").job_id.clone()
    }

    fn push_output(&self, stream: &'static str, text: &str) {
        let mut entry = self.entry.lock().expect("job entry lock");
        if entry.state.is_terminal() || text.is_empty() {
            return;
        }

        entry.output.append(stream, text);
    }

    fn attach_child(&self, child: Arc<Mutex<Child>>) {
        let mut entry = self.entry.lock().expect("job entry lock");
        if entry.state.is_terminal() {
            return;
        }
        entry.child = Some(child);
    }

    fn clear_child(&self) {
        self.entry.lock().expect("job entry lock").child = None;
    }

    fn fail_if_unresolved(&self) {
        let mut entry = self.entry.lock().expect("job entry lock");
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
    let entry = entry.lock().expect("job entry lock");
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

fn run_command_job(context: ManagedJobContext, command: Vec<String>) {
    context.mark_running();

    let mut child = match spawn_command(&command) {
        Ok(child) => child,
        Err(error) => {
            context.fail(
                "job_command_spawn_failed",
                format!("Threadline could not start the requested command: {error}"),
            );
            return;
        }
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let child = Arc::new(Mutex::new(child));
    context.attach_child(Arc::clone(&child));

    let stdout_reader = stdout.map(|stdout| spawn_output_reader(stdout, context.clone(), "stdout"));
    let stderr_reader = stderr.map(|stderr| spawn_output_reader(stderr, context.clone(), "stderr"));

    let status = loop {
        if context.is_cancelled() {
            let _ = child.lock().expect("child lock").kill();
        }

        match child.lock().expect("child lock").try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(10)),
            Err(error) => {
                context.clear_child();
                join_reader(stdout_reader);
                join_reader(stderr_reader);
                context.fail(
                    "job_command_failed",
                    format!("Threadline could not observe the command status: {error}"),
                );
                return;
            }
        }
    };

    join_reader(stdout_reader);
    join_reader(stderr_reader);
    context.clear_child();

    if context.is_cancelled() {
        return;
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
}

fn spawn_command(command: &[String]) -> Result<Child, std::io::Error> {
    let mut child = Command::new(&command[0]);
    if command.len() > 1 {
        child.args(&command[1..]);
    }

    child.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()
}

fn spawn_output_reader<R>(
    reader: R,
    context: ManagedJobContext,
    stream: &'static str,
) -> thread::JoinHandle<()>
where
    R: std::io::Read + Send + 'static,
{
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        let mut buffer = Vec::new();

        loop {
            buffer.clear();
            match reader.read_until(b'\n', &mut buffer) {
                Ok(0) => break,
                Ok(_) => {
                    let text = String::from_utf8_lossy(&buffer).to_string();
                    match stream {
                        "stdout" => context.push_stdout(&text),
                        "stderr" => context.push_stderr(&text),
                        _ => {}
                    }
                }
                Err(_) => break,
            }
        }
    })
}

fn join_reader(reader: Option<thread::JoinHandle<()>>) {
    if let Some(reader) = reader {
        let _ = reader.join();
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
