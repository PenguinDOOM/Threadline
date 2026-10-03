use super::*;

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
}

impl ThreadlineJobManager {
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

impl JobTerminalState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
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
