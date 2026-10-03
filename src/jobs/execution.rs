use super::*;

impl ThreadlineJobManager {
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
}

impl<Fut> Future for ExecutingFuture<Fut>
where
    Fut: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
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
