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

fn seed_entry(manager: &ThreadlineJobManager, state: JobState, execution_finished: bool) -> String {
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

mod command;
mod execution;
mod retention;
