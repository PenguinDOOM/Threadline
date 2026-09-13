use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use serde_json::json;
use threadline::jobs::{JobTerminalState, ThreadlineJobManager, ThreadlineJobManagerConfig};
use tokio::sync::{oneshot, watch};
use tokio::time::{Duration, sleep};

const JOB_START_NEXT_ACTION_HINT: &str = "This job is running in the background. Continue other useful work if available, then poll status or read output later when needed.";

struct FutureExitSignal(Option<oneshot::Sender<()>>);

impl Drop for FutureExitSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

struct WorkerReleaseGuard(watch::Sender<()>);

impl Drop for WorkerReleaseGuard {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

fn shell_program() -> String {
    if cfg!(windows) {
        "pwsh".to_string()
    } else {
        "sh".to_string()
    }
}

fn shell_command(script: &str) -> Vec<String> {
    if cfg!(windows) {
        vec![
            "pwsh".to_string(),
            "-NoProfile".to_string(),
            "-Command".to_string(),
            script.to_string(),
        ]
    } else {
        vec!["sh".to_string(), "-lc".to_string(), script.to_string()]
    }
}

fn shell_job_manager() -> ThreadlineJobManager {
    ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 16,
        max_retained_jobs: 128,
        allowed_commands: vec![shell_program()],
    })
}

async fn wait_for_output_items(
    manager: &ThreadlineJobManager,
    job_id: &str,
    offset: u64,
    expected_item_count: usize,
    timeout: Duration,
) -> serde_json::Value {
    let deadline = Instant::now() + timeout;
    loop {
        let output = manager.read_output_json(job_id, offset);
        if output["items"]
            .as_array()
            .map(|items| items.len() >= expected_item_count)
            .unwrap_or(false)
        {
            return output;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for job output"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_terminal_result(
    manager: &ThreadlineJobManager,
    job_id: &str,
    timeout: Duration,
) -> serde_json::Value {
    let deadline = Instant::now() + timeout;
    loop {
        let result = manager.get_result_json(job_id);
        if result["status"] == "completed"
            || result["status"] == "failed"
            || result["status"] == "cancelled"
        {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for terminal job result"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_lazy_removal(manager: &ThreadlineJobManager, job_id: &str) {
    for _ in 0..64 {
        if manager.poll_json(job_id)["code"] == "job_not_found" {
            return;
        }
        tokio::task::yield_now().await;
    }

    panic!("job was not lazily removed after its worker exited");
}

async fn assert_no_output_for(
    manager: &ThreadlineJobManager,
    job_id: &str,
    offset: u64,
    duration: Duration,
) {
    let deadline = Instant::now() + duration;
    loop {
        let output = manager.read_output_json(job_id, offset);
        assert_eq!(output["items"], json!([]));
        if Instant::now() >= deadline {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn job_manager_transitions_through_starting_running_and_completed() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 16,
        max_retained_jobs: 128,
        allowed_commands: vec![],
    });
    let (start_tx, start_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let (running_tx, running_rx) = oneshot::channel();

    let start = manager.spawn_job("contract-job", move |context| async move {
        let _ = start_rx.await;
        context.mark_running();
        let _ = running_tx.send(());
        let _ = finish_rx.await;
        context.push_stdout("alpha\n");
        context.complete(json!({"summary": "done"}));
    });

    assert!(start["ok"].as_bool().unwrap_or(false));
    let job_id = start["job_id"].as_str().expect("job id");
    assert_eq!(start["status"], "starting");
    assert_eq!(start["next_action_hint"], JOB_START_NEXT_ACTION_HINT);

    let initial_poll = manager.poll_json(job_id);
    assert_eq!(initial_poll["status"], "starting");

    let _ = start_tx.send(());
    let _ = running_rx.await;

    let running_poll = manager.poll_json(job_id);
    assert_eq!(running_poll["status"], "running");
    assert_eq!(running_poll["finished"], false);

    let _ = finish_tx.send(());
    sleep(Duration::from_millis(20)).await;

    let completed_poll = manager.poll_json(job_id);
    assert_eq!(completed_poll["status"], "completed");
    assert_eq!(completed_poll["finished"], true);

    let result = manager.get_result_json(job_id);
    assert_eq!(result["status"], "completed");
    assert_eq!(result["result"]["summary"], "done");
}

#[tokio::test]
async fn job_output_reads_are_incremental_and_bounded() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 10,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 16,
        max_retained_jobs: 128,
        allowed_commands: vec![],
    });

    let start = manager.spawn_job("buffer-job", move |context| async move {
        context.mark_running();
        context.push_stdout("12345");
        context.push_stdout("67890");
        context.push_stderr("abc");
        context.complete(json!({"summary": "buffered"}));
    });

    let job_id = start["job_id"].as_str().expect("job id");
    sleep(Duration::from_millis(20)).await;

    let output = manager.read_output_json(job_id, 0);
    assert_eq!(output["status"], "completed");
    assert_eq!(output["truncated_before"], 3);
    assert_eq!(output["next_offset"], 13);

    let items = output["items"].as_array().expect("items array");
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["offset"], 3);
    assert_eq!(items[0]["text"], "45");
    assert_eq!(items[1]["text"], "67890");
    assert_eq!(items[2]["stream"], "stderr");
    assert_eq!(items[2]["text"], "abc");

    let incremental = manager.read_output_json(job_id, 13);
    assert_eq!(incremental["items"], json!([]));
    assert_eq!(incremental["next_offset"], 13);
}

#[tokio::test]
async fn job_output_limit_and_offsets_use_utf8_bytes() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 8,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 16,
        max_retained_jobs: 128,
        allowed_commands: vec![],
    });

    let start = manager.spawn_job("utf8-buffer-job", move |context| async move {
        context.mark_running();
        context.push_stdout("éé");
        context.push_stderr("🙂");
        context.push_stdout("a");
        context.complete(json!({"summary": "utf8 buffered"}));
    });

    let job_id = start["job_id"].as_str().expect("job id");
    sleep(Duration::from_millis(20)).await;

    let output = manager.read_output_json(job_id, 0);
    assert_eq!(output["status"], "completed");
    assert_eq!(output["truncated_before"], 2);
    assert_eq!(output["next_offset"], 9);

    let items = output["items"].as_array().expect("items array");
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["offset"], 2);
    assert_eq!(items[0]["stream"], "stdout");
    assert_eq!(items[0]["text"], "é");
    assert_eq!(items[1]["offset"], 4);
    assert_eq!(items[1]["stream"], "stderr");
    assert_eq!(items[1]["text"], "🙂");
    assert_eq!(items[2]["offset"], 8);
    assert_eq!(items[2]["stream"], "stdout");
    assert_eq!(items[2]["text"], "a");

    let incremental = manager.read_output_json(job_id, 4);
    let incremental_items = incremental["items"].as_array().expect("items array");
    assert_eq!(incremental_items.len(), 2);
    assert_eq!(incremental_items[0]["offset"], 4);
    assert_eq!(incremental_items[0]["text"], "🙂");
    assert_eq!(incremental_items[1]["offset"], 8);
    assert_eq!(incremental_items[1]["text"], "a");
    assert_eq!(incremental["next_offset"], 9);
}

#[tokio::test]
async fn completed_and_cancelled_jobs_persist_until_ttl_cleanup() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 16,
        max_retained_jobs: 128,
        allowed_commands: vec![],
    });

    let completed = manager.spawn_job("ttl-complete", move |context| async move {
        context.mark_running();
        context.complete(json!({"summary": "done"}));
    });
    let completed_id = completed["job_id"]
        .as_str()
        .expect("completed job id")
        .to_string();

    let cancelled = manager.spawn_job("ttl-cancel", move |context| async move {
        context.mark_running();
        while !context.is_cancelled() {
            sleep(Duration::from_millis(5)).await;
        }
    });
    let cancelled_id = cancelled["job_id"]
        .as_str()
        .expect("cancelled job id")
        .to_string();

    sleep(Duration::from_millis(20)).await;

    let cancel = manager.cancel_json(&cancelled_id);
    assert_eq!(cancel["status"], "cancelled");
    assert_eq!(
        cancel["terminal_state"],
        JobTerminalState::Cancelled.as_str()
    );

    assert_eq!(manager.poll_json(&completed_id)["status"], "completed");
    assert_eq!(manager.poll_json(&cancelled_id)["status"], "cancelled");

    assert_eq!(manager.prune_expired(), 0);
    assert_eq!(manager.poll_json(&completed_id)["status"], "completed");
    assert_eq!(manager.poll_json(&cancelled_id)["status"], "cancelled");
}

#[tokio::test]
async fn completed_and_failed_jobs_lazily_expire_after_their_workers_exit() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::ZERO,
        max_active_jobs: 16,
        max_retained_jobs: 128,
        allowed_commands: vec![],
    });
    let (completed_tx, completed_rx) = oneshot::channel();
    let (failed_tx, failed_rx) = oneshot::channel();

    let completed = manager.spawn_job("expired-completed", move |context| async move {
        let _worker_exit = FutureExitSignal(Some(completed_tx));
        context.complete(json!({"summary": "expired"}));
    });
    let completed_id = completed["job_id"].as_str().expect("job id").to_string();
    let failed = manager.spawn_job("expired-failed", move |context| async move {
        let _worker_exit = FutureExitSignal(Some(failed_tx));
        context.fail("expected_failure", "failed before expiry");
    });
    let failed_id = failed["job_id"].as_str().expect("job id").to_string();

    completed_rx.await.expect("completed worker exited");
    failed_rx.await.expect("failed worker exited");

    wait_for_lazy_removal(&manager, &completed_id).await;
    wait_for_lazy_removal(&manager, &failed_id).await;
}

#[tokio::test]
async fn cancelled_running_work_keeps_its_slot_and_entry_until_worker_exit() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::ZERO,
        max_active_jobs: 1,
        max_retained_jobs: 2,
        allowed_commands: vec![],
    });
    let (release_tx, release_rx) = watch::channel(());
    let _release_guard = WorkerReleaseGuard(release_tx);
    let (running_tx, running_rx) = oneshot::channel();
    let (worker_exit_tx, worker_exit_rx) = oneshot::channel();

    let first = manager.spawn_job("cancelled-running", move |context| async move {
        let _worker_exit = FutureExitSignal(Some(worker_exit_tx));
        context.mark_running();
        let _ = running_tx.send(());
        let mut release_rx = release_rx;
        let _ = release_rx.changed().await;
    });
    let first_id = first["job_id"].as_str().expect("job id").to_string();
    running_rx.await.expect("worker started");

    assert_eq!(manager.poll_json(&first_id)["status"], "running");
    let cancellation_snapshot = manager.cancel_json(&first_id);
    assert_eq!(cancellation_snapshot["ok"], true);
    assert_eq!(cancellation_snapshot["status"], "cancelled");
    let rejected = manager.spawn_job("must-not-run", |_| async move {
        panic!("rejected task must not run");
    });
    assert_eq!(rejected["code"], "job_capacity_exceeded");
    assert_eq!(rejected.get("next_action_hint"), None);
    assert_eq!(manager.poll_json(&first_id)["status"], "cancelled");

    drop(_release_guard);
    worker_exit_rx
        .await
        .expect("cancelled worker future exited");
    wait_for_lazy_removal(&manager, &first_id).await;

    let accepted = manager.spawn_job("after-exit", |context| async move {
        context.complete(json!({"summary": "accepted"}));
    });
    assert_eq!(accepted["ok"], true);
}

#[tokio::test]
async fn concurrent_starts_admit_exactly_available_active_slots_without_running_rejected_closures()
{
    for (max_active_jobs, max_retained_jobs) in [(2, 4), (4, 2)] {
        let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            jobs_enabled: true,
            output_buffer_limit_bytes: 1024,
            retention_ttl: Duration::from_secs(60),
            max_active_jobs,
            max_retained_jobs,
            allowed_commands: vec![],
        });
        let start_gate = Arc::new(Barrier::new(5));
        let (release_tx, release_rx) = watch::channel(());
        let release_guard = WorkerReleaseGuard(release_tx);
        let (ran_tx, mut ran_rx) = tokio::sync::mpsc::unbounded_channel();
        let closure_invocations = Arc::new(AtomicUsize::new(0));
        let runtime = tokio::runtime::Handle::current();
        let starts = std::thread::scope(|scope| {
            let attempts = (0..4)
                .map(|index| {
                    let manager = manager.clone();
                    let start_gate = start_gate.clone();
                    let release_rx = release_rx.clone();
                    let ran_tx = ran_tx.clone();
                    let closure_invocations = closure_invocations.clone();
                    let runtime = runtime.clone();
                    scope.spawn(move || {
                        let _runtime_guard = runtime.enter();
                        start_gate.wait();
                        manager.spawn_job("concurrent", move |context| {
                            closure_invocations.fetch_add(1, Ordering::SeqCst);
                            async move {
                                ran_tx.send(index).expect("accepted closure receiver");
                                let mut release_rx = release_rx;
                                let _ = release_rx.changed().await;
                                context.complete(json!({"index": index}));
                            }
                        })
                    })
                })
                .collect::<Vec<_>>();
            start_gate.wait();
            attempts
                .into_iter()
                .map(|attempt| attempt.join().expect("start thread joined"))
                .collect::<Vec<_>>()
        });
        let admitted = starts.iter().filter(|start| start["ok"] == true).count();
        let rejected = starts
            .iter()
            .filter(|start| start["code"] == "job_capacity_exceeded")
            .count();
        let first_ran = tokio::time::timeout(Duration::from_secs(1), ran_rx.recv())
            .await
            .expect("first accepted closure started");
        let second_ran = tokio::time::timeout(Duration::from_secs(1), ran_rx.recv())
            .await
            .expect("second accepted closure started");
        drop(release_guard);
        for job_id in starts.iter().filter_map(|start| start["job_id"].as_str()) {
            wait_for_terminal_result(&manager, job_id, Duration::from_secs(1)).await;
        }
        assert_eq!(admitted, 2);
        assert_eq!(rejected, 2);
        assert_eq!(closure_invocations.load(Ordering::SeqCst), 2);
        assert_ne!(first_ran, second_ran);
        assert!(ran_rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn retained_capacity_evicts_oldest_finished_entry_without_changing_survivor_output() {
    let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 5,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 2,
        max_retained_jobs: 2,
        allowed_commands: vec![],
    });
    let (first_tx, first_rx) = oneshot::channel();
    let (second_tx, second_rx) = oneshot::channel();
    let (first_done_tx, first_done_rx) = oneshot::channel();
    let (second_done_tx, second_done_rx) = oneshot::channel();

    let first = manager.spawn_job("first", move |context| async move {
        let _worker_exit = FutureExitSignal(Some(first_done_tx));
        let _ = first_rx.await;
        context.push_stdout("éé");
        context.push_stderr("🙂");
        context.push_stdout("a");
        context.complete(json!({"summary": "survivor"}));
    });
    let second = manager.spawn_job("second", move |context| async move {
        let _worker_exit = FutureExitSignal(Some(second_done_tx));
        let _ = second_rx.await;
        context.complete(json!({"summary": "evicted"}));
    });
    let first_id = first["job_id"].as_str().expect("first id").to_string();
    let second_id = second["job_id"].as_str().expect("second id").to_string();

    let _ = second_tx.send(());
    second_done_rx.await.expect("second worker exited");
    let _ = first_tx.send(());
    first_done_rx.await.expect("first worker exited");

    assert_eq!(manager.read_output_json(&second_id, 0)["next_offset"], 0);

    let third = manager.spawn_job("third", |context| async move {
        context.complete(json!({"summary": "third"}));
    });
    assert_eq!(third["ok"], true);
    assert_eq!(manager.poll_json(&second_id)["code"], "job_not_found");
    let surviving_output = manager.read_output_json(&first_id, 0);
    assert_eq!(surviving_output["truncated_before"], 4);
    assert_eq!(surviving_output["next_offset"], 9);
    assert_eq!(
        surviving_output["items"],
        json!([
            {"offset": 4, "stream": "stderr", "text": "🙂"},
            {"offset": 8, "stream": "stdout", "text": "a"},
        ])
    );
    let survivor_result = manager.get_result_json(&first_id);
    assert_eq!(survivor_result["status"], "completed");
    assert_eq!(survivor_result["result"]["summary"], "survivor");
}

#[tokio::test]
async fn zero_active_or_retained_capacity_rejects_new_admissions() {
    for (max_active_jobs, max_retained_jobs) in [(0, 1), (1, 0), (0, 0)] {
        let manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            jobs_enabled: true,
            output_buffer_limit_bytes: 1024,
            retention_ttl: Duration::from_secs(60),
            max_active_jobs,
            max_retained_jobs,
            allowed_commands: vec![],
        });
        let rejected = manager.spawn_job("zero-capacity", |_| async move {});
        assert_eq!(rejected["code"], "job_capacity_exceeded");
    }
}

#[tokio::test]
async fn disabled_jobs_and_disallowed_commands_are_rejected_with_stable_json() {
    let disabled_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: false,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 16,
        max_retained_jobs: 128,
        allowed_commands: vec![],
    });

    let disabled = disabled_manager.start_command_json(vec!["echo".to_string()]);
    assert_eq!(disabled["ok"], false);
    assert_eq!(disabled["code"], "jobs_disabled");
    assert_eq!(disabled.get("next_action_hint"), None);

    let invalid = disabled_manager.start_command_json(Vec::new());
    assert_eq!(invalid["ok"], false);
    assert_eq!(invalid["code"], "jobs_disabled");
    assert_eq!(invalid.get("next_action_hint"), None);

    let restricted_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: true,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
        max_active_jobs: 16,
        max_retained_jobs: 128,
        allowed_commands: vec!["allowed".to_string()],
    });

    let rejected = restricted_manager.start_command_json(vec!["echo".to_string()]);
    assert_eq!(rejected["ok"], false);
    assert_eq!(rejected["code"], "job_command_not_allowed");
    assert_eq!(rejected.get("next_action_hint"), None);

    let empty = restricted_manager.start_command_json(Vec::new());
    assert_eq!(empty["ok"], false);
    assert_eq!(empty["code"], "invalid_job_request");
    assert_eq!(empty.get("next_action_hint"), None);

    assert_eq!(
        restricted_manager.poll_json("missing")["code"],
        "job_not_found"
    );
    assert_eq!(
        restricted_manager.read_output_json("missing", 0)["code"],
        "job_not_found"
    );
    assert_eq!(
        restricted_manager.get_result_json("missing")["code"],
        "job_not_found"
    );
    assert_eq!(
        restricted_manager.cancel_json("missing")["code"],
        "job_not_found"
    );
}

#[tokio::test]
async fn command_job_stdout_without_newline_becomes_visible_before_exit() {
    let manager = shell_job_manager();
    let command = if cfg!(windows) {
        shell_command("[Console]::Out.Write('partial stdout'); Start-Sleep -Milliseconds 1000")
    } else {
        shell_command("printf 'partial stdout'; sleep 1.0")
    };

    let start = manager.start_command_json(command);
    assert_eq!(start["status"], "starting");
    assert_eq!(start["next_action_hint"], JOB_START_NEXT_ACTION_HINT);

    let job_id = start["job_id"].as_str().expect("job id").to_string();
    let output = wait_for_output_items(&manager, &job_id, 0, 1, Duration::from_millis(1200)).await;

    assert_eq!(manager.poll_json(&job_id)["status"], "running");
    let items = output["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["stream"], "stdout");
    assert_eq!(items[0]["offset"], 0);
    assert_eq!(items[0]["text"], "partial stdout");
    assert_eq!(output["next_offset"], 14);

    let result = wait_for_terminal_result(&manager, &job_id, Duration::from_millis(2000)).await;
    assert_eq!(result["status"], "completed");
    assert_eq!(result["result"]["success"], true);
}

#[tokio::test]
async fn command_job_stderr_without_newline_becomes_visible_before_exit() {
    let manager = shell_job_manager();
    let command = if cfg!(windows) {
        shell_command("[Console]::Error.Write('partial stderr'); Start-Sleep -Milliseconds 1000")
    } else {
        shell_command("printf 'partial stderr' >&2; sleep 1.0")
    };

    let start = manager.start_command_json(command);
    assert_eq!(start["status"], "starting");

    let job_id = start["job_id"].as_str().expect("job id").to_string();
    let output = wait_for_output_items(&manager, &job_id, 0, 1, Duration::from_millis(1200)).await;

    assert_eq!(manager.poll_json(&job_id)["status"], "running");
    let items = output["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["stream"], "stderr");
    assert_eq!(items[0]["offset"], 0);
    assert_eq!(items[0]["text"], "partial stderr");
    assert_eq!(output["next_offset"], 14);

    let result = wait_for_terminal_result(&manager, &job_id, Duration::from_millis(2000)).await;
    assert_eq!(result["status"], "completed");
    assert_eq!(result["result"]["success"], true);
}

#[tokio::test]
async fn command_job_split_utf8_bytes_wait_for_valid_prefix_and_keep_byte_offsets() {
    let manager = shell_job_manager();
    let command = if cfg!(windows) {
        shell_command(
            "$stdout = [Console]::OpenStandardOutput(); \
            $emojiBytes = [byte[]](0xF0, 0x9F, 0x99, 0x82); \
            $asciiBytes = [byte[]](0x61); \
            $stdout.Write($emojiBytes, 0, 2); \
            $stdout.Flush(); \
            Start-Sleep -Milliseconds 1000; \
            $stdout.Write($emojiBytes, 2, 2); \
            $stdout.Flush(); \
            Start-Sleep -Milliseconds 800; \
            $stdout.Write($asciiBytes, 0, 1); \
            $stdout.Flush(); \
            Start-Sleep -Milliseconds 500",
        )
    } else {
        shell_command(
            r"printf '\360\237'; sleep 1.0; printf '\231\202'; sleep 0.8; printf 'a'; sleep 0.5",
        )
    };

    let start = manager.start_command_json(command);
    let job_id = start["job_id"].as_str().expect("job id").to_string();

    assert_no_output_for(&manager, &job_id, 0, Duration::from_millis(300)).await;
    assert_eq!(manager.poll_json(&job_id)["status"], "running");

    let emoji_output =
        wait_for_output_items(&manager, &job_id, 0, 1, Duration::from_millis(1800)).await;
    let emoji_items = emoji_output["items"].as_array().expect("items array");
    assert_eq!(emoji_items.len(), 1);
    assert_eq!(emoji_items[0]["stream"], "stdout");
    assert_eq!(emoji_items[0]["offset"], 0);
    assert_eq!(emoji_items[0]["text"], "🙂");
    assert_eq!(emoji_output["next_offset"], 4);

    let ascii_output =
        wait_for_output_items(&manager, &job_id, 4, 1, Duration::from_millis(1400)).await;
    let ascii_items = ascii_output["items"].as_array().expect("items array");
    assert_eq!(ascii_items.len(), 1);
    assert_eq!(ascii_items[0]["stream"], "stdout");
    assert_eq!(ascii_items[0]["offset"], 4);
    assert_eq!(ascii_items[0]["text"], "a");
    assert_eq!(ascii_output["next_offset"], 5);

    let result = wait_for_terminal_result(&manager, &job_id, Duration::from_millis(1200)).await;
    assert_eq!(result["status"], "completed");
    assert_eq!(result["result"]["success"], true);
}
