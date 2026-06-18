use std::time::Instant;

use serde_json::json;
use threadline::jobs::{JobTerminalState, ThreadlineJobManager, ThreadlineJobManagerConfig};
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};

const JOB_START_NEXT_ACTION_HINT: &str = "This job is running in the background. Continue other useful work if available, then poll status or read output later when needed.";

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
        retention_ttl: Duration::from_millis(30),
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

    sleep(Duration::from_millis(40)).await;
    assert_eq!(manager.prune_expired(), 2);

    assert_eq!(manager.poll_json(&completed_id)["code"], "job_not_found");
    assert_eq!(manager.poll_json(&cancelled_id)["code"], "job_not_found");
}

#[tokio::test]
async fn disabled_jobs_and_disallowed_commands_are_rejected_with_stable_json() {
    let disabled_manager = ThreadlineJobManager::new(ThreadlineJobManagerConfig {
        jobs_enabled: false,
        output_buffer_limit_bytes: 1024,
        retention_ttl: Duration::from_secs(60),
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
