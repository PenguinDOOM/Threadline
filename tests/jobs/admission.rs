use super::*;

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
        let (release_tx, release_rx) = watch::channel(());
        let release_guard = WorkerReleaseGuard(release_tx);
        let (ran_tx, mut ran_rx) = tokio::sync::mpsc::unbounded_channel();
        let closure_invocations = Arc::new(AtomicUsize::new(0));
        let starts = concurrent_job_starts(
            &manager,
            release_rx,
            ran_tx,
            Arc::clone(&closure_invocations),
        );
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

fn concurrent_job_starts(
    manager: &ThreadlineJobManager,
    release_rx: watch::Receiver<()>,
    ran_tx: tokio::sync::mpsc::UnboundedSender<usize>,
    closure_invocations: Arc<AtomicUsize>,
) -> Vec<serde_json::Value> {
    let start_gate = Arc::new(Barrier::new(5));
    let runtime = tokio::runtime::Handle::current();
    std::thread::scope(|scope| {
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
    })
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
    assert_surviving_output(&manager, &first_id);
}

fn assert_surviving_output(manager: &ThreadlineJobManager, first_id: &str) {
    let surviving_output = manager.read_output_json(first_id, 0);
    assert_eq!(surviving_output["truncated_before"], 4);
    assert_eq!(surviving_output["next_offset"], 9);
    assert_eq!(
        surviving_output["items"],
        json!([
            {"offset": 4, "stream": "stderr", "text": "🙂"},
            {"offset": 8, "stream": "stdout", "text": "a"},
        ])
    );
    let survivor_result = manager.get_result_json(first_id);
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
