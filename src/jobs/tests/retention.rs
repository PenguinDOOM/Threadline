use super::*;

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
    lock_job_entry(&exact.context.entry).finished_at = Some(scan_time - Duration::from_secs(60));
    lock_job_entry(&just_before.context.entry).finished_at =
        Some(scan_time - Duration::from_secs(59));
    lock_job_entry(&future.context.entry).finished_at = Some(scan_time + Duration::from_secs(60));

    assert_eq!(manager.prune_expired_at(scan_time), 1);
    assert_eq!(lock_entries(&manager.inner.entries).len(), 2);
    assert!(lock_entries(&manager.inner.entries).contains_key(&just_before.context.job_id()));
    assert!(lock_entries(&manager.inner.entries).contains_key(&future.context.job_id()));
}

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

fn expiry_boundary_manager(boundary: &Boundary, command: &[String]) -> ThreadlineJobManager {
    match boundary {
        Boundary::StartCommand => ttl_zero_manager(vec![command[0].clone()]),
        Boundary::DisabledStart => ThreadlineJobManager::new(ThreadlineJobManagerConfig {
            retention_ttl: Duration::ZERO,
            ..ThreadlineJobManagerConfig::default()
        }),
        _ => ttl_zero_manager(Vec::new()),
    }
}

#[tokio::test]
async fn lazy_expiry_removes_eligible_entries_at_each_public_boundary() {
    let _serial = COMMAND_RESOURCE_TEST_LOCK.lock().await;

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
        let manager = expiry_boundary_manager(&boundary, &command);
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
                wait_for_execution_to_finish(&manager, response["job_id"].as_str().unwrap()).await;
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
    set_finished_at(&manager, &expired_id, scan_time - Duration::from_secs(60));
    set_finished_at(&manager, &fresh_id, scan_time - Duration::from_secs(59));

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

fn set_finished_at(manager: &ThreadlineJobManager, job_id: &str, finished_at: Instant) {
    lock_job_entry(
        lock_entries(&manager.inner.entries)
            .get(job_id)
            .expect("retained entry"),
    )
    .finished_at = Some(finished_at);
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
    let cancellation = thread::spawn(move || cancelling_manager.cancel_json(&cancelling_job_id));
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
