use super::*;

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

#[test]
fn unconfirmed_command_cleanup_keeps_execution_reservation_fail_closed() {
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
        wait_for_command_observation_failure(&manager, &job_id);
        assert_command_resources_cleaned();

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

fn wait_for_command_observation_failure(manager: &ThreadlineJobManager, job_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let result = manager.get_result_json(job_id);
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
}

fn assert_command_resources_cleaned() {
    assert_eq!(COMMAND_CHILD_KILL_ATTEMPTS.load(Ordering::SeqCst), 1);
    assert!(COMMAND_CHILD_REAPED.load(Ordering::SeqCst));
    assert_eq!(COMMAND_READER_COMPLETIONS.load(Ordering::SeqCst), 2);
    assert_eq!(COMMAND_READER_JOINS.load(Ordering::SeqCst), 2);
}

fn lock_job_entry_log_buffer(buffer: &Arc<Mutex<Vec<u8>>>) -> std::sync::MutexGuard<'_, Vec<u8>> {
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
