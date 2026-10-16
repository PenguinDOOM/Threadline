use super::*;

struct DestructorGateFuture {
    ready: bool,
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl Future for DestructorGateFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.ready {
            Poll::Ready(())
        } else {
            self.ready = true;
            Poll::Pending
        }
    }
}

impl Drop for DestructorGateFuture {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.recv();
    }
}

struct DestructorGateRelease(Option<mpsc::Sender<()>>);

impl DestructorGateRelease {
    fn release(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for DestructorGateRelease {
    fn drop(&mut self) {
        self.release();
    }
}

fn run_gated_destructor_case(initially_ready: bool) {
    let manager = manager(1);
    let PendingJob {
        context,
        reservation,
    } = manager.admit_job("destructor-gate").expect("admitted job");
    let job_id = context.job_id();
    let result = json!({"summary": "early terminal"});
    context.complete(result.clone());
    let finished_at = lock_job_entry(&context.entry)
        .finished_at
        .expect("early terminal timestamp");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();
    let worker_context = context.clone();
    let worker = thread::spawn(move || {
        poll_destructor_future(
            worker_context,
            reservation,
            initially_ready,
            entered_tx,
            release_rx,
        );
        let _ = completed_tx.send(());
    });

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("future destructor entered");
    let mut release_guard = DestructorGateRelease(Some(release_tx));
    assert_destructor_reservation_retained(&manager, &job_id, finished_at);
    release_guard.release();
    completed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("destructor worker completion");
    worker.join().expect("destructor worker");
    let entry = lock_job_entry(&context.entry);
    assert!(entry.execution_finished);
    assert_eq!(entry.result.as_ref(), Some(&result));
    assert_eq!(entry.finished_at, Some(finished_at));
    drop(entry);
    assert!(manager.admit_job("reused-after-destructor").is_ok());
    assert_eq!(
        manager.prune_expired_at(finished_at + Duration::from_secs(60)),
        1
    );
    assert!(!registry_contains(&manager, &job_id));
}

fn assert_destructor_reservation_retained(
    manager: &ThreadlineJobManager,
    job_id: &str,
    finished_at: Instant,
) {
    assert_eq!(
        manager.admit_job("must-wait-for-destructor").unwrap_err()["code"],
        "job_capacity_exceeded"
    );
    assert_eq!(
        manager.prune_expired_at(finished_at + Duration::from_secs(60)),
        0
    );
    assert!(registry_contains(manager, job_id));
}

fn poll_destructor_future(
    context: ManagedJobContext,
    reservation: ExecutionReservation,
    initially_ready: bool,
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
) {
    let mut future = Box::pin(ExecutingFuture {
        future: Some(Box::pin(DestructorGateFuture {
            ready: initially_ready,
            entered,
            release,
        })),
        context,
        reservation: Some(reservation),
    });
    let mut task_context = Context::from_waker(std::task::Waker::noop());
    if initially_ready {
        assert!(future.as_mut().poll(&mut task_context).is_ready());
    } else {
        assert!(future.as_mut().poll(&mut task_context).is_pending());
        drop(future);
    }
}

#[test]
fn never_polled_execution_future_releases_its_reservation_after_dropping_user_future() {
    let manager = manager(1);
    let PendingJob {
        context,
        reservation,
    } = manager.admit_job("never-polled").expect("admitted job");
    let entry = Arc::clone(&context.entry);
    let future = ExecutingFuture {
        future: Some(Box::pin(pending())),
        context,
        reservation: Some(reservation),
    };

    drop(future);

    let entry = lock_job_entry(&entry);
    assert!(entry.execution_finished);
    assert_eq!(entry.state, JobState::Failed);
    assert_eq!(
        entry.error.as_ref().map(|error| error.code),
        Some("job_did_not_finalize")
    );
}

#[test]
fn unresolved_ready_execution_future_finalizes_before_releasing_its_reservation() {
    let manager = manager(1);
    let PendingJob {
        context,
        reservation,
    } = manager.admit_job("unresolved-ready").expect("admitted job");
    let entry = Arc::clone(&context.entry);
    let mut future = Box::pin(ExecutingFuture {
        future: Some(Box::pin(async {})),
        context,
        reservation: Some(reservation),
    });
    let mut task_context = Context::from_waker(std::task::Waker::noop());

    assert!(future.as_mut().poll(&mut task_context).is_ready());
    let entry = lock_job_entry(&entry);
    assert!(entry.execution_finished);
    assert_eq!(
        entry.error.as_ref().map(|error| error.code),
        Some("job_did_not_finalize")
    );
}

#[test]
fn panicking_user_future_finalizes_when_its_execution_wrapper_is_dropped() {
    let manager = manager(1);
    let PendingJob {
        context,
        reservation,
    } = manager.admit_job("panicking-future").expect("admitted job");
    let job_id = context.job_id();
    let mut future = Box::pin(ExecutingFuture {
        future: Some(Box::pin(async {
            panic!("user future panic");
        })),
        context,
        reservation: Some(reservation),
    });
    let mut task_context = Context::from_waker(std::task::Waker::noop());

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = future.as_mut().poll(&mut task_context);
    }));
    assert!(panic.is_err());
    drop(future);

    assert_eq!(
        manager.get_result_json(&job_id)["error"]["code"],
        "job_did_not_finalize"
    );
    assert!(manager.admit_job("reused-after-panic").is_ok());
}

#[test]
fn ready_future_destructor_holds_capacity_until_drop_completes() {
    run_gated_destructor_case(true);
}

#[test]
fn pending_future_destructor_holds_capacity_until_drop_completes() {
    run_gated_destructor_case(false);
}

#[test]
fn panicking_message_conversion_leaves_entry_recoverable_after_worker_drop() {
    struct PanicMessage;
    impl From<PanicMessage> for String {
        fn from(_: PanicMessage) -> Self {
            panic!("message conversion panic");
        }
    }

    let manager = manager(1);
    let PendingJob {
        context,
        reservation,
    } = manager.admit_job("conversion-panic").expect("admitted job");
    let job_id = context.job_id();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        context.fail("failure", PanicMessage)
    }));
    assert!(panic.is_err());
    drop(reservation);

    assert_eq!(
        manager.get_result_json(&job_id)["error"]["code"],
        "job_did_not_finalize"
    );
    assert!(lock_job_entry(&context.entry).finished_at.is_some());
    assert!(manager.admit_job("post-panic").is_ok());
    assert_eq!(
        manager.prune_expired_at(Instant::now() + Duration::from_secs(61)),
        2
    );
}

#[test]
fn poisoned_entry_is_recovered_for_result_admission_and_expiry() {
    let manager = manager(1);
    let PendingJob {
        context,
        reservation,
    } = manager.admit_job("poisoned-entry").expect("admitted job");
    let job_id = context.job_id();
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _entry = context.entry.lock().expect("entry lock");
        panic!("poison entry");
    }));
    drop(reservation);

    assert_eq!(
        manager.get_result_json(&job_id)["error"]["code"],
        "job_did_not_finalize"
    );
    assert!(lock_job_entry(&context.entry).finished_at.is_some());
    assert!(manager.admit_job("post-poison").is_ok());
    assert_eq!(
        manager.prune_expired_at(Instant::now() + Duration::from_secs(61)),
        2
    );
}
