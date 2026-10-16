use super::*;

pub(super) fn drain_ready_queue(
    receiver: &mut mpsc::Receiver<InboundEnvelope>,
    diagnostics: &CloseDiagnostics,
    terminal: &Arc<StdMutex<UpstreamTerminalState>>,
    expected: &mut usize,
    deltas: usize,
) {
    while let Ok(envelope) = receiver.try_recv() {
        let event: serde_json::Value = serde_json::from_str(&envelope.payload).unwrap();
        match *expected {
            0 => assert_eq!(event["type"], "response.created"),
            ordinal if ordinal == deltas + 1 => assert_eq!(event["type"], "response.completed"),
            _ => assert_eq!(event["sequence_number"], *expected),
        }
        diagnostics
            .response
            .record_dequeue(terminal, envelope.payload.len());
        diagnostics.response.record_response_event(
            terminal,
            match *expected {
                0 => ResponseEvent::Started,
                ordinal if ordinal == deltas + 1 => ResponseEvent::Completed,
                _ => ResponseEvent::Progress,
            },
        );
        *expected += 1;
    }
    assert_eq!(receiver.len(), 0);
    assert_eq!(diagnostics.response.test_queue_sample().0, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_pump_ready_source_drains_between_pending_polls() {
    let trace = FiniteTrace::new("ready_scheduled");
    let (client, open, reader) = finite_ready_pair(&trace).await;
    let (owned, mut receiver) = test_diagnostic_pump_state(true);
    owned.diagnostics.response.test_set_trace(trace.clone());
    let diagnostics = owned.diagnostics.clone();
    let terminal = owned.terminal_state.clone();
    let budget = owned.byte_budget.clone();
    let (_outbound, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
    let pump = observe_pump_future(
        run_pump(client, outbound_rx, owned, UPSTREAM_PING_INTERVAL),
        Some(trace.clone()),
    );
    tokio::pin!(pump);
    assert!(futures_util::poll!(&mut pump).is_pending());
    {
        let waiting = trace.observe(receiver.recv());
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
    }
    open.store(true, Ordering::SeqCst);
    reader.wake();
    trace.event("input_gate_open", None, None, None, None);
    drive_ready_schedule(
        pump.as_mut(),
        &mut receiver,
        &diagnostics,
        &terminal,
        &budget,
        &trace,
    )
    .await;
}

async fn drive_ready_schedule<F: std::future::Future<Output = ()>>(
    mut pump: Pin<&mut F>,
    receiver: &mut mpsc::Receiver<InboundEnvelope>,
    diagnostics: &CloseDiagnostics,
    terminal: &Arc<StdMutex<UpstreamTerminalState>>,
    budget: &Semaphore,
    trace: &FiniteTrace,
) {
    let mut expected = 0;
    for cycle in 0..16 {
        let ended = futures_util::poll!(&mut pump).is_ready();
        if ended {
            assert_eq!(cycle, 0);
            assert_eq!(receiver.len(), 256);
            assert_ready_outcome(diagnostics, terminal, trace, false);
            return;
        }
        tokio::task::yield_now().await;
        drain_ready_queue(receiver, diagnostics, terminal, &mut expected, 400);
        trace.event(
            "queue_drained",
            Some(expected),
            None,
            Some(diagnostics.response.test_queue_sample()),
            None,
        );
        assert_eq!(
            budget.available_permits(),
            DEFAULT_UPSTREAM_INBOUND_MAX_BYTES
        );
        if expected == 402 {
            assert_ready_outcome(diagnostics, terminal, trace, true);
            return;
        }
    }
    panic!("finite scheduled pump did not finish input");
}

fn assert_ready_outcome(
    diagnostics: &CloseDiagnostics,
    terminal: &Arc<StdMutex<UpstreamTerminalState>>,
    trace: &FiniteTrace,
    completed: bool,
) {
    let baseline_expected = std::env::var("THREADLINE_TEST_COOPERATIVE_EXPECT")
        .ok()
        .as_deref()
        == Some("baseline");
    if completed {
        assert_eq!(*terminal.lock().unwrap(), UpstreamTerminalState::Open);
    } else {
        assert!(matches!(
            *terminal.lock().unwrap(),
            UpstreamTerminalState::InboundBufferOverflow(_)
        ));
    }
    trace.event(
        "outcome",
        Some(if completed { 402 } else { 257 }),
        None,
        Some(diagnostics.response.test_queue_sample()),
        Some(if completed {
            "candidate_complete"
        } else {
            "baseline_overflow"
        }),
    );
    assert_ne!(completed, baseline_expected);
}

async fn receive_ready_source(
    pump: Arc<LiveUpstreamWebSocket>,
    trace: FiniteTrace,
    pending_tx: oneshot::Sender<()>,
) -> usize {
    let created = trace.observe(pump.recv_text());
    tokio::pin!(created);
    assert!(futures_util::poll!(&mut created).is_pending());
    pending_tx.send(()).unwrap();
    let mut expected = 0;
    let mut next = created.await;
    loop {
        match next {
            Ok(Some(text)) => {
                let event: serde_json::Value = serde_json::from_str(&text).unwrap();
                if expected == 0 {
                    assert_eq!(event["type"], "response.created");
                    pump.observe_response_event(ResponseEvent::Started);
                } else if expected <= 400 {
                    assert_eq!(event["sequence_number"], expected);
                    pump.observe_response_event(ResponseEvent::Progress);
                } else {
                    assert_eq!(event["type"], "response.completed");
                    pump.observe_response_event(ResponseEvent::Completed);
                    return expected;
                }
                expected += 1;
            }
            Err(UpstreamWebSocketError::InboundBufferOverflow) => return expected,
            other => panic!("unexpected finite ready-source receive: {other:?}"),
        }
        next = trace.observe(pump.recv_text()).await;
    }
}

async fn observe_ready_consumer(name: &'static str, workers: usize) {
    let trace = FiniteTrace::with_workers(name, workers);
    let (client, open, reader) = finite_ready_pair(&trace).await;
    let pump = Arc::new(LiveUpstreamWebSocket::test_traced_stream(
        client,
        trace.clone(),
    ));
    let (pending_tx, pending_rx) = oneshot::channel();
    let consumer = tokio::spawn(receive_ready_source(
        pump.clone(),
        trace.clone(),
        pending_tx,
    ));
    pending_rx.await.unwrap();
    trace.event(
        "input_gate_open",
        None,
        None,
        Some(pump.test_queue_sample()),
        None,
    );
    open.store(true, Ordering::SeqCst);
    reader.wake();
    let progressed = timeout(Duration::from_secs(2), consumer)
        .await
        .unwrap()
        .unwrap();
    let outcome = match pump.terminal_state() {
        UpstreamTerminalState::Open => {
            assert_eq!(progressed, 401);
            "not_reproduced"
        }
        UpstreamTerminalState::InboundBufferOverflow(_) => "expected_overflow",
        other => panic!("unexpected finite ready-source terminal: {other:?}"),
    };
    trace.event(
        "outcome",
        Some(progressed),
        None,
        Some(pump.test_queue_sample()),
        Some(outcome),
    );
    if outcome == "expected_overflow" {
        trace.diagnostic(&pump.diagnostic_output().1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_pump_ready_source_records_natural_consumer_progress() {
    observe_ready_consumer("ready_source", 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_pump_ready_source_records_two_worker_consumer_progress() {
    observe_ready_consumer("ready_source_two_workers", 2).await;
}
