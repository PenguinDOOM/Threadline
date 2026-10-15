use super::*;

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
