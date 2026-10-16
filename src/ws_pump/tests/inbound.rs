use super::*;

async fn process_gated_batch(
    pump: &LiveUpstreamWebSocket,
    server: &mut WebSocketStream<DuplexStream>,
    trace: &FiniteTrace,
    round: usize,
) {
    for ordinal in round * 32 + 1..=round * 32 + 32 {
        finite_send(server, trace, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(server, trace).await;
    assert_eq!(pump.test_queue_sample().0, round * 16 + 32);
    let first = round * 16 + 1;
    let text = trace.observe(pump.recv_text()).await.unwrap().unwrap();
    let event: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(event["sequence_number"], first);
    assert_eq!(event["delta"], format!("{first:06}"));
    let (release, mut gate) = oneshot::channel();
    assert!(futures_util::poll!(&mut gate).is_pending());
    trace.event(
        "processing_gate_pending",
        Some(first),
        None,
        Some(pump.test_queue_sample()),
        None,
    );
    release.send(()).unwrap();
    gate.await.unwrap();
    pump.observe_response_event(ResponseEvent::Progress);
    trace.event(
        "processing_gate_released",
        Some(first),
        None,
        Some(pump.test_queue_sample()),
        None,
    );
    for ordinal in first + 1..first + 16 {
        finite_receive(pump, trace, ordinal).await;
    }
    assert_eq!(pump.test_queue_sample().0, (round + 1) * 16);
}

async fn start_tested_pump(
    client: WebSocketStream<DuplexStream>,
    trace: &FiniteTrace,
    server: &mut WebSocketStream<DuplexStream>,
) -> LiveUpstreamWebSocket {
    let pump = LiveUpstreamWebSocket::test_traced_stream(client, trace.clone());
    finite_send(server, trace, 0, synthetic_created()).await;
    trace.observe(pump.recv_text()).await.unwrap().unwrap();
    pump.observe_response_event(ResponseEvent::Started);
    pump
}

async fn assert_pump_overflowed(pump: &LiveUpstreamWebSocket) {
    timeout(Duration::from_secs(2), async {
        while !pump.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        pump.terminal_state(),
        UpstreamTerminalState::InboundBufferOverflow(_)
    ));
}

async fn complete_isolated_pump(
    pump: &LiveUpstreamWebSocket,
    server: &mut WebSocketStream<DuplexStream>,
    trace: &FiniteTrace,
) {
    for ordinal in 9..=16 {
        finite_send(server, trace, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(server, trace).await;
    for ordinal in 9..=16 {
        finite_receive(pump, trace, ordinal).await;
    }
    finite_send(server, trace, 401, synthetic_completed_through(16)).await;
    trace.observe(pump.recv_text()).await.unwrap().unwrap();
    pump.observe_response_event(ResponseEvent::Completed);
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_pump_processing_gate_accumulates_then_drains_fixed_quotas() {
    let trace = FiniteTrace::new("processing_gate");
    let (client, mut server) = raw_pair(8192).await;
    let pump = LiveUpstreamWebSocket::test_traced_stream(client, trace.clone());
    finite_send(&mut server, &trace, 0, synthetic_created()).await;
    trace.observe(pump.recv_text()).await.unwrap().unwrap();
    pump.observe_response_event(ResponseEvent::Started);
    for round in 0..8 {
        process_gated_batch(&pump, &mut server, &trace, round).await;
    }
    trace.event(
        "supply_stopped",
        None,
        None,
        Some(pump.test_queue_sample()),
        None,
    );
    for ordinal in 129..=256 {
        finite_receive(&pump, &trace, ordinal).await;
    }
    assert_eq!(pump.test_queue_sample().0, 0);
    for first in (257..=400).step_by(16) {
        for ordinal in first..first + 16 {
            finite_send(&mut server, &trace, ordinal, synthetic_delta(ordinal)).await;
        }
        finite_read_barrier(&mut server, &trace).await;
        for ordinal in first..first + 16 {
            finite_receive(&pump, &trace, ordinal).await;
        }
    }
    finite_send(&mut server, &trace, 401, synthetic_completed()).await;
    trace.observe(pump.recv_text()).await.unwrap().unwrap();
    pump.observe_response_event(ResponseEvent::Completed);
    assert_eq!(pump.terminal_state(), UpstreamTerminalState::Open);
    assert_eq!(pump.test_queue_sample().0, 0);
    trace.event(
        "outcome",
        None,
        None,
        Some(pump.test_queue_sample()),
        Some("not_reproduced"),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_pump_overflow_does_not_affect_other_pump_fifo_or_control_traffic() {
    let trace_a = FiniteTrace::new("isolation_paused");
    let trace_b = FiniteTrace::for_connection("isolation_active", 1);
    let (client_a, mut server_a) = raw_pair(8192).await;
    let (client_b, mut server_b) = raw_pair(8192).await;
    let pump_a = start_tested_pump(client_a, &trace_a, &mut server_a).await;
    let pump_b = start_tested_pump(client_b, &trace_b, &mut server_b).await;
    for ordinal in 1..=256 {
        finite_send(&mut server_a, &trace_a, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(&mut server_a, &trace_a).await;
    for ordinal in 1..=8 {
        finite_send(&mut server_b, &trace_b, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(&mut server_b, &trace_b).await;
    for ordinal in 1..=8 {
        finite_receive(&pump_b, &trace_b, ordinal).await;
    }
    finite_send(&mut server_a, &trace_a, 257, synthetic_delta(257)).await;
    assert_pump_overflowed(&pump_a).await;
    complete_isolated_pump(&pump_b, &mut server_b, &trace_b).await;
    assert_eq!(pump_b.terminal_state(), UpstreamTerminalState::Open);
    assert_eq!(pump_b.test_queue_sample().0, 0);
    trace_a.event(
        "outcome",
        Some(257),
        None,
        Some(pump_a.test_queue_sample()),
        Some("expected_overflow"),
    );
    trace_b.event(
        "outcome",
        Some(16),
        None,
        Some(pump_b.test_queue_sample()),
        Some("not_reproduced"),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_pump_acknowledged_batches_recover_from_empty_and_preloaded_queues() {
    for preload in [0, 192] {
        let trace = FiniteTrace::new(if preload == 0 {
            "alternating"
        } else {
            "preloaded"
        });
        let (client, mut server) = raw_pair(8192).await;
        let pump = LiveUpstreamWebSocket::test_traced_stream(client, trace.clone());
        finite_send(&mut server, &trace, 0, synthetic_created()).await;
        trace.observe(pump.recv_text()).await.unwrap().unwrap();
        pump.observe_response_event(ResponseEvent::Started);
        for ordinal in 1..=preload {
            finite_send(&mut server, &trace, ordinal, synthetic_delta(ordinal)).await;
        }
        finite_read_barrier(&mut server, &trace).await;
        assert_eq!(pump.test_queue_sample().0, preload);
        trace.event("resume", None, None, Some(pump.test_queue_sample()), None);
        for ordinal in 1..=preload {
            finite_receive(&pump, &trace, ordinal).await;
        }
        assert_eq!(pump.test_queue_sample().0, 0);
        for first in (preload + 1..=400).step_by(16) {
            for ordinal in first..first + 16 {
                finite_send(&mut server, &trace, ordinal, synthetic_delta(ordinal)).await;
            }
            finite_read_barrier(&mut server, &trace).await;
            for ordinal in first..first + 16 {
                finite_receive(&pump, &trace, ordinal).await;
            }
            assert_eq!(pump.test_queue_sample().0, 0);
        }
        finite_send(&mut server, &trace, 401, synthetic_completed()).await;
        let completed = trace.observe(pump.recv_text()).await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&completed).unwrap()["type"],
            "response.completed"
        );
        pump.observe_response_event(ResponseEvent::Completed);
        assert_eq!(pump.test_queue_sample().0, 0);
        assert_eq!(pump.terminal_state(), UpstreamTerminalState::Open);
        trace.event(
            "outcome",
            None,
            None,
            Some(pump.test_queue_sample()),
            Some("not_reproduced"),
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_pump_paused_consumer_overflows_only_after_exact_default_count_bound() {
    let trace = FiniteTrace::new("paused");
    let (client, mut server) = raw_pair(8192).await;
    let pump = LiveUpstreamWebSocket::test_traced_stream(client, trace.clone());
    finite_send(&mut server, &trace, 0, synthetic_created()).await;
    trace.observe(pump.recv_text()).await.unwrap().unwrap();
    pump.observe_response_event(ResponseEvent::Started);
    for ordinal in 1..=256 {
        finite_send(&mut server, &trace, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(&mut server, &trace).await;
    assert_eq!(pump.test_queue_sample(), (256, 256, 256));
    assert_eq!(pump.terminal_state(), UpstreamTerminalState::Open);
    finite_send(&mut server, &trace, 257, synthetic_delta(257)).await;
    timeout(Duration::from_secs(2), async {
        while !pump.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        pump.terminal_state(),
        UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
            cause: InboundBufferOverflowCause::MessageCount,
            queued_messages: 256,
            queued_bytes: 256 * 199,
            incoming_bytes: 199,
            max_messages: 256,
            max_bytes: 16 * 1024 * 1024,
        })
    );
    assert_eq!(
        pump.recv_text().await,
        Err(UpstreamWebSocketError::InboundBufferOverflow)
    );
    assert_eq!(pump.test_queue_sample(), (256, 257, 256));
    trace.event(
        "outcome",
        Some(257),
        None,
        Some(pump.test_queue_sample()),
        Some("expected_overflow"),
    );
}

#[test]
fn inbound_enqueue_failure_and_envelope_drop_return_byte_permits() {
    let limits = UpstreamInboundLimits::new(1, 4).expect("valid limits");
    let (sender, mut receiver) = mpsc::channel(limits.max_messages());
    let budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));

    assert!(try_enqueue_inbound(
        &sender,
        &budget,
        limits,
        "four".to_string(),
        &terminal_state,
        &CloseDiagnostics::DISABLED,
    ));
    assert_eq!(budget.available_permits(), 0);

    assert!(!try_enqueue_inbound(
        &sender,
        &budget,
        limits,
        "next".to_string(),
        &terminal_state,
        &CloseDiagnostics::DISABLED,
    ));
    assert_eq!(budget.available_permits(), 0);
    assert!(matches!(
        *terminal_state.lock().expect("terminal state lock"),
        UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
            cause: InboundBufferOverflowCause::PayloadBytes,
            ..
        })
    ));

    drop(receiver.try_recv().expect("queued envelope"));
    assert_eq!(budget.available_permits(), limits.max_bytes());

    drop(receiver);
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    assert!(!try_enqueue_inbound(
        &sender,
        &budget,
        limits,
        "four".to_string(),
        &terminal_state,
        &CloseDiagnostics::DISABLED,
    ));
    assert_eq!(budget.available_permits(), limits.max_bytes());
    assert!(matches!(
        *terminal_state.lock().expect("terminal state lock"),
        UpstreamTerminalState::Open
    ));
}

#[test]
fn inbound_payload_size_check_precedes_semaphore_permit_conversion() {
    let limits = UpstreamInboundLimits::new(1, 4).expect("valid limits");

    assert!(payload_fits_inbound_byte_limit(4, limits));
    assert!(!payload_fits_inbound_byte_limit(5, limits));
    #[cfg(target_pointer_width = "64")]
    {
        let unrepresentable_permit_count =
            usize::try_from(u32::MAX).expect("u32 maximum fits usize") + 1;
        assert!(!payload_fits_inbound_byte_limit(
            unrepresentable_permit_count,
            limits
        ));
        assert!(u32::try_from(unrepresentable_permit_count).is_err());
    }
}

#[test]
fn transport_overflow_classifies_only_message_too_long() {
    let limits = UpstreamInboundLimits::new(1, 4).expect("valid limits");
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let (sender, _receiver) = mpsc::channel(limits.max_messages());
    let budget = Arc::new(Semaphore::new(limits.max_bytes()));

    assert!(!record_transport_overflow(
        &state,
        &sender,
        &budget,
        limits,
        &TungsteniteError::Capacity(CapacityError::TooManyHeaders),
        &CloseDiagnostics::DISABLED,
    ));
    assert!(matches!(
        *state.lock().expect("terminal state lock"),
        UpstreamTerminalState::Open
    ));
    assert!(!record_transport_overflow(
        &state,
        &sender,
        &budget,
        limits,
        &TungsteniteError::WriteBufferFull(Message::Text("queued".to_string())),
        &CloseDiagnostics::DISABLED,
    ));
    assert!(matches!(
        *state.lock().expect("terminal state lock"),
        UpstreamTerminalState::Open
    ));
}

#[test]
fn transport_overflow_records_pending_queue_occupancy_and_logs_once() {
    let limits = UpstreamInboundLimits::new(2, 8).expect("valid limits");
    let (sender, _receiver) = mpsc::channel(limits.max_messages());
    let budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    assert!(try_enqueue_inbound(
        &sender,
        &budget,
        limits,
        "four".to_string(),
        &state,
        &CloseDiagnostics::DISABLED,
    ));
    let overflow_error = TungsteniteError::Capacity(CapacityError::MessageTooLong {
        size: 126,
        max_size: 125,
    });
    let capture = OverflowLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    tracing::subscriber::with_default(subscriber, || {
        assert!(record_transport_overflow(
            &state,
            &sender,
            &budget,
            limits,
            &overflow_error,
            &CloseDiagnostics::DISABLED,
        ));
        assert!(record_transport_overflow(
            &state,
            &sender,
            &budget,
            limits,
            &overflow_error,
            &CloseDiagnostics::DISABLED,
        ));
    });

    assert!(matches!(
        *state.lock().expect("terminal state lock"),
        UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
            cause: InboundBufferOverflowCause::TransportSize,
            queued_messages: 1,
            queued_bytes: 4,
            incoming_bytes: 126,
            ..
        })
    ));
    assert_transport_overflow_log(&capture);
}

fn assert_transport_overflow_log(capture: &OverflowLogCapture) {
    let events = capture.0.lock().expect("overflow log capture lock");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].get("queued_messages"), Some(&"1".to_string()));
    assert_eq!(events[0].get("queued_bytes"), Some(&"4".to_string()));
    assert!(
        events[0]
            .values()
            .all(|value| !value.contains("transport-payload-sentinel"))
    );
}

#[test]
fn inbound_overflow_logs_one_structured_event_without_payload_contents() {
    let limits = UpstreamInboundLimits::new(1, 4).expect("valid limits");
    let (sender, _receiver) = mpsc::channel(limits.max_messages());
    let budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let capture = OverflowLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    tracing::subscriber::with_default(subscriber, || {
        assert!(!try_enqueue_inbound(
            &sender,
            &budget,
            limits,
            "diagnostic-payload-sentinel".to_string(),
            &terminal_state,
            &CloseDiagnostics::DISABLED,
        ));
    });

    let events = capture.0.lock().expect("overflow log capture lock");
    assert_eq!(events.len(), 1);
    let fields = &events[0];
    assert_eq!(
        fields.get("overflow_cause"),
        Some(&"PayloadBytes".to_string())
    );
    assert_eq!(fields.get("queued_messages"), Some(&"0".to_string()));
    assert_eq!(fields.get("queued_bytes"), Some(&"0".to_string()));
    assert_eq!(fields.get("incoming_bytes"), Some(&"27".to_string()));
    assert_eq!(fields.get("max_messages"), Some(&"1".to_string()));
    assert_eq!(fields.get("max_bytes"), Some(&"4".to_string()));
    assert_eq!(fields.get("recoverable"), Some(&"false".to_string()));
    assert!(
        fields
            .values()
            .all(|value| !value.contains("diagnostic-payload-sentinel"))
    );
}
