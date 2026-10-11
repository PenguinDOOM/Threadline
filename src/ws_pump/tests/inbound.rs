use super::*;

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
