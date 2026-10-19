use super::*;

pub(crate) fn synthetic_created() -> String {
    serde_json::json!({"type":"response.created", "response":{"id":"resp_synthetic"}}).to_string()
}

pub(crate) fn synthetic_delta(ordinal: usize) -> String {
    assert!((1..=400).contains(&ordinal));
    let mut event = serde_json::json!({"type":"response.output_text.delta", "item_id":"synthetic_item",
        "output_index":0, "content_index":0, "sequence_number":ordinal,
        "delta":format!("{ordinal:06}"), "padding":""});
    let padding = 199 - event.to_string().len();
    event["padding"] = serde_json::Value::String("x".repeat(padding));
    let text = event.to_string();
    assert_eq!(text.len(), 199);
    text
}

pub(crate) fn synthetic_completed() -> String {
    synthetic_completed_through(400)
}

pub(super) fn synthetic_completed_through(last: usize) -> String {
    let text: String = (1..=last).map(|ordinal| format!("{ordinal:06}")).collect();
    serde_json::json!({"type":"response.completed", "response":{"id":"resp_synthetic", "output":[
        {"type":"message", "role":"assistant", "content":[{"type":"output_text", "text":text}]}]}})
    .to_string()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum SyntheticSeriesKind {
    Text,
    Arguments,
}

pub(crate) struct SyntheticSeries {
    pub(crate) events: Vec<String>,
    pub(crate) semantic_text: String,
    pub(crate) delta_start: usize,
    pub(crate) metadata: serde_json::Value,
}

pub(crate) fn measurement_series(
    kind: SyntheticSeriesKind,
    deltas: usize,
    delta_bytes: usize,
) -> SyntheticSeries {
    use serde_json::{Value, json};
    assert!([16, 1536].contains(&deltas) && [199, 4096].contains(&delta_bytes));
    let text: String = (1..=deltas)
        .map(|ordinal| format!("{ordinal:06}"))
        .collect();
    let arguments = format!("{{\"s\":\"{text}\"}}");
    let is_text = matches!(kind, SyntheticSeriesKind::Text);
    let mut item = if is_text {
        json!({"id":"m0","type":"message","status":"in_progress","role":"assistant","content":[]})
    } else {
        json!({"id":"f0","type":"function_call","status":"in_progress","call_id":"c0","name":"external_probe","arguments":""})
    };
    let mut values = vec![
        json!({"type":"response.created","response":{"id":"r0","status":"in_progress","output":[]}}),
        json!({"type":"response.output_item.added","output_index":0,"item":item}),
    ];
    if is_text {
        values.push(json!({"type":"response.content_part.added","item_id":"m0","output_index":0,"content_index":0,"part":{"type":"output_text","text":"","annotations":[]}}));
    }
    let delta_start = values.len();
    for ordinal in 1..=deltas {
        let fragment = format!("{ordinal:06}");
        values.push(if is_text {
            json!({"type":"response.output_text.delta","item_id":"m0","output_index":0,"content_index":0,"delta":fragment,"padding":""})
        } else {
            let fragment = if ordinal == 1 { format!("{{\"s\":\"{fragment}") }
                else if ordinal == deltas { format!("{fragment}\"}}") } else {fragment};
            json!({"type":"response.function_call_arguments.delta","item_id":"f0","output_index":0,"delta":fragment,"padding":""})
        });
    }
    item["status"] = json!("completed");
    if is_text {
        let part = json!({"type":"output_text","text":text,"annotations":[]});
        item["content"] = json!([part]);
        values.push(json!({"type":"response.output_text.done","item_id":"m0","output_index":0,"content_index":0,"text":text}));
        values.push(json!({"type":"response.content_part.done","item_id":"m0","output_index":0,"content_index":0,"part":part}));
    } else {
        item["arguments"] = json!(arguments);
        values.push(json!({"type":"response.function_call_arguments.done","item_id":"f0","output_index":0,"arguments":arguments}));
    }
    values.push(json!({"type":"response.output_item.done","output_index":0,"item":item}));
    values.push(json!({"type":"response.completed","response":{"id":"r0","status":"completed","output":[item]}}));
    let mut events = Vec::with_capacity(values.len());
    let mut payload_bytes = 0usize;
    let mut wire_bytes = 0usize;
    for (sequence, mut value) in values.into_iter().enumerate() {
        value["sequence_number"] = json!(sequence);
        value["response_id"] = json!("r0");
        if value.get("padding").is_some() {
            let base = serde_json::to_string(&value).unwrap().len();
            value["padding"] = Value::String("x".repeat(delta_bytes.checked_sub(base).unwrap()));
        }
        let encoded = serde_json::to_string(&value).unwrap();
        assert!(encoded.len() <= 16 * 1024);
        payload_bytes = payload_bytes.checked_add(encoded.len()).unwrap();
        wire_bytes = wire_bytes
            .checked_add(encoded.len() + if encoded.len() <= 125 { 2 } else { 4 })
            .unwrap();
        assert!(wire_bytes <= 8 * 1024 * 1024);
        events.push(encoded);
    }
    let metadata = json!({"generator_version":1,"series":if is_text {"text"} else {"arguments"},
        "deltas":deltas,"frames":events.len(),"delta_start":delta_start,"delta_payload_bytes":delta_bytes,
        "frame_payload_bytes":events.iter().map(String::len).collect::<Vec<_>>(),"payload_bytes":payload_bytes,"wire_bytes":wire_bytes,
        "sequence":"q starts at zero, consecutive; response_id r0; semantic fragment is six-digit zero-padded ordinal 1..=D",
        "identity":"text m0/message/assistant; arguments f0/function_call/c0/external_probe; r0 is synthetic",
        "text_order":"created,item.added,part.added,D deltas,text.done,part.done,item.done,completed",
        "arguments_order":"created,item.added,D deltas,arguments.done,item.done,completed",
        "content":"text is concatenated semantic fragments; arguments is JSON object with s equal to text; first/last fragments carry JSON delimiters",
        "encoding":"ASCII compact JSON; delta padding x fills complete payload to target; unmasked unfragmented Text frames; empty initial content/arguments; completed output repeats same item and text/arguments; annotations empty",
        "limits":{"encoded_bytes":8388608,"payload_bytes":16384},
        "limitation":"4096-byte deltas use padding, not larger semantic arguments; no wire-arrival time is observed"});
    SyntheticSeries {
        events,
        semantic_text: text,
        delta_start,
        metadata,
    }
}

pub(crate) fn finite_warning_capture() -> (
    tracing::Dispatch,
    tracing::subscriber::DefaultGuard,
    impl Fn() -> usize,
) {
    let keepalive = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let capture = OverflowLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    (keepalive, guard, move || {
        capture
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|fields| {
                fields
                    .get("message")
                    .is_some_and(|message| message.contains("ws_pump_inbound_overflow"))
            })
            .count()
    })
}

pub(super) fn test_diagnostic_pump_state(
    enabled: bool,
) -> (OwnedPumpState, mpsc::Receiver<InboundEnvelope>) {
    let limits = UpstreamInboundLimits::DEFAULT;
    let (inbound_tx, inbound_rx) = mpsc::channel(limits.max_messages());
    (
        OwnedPumpState {
            inbound_tx,
            byte_budget: Arc::new(Semaphore::new(limits.max_bytes())),
            limits,
            terminal_state: Arc::new(StdMutex::new(UpstreamTerminalState::Open)),
            watchdog_policy: UpstreamWatchdogPolicy::DEFAULT,
            diagnostics: CloseDiagnostics::new(enabled),
        },
        inbound_rx,
    )
}

pub(super) fn handle_test_inbound(
    inbound: Option<Result<Message, TungsteniteError>>,
    state: &Arc<StdMutex<UpstreamTerminalState>>,
    diagnostics: &mut CloseDiagnostics,
) -> bool {
    let limits = UpstreamInboundLimits::DEFAULT;
    let (inbound_tx, _inbound_rx) = mpsc::channel(limits.max_messages());
    let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let mut pump_state = PumpState {
        inbound_tx: &inbound_tx,
        byte_budget: &byte_budget,
        limits,
        terminal_state: state,
        watchdog_policy: UpstreamWatchdogPolicy::DEFAULT,
        diagnostics,
    };
    handle_inbound_message(inbound, &mut pump_state, &mut None, &mut false)
}

pub(super) fn empty_close_metadata() -> UpstreamCloseMetadata {
    UpstreamCloseMetadata {
        code: None,
        reason: None,
        error: None,
    }
}

pub(super) fn assert_terminal_observation_freezes(
    observer_first: bool,
    next: UpstreamTerminalState,
) {
    let diagnostics = CloseDiagnostics::new(true);
    let terminal = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    diagnostics.capture().terminal_state = Some(Arc::clone(&terminal));
    diagnostics.capture().response = diagnostics.response.record.clone();
    if observer_first {
        diagnostics.response.record_unclassified(&terminal);
        diagnostics
            .response
            .record_response_event(&terminal, ResponseEvent::Completed);
    }
    assert!(commit_terminal(
        &terminal,
        next.clone(),
        &diagnostics,
        Some(UpstreamCloseSource::ReadError),
        None
    ));
    let frozen = diagnostics.response.response_state();
    let bytes = diagnostics.capture().bytes.clone();
    diagnostics.response.record_unclassified(&terminal);
    diagnostics
        .response
        .record_response_event(&terminal, ResponseEvent::Started);
    diagnostics.response.record_create_pending(&terminal);
    diagnostics.response.record_create_sent(&terminal);
    diagnostics.response.record_generic_send(&terminal);
    assert!(!commit_terminal(
        &terminal,
        UpstreamTerminalState::Closed(empty_close_metadata()),
        &diagnostics,
        Some(UpstreamCloseSource::PumpExitFallback),
        None
    ));
    assert_eq!(*terminal.lock().unwrap(), next);
    assert_eq!(diagnostics.response.response_state(), frozen);
    assert_eq!(
        frozen,
        Some(if observer_first {
            ResponseState::Completed
        } else {
            ResponseState::NotStarted
        })
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    assert_eq!(capture.bytes, bytes);
    let output = String::from_utf8(bytes).unwrap();
    assert!(output.contains(if observer_first {
        " response_state=completed"
    } else {
        " response_state=not_started"
    }));
}

pub(super) fn assert_redacted_peer_close(hostile: &str, reason: &'static str) {
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    let mut diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    for frame in [
        Message::Text(hostile.to_string()),
        Message::Binary(hostile.as_bytes().to_vec()),
        Message::Ping(hostile.as_bytes().to_vec()),
        Message::Pong(hostile.as_bytes().to_vec()),
    ] {
        assert!(handle_test_inbound(
            Some(Ok(frame)),
            &state,
            &mut diagnostics
        ));
        assert!(diagnostics.last_rx.is_some());
        assert_eq!(diagnostics.capture().calls, 0);
    }
    assert!(!handle_test_inbound(
        Some(Ok(Message::Close(Some(CloseFrame {
            code: CloseCode::Away,
            reason: reason.into()
        })))),
        &state,
        &mut diagnostics
    ));
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::Closed(UpstreamCloseMetadata {
            code: Some(1001),
            reason: Some(reason.to_string()),
            error: None,
        })
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    let output = String::from_utf8(capture.bytes.clone()).unwrap();
    let expected = match reason {
        "" | "going away" | "normal closure" => reason,
        _ => "[redacted]",
    };
    assert!(output.contains(&format!("reason={expected} error=- protocol_kind=-")));
    assert!(!output.contains("secret"));
    assert!(!output.contains(['\r', '\x1b']));
    assert_eq!(output.lines().count(), 1);
}

pub(super) fn assert_redacted_write_buffer_error(frame: Message) {
    let diagnostics = CloseDiagnostics::new(true);
    let state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
    let error = TungsteniteError::WriteBufferFull(frame);
    let original = error.to_string();
    record_error(
        &state,
        &error,
        UpstreamCloseSource::WriteError,
        &diagnostics,
    );
    record_close(
        &state,
        empty_close_metadata(),
        UpstreamCloseSource::PumpExitFallback,
        &diagnostics,
    );
    assert_eq!(
        *state.lock().unwrap(),
        UpstreamTerminalState::TransportClosed {
            cause: UpstreamCloseCause::Other,
            metadata: UpstreamCloseMetadata {
                code: None,
                reason: None,
                error: Some(original)
            }
        }
    );
    let capture = diagnostics.capture();
    assert_eq!(capture.calls, 1);
    let output = String::from_utf8(capture.bytes.clone()).unwrap();
    assert!(output.contains("source=write_error"));
    assert!(output.contains("error=write_buffer_full protocol_kind=-"));
    assert!(!output.contains("secret"));
    assert!(!output.contains(['\r', '\x1b']));
    assert_eq!(output.lines().count(), 1);
}

pub(super) async fn drive_test_write_operation<S>(
    writer: &mut SplitSink<WebSocketStream<S>, Message>,
    reader: &mut SplitStream<WebSocketStream<S>>,
    pump_state: &mut PumpState<'_>,
    message: Message,
    kind: UpstreamOutboundKind,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut next_ping_due = Instant::now() + UPSTREAM_PING_INTERVAL;
    drive_write_operation(
        writer,
        reader,
        pump_state,
        message,
        kind,
        PumpLivenessState {
            pending_challenge: &mut None,
            control_flush_needed: &mut false,
            next_ping_due: &mut next_ping_due,
            ping_interval: UPSTREAM_PING_INTERVAL,
        },
    )
    .await
}

pub(super) async fn connect_test_pump() -> LiveUpstreamWebSocket {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("local addr");
    let accept_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept client");
        let websocket = accept_async(stream).await.expect("accept websocket");
        let (_sink, _stream) = websocket.split();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let (stream, _) = connect_async(format!("ws://{address}"))
        .await
        .expect("connect websocket");
    let pump = LiveUpstreamWebSocket::from_stream(stream);

    drop(accept_task);
    pump
}

pub(super) struct WriteGateIo {
    pub(super) inner: DuplexStream,
    pub(super) write_started: Arc<Notify>,
    pub(super) write_open: Arc<AtomicBool>,
    pub(super) write_waker: Arc<AtomicWaker>,
    pub(super) _release_token: Option<Arc<()>>,
}

impl AsyncRead for WriteGateIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for WriteGateIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.write_started.notify_waiters();
        if !self.write_open.load(Ordering::SeqCst) {
            self.write_waker.register(context.waker());
            return if self.write_open.load(Ordering::SeqCst) {
                Pin::new(&mut self.inner).poll_write(context, buffer)
            } else {
                Poll::Pending
            };
        }
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

pub(super) struct FlushGateIo {
    pub(super) inner: DuplexStream,
    pub(super) flush_started: Arc<Notify>,
    pub(super) flush_open: Arc<AtomicBool>,
    pub(super) flush_waker: Arc<AtomicWaker>,
    pub(super) fail_on_flush: bool,
}

impl AsyncRead for FlushGateIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for FlushGateIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.fail_on_flush {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write-secret\r\n\x1b[31m",
            )));
        }
        self.flush_started.notify_waiters();
        if !self.flush_open.load(Ordering::SeqCst) {
            self.flush_waker.register(context.waker());
            return if self.flush_open.load(Ordering::SeqCst) {
                Pin::new(&mut self.inner).poll_flush(context)
            } else {
                Poll::Pending
            };
        }
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

pub(super) async fn raw_pair(
    capacity: usize,
) -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
    let (client_io, server_io) = duplex(capacity);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    (client, server)
}

pub(super) type GatedPair<S> = (
    WebSocketStream<S>,
    WebSocketStream<DuplexStream>,
    Arc<Notify>,
    Arc<AtomicBool>,
    Arc<AtomicWaker>,
);

pub(super) async fn write_gate_pair(
    capacity: usize,
    release_token: Option<Arc<()>>,
) -> GatedPair<WriteGateIo> {
    let (client_io, server_io) = duplex(capacity);
    let write_started = Arc::new(Notify::new());
    let write_open = Arc::new(AtomicBool::new(false));
    let write_waker = Arc::new(AtomicWaker::new());
    let client_io = WriteGateIo {
        inner: client_io,
        write_started: Arc::clone(&write_started),
        write_open: Arc::clone(&write_open),
        write_waker: Arc::clone(&write_waker),
        _release_token: release_token,
    };
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    (client, server, write_started, write_open, write_waker)
}

pub(super) async fn flush_gate_pair(
    capacity: usize,
    fail_on_flush: bool,
) -> GatedPair<FlushGateIo> {
    let (client_io, server_io) = duplex(capacity);
    let flush_started = Arc::new(Notify::new());
    let flush_open = Arc::new(AtomicBool::new(fail_on_flush));
    let flush_waker = Arc::new(AtomicWaker::new());
    let client_io = FlushGateIo {
        inner: client_io,
        flush_started: Arc::clone(&flush_started),
        flush_open: Arc::clone(&flush_open),
        flush_waker: Arc::clone(&flush_waker),
        fail_on_flush,
    };
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    (client, server, flush_started, flush_open, flush_waker)
}

pub(super) fn fill_gated_outbound_channel(pump: &LiveUpstreamWebSocket) {
    for _ in 0..OUTBOUND_CHANNEL_CAPACITY {
        pump.outbound_tx
            .try_send(OutboundCommand::Text("queued".to_string()))
            .expect("fill outbound channel while writer is gated");
    }
}

#[derive(Clone, Default)]
pub(super) struct OverflowLogCapture(pub(super) Arc<StdMutex<Vec<BTreeMap<String, String>>>>);

impl<S> Layer<S> for OverflowLogCapture
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
        let mut fields = EventFields::default();
        event.record(&mut fields);
        self.0
            .lock()
            .expect("overflow log capture lock")
            .push(fields.0);
    }
}

#[derive(Default)]
pub(super) struct EventFields(BTreeMap<String, String>);

impl Visit for EventFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

pub(super) type TestPeerWriter = SplitSink<WebSocketStream<DuplexStream>, Message>;
pub(super) type TestPeerReader = SplitStream<WebSocketStream<DuplexStream>>;

pub(crate) type DiagnosticGatedPump = (
    LiveUpstreamWebSocket,
    WebSocketStream<DuplexStream>,
    Arc<Notify>,
    Arc<AtomicBool>,
    Arc<AtomicWaker>,
);

impl LiveUpstreamWebSocket {
    pub(crate) async fn test_diagnostic_write_gate() -> DiagnosticGatedPump {
        let (client, server, started, open, waker) = write_gate_pair(8192, None).await;
        let pump = Self::from_stream_with_close_diagnostics(
            client,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::DEFAULT,
            true,
        );
        (pump, server, started, open, waker)
    }
}
