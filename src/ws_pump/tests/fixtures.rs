use super::*;

pub(super) fn handle_test_inbound(
    inbound: Option<Result<Message, TungsteniteError>>,
    state: &Arc<StdMutex<UpstreamTerminalState>>,
    diagnostics: &CloseDiagnostics,
) -> bool {
    let limits = UpstreamInboundLimits::DEFAULT;
    let (inbound_tx, _inbound_rx) = mpsc::channel(limits.max_messages());
    let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
    let pump_state = PumpState {
        inbound_tx: &inbound_tx,
        byte_budget: &byte_budget,
        limits,
        terminal_state: state,
        watchdog_policy: UpstreamWatchdogPolicy::DEFAULT,
        diagnostics,
    };
    handle_inbound_message(inbound, &pump_state, &mut None, &mut false)
}

pub(super) fn empty_close_metadata() -> UpstreamCloseMetadata {
    UpstreamCloseMetadata {
        code: None,
        reason: None,
        error: None,
    }
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
