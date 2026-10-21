use super::finite_trace::{ReadyInputIo, finite_ready_io};
use super::inbound_ready::drain_ready_queue;
use super::*;

#[derive(Default)]
struct ReadyWriteGate {
    open: AtomicBool,
    waker: AtomicWaker,
    bytes: StdMutex<Vec<u8>>,
    dropped: AtomicBool,
}

struct GatedReadyIo {
    inner: ReadyInputIo,
    gate: Arc<ReadyWriteGate>,
}

impl AsyncRead for GatedReadyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for GatedReadyIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.gate.waker.register(context.waker());
        if !self.gate.open.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        let mut captured = self.gate.bytes.lock().unwrap();
        assert!(captured.len() + bytes.len() <= 64 * 1024);
        captured.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for GatedReadyIo {
    fn drop(&mut self) {
        self.gate.dropped.store(true, Ordering::SeqCst);
    }
}

struct CooperativePump {
    future: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    receiver: mpsc::Receiver<InboundEnvelope>,
    diagnostics: CloseDiagnostics,
    terminal: Arc<StdMutex<UpstreamTerminalState>>,
    budget: Arc<Semaphore>,
    trace: FiniteTrace,
    input_open: Arc<AtomicBool>,
    reader: Arc<AtomicWaker>,
    gate: Arc<ReadyWriteGate>,
    _outbound: mpsc::Sender<OutboundCommand>,
}

impl CooperativePump {
    async fn new(name: &'static str, deltas: usize, held_write: bool) -> Self {
        let trace = FiniteTrace::new(name);
        let (inner, input_open, reader) = finite_ready_io(&trace, deltas);
        let gate = Arc::new(ReadyWriteGate::default());
        let client = WebSocketStream::from_raw_socket(
            GatedReadyIo {
                inner,
                gate: gate.clone(),
            },
            Role::Client,
            None,
        )
        .await;
        let (mut owned, receiver) = test_diagnostic_pump_state(true);
        owned.watchdog_policy =
            UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5)).unwrap();
        owned.diagnostics.response.test_set_trace(trace.clone());
        let diagnostics = owned.diagnostics.clone();
        let terminal = owned.terminal_state.clone();
        let budget = owned.byte_budget.clone();
        let (outbound, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        if held_write {
            outbound
                .try_send(OutboundCommand::Text("held-once".into()))
                .unwrap();
        }
        let future = Box::pin(observe_pump_future(
            run_pump(client, outbound_rx, owned, UPSTREAM_PING_INTERVAL),
            Some(trace.clone()),
        ));
        Self {
            future,
            receiver,
            diagnostics,
            terminal,
            budget,
            trace,
            input_open,
            reader,
            gate,
            _outbound: outbound,
        }
    }

    fn open_input(&self) {
        self.trace.event("input_gate_open", None, None, None, None);
        self.input_open.store(true, Ordering::SeqCst);
        self.reader.wake();
    }

    async fn poll(&mut self) -> bool {
        futures_util::poll!(&mut self.future).is_ready()
    }

    async fn exhaust(&mut self) {
        assert!(!self.poll().await);
        self.open_input();
        assert!(!self.poll().await);
        assert!((1..=129).contains(&self.receiver.len()));
        assert_eq!(*self.terminal.lock().unwrap(), UpstreamTerminalState::Open);
        self.trace.event(
            "cooperative_pending",
            None,
            None,
            Some(self.diagnostics.response.test_queue_sample()),
            None,
        );
    }

    fn drain(&mut self, expected: &mut usize, deltas: usize) {
        drain_ready_queue(
            &mut self.receiver,
            &self.diagnostics,
            &self.terminal,
            expected,
            deltas,
        );
        assert_eq!(
            self.budget.available_permits(),
            DEFAULT_UPSTREAM_INBOUND_MAX_BYTES
        );
        self.trace.event(
            "queue_drained",
            Some(*expected),
            None,
            Some(self.diagnostics.response.test_queue_sample()),
            None,
        );
    }
}

#[tokio::test]
async fn websocket_pump_ready_sixteen_messages_do_not_yield_per_message() {
    let mut pump = CooperativePump::new("ready_sixteen", 14, false).await;
    assert!(!pump.poll().await);
    pump.open_input();
    assert!(!pump.poll().await);
    assert_eq!(pump.receiver.len(), 16);
    let mut expected = 0;
    pump.drain(&mut expected, 14);
    assert_eq!(expected, 16);
    assert_eq!(*pump.terminal.lock().unwrap(), UpstreamTerminalState::Open);
}

#[tokio::test]
async fn websocket_pump_cancel_at_cooperative_pending_releases_socket_and_permits() {
    let mut pump = CooperativePump::new("ready_cancel", 400, false).await;
    assert!(!pump.poll().await);
    pump.open_input();
    let (pending_tx, pending_rx) = oneshot::channel();
    let mut pending_tx = Some(pending_tx);
    let mut future = std::mem::replace(&mut pump.future, Box::pin(async {}));
    let owner = tokio::spawn(futures_util::future::poll_fn(move |context| {
        let result = future.as_mut().poll(context);
        assert!(result.is_pending());
        if let Some(sender) = pending_tx.take() {
            sender.send(()).unwrap();
        }
        result
    }));
    pending_rx.await.unwrap();
    assert!((1..=129).contains(&pump.receiver.len()));
    assert!(!pump.gate.dropped.load(Ordering::SeqCst));
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());
    assert!(pump.gate.dropped.load(Ordering::SeqCst));
    assert_eq!(Arc::strong_count(&pump.terminal), 1);
    let weak = Arc::downgrade(&pump.terminal);
    pump.trace.event(
        "owner_cancelled",
        None,
        None,
        Some(pump.diagnostics.response.test_queue_sample()),
        None,
    );
    let budget = pump.budget.clone();
    assert!(budget.available_permits() < DEFAULT_UPSTREAM_INBOUND_MAX_BYTES);
    drop(pump);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        budget.available_permits(),
        DEFAULT_UPSTREAM_INBOUND_MAX_BYTES
    );
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_held_write_completes_once_after_cooperative_exhaustion() {
    let mut pump = CooperativePump::new("ready_held_complete", 400, true).await;
    pump.exhaust().await;
    let mut expected = 0;
    for _ in 0..16 {
        pump.drain(&mut expected, 400);
        if expected == 402 {
            break;
        }
        tokio::task::yield_now().await;
        assert!(!pump.poll().await);
    }
    assert_eq!(expected, 402);
    assert!(pump.gate.bytes.lock().unwrap().is_empty());
    pump.gate.open.store(true, Ordering::SeqCst);
    pump.gate.waker.wake();
    assert!(!pump.poll().await);
    assert_eq!(*pump.terminal.lock().unwrap(), UpstreamTerminalState::Open);
    let bytes = pump.gate.bytes.lock().unwrap().clone();
    let mut cursor = std::io::Cursor::new(bytes);
    let mut server =
        tokio_tungstenite::tungstenite::WebSocket::from_raw_socket(&mut cursor, Role::Server, None);
    assert_eq!(server.read().unwrap(), Message::Text("held-once".into()));
    drop(server);
    assert_eq!(cursor.position() as usize, cursor.get_ref().len());
    pump.trace.event(
        "held_write_once",
        None,
        Some(cursor.get_ref().len()),
        None,
        None,
    );
}

#[tokio::test(start_paused = true)]
async fn websocket_pump_held_write_keeps_original_deadline_after_cooperative_exhaustion() {
    let mut pump = CooperativePump::new("ready_held_deadline", 400, true).await;
    pump.exhaust().await;
    let queued = pump.receiver.len();
    advance(Duration::from_secs(5)).await;
    assert!(pump.poll().await);
    assert_eq!(pump.receiver.len(), queued);
    assert!(pump.gate.bytes.lock().unwrap().is_empty());
    assert!(pump.gate.dropped.load(Ordering::SeqCst));
    assert!(
        matches!(*pump.terminal.lock().unwrap(), UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
        cause: UpstreamLivenessTimeoutCause::WriteDeadline,
        timeout, elapsed, outbound_kind: Some(UpstreamOutboundKind::Text),
    }) if timeout == Duration::from_secs(5) && elapsed == timeout)
    );
    pump.trace.event(
        "fixed_deadline_expired",
        None,
        None,
        Some(pump.diagnostics.response.test_queue_sample()),
        None,
    );
}
