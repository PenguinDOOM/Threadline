use super::super::*;
use super::fixtures::{synthetic_completed_through, synthetic_created, synthetic_delta};

use futures_util::task::AtomicWaker;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::Role;

#[derive(Clone)]
pub(crate) struct FiniteTrace(Arc<StdMutex<FiniteTraceRecords>>);

struct FiniteTraceRecords {
    records: Vec<FiniteTraceRecord>,
    started: Instant,
    monotonic: std::time::Instant,
    file: Option<std::fs::File>,
    last_rx: Option<usize>,
    queued_ordinals: std::collections::VecDeque<usize>,
    dequeued_ordinals: std::collections::VecDeque<usize>,
    poll_records: usize,
    name: &'static str,
    workers: usize,
    connection: usize,
}

#[derive(serde::Serialize)]
struct FiniteTraceRecord {
    order: usize,
    virtual_ns: u64,
    monotonic_ns: u64,
    runtime: &'static str,
    workers: usize,
    connection: usize,
    actor: &'static str,
    kind: &'static str,
    ordinal: Option<usize>,
    bytes: Option<usize>,
    q_sample: Option<usize>,
    u_sample: Option<usize>,
    peak: Option<usize>,
    outcome: Option<&'static str>,
}

fn trace_ordinal(
    trace: &mut FiniteTraceRecords,
    kind: &'static str,
    ordinal: Option<usize>,
) -> Option<usize> {
    match kind {
        "rx" => {
            trace.last_rx = ordinal;
            ordinal
        }
        "enqueue" => {
            let ordinal = trace.last_rx.unwrap();
            assert!(trace.queued_ordinals.len() < 402);
            trace.queued_ordinals.push_back(ordinal);
            Some(ordinal)
        }
        "dequeue" => {
            let ordinal = trace.queued_ordinals.pop_front().unwrap();
            assert!(trace.dequeued_ordinals.len() < 402);
            trace.dequeued_ordinals.push_back(ordinal);
            Some(ordinal)
        }
        "classify" => Some(trace.dequeued_ordinals.pop_front().unwrap()),
        "reject" => trace.last_rx,
        "terminal_dequeue" | "physical_drain" => {
            let removed = trace.queued_ordinals.pop_front().unwrap();
            if let Some(ordinal) = ordinal {
                assert_eq!(removed, ordinal);
            }
            Some(removed)
        }
        _ => ordinal,
    }
}

fn trace_actor(kind: &'static str) -> &'static str {
    match kind {
        "send" | "input_encoded" => "producer",
        "rx" | "enqueue" | "reject" | "terminal" | "pump_poll_start" | "pump_poll_end" => "pump",
        _ => "consumer",
    }
}

impl FiniteTrace {
    pub(crate) fn new(name: &'static str) -> Self {
        Self::with_runtime(name, 1, 0)
    }

    pub(super) fn with_workers(name: &'static str, workers: usize) -> Self {
        Self::with_runtime(name, workers, 0)
    }

    pub(super) fn for_connection(name: &'static str, connection: usize) -> Self {
        Self::with_runtime(name, 1, connection)
    }

    fn with_runtime(name: &'static str, workers: usize, connection: usize) -> Self {
        let file = std::env::var_os("THREADLINE_TEST_TRACE_DIR").map(|directory| {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(std::path::Path::new(&directory).join(format!("{name}.jsonl")))
                .expect("unique finite trace file")
        });
        let trace = Self(Arc::new(StdMutex::new(FiniteTraceRecords {
            records: Vec::with_capacity(16_384),
            started: Instant::now(),
            monotonic: std::time::Instant::now(),
            file,
            last_rx: None,
            queued_ordinals: std::collections::VecDeque::with_capacity(402),
            dequeued_ordinals: std::collections::VecDeque::with_capacity(402),
            poll_records: 0,
            name,
            workers,
            connection,
        })));
        trace.event("metadata", None, None, None, None);
        trace
    }

    pub(crate) fn event(
        &self,
        kind: &'static str,
        ordinal: Option<usize>,
        bytes: Option<usize>,
        sample: Option<(usize, usize, usize)>,
        outcome: Option<&'static str>,
    ) {
        let mut trace = self.0.lock().unwrap();
        assert!(
            trace.records.len() < 16_384,
            "finite trace capacity exceeded"
        );
        if kind.contains("poll") || kind == "consumer_wake" {
            trace.poll_records += 1;
            assert!(
                trace.poll_records <= 4096,
                "finite poll/wake budget exceeded"
            );
        }
        let ordinal = trace_ordinal(&mut trace, kind, ordinal);
        let record = FiniteTraceRecord {
            order: trace.records.len(),
            virtual_ns: trace.started.elapsed().as_nanos() as u64,
            runtime: if trace.workers == 1 {
                "current_thread"
            } else {
                "multi_thread"
            },
            workers: trace.workers,
            monotonic_ns: trace.monotonic.elapsed().as_nanos() as u64,
            connection: trace.connection,
            actor: trace_actor(kind),
            kind,
            ordinal,
            bytes,
            q_sample: sample.map(|value| value.0),
            u_sample: sample.map(|value| value.1),
            peak: sample.map(|value| value.2),
            outcome,
        };
        trace.records.push(record);
    }

    pub(crate) fn diagnostic(&self, output: &str) {
        use std::io::Write;
        if let Some(directory) = std::env::var_os("THREADLINE_TEST_TRACE_DIR") {
            let trace = self.0.lock().unwrap();
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(std::path::Path::new(&directory).join(format!("{}.terminal.log", trace.name)))
                .unwrap();
            file.write_all(output.as_bytes()).unwrap();
        }
    }

    pub(crate) async fn observe<F: std::future::Future>(&self, future: F) -> F::Output {
        tokio::pin!(future);
        futures_util::future::poll_fn(|context| {
            self.event("consumer_poll", None, None, None, None);
            let wake = futures_util::task::waker(Arc::new(TraceWake {
                trace: self.clone(),
                parent: context.waker().clone(),
            }));
            let result = future.as_mut().poll(&mut TaskContext::from_waker(&wake));
            self.event(
                if result.is_pending() {
                    "consumer_pending"
                } else {
                    "consumer_yield"
                },
                None,
                None,
                None,
                None,
            );
            result
        })
        .await
    }
}

struct TraceWake {
    trace: FiniteTrace,
    parent: std::task::Waker,
}

impl futures_util::task::ArcWake for TraceWake {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.trace.event("consumer_wake", None, None, None, None);
        wake.parent.wake_by_ref();
    }
}

pub(crate) async fn observe_pump_future<F: std::future::Future<Output = ()>>(
    future: F,
    trace: Option<FiniteTrace>,
) {
    tokio::pin!(future);
    futures_util::future::poll_fn(|context| {
        if let Some(trace) = &trace {
            trace.event("pump_poll_start", None, None, None, None);
        }
        let result = future.as_mut().poll(context);
        if let Some(trace) = &trace {
            trace.event("pump_poll_end", None, None, None, None);
        }
        result
    })
    .await
}

pub(super) struct ReadyInputIo {
    encoded: Vec<u8>,
    cursor: usize,
    open: Arc<AtomicBool>,
    reader: Arc<AtomicWaker>,
    writes: Vec<u8>,
}

impl AsyncRead for ReadyInputIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.reader.register(context.waker());
        if !self.open.load(Ordering::SeqCst) || self.cursor == self.encoded.len() {
            return Poll::Pending;
        }
        let end = self.encoded.len().min(self.cursor + buffer.remaining());
        buffer.put_slice(&self.encoded[self.cursor..end]);
        self.cursor = end;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ReadyInputIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        assert!(
            self.writes.len() + bytes.len() <= 64 * 1024,
            "finite write capture exceeded"
        );
        self.writes.extend_from_slice(bytes);
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

pub(super) async fn finite_ready_pair(
    trace: &FiniteTrace,
) -> (
    WebSocketStream<ReadyInputIo>,
    Arc<AtomicBool>,
    Arc<AtomicWaker>,
) {
    let (io, open, reader) = finite_ready_io(trace, 400);
    (
        WebSocketStream::from_raw_socket(io, Role::Client, None).await,
        open,
        reader,
    )
}

pub(super) fn finite_ready_io(
    trace: &FiniteTrace,
    deltas: usize,
) -> (ReadyInputIo, Arc<AtomicBool>, Arc<AtomicWaker>) {
    use tokio_tungstenite::tungstenite::protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    };
    let mut encoded = Vec::new();
    for text in std::iter::once(synthetic_created())
        .chain((1..=deltas).map(synthetic_delta))
        .chain(std::iter::once(synthetic_completed_through(deltas)))
    {
        Frame::message(text.into_bytes(), OpCode::Data(Data::Text), true)
            .format(&mut encoded)
            .unwrap();
    }
    assert!(encoded.len() <= 256 * 1024);
    trace.event("input_encoded", Some(401), Some(encoded.len()), None, None);
    let open = Arc::new(AtomicBool::new(false));
    let reader = Arc::new(AtomicWaker::new());
    let io = ReadyInputIo {
        encoded,
        cursor: 0,
        open: open.clone(),
        reader: reader.clone(),
        writes: Vec::new(),
    };
    (io, open, reader)
}

pub(crate) async fn finite_drive<F: std::future::Future>(future: F) -> F::Output {
    tokio::pin!(future);
    let mut polls = 0;
    futures_util::future::poll_fn(|context| {
        polls += 1;
        assert!(polls <= 65_536, "finite self-waking driver exhausted");
        let result = future.as_mut().poll(context);
        if result.is_pending() {
            context.waker().wake_by_ref();
        }
        result
    })
    .await
}

impl Drop for FiniteTraceRecords {
    fn drop(&mut self) {
        use std::io::Write;
        if let Some(file) = &mut self.file {
            for record in &self.records {
                serde_json::to_writer(&mut *file, record).expect("write finite trace");
                writeln!(file).expect("terminate trace record");
            }
            file.flush().expect("flush finite trace");
        }
    }
}

impl LiveUpstreamWebSocket {
    pub(crate) fn test_traced_stream<S>(stream: WebSocketStream<S>, trace: FiniteTrace) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let diagnostics = CloseDiagnostics::new(true);
        diagnostics.response.test_set_trace(trace);
        Self::from_stream_with_policy_and_limits(
            stream,
            UPSTREAM_PING_INTERVAL,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::DEFAULT,
            diagnostics,
        )
    }

    pub(crate) fn test_queue_sample(&self) -> (usize, usize, usize) {
        self.diagnostics.response.test_queue_sample()
    }

    pub(crate) async fn test_drain_after_terminal(&self, trace: &FiniteTrace) -> usize {
        let mut receiver = self.inbound_rx.lock().await;
        let mut drained = 0;
        let mut budget = None;
        while let Ok(envelope) = receiver.try_recv() {
            let event: serde_json::Value = serde_json::from_str(&envelope.payload).unwrap();
            trace.event(
                "physical_drain",
                event["sequence_number"]
                    .as_u64()
                    .map(|value| value as usize),
                Some(envelope.payload.len()),
                None,
                None,
            );
            budget = Some(Arc::clone(envelope._byte_permit.semaphore()));
            drop(envelope);
            drained += 1;
        }
        assert_eq!(
            budget.unwrap().available_permits(),
            UpstreamInboundLimits::DEFAULT.max_bytes()
        );
        drained
    }
}

pub(crate) async fn finite_send(
    server: &mut WebSocketStream<DuplexStream>,
    trace: &FiniteTrace,
    ordinal: usize,
    text: String,
) {
    let bytes = text.len();
    server.send(Message::Text(text)).await.unwrap();
    trace.event("send", Some(ordinal), Some(bytes), None, None);
}

pub(crate) async fn finite_read_barrier(
    server: &mut WebSocketStream<DuplexStream>,
    trace: &FiniteTrace,
) {
    server.send(Message::Ping(vec![7])).await.unwrap();
    assert_eq!(
        server.next().await.unwrap().unwrap(),
        Message::Pong(vec![7])
    );
    trace.event("pong_barrier", None, None, None, None);
}

pub(crate) async fn finite_receive(
    pump: &LiveUpstreamWebSocket,
    trace: &FiniteTrace,
    ordinal: usize,
) {
    trace.event(
        "consumer_poll",
        Some(ordinal),
        None,
        Some(pump.test_queue_sample()),
        None,
    );
    let text = trace.observe(pump.recv_text()).await.unwrap().unwrap();
    let event: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(event["sequence_number"], ordinal);
    assert_eq!(event["delta"], format!("{ordinal:06}"));
    pump.observe_response_event(ResponseEvent::Progress);
    trace.event(
        "consumer_ack",
        Some(ordinal),
        None,
        Some(pump.test_queue_sample()),
        None,
    );
}
