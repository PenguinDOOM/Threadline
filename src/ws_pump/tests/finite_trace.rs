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

const TRACE_RECORD_LIMIT: usize = 262_144;
const TRACE_CLEANUP_RESERVE: usize = 256;
type FiniteTraceRecord = [Option<u64>; 24];

const TRACE_KINDS: &[&str] = &[
    "metadata",
    "planned",
    "send_start",
    "send",
    "input_encoded",
    "rx",
    "enqueue",
    "reject",
    "physical_dequeue",
    "dequeue",
    "classify",
    "terminal",
    "physical_drain",
    "terminal_dequeue",
    "consumer_poll",
    "consumer_pending",
    "consumer_yield",
    "consumer_wake",
    "pump_poll_start",
    "pump_poll_end",
    "pong_barrier",
    "consumer_ack",
    "processing_gate_pending",
    "processing_gate_released",
    "supply_stopped",
    "resume",
    "outcome",
    "input_gate_open",
    "queue_drained",
    "cooperative_pending",
    "owner_cancelled",
    "held_write_once",
    "fixed_deadline_expired",
    "recovered",
    "sync_start",
    "sync_end",
    "async_start",
    "async_end",
    "poll_start",
    "poll_end",
    "wake",
    "yield",
    "next_poll",
    "parse_start",
    "parse_end",
    "pending_input",
    "resource_released",
    "invalid",
    "eof",
];
const TRACE_OUTCOMES: &[&str] = &[
    "pending",
    "ready",
    "expected_overflow",
    "not_reproduced",
    "candidate_complete",
    "baseline_overflow",
    "completed",
    "overflow",
    "cancelled",
];

#[derive(Clone, Copy)]
#[repr(u64)]
pub(crate) enum TraceOperation {
    Body = 1,
    Pump,
    Progress,
    Receive,
    Parse,
    Process,
    RouteBeforeTool,
    RouteAfterTool,
    Visible,
    CompletedSynthesis,
    Sanitize,
    Observable,
    Accept,
    QueuedMetadata,
    TerminalPayload,
    Clear,
    Finish,
    Serialize,
    Encode,
    Send,
    Control,
    RawReceive,
    BodyNext,
}

#[derive(Clone)]
pub(crate) struct FiniteTrace(Option<Arc<FiniteTraceShared>>);

struct FiniteTraceShared {
    records: StdMutex<FiniteTraceRecords>,
    physical_dequeued: std::sync::Condvar,
    before_enqueue_record: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>>,
    started: Instant,
    monotonic: std::time::Instant,
    next_id: std::sync::atomic::AtomicU64,
    connection: usize,
    natural: bool,
    input_frames: usize,
    virtual_clock: bool,
    record_limit: usize,
}

struct FiniteTraceRecords {
    records: Vec<FiniteTraceRecord>,
    file: Option<std::fs::File>,
    status_file: Option<std::fs::File>,
    directory: Option<std::path::PathBuf>,
    rx_count: usize,
    last_rx: Option<usize>,
    pending_enqueue: Option<usize>,
    early_dequeue: Option<usize>,
    queued_ordinals: std::collections::VecDeque<usize>,
    dequeued_ordinals: std::collections::VecDeque<usize>,
    physical_pending: std::collections::VecDeque<usize>,
    polls: usize,
    wakes: usize,
    attempts: u64,
    missing: u64,
    invalid: Option<u64>,
    current_input: Option<usize>,
    sending_input: Option<usize>,
    output_count: usize,
    barrier_count: usize,
    delta_start: usize,
    deltas: usize,
    name: &'static str,
    workers: usize,
    runtime_flavor: &'static str,
}

fn trace_record_allowed(
    trace: &mut FiniteTraceRecords,
    shared: &FiniteTraceShared,
    kind: &str,
) -> bool {
    if matches!(kind, "poll_start" | "pump_poll_start" | "consumer_poll") {
        trace.polls += 1;
        let frames = shared.input_frames;
        let outputs = frames + 4;
        let controls = 2 * frames.div_ceil(8) + 16;
        let invocations = frames + outputs + 1;
        let asynchronous = invocations + 3 * frames + outputs + 1;
        let first_polls = asynchronous + frames + outputs + 2 + 4 * controls;
        if trace.polls > first_polls + if shared.natural { 32768 } else { 16384 } {
            trace.invalid.get_or_insert(2);
        }
    }
    if matches!(kind, "wake" | "consumer_wake") {
        trace.wakes += 1;
        if trace.wakes > if shared.natural { 65536 } else { 32768 } {
            trace.invalid.get_or_insert(3);
        }
    }
    let cleanup = matches!(
        kind,
        "resource_released" | "owner_cancelled" | "invalid" | "physical_drain" | "eof"
    );
    if trace.records.len() >= shared.record_limit - TRACE_CLEANUP_RESERVE && !cleanup {
        trace.invalid.get_or_insert(1);
    }
    let allowed =
        !(trace.invalid.is_some() && !cleanup || trace.records.len() == shared.record_limit);
    if !allowed {
        trace.missing += 1;
    }
    allowed
}

thread_local! {
    static ACTIVE_TRACE: std::cell::RefCell<Option<FiniteTrace>> = const { std::cell::RefCell::new(None) };
}

struct TraceContext(Option<FiniteTrace>);

impl Drop for TraceContext {
    fn drop(&mut self) {
        ACTIVE_TRACE.with(|active| active.replace(self.0.take()));
    }
}

pub(crate) struct TraceSpan {
    trace: FiniteTrace,
    operation: TraceOperation,
    id: u64,
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        self.trace
            .span_event("sync_end", self.operation, self.id, None, None);
    }
}

struct AsyncTraceSpan {
    trace: FiniteTrace,
    operation: TraceOperation,
    id: u64,
    outcome: u64,
}

impl Drop for AsyncTraceSpan {
    fn drop(&mut self) {
        self.trace.span_event(
            "async_end",
            self.operation,
            self.id,
            None,
            Some(self.outcome),
        );
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
        Self::configured(name, workers, connection, 402, false, false, true)
    }

    pub(crate) fn configured(
        name: &'static str,
        workers: usize,
        connection: usize,
        input_frames: usize,
        natural: bool,
        virtual_clock: bool,
        detail: bool,
    ) -> Self {
        if !detail {
            return Self(None);
        }
        assert!(input_frames <= 1543 && workers > 0);
        assert!(std::mem::size_of::<FiniteTraceRecord>() <= 512);
        assert!(
            std::mem::size_of::<FiniteTraceRecord>()
                .checked_mul(TRACE_RECORD_LIMIT)
                .unwrap()
                <= 128 * 1024 * 1024
        );
        let trace = Self(Some(Arc::new(FiniteTraceShared {
            records: StdMutex::new(FiniteTraceRecords {
                records: Vec::new(),
                file: None,
                status_file: None,
                directory: None,
                rx_count: 0,
                last_rx: None,
                pending_enqueue: None,
                early_dequeue: None,
                queued_ordinals: std::collections::VecDeque::new(),
                dequeued_ordinals: std::collections::VecDeque::new(),
                physical_pending: std::collections::VecDeque::new(),
                polls: 0,
                wakes: 0,
                attempts: 0,
                missing: 0,
                invalid: None,
                current_input: None,
                sending_input: None,
                output_count: 0,
                barrier_count: 0,
                delta_start: 1,
                deltas: input_frames.saturating_sub(2),
                name,
                workers,
                runtime_flavor: match tokio::runtime::Handle::try_current()
                    .map(|handle| handle.runtime_flavor())
                {
                    Ok(tokio::runtime::RuntimeFlavor::CurrentThread) => "current_thread",
                    Ok(tokio::runtime::RuntimeFlavor::MultiThread) => "multi_thread",
                    Ok(_) => "unknown",
                    Err(_) => "no_runtime",
                },
            }),
            physical_dequeued: std::sync::Condvar::new(),
            before_enqueue_record: std::sync::OnceLock::new(),
            started: Instant::now(),
            monotonic: std::time::Instant::now(),
            next_id: std::sync::atomic::AtomicU64::new(1),
            connection,
            natural,
            input_frames,
            virtual_clock,
            record_limit: TRACE_RECORD_LIMIT,
        })));
        if let Some(directory) = std::env::var_os("THREADLINE_TEST_TRACE_DIR") {
            trace.set_output_directory(std::path::Path::new(&directory));
        }
        trace.event("metadata", None, None, None, None);
        trace
    }

    pub(crate) fn set_output_directory(&self, directory: &std::path::Path) {
        let shared = self.0.as_ref().unwrap();
        let mut records = shared.records.lock().unwrap();
        assert!(records.file.is_none() && records.status_file.is_none());
        let open = |suffix| {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join(format!("{}.{suffix}", records.name)))
                .expect("unique finite trace file")
        };
        let file = open("jsonl");
        let status = open("status.json");
        serde_json::to_writer(open("schema.json"), &Self::schema()).unwrap();
        records.file = Some(file);
        records.status_file = Some(status);
        records.directory = Some(directory.to_path_buf());
    }

    pub(crate) fn with_record_budget(mut self, limit: usize) -> Self {
        assert!(limit > TRACE_CLEANUP_RESERVE && limit <= TRACE_RECORD_LIMIT);
        Arc::get_mut(self.0.as_mut().unwrap()).unwrap().record_limit = limit;
        self
    }

    pub(crate) fn record_budget(&self) -> usize {
        self.0
            .as_ref()
            .map_or(TRACE_RECORD_LIMIT, |shared| shared.record_limit)
    }

    pub(crate) fn schema() -> serde_json::Value {
        serde_json::json!({"version":1,"slots":["collection_sequence","monotonic_ns","virtual_ns","actor","event_kind","operation","outcome","input_ordinal","output_ordinal","delta_ordinal","async_span_id","poll_id","wake_id","barrier_id","bytes","queue_messages","queue_bytes","peak_messages","peak_bytes","unclassified_count","connection_id","event_class","aux0","aux1"],
            "event_kinds_one_based":TRACE_KINDS,"outcomes_one_based":TRACE_OUTCOMES,
            "actors":{"producer":1,"pump":2,"consumer":3},
            "classes":{"text":1,"arguments":2,"lifecycle":3,"other":4},
            "operations_one_based":["body","pump","progress","receive","parse","process","route_before_tool","route_after_tool","visible","completed_synthesis","sanitize","observable","accept","queued_metadata","terminal_payload","clear","finish","serialize","encode","send","control","raw_receive","body_next"],
            "aux0":"planned: scheduled monotonic_ns; invalid: reason code; otherwise null",
            "aux1":"sync endpoints: span identity; diagnostic dequeue: 1 updated, 0 frozen; otherwise null",
            "invalid_reasons":{"records":1,"polls":2,"wakes":3,"input_or_fifo":4,"timeout":5,"test_failure":6},
            "timestamp":"taken before trace mutex; collection sequence is not causality; wall spans include preemption; virtual clock only for paused tests",
            "ordinal_semantics":{
                "fifo_correspondence":"rx assigns the synthetic frame ordinal; the single pump finishes enqueue/reject before its next RX; physical_dequeue removes the oldest accepted FIFO ordinal or identifies the one in-flight RX when try_send wakes the consumer before enqueue is recorded; that later enqueue reconciles the early removal without requeueing it; physical_drain removes accepted FIFO ordinals; dequeue matches physical_dequeue; classify consumes dequeued FIFO ordinals; pending_input references the next dequeued ordinal without another physical removal",
                "producer":"planned/send_start/send carry the explicit synthetic input ordinal; send operation endpoints may inherit the currently sending input",
                "consumer_context":"other consumer records carry the current or most recently dequeued/classified input, not necessarily the input being awaited or processed; span endpoints may have different input_ordinal values",
                "delta_ordinal":"derived from input_ordinal and the configured delta range; contextual when input_ordinal is contextual",
                "output":"yield assigns output_ordinal; next_poll references the previous yield; input events and output chunks are not one-to-one"
            },
            "span_matching":{
                "async":["connection_id","async_span_id"],
                "poll":["connection_id","async_span_id","poll_id"],
                "sync":["connection_id","aux1"],
                "rule":"match start/end by these identities and operation, independently of input_ordinal or delta_ordinal"
            },
            "bounds":{"records":TRACE_RECORD_LIMIT,"cleanup_reserve":TRACE_CLEANUP_RESERVE,"max_line_bytes":506,"record_size_bytes":std::mem::size_of::<FiniteTraceRecord>(),"record_area_bytes":std::mem::size_of::<FiniteTraceRecord>()*TRACE_RECORD_LIMIT,"input_frames":1543,"output_chunks":1547,"pending_enqueue_ordinals":1,"early_dequeue_ordinals":1,"controlled_extra_polls":16384,"natural_extra_polls":32768,"controlled_wakes":32768,"natural_wakes":65536},
            "observations":"RX is read observation, not wire arrival; enqueue and physical dequeue are distinct from diagnostic updates; queue samples are diagnostic, not linearized physical occupancy; pending_input does not dequeue; raw validation outside timing; no RX JSON parse"})
    }

    pub(crate) fn enabled(&self) -> bool {
        self.0.is_some()
    }

    fn record(&self, mut record: FiniteTraceRecord) {
        let Some(shared) = &self.0 else {
            return;
        };
        let kind = TRACE_KINDS[(record[4].unwrap() - 1) as usize];
        if kind == "enqueue"
            && let Some(before) = shared.before_enqueue_record.get()
        {
            before();
        }
        record[1].get_or_insert_with(|| shared.monotonic.elapsed().as_nanos().try_into().unwrap());
        if shared.virtual_clock && record[2].is_none() {
            record[2] = Some(shared.started.elapsed().as_nanos().try_into().unwrap());
        }
        record[20] = Some(shared.connection as u64);
        let mut trace = shared.records.lock().unwrap();
        trace.attempts += 1;
        if !trace_record_allowed(&mut trace, shared, kind) {
            return;
        }
        let ordinal = record[7].map(|value| value as usize);
        let ordinal = match kind {
            "rx" => {
                let ordinal = trace.rx_count;
                trace.rx_count += 1;
                trace.last_rx = Some(ordinal);
                trace.pending_enqueue = Some(ordinal);
                if ordinal >= shared.input_frames {
                    trace.invalid.get_or_insert(4);
                }
                Some(ordinal)
            }
            "enqueue" => {
                let ordinal = trace.pending_enqueue.take();
                if let Some(early) = trace.early_dequeue.take() {
                    if ordinal != Some(early) {
                        trace.invalid.get_or_insert(4);
                    }
                } else if let Some(ordinal) = ordinal {
                    trace.queued_ordinals.push_back(ordinal);
                }
                ordinal
            }
            "physical_dequeue" => {
                let ordinal = trace.queued_ordinals.pop_front().or_else(|| {
                    let ordinal = trace
                        .pending_enqueue
                        .filter(|_| trace.early_dequeue.is_none());
                    trace.early_dequeue = ordinal;
                    ordinal
                });
                if let Some(ordinal) = ordinal {
                    trace.dequeued_ordinals.push_back(ordinal);
                    trace.physical_pending.push_back(ordinal);
                } else {
                    trace.invalid.get_or_insert(4);
                }
                trace.current_input = ordinal;
                ordinal
            }
            "dequeue" => trace.physical_pending.pop_front().or_else(|| {
                let ordinal = trace.queued_ordinals.pop_front();
                if let Some(ordinal) = ordinal {
                    trace.dequeued_ordinals.push_back(ordinal);
                }
                ordinal
            }),
            "classify" => {
                let ordinal = trace.dequeued_ordinals.pop_front();
                trace.current_input = ordinal;
                ordinal
            }
            "pending_input" => {
                let ordinal = trace.dequeued_ordinals.front().copied();
                trace.current_input = ordinal;
                ordinal
            }
            "send_start" => {
                trace.sending_input = ordinal;
                ordinal
            }
            "reject" => {
                trace.pending_enqueue = None;
                trace.last_rx
            }
            "physical_drain" => trace.queued_ordinals.pop_front(),
            _ => ordinal.or_else(|| match record[3] {
                Some(1) if record[5] == Some(TraceOperation::Send as u64) => trace.sending_input,
                Some(3) => trace.current_input,
                _ => None,
            }),
        };
        record[7] = ordinal.map(|value| value as u64);
        if let Some(ordinal) = ordinal.filter(|value| {
            *value >= trace.delta_start && *value < trace.delta_start + trace.deltas
        }) {
            record[9] = Some((ordinal - trace.delta_start + 1) as u64);
        }
        if kind == "yield" {
            record[8] = Some(trace.output_count as u64);
            trace.output_count += 1;
            if trace.output_count > shared.input_frames + 4 {
                trace.invalid.get_or_insert(4);
            }
        }
        if kind == "next_poll" {
            record[8] = trace.output_count.checked_sub(1).map(|value| value as u64);
        }
        if kind == "pong_barrier" {
            record[13] = Some(trace.barrier_count as u64);
            trace.barrier_count += 1;
            if trace.barrier_count > 2 * shared.input_frames.div_ceil(8) + 16 {
                trace.invalid.get_or_insert(4);
            }
        }
        record[0] = Some(trace.attempts - 1);
        trace.records.push(record);
        if kind == "physical_dequeue" {
            shared.physical_dequeued.notify_all();
        }
    }

    pub(crate) fn physical_dequeue(
        &self,
        bytes: usize,
        observed_at: (std::time::Instant, Instant),
    ) {
        let Some(shared) = &self.0 else {
            return;
        };
        let mut record = [None; 24];
        record[1] = Some(
            observed_at
                .0
                .duration_since(shared.monotonic)
                .as_nanos()
                .try_into()
                .unwrap(),
        );
        record[2] = shared.virtual_clock.then(|| {
            observed_at
                .1
                .duration_since(shared.started)
                .as_nanos()
                .try_into()
                .unwrap()
        });
        record[3] = Some(3);
        record[4] = Some(
            (TRACE_KINDS
                .iter()
                .position(|kind| *kind == "physical_dequeue")
                .unwrap()
                + 1) as u64,
        );
        record[14] = Some(bytes as u64);
        self.record(record);
    }

    pub(crate) fn event(
        &self,
        kind: &'static str,
        ordinal: Option<usize>,
        bytes: Option<usize>,
        sample: Option<(usize, usize, usize)>,
        outcome: Option<&'static str>,
    ) {
        if !self.enabled() {
            return;
        }
        let mut record = [None; 24];
        record[3] = Some(match kind {
            "planned" | "send_start" | "send" | "input_encoded" => 1,
            "rx" | "enqueue" | "reject" | "terminal" | "pump_poll_start" | "pump_poll_end" => 2,
            _ => 3,
        });
        record[4] = Some(
            (TRACE_KINDS
                .iter()
                .position(|value| *value == kind)
                .expect("fixed trace kind")
                + 1) as u64,
        );
        record[6] = outcome.map(|outcome| {
            (TRACE_OUTCOMES
                .iter()
                .position(|value| *value == outcome)
                .expect("fixed trace outcome")
                + 1) as u64
        });
        record[7] = ordinal.map(|value| value as u64);
        record[14] = bytes.map(|value| value as u64);
        record[15] = sample.map(|value| value.0 as u64);
        record[17] = sample.map(|value| value.2 as u64);
        record[19] = sample.map(|value| value.1 as u64);
        self.record(record);
    }

    pub(crate) fn diagnostic(&self, output: &str) {
        use std::io::Write;
        let Some(shared) = &self.0 else {
            return;
        };
        if let Some(directory) = std::env::var_os("THREADLINE_TEST_TRACE_DIR") {
            let name = shared.records.lock().unwrap().name;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(std::path::Path::new(&directory).join(format!("{name}.terminal.log")))
                .unwrap();
            file.write_all(output.as_bytes()).unwrap();
        }
    }

    pub(crate) async fn observe<F: std::future::Future>(&self, future: F) -> F::Output {
        self.observe_operation(TraceOperation::BodyNext, future)
            .await
    }

    pub(crate) async fn observe_operation<F: std::future::Future>(
        &self,
        operation: TraceOperation,
        future: F,
    ) -> F::Output {
        if !self.enabled() {
            return future.await;
        }
        let id = self.id();
        let endpoints = !matches!(
            operation,
            TraceOperation::Send | TraceOperation::Control | TraceOperation::BodyNext
        );
        if endpoints {
            self.span_event("async_start", operation, id, None, None);
        }
        let mut observation = endpoints.then(|| AsyncTraceSpan {
            trace: self.clone(),
            operation,
            id,
            outcome: 9,
        });
        tokio::pin!(future);
        let result = futures_util::future::poll_fn(|context| {
            let poll_id = self.id();
            self.span_event("poll_start", operation, id, Some(poll_id), None);
            let wake = futures_util::task::waker(Arc::new(TraceWake {
                trace: self.clone(),
                parent: context.waker().clone(),
                operation,
                id,
                poll_id,
            }));
            let previous = ACTIVE_TRACE.with(|active| active.replace(Some(self.clone())));
            let _context = TraceContext(previous);
            let result = future.as_mut().poll(&mut TaskContext::from_waker(&wake));
            self.span_event(
                "poll_end",
                operation,
                id,
                Some(poll_id),
                Some(if result.is_pending() { 1 } else { 2 }),
            );
            result
        })
        .await;
        if let Some(observation) = &mut observation {
            observation.outcome = 2;
        }
        result
    }

    fn id(&self) -> u64 {
        self.0
            .as_ref()
            .unwrap()
            .next_id
            .fetch_add(1, Ordering::Relaxed)
    }

    fn span_event(
        &self,
        kind: &str,
        operation: TraceOperation,
        id: u64,
        poll: Option<u64>,
        outcome: Option<u64>,
    ) {
        let mut record = [None; 24];
        record[3] = Some(if matches!(operation, TraceOperation::Pump) {
            2
        } else if matches!(operation, TraceOperation::Send | TraceOperation::Control) {
            1
        } else {
            3
        });
        record[4] = Some((TRACE_KINDS.iter().position(|value| *value == kind).unwrap() + 1) as u64);
        record[5] = Some(operation as u64);
        record[6] = outcome;
        if kind.starts_with("sync_") {
            record[23] = Some(id);
        } else {
            record[10] = Some(id);
        }
        record[11] = poll;
        if kind == "wake" {
            record[12] = Some(self.id());
        }
        self.record(record);
    }

    pub(crate) fn current() -> Option<Self> {
        ACTIVE_TRACE.with(|active| active.borrow().clone())
    }

    pub(crate) fn current_span(operation: TraceOperation) -> Option<TraceSpan> {
        let trace = Self::current()?;
        let id = trace.id();
        trace.span_event("sync_start", operation, id, None, None);
        Some(TraceSpan {
            trace,
            operation,
            id,
        })
    }

    pub(crate) fn classification(&self, parsed: &serde_json::Value) {
        let class = match parsed["type"].as_str() {
            Some("response.output_text.delta") => 1,
            Some("response.function_call_arguments.delta") => 2,
            Some(
                "response.created"
                | "response.in_progress"
                | "response.completed"
                | "response.failed"
                | "response.incomplete"
                | "response.output_item.added"
                | "response.output_item.done"
                | "response.content_part.added"
                | "response.content_part.done"
                | "response.output_text.done"
                | "response.function_call_arguments.done",
            ) => 3,
            _ => 4,
        };
        let mut record = [None; 24];
        record[3] = Some(3);
        record[4] = Some(11);
        record[21] = Some(class);
        self.record(record);
    }

    pub(crate) fn queue_sample(
        &self,
        kind: &'static str,
        bytes: usize,
        sample: Option<[usize; 5]>,
        updated: bool,
    ) {
        if !self.enabled() {
            return;
        }
        let mut record = [None; 24];
        record[3] = Some(if kind == "enqueue" { 2 } else { 3 });
        record[4] = Some((TRACE_KINDS.iter().position(|value| *value == kind).unwrap() + 1) as u64);
        record[14] = Some(bytes as u64);
        if let Some(sample) = sample {
            for (index, value) in sample.into_iter().enumerate() {
                record[15 + index] = Some(value as u64);
            }
        }
        record[23] = (kind == "dequeue").then_some(u64::from(updated));
        self.record(record);
    }

    pub(crate) fn assert_valid(&self) {
        assert_eq!(self.invalid_reason(), None, "invalid measurement");
    }

    pub(crate) fn invalid_reason(&self) -> Option<u64> {
        self.0
            .as_ref()
            .and_then(|shared| shared.records.lock().unwrap().invalid)
    }

    pub(crate) fn flush(&self) {
        if let Some(shared) = &self.0 {
            shared.records.lock().unwrap().flush();
        }
    }

    pub(crate) fn invalidate(&self, reason: u64) {
        let Some(shared) = &self.0 else {
            return;
        };
        shared.records.lock().unwrap().invalid.get_or_insert(reason);
        let mut record = [None; 24];
        record[3] = Some(3);
        record[4] = Some(48);
        record[22] = Some(reason);
        self.record(record);
    }

    pub(crate) fn set_series(&self, delta_start: usize, deltas: usize) {
        if let Some(shared) = &self.0 {
            assert!(delta_start.checked_add(deltas).unwrap() <= shared.input_frames);
            let mut trace = shared.records.lock().unwrap();
            trace.delta_start = delta_start;
            trace.deltas = deltas;
        }
    }

    pub(crate) fn planned(&self, ordinal: usize, bytes: usize, scheduled_ns: Option<u64>) {
        let Some(shared) = &self.0 else {
            return;
        };
        let mut record = [None; 24];
        record[3] = Some(1);
        record[4] = Some(2);
        record[7] = Some(ordinal as u64);
        record[14] = Some(bytes as u64);
        record[22] = Some(
            scheduled_ns
                .unwrap_or_else(|| shared.monotonic.elapsed().as_nanos().try_into().unwrap()),
        );
        self.record(record);
    }

    pub(crate) fn scenario(&self, scenario: &serde_json::Value) {
        let Some(shared) = &self.0 else {
            return;
        };
        let name = shared.records.lock().unwrap().name;
        self.write_scenario(name, scenario);
    }

    pub(crate) fn write_scenario(&self, name: &str, scenario: &serde_json::Value) {
        use std::io::Write;
        let directory = self
            .0
            .as_ref()
            .and_then(|shared| shared.records.lock().unwrap().directory.clone())
            .or_else(|| {
                std::env::var_os("THREADLINE_TEST_TRACE_DIR").map(std::path::PathBuf::from)
            });
        if let Some(directory) = directory {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(std::path::Path::new(&directory).join(format!("{name}.scenario.json")))
                .unwrap();
            serde_json::to_writer(&mut file, scenario).unwrap();
            file.flush().unwrap();
        }
    }

    pub(crate) fn assert_measurement_records(&self, classes: Option<[usize; 4]>) {
        let Some(shared) = &self.0 else {
            return;
        };
        self.assert_valid();
        let trace = shared.records.lock().unwrap();
        let mut observed = [0; 4];
        let mut enqueued = (0u64, 0u64);
        let mut removed = (0u64, 0u64);
        for record in &trace.records {
            let encoded = serde_json::to_string(record).unwrap();
            assert!(encoded.len() + 1 <= 506);
            assert!(!encoded.contains("secret"));
            if record[4] == Some(11) {
                observed[record[21].unwrap() as usize - 1] += 1;
            }
            match TRACE_KINDS[record[4].unwrap() as usize - 1] {
                "enqueue" => {
                    enqueued.0 += 1;
                    enqueued.1 += record[14].unwrap();
                }
                "physical_dequeue" | "physical_drain" => {
                    removed.0 += 1;
                    removed.1 += record[14].unwrap();
                }
                _ => (),
            }
        }
        if let Some(classes) = classes {
            assert_eq!(observed, classes);
        }
        assert_eq!(enqueued, removed, "quiescent queue conservation");
        assert!(trace.queued_ordinals.is_empty());
        assert!(trace.pending_enqueue.is_none() && trace.early_dequeue.is_none());
    }
}

struct TraceWake {
    trace: FiniteTrace,
    parent: std::task::Waker,
    operation: TraceOperation,
    id: u64,
    poll_id: u64,
}

impl futures_util::task::ArcWake for TraceWake {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.trace
            .span_event("wake", wake.operation, wake.id, Some(wake.poll_id), None);
        wake.parent.wake_by_ref();
    }
}

pub(crate) async fn observe_pump_future<F: std::future::Future<Output = ()>>(
    future: F,
    trace: Option<FiniteTrace>,
) {
    match trace {
        Some(trace) => trace.observe_operation(TraceOperation::Pump, future).await,
        None => future.await,
    }
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
    let payloads = std::iter::once(synthetic_created())
        .chain((1..=deltas).map(synthetic_delta))
        .chain(std::iter::once(synthetic_completed_through(deltas)));
    ready_io_from_payloads(trace, payloads, 256 * 1024, deltas + 1)
}

pub(crate) fn measurement_encoded_bytes(payloads: &[String]) -> usize {
    let trace = FiniteTrace(None);
    measurement_ready_io(&trace, payloads).0.encoded.len()
}

pub(crate) fn measurement_ready_io(
    trace: &FiniteTrace,
    payloads: &[String],
) -> (ReadyInputIo, Arc<AtomicBool>, Arc<AtomicWaker>) {
    ready_io_from_payloads(
        trace,
        payloads.iter().cloned(),
        8 * 1024 * 1024,
        payloads.len() - 1,
    )
}

fn ready_io_from_payloads(
    trace: &FiniteTrace,
    payloads: impl IntoIterator<Item = String>,
    max_encoded: usize,
    completed: usize,
) -> (ReadyInputIo, Arc<AtomicBool>, Arc<AtomicWaker>) {
    use tokio_tungstenite::tungstenite::protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    };
    let mut encoded = Vec::new();
    for text in payloads {
        assert!(text.len() <= 16 * 1024);
        let required = encoded
            .len()
            .checked_add(text.len())
            .unwrap()
            .checked_add(if text.len() <= 125 { 2 } else { 4 })
            .unwrap();
        assert!(required <= max_encoded);
        Frame::message(text.into_bytes(), OpCode::Data(Data::Text), true)
            .format(&mut encoded)
            .unwrap();
        assert_eq!(encoded.len(), required);
    }
    trace.event(
        "input_encoded",
        Some(completed),
        Some(encoded.len()),
        None,
        None,
    );
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

impl FiniteTraceRecords {
    fn flush(&mut self) {
        use std::io::Write;
        if let Some(mut file) = self.file.take() {
            for record in &self.records {
                serde_json::to_writer(&mut file, record).expect("write finite trace");
                writeln!(file).expect("terminate trace record");
            }
            file.flush().expect("flush finite trace");
        }
        if let Some(mut file) = self.status_file.take() {
            serde_json::to_writer(&mut file, &serde_json::json!({"name":self.name,"runtime_flavor":self.runtime_flavor,"workers":self.workers,"valid":self.invalid.is_none(),"invalid_reason":self.invalid,"recorded":self.records.len(),"attempted":self.attempts,"missing_records":self.missing,"missing_scope":if self.missing>0 {Some("after sticky invalid except reserved cleanup observations")} else {None},"poll_attempts":self.polls,"wake_attempts":self.wakes})).unwrap();
            file.flush().expect("flush finite trace status");
        }
    }
}

impl Drop for FiniteTraceRecords {
    fn drop(&mut self) {
        self.flush();
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

    pub(crate) fn test_measurement_stream<S>(
        stream: WebSocketStream<S>,
        aggregate: bool,
        trace: FiniteTrace,
        limits: UpstreamInboundLimits,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let diagnostics = CloseDiagnostics::new(aggregate);
        diagnostics.response.test_set_trace(trace);
        Self::from_stream_with_policy_and_limits(
            stream,
            UPSTREAM_PING_INTERVAL,
            UpstreamWatchdogPolicy::DEFAULT,
            limits,
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
            trace.event(
                "physical_drain",
                None,
                Some(envelope.payload.len()),
                None,
                None,
            );
            budget = Some(Arc::clone(envelope._byte_permit.semaphore()));
            drop(envelope);
            drained += 1;
        }
        if let Some(budget) = budget {
            assert_eq!(
                budget.available_permits(),
                UpstreamInboundLimits::DEFAULT.max_bytes()
            );
        }
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
    trace.planned(ordinal, bytes, None);
    trace.event("send_start", Some(ordinal), Some(bytes), None, None);
    trace
        .observe_operation(TraceOperation::Send, server.send(Message::Text(text)))
        .await
        .unwrap();
    trace.event("send", Some(ordinal), Some(bytes), None, None);
}

pub(crate) async fn finite_read_barrier(
    server: &mut WebSocketStream<DuplexStream>,
    trace: &FiniteTrace,
) {
    trace
        .observe_operation(TraceOperation::Control, async {
            server.send(Message::Ping(vec![7])).await.unwrap();
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Pong(vec![7])
            );
        })
        .await;
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
    let text = trace
        .observe_operation(TraceOperation::RawReceive, pump.recv_text())
        .await
        .unwrap()
        .unwrap();
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

#[test]
fn finite_trace_invalid_budget_preserves_cleanup_and_forwards_wakes() {
    use futures_util::task::ArcWake;
    struct WakeCount(std::sync::atomic::AtomicUsize);
    impl ArcWake for WakeCount {
        fn wake_by_ref(wake: &Arc<Self>) {
            wake.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let trace = FiniteTrace::configured("budget_invalid", 1, 0, 0, false, false, true);
    let shared = trace.0.as_ref().unwrap();
    shared.records.lock().unwrap().polls = 16_384 + 80;
    trace.span_event("poll_start", TraceOperation::Body, 1, Some(1), None);
    let counter = Arc::new(WakeCount(std::sync::atomic::AtomicUsize::new(0)));
    let wake = Arc::new(TraceWake {
        trace: trace.clone(),
        parent: futures_util::task::waker(counter.clone()),
        operation: TraceOperation::Body,
        id: 1,
        poll_id: 1,
    });
    TraceWake::wake_by_ref(&wake);
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    trace.invalidate(2);
    trace.event("resource_released", None, None, None, None);
    let records = shared.records.lock().unwrap();
    assert_eq!(records.invalid, Some(2));
    assert!(records.missing > 0);
    assert_eq!(records.records.last().unwrap()[4], Some(47));
    let maximum: FiniteTraceRecord = [Some(u64::MAX); 24];
    assert_eq!(serde_json::to_string(&maximum).unwrap().len() + 1, 506);
}

#[tokio::test]
async fn physical_dequeue_timestamp_precedes_contended_diagnostic_update() {
    let trace = FiniteTrace::new("contended_dequeue");
    let (client_io, server_io) = tokio::io::duplex(4096);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let mut pump = LiveUpstreamWebSocket::test_traced_stream(client, trace.clone());
    finite_send(&mut server, &trace, 0, synthetic_created()).await;
    finite_read_barrier(&mut server, &trace).await;
    let shared = trace.0.as_ref().unwrap();
    let diagnostic = pump.diagnostics.response.record.as_ref().unwrap().clone();
    let diagnostic_guard = diagnostic.lock().unwrap();
    let runtime = tokio::runtime::Handle::current();
    let kind_of = |record: &FiniteTraceRecord| TRACE_KINDS[record[4].unwrap() as usize - 1];
    let unlocked_at = std::thread::scope(|scope| {
        let receiver = scope.spawn(|| runtime.block_on(pump.recv_text()));
        let (records, wait) = shared
            .physical_dequeued
            .wait_timeout_while(
                shared.records.lock().unwrap(),
                Duration::from_secs(10),
                |records| {
                    !records
                        .records
                        .iter()
                        .any(|record| kind_of(record) == "physical_dequeue")
                },
            )
            .unwrap();
        drop(records);
        let unlocked_at: u64 = shared.monotonic.elapsed().as_nanos().try_into().unwrap();
        drop(diagnostic_guard);
        assert!(receiver.join().unwrap().unwrap().is_some());
        assert!(
            !wait.timed_out(),
            "physical dequeue blocked by diagnostic mutex"
        );
        unlocked_at
    });
    {
        let records = shared.records.lock().unwrap();
        let timestamp = |kind| {
            records
                .records
                .iter()
                .find(|record| kind_of(record) == kind)
                .unwrap()[1]
                .unwrap()
        };
        assert!(timestamp("physical_dequeue") <= unlocked_at);
        assert!(timestamp("dequeue") >= unlocked_at);
    }
    pump.test_stop_and_join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_dequeue_keeps_input_identity_before_enqueue_trace() {
    let trace = FiniteTrace::configured("dequeue_before_enqueue", 2, 0, 1, true, false, true);
    let shared = trace.0.as_ref().unwrap().clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let entered_tx = StdMutex::new(Some(entered_tx));
    let release_rx = StdMutex::new(release_rx);
    assert!(
        shared
            .before_enqueue_record
            .set(Box::new(move || {
                entered_tx.lock().unwrap().take().unwrap().send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }))
            .is_ok()
    );
    let (client_io, server_io) = tokio::io::duplex(4096);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let mut server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
    let mut pump = LiveUpstreamWebSocket::test_traced_stream(client, trace.clone());
    let mut receive = Box::pin(pump.recv_text());
    assert!(futures_util::poll!(receive.as_mut()).is_pending());
    finite_send(&mut server, &trace, 0, synthetic_created()).await;
    tokio::time::timeout(Duration::from_secs(10), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let observer = std::thread::spawn(move || {
        let kind_of = |record: &FiniteTraceRecord| TRACE_KINDS[record[4].unwrap() as usize - 1];
        let (records, wait) = shared
            .physical_dequeued
            .wait_timeout_while(
                shared.records.lock().unwrap(),
                Duration::from_secs(10),
                |records| {
                    !records
                        .records
                        .iter()
                        .any(|record| kind_of(record) == "physical_dequeue")
                },
            )
            .unwrap();
        let physical = records
            .records
            .iter()
            .find(|record| kind_of(record) == "physical_dequeue")
            .copied();
        let enqueued = records
            .records
            .iter()
            .any(|record| kind_of(record) == "enqueue");
        let released_at: u64 = shared.monotonic.elapsed().as_nanos().try_into().unwrap();
        drop(records);
        release_tx.send(()).unwrap();
        (physical, enqueued, released_at, wait.timed_out())
    });
    let received = receive.await;
    let (physical, enqueued, released_at, timed_out) = observer.join().unwrap();
    pump.test_stop_and_join().await;
    trace.flush();
    assert!(received.unwrap().is_some());
    assert!(!timed_out && !enqueued);
    let physical = physical.unwrap();
    assert_eq!(physical[7], Some(0));
    assert!(physical[1].unwrap() <= released_at);
    {
        let records = trace.0.as_ref().unwrap().records.lock().unwrap();
        for kind in ["enqueue", "dequeue"] {
            let record = records
                .records
                .iter()
                .find(|record| TRACE_KINDS[record[4].unwrap() as usize - 1] == kind)
                .unwrap();
            assert_eq!(record[7], Some(0));
            assert_eq!(record[14], physical[14]);
            assert!(record[1].unwrap() >= released_at);
            assert!(record[0] > physical[0]);
        }
    }
    trace.assert_measurement_records(None);
}
