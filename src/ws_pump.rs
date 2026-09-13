use std::sync::{Arc, Mutex as StdMutex};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt, pin_mut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::error::CapacityError;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamCloseMetadata {
    pub code: Option<u16>,
    pub reason: Option<String>,
    pub error: Option<String>,
}

pub const DEFAULT_UPSTREAM_INBOUND_MAX_MESSAGES: usize = 256;
pub const DEFAULT_UPSTREAM_INBOUND_MAX_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_UPSTREAM_INBOUND_MAX_MESSAGES: usize = 65_536;
pub const MAX_UPSTREAM_INBOUND_MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_UPSTREAM_WATCHDOG_TIMEOUT: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamWatchdogPolicy {
    pong_timeout: Duration,
    write_timeout: Duration,
}

impl UpstreamWatchdogPolicy {
    pub const DEFAULT: Self = Self {
        pong_timeout: Duration::from_secs(60),
        write_timeout: Duration::from_secs(60),
    };

    pub fn new(pong_timeout: Duration, write_timeout: Duration) -> Result<Self, &'static str> {
        if pong_timeout.is_zero() || pong_timeout > MAX_UPSTREAM_WATCHDOG_TIMEOUT {
            return Err("upstream pong timeout must be greater than zero and at most 3600 seconds");
        }
        if write_timeout.is_zero() || write_timeout > MAX_UPSTREAM_WATCHDOG_TIMEOUT {
            return Err(
                "upstream write timeout must be greater than zero and at most 3600 seconds",
            );
        }
        Ok(Self {
            pong_timeout,
            write_timeout,
        })
    }

    pub const fn pong_timeout(self) -> Duration {
        self.pong_timeout
    }

    pub const fn write_timeout(self) -> Duration {
        self.write_timeout
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamLivenessTimeoutCause {
    PongDeadline,
    WriteDeadline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamOutboundKind {
    Text,
    Ping,
    ControlFlush,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamLivenessTimeout {
    pub cause: UpstreamLivenessTimeoutCause,
    pub timeout: Duration,
    pub elapsed: Duration,
    pub outbound_kind: Option<UpstreamOutboundKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamInboundLimits {
    max_messages: usize,
    max_bytes: usize,
}

impl UpstreamInboundLimits {
    pub const DEFAULT: Self = Self {
        max_messages: DEFAULT_UPSTREAM_INBOUND_MAX_MESSAGES,
        max_bytes: DEFAULT_UPSTREAM_INBOUND_MAX_BYTES,
    };

    pub fn new(max_messages: usize, max_bytes: usize) -> Result<Self, &'static str> {
        if !(1..=MAX_UPSTREAM_INBOUND_MAX_MESSAGES).contains(&max_messages) {
            return Err("upstream inbound message limit must be between 1 and 65536");
        }
        if !(1..=MAX_UPSTREAM_INBOUND_MAX_BYTES).contains(&max_bytes) {
            return Err("upstream inbound byte limit must be between 1 and 67108864");
        }
        Ok(Self {
            max_messages,
            max_bytes,
        })
    }

    pub const fn max_messages(self) -> usize {
        self.max_messages
    }

    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundBufferOverflowCause {
    MessageCount,
    PayloadBytes,
    TransportSize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundBufferOverflow {
    pub cause: InboundBufferOverflowCause,
    pub queued_messages: usize,
    pub queued_bytes: usize,
    pub incoming_bytes: usize,
    pub max_messages: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamTerminalState {
    Open,
    Closed(UpstreamCloseMetadata),
    InboundBufferOverflow(InboundBufferOverflow),
    LivenessTimeout(UpstreamLivenessTimeout),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum UpstreamWebSocketError {
    #[error("Threadline could not queue an outbound upstream websocket message.")]
    OutboundQueueClosed,
    #[error("The upstream websocket inbound buffer overflowed.")]
    InboundBufferOverflow,
    #[error("The upstream websocket liveness check timed out.")]
    LivenessTimeout,
}

pub struct LiveUpstreamWebSocket {
    outbound_tx: mpsc::Sender<OutboundCommand>,
    inbound_rx: Mutex<mpsc::Receiver<InboundEnvelope>>,
    terminal_state: Arc<StdMutex<UpstreamTerminalState>>,
    task: JoinHandle<()>,
    #[cfg(test)]
    after_text_send: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    pending_text_send: Option<Arc<TestPendingTextSendState>>,
}

enum OutboundCommand {
    Text(String),
}

struct InboundEnvelope {
    payload: Box<str>,
    _byte_permit: OwnedSemaphorePermit,
}

#[cfg(test)]
pub(crate) struct TestPendingTextSend {
    inbound_tx: mpsc::Sender<InboundEnvelope>,
    inbound_byte_budget: Arc<Semaphore>,
    state: Arc<TestPendingTextSendState>,
}

#[cfg(test)]
struct TestPendingTextSendState {
    outbound_rx: Mutex<Option<mpsc::Receiver<OutboundCommand>>>,
    terminal_state: Arc<StdMutex<UpstreamTerminalState>>,
    pending: Notify,
    send_attempts: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl TestPendingTextSend {
    pub(crate) async fn send_inbound_text(&self, text: impl Into<String>) {
        let text = text.into();
        let byte_permit = self
            .inbound_byte_budget
            .clone()
            .acquire_many_owned(text.len().try_into().expect("test inbound text length"))
            .await
            .expect("test inbound byte budget remains open");
        self.inbound_tx
            .send(InboundEnvelope {
                payload: text.into_boxed_str(),
                _byte_permit: byte_permit,
            })
            .await
            .expect("test inbound receiver remains open");
    }

    pub(crate) async fn wait_for_send_pending(&self) {
        self.state.pending.notified().await;
    }

    pub(crate) async fn trigger_liveness_timeout(&self) {
        record_liveness_timeout(
            &self.state.terminal_state,
            UpstreamLivenessTimeoutCause::WriteDeadline,
            Duration::from_millis(1),
            Duration::from_millis(1),
            Some(UpstreamOutboundKind::Text),
        );
        self.state.outbound_rx.lock().await.take();
    }

    pub(crate) fn text_send_attempts(&self) -> usize {
        self.state
            .send_attempts
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

const OUTBOUND_CHANNEL_CAPACITY: usize = 32;
const UPSTREAM_PING_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct PendingPongChallenge {
    nonce: Vec<u8>,
    sent_at: Option<Instant>,
    acknowledged_early: bool,
}

struct PumpState<'a> {
    inbound_tx: &'a mpsc::Sender<InboundEnvelope>,
    byte_budget: &'a Arc<Semaphore>,
    limits: UpstreamInboundLimits,
    terminal_state: &'a Arc<StdMutex<UpstreamTerminalState>>,
    watchdog_policy: UpstreamWatchdogPolicy,
}

struct PumpLivenessState<'a> {
    pending_challenge: &'a mut Option<PendingPongChallenge>,
    control_flush_needed: &'a mut bool,
    next_ping_due: &'a mut Instant,
    ping_interval: Duration,
}

impl LiveUpstreamWebSocket {
    pub fn from_stream<S>(stream: WebSocketStream<S>) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_limits(stream, UpstreamInboundLimits::DEFAULT)
    }

    pub fn from_stream_with_limits<S>(
        stream: WebSocketStream<S>,
        limits: UpstreamInboundLimits,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_policy_and_limits(
            stream,
            UPSTREAM_PING_INTERVAL,
            UpstreamWatchdogPolicy::DEFAULT,
            limits,
        )
    }

    pub fn from_stream_with_watchdog_policy_and_limits<S>(
        stream: WebSocketStream<S>,
        watchdog_policy: UpstreamWatchdogPolicy,
        limits: UpstreamInboundLimits,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_policy_and_limits(
            stream,
            UPSTREAM_PING_INTERVAL,
            watchdog_policy,
            limits,
        )
    }

    #[cfg(test)]
    fn from_stream_with_ping_interval<S>(
        stream: WebSocketStream<S>,
        ping_interval: Duration,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_policy_and_limits(
            stream,
            ping_interval,
            UpstreamWatchdogPolicy::DEFAULT,
            UpstreamInboundLimits::DEFAULT,
        )
    }

    pub fn from_stream_with_ping_interval_and_watchdog_policy<S>(
        stream: WebSocketStream<S>,
        ping_interval: Duration,
        watchdog_policy: UpstreamWatchdogPolicy,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_policy_and_limits(
            stream,
            ping_interval,
            watchdog_policy,
            UpstreamInboundLimits::DEFAULT,
        )
    }

    fn from_stream_with_policy_and_limits<S>(
        stream: WebSocketStream<S>,
        ping_interval: Duration,
        watchdog_policy: UpstreamWatchdogPolicy,
        limits: UpstreamInboundLimits,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut writer, mut reader) = stream.split();
        let (outbound_tx, mut outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let (inbound_tx, inbound_rx) = mpsc::channel(limits.max_messages());
        let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let task_terminal_state = Arc::clone(&terminal_state);

        let task = tokio::spawn(async move {
            let mut next_ping_due = Instant::now() + ping_interval;
            let mut next_challenge = 0_u64;
            let mut pending_challenge = None;
            let mut control_flush_needed = false;
            let mut prefer_inbound = false;

            debug!(
                outbound_capacity = OUTBOUND_CHANNEL_CAPACITY,
                ping_interval_secs = ping_interval.as_secs_f64(),
                "ws_pump_started"
            );
            let pump_state = PumpState {
                inbound_tx: &inbound_tx,
                byte_budget: &byte_budget,
                limits,
                terminal_state: &task_terminal_state,
                watchdog_policy,
            };
            loop {
                if !check_liveness_deadline(
                    pump_state.terminal_state,
                    pending_challenge.as_ref(),
                    None,
                    pump_state.watchdog_policy,
                ) {
                    break;
                }

                if Instant::now() >= next_ping_due && pending_challenge.is_none() {
                    let nonce = next_watchdog_nonce(&mut next_challenge);
                    pending_challenge = Some(PendingPongChallenge {
                        nonce: nonce.clone(),
                        sent_at: None,
                        acknowledged_early: false,
                    });
                    if !drive_write_operation(
                        &mut writer,
                        &mut reader,
                        &pump_state,
                        Message::Ping(nonce),
                        UpstreamOutboundKind::Ping,
                        PumpLivenessState {
                            pending_challenge: &mut pending_challenge,
                            control_flush_needed: &mut control_flush_needed,
                            next_ping_due: &mut next_ping_due,
                            ping_interval,
                        },
                    )
                    .await
                    {
                        break;
                    }
                    if let Some(challenge) = pending_challenge.as_mut() {
                        if challenge.acknowledged_early {
                            pending_challenge = None;
                            next_ping_due = Instant::now() + ping_interval;
                        } else {
                            challenge.sent_at = Some(Instant::now());
                        }
                    }
                    continue;
                }

                if control_flush_needed {
                    control_flush_needed = false;
                    if !drive_write_operation(
                        &mut writer,
                        &mut reader,
                        &pump_state,
                        Message::Pong(Vec::new()),
                        UpstreamOutboundKind::ControlFlush,
                        PumpLivenessState {
                            pending_challenge: &mut pending_challenge,
                            control_flush_needed: &mut control_flush_needed,
                            next_ping_due: &mut next_ping_due,
                            ping_interval,
                        },
                    )
                    .await
                    {
                        break;
                    }
                    continue;
                }

                let pong_deadline = pending_challenge
                    .as_ref()
                    .and_then(|challenge| challenge.sent_at)
                    .map(|sent_at| sent_at + pump_state.watchdog_policy.pong_timeout());
                if prefer_inbound {
                    tokio::select! {
                        biased;
                        _ = async {
                            if let Some(deadline) = pong_deadline {
                                tokio::time::sleep_until(deadline).await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        } => {},
                        _ = async {
                            if pending_challenge.is_none() {
                                tokio::time::sleep_until(next_ping_due).await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        } => {},
                        inbound = reader.next() => {
                            prefer_inbound = false;
                            let challenge_was_pending = pending_challenge.is_some();
                            if !handle_inbound_message(inbound, &pump_state, &mut pending_challenge, &mut control_flush_needed) {
                                break;
                            }
                            if challenge_was_pending && pending_challenge.is_none() {
                                next_ping_due = Instant::now() + ping_interval;
                            }
                        },
                        outbound = outbound_rx.recv() => match outbound {
                            Some(OutboundCommand::Text(text)) => {
                                prefer_inbound = true;
                                if !drive_write_operation(&mut writer, &mut reader, &pump_state, Message::Text(text), UpstreamOutboundKind::Text, PumpLivenessState { pending_challenge: &mut pending_challenge, control_flush_needed: &mut control_flush_needed, next_ping_due: &mut next_ping_due, ping_interval }).await {
                                    break;
                                }
                            }
                            None => {
                                record_close(pump_state.terminal_state, outbound_channel_closed_metadata());
                                break;
                            }
                        },
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = async {
                            if let Some(deadline) = pong_deadline {
                                tokio::time::sleep_until(deadline).await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        } => {},
                        _ = async {
                            if pending_challenge.is_none() {
                                tokio::time::sleep_until(next_ping_due).await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        } => {},
                        outbound = outbound_rx.recv() => match outbound {
                            Some(OutboundCommand::Text(text)) => {
                                prefer_inbound = true;
                                if !drive_write_operation(&mut writer, &mut reader, &pump_state, Message::Text(text), UpstreamOutboundKind::Text, PumpLivenessState { pending_challenge: &mut pending_challenge, control_flush_needed: &mut control_flush_needed, next_ping_due: &mut next_ping_due, ping_interval }).await {
                                    break;
                                }
                            }
                            None => {
                                record_close(pump_state.terminal_state, outbound_channel_closed_metadata());
                                break;
                            }
                        },
                        inbound = reader.next() => {
                            prefer_inbound = false;
                            let challenge_was_pending = pending_challenge.is_some();
                            if !handle_inbound_message(inbound, &pump_state, &mut pending_challenge, &mut control_flush_needed) {
                                break;
                            }
                            if challenge_was_pending && pending_challenge.is_none() {
                                next_ping_due = Instant::now() + ping_interval;
                            }
                        },
                    }
                }
            }

            record_close(
                &task_terminal_state,
                UpstreamCloseMetadata {
                    code: None,
                    reason: None,
                    error: None,
                },
            );
        });

        Self {
            outbound_tx,
            inbound_rx: Mutex::new(inbound_rx),
            terminal_state,
            task,
            #[cfg(test)]
            after_text_send: None,
            #[cfg(test)]
            pending_text_send: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_liveness_timeout_after_first_text_send(
        send_attempts: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        use std::sync::atomic::Ordering;

        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let (_inbound_tx, inbound_rx) = mpsc::channel(1);
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let callback_terminal_state = Arc::clone(&terminal_state);
        let after_text_send = Arc::new(move || {
            send_attempts.fetch_add(1, Ordering::SeqCst);
            record_liveness_timeout(
                &callback_terminal_state,
                UpstreamLivenessTimeoutCause::WriteDeadline,
                Duration::from_millis(1),
                Duration::from_millis(1),
                Some(UpstreamOutboundKind::Text),
            );
        });

        Self {
            outbound_tx,
            inbound_rx: Mutex::new(inbound_rx),
            terminal_state,
            task: tokio::spawn(async move {
                let _outbound_rx = outbound_rx;
                std::future::pending::<()>().await;
            }),
            after_text_send: Some(after_text_send),
            pending_text_send: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_followup_send_pending_for_liveness_timeout() -> (Self, TestPendingTextSend) {
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        for _ in 0..OUTBOUND_CHANNEL_CAPACITY {
            outbound_tx
                .try_send(OutboundCommand::Text("occupied".to_string()))
                .expect("fill test outbound queue");
        }
        let (inbound_tx, inbound_rx) = mpsc::channel(2);
        let inbound_byte_budget = Arc::new(Semaphore::new(4096));
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let state = Arc::new(TestPendingTextSendState {
            outbound_rx: Mutex::new(Some(outbound_rx)),
            terminal_state: Arc::clone(&terminal_state),
            pending: Notify::new(),
            send_attempts: std::sync::atomic::AtomicUsize::new(0),
        });
        let fixture = TestPendingTextSend {
            inbound_tx,
            inbound_byte_budget,
            state: Arc::clone(&state),
        };

        (
            Self {
                outbound_tx,
                inbound_rx: Mutex::new(inbound_rx),
                terminal_state,
                task: tokio::spawn(std::future::pending()),
                after_text_send: None,
                pending_text_send: Some(state),
            },
            fixture,
        )
    }

    pub async fn send_text(&self, text: impl Into<String>) -> Result<(), UpstreamWebSocketError> {
        self.terminal_error()?;
        #[cfg(test)]
        if let Some(pending_text_send) = &self.pending_text_send {
            pending_text_send
                .send_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let permit = match self.outbound_tx.try_reserve() {
                Ok(permit) => permit,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    pending_text_send.pending.notify_one();
                    self.outbound_tx.reserve().await.map_err(|_| {
                        self.terminal_error()
                            .err()
                            .unwrap_or(UpstreamWebSocketError::OutboundQueueClosed)
                    })?
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(self
                        .terminal_error()
                        .err()
                        .unwrap_or(UpstreamWebSocketError::OutboundQueueClosed));
                }
            };
            permit.send(OutboundCommand::Text(text.into()));
            return self.terminal_error();
        }
        self.outbound_tx
            .send(OutboundCommand::Text(text.into()))
            .await
            .map_err(|_| {
                self.terminal_error()
                    .err()
                    .unwrap_or(UpstreamWebSocketError::OutboundQueueClosed)
            })?;
        #[cfg(test)]
        if let Some(after_text_send) = &self.after_text_send {
            after_text_send();
        }
        self.terminal_error()
    }

    pub async fn recv_text(&self) -> Result<Option<String>, UpstreamWebSocketError> {
        self.terminal_error()?;
        let envelope = self.inbound_rx.lock().await.recv().await;
        self.terminal_error()?;
        Ok(envelope.map(|envelope| envelope.payload.into_string()))
    }

    pub fn is_closed(&self) -> bool {
        !matches!(self.terminal_state(), UpstreamTerminalState::Open)
    }

    pub async fn close_metadata(&self) -> Option<UpstreamCloseMetadata> {
        match self.terminal_state() {
            UpstreamTerminalState::Closed(metadata) => Some(metadata),
            UpstreamTerminalState::LivenessTimeout(_) => Some(liveness_timeout_close_metadata()),
            UpstreamTerminalState::Open | UpstreamTerminalState::InboundBufferOverflow(_) => None,
        }
    }

    pub fn terminal_state(&self) -> UpstreamTerminalState {
        self.terminal_state
            .lock()
            .expect("terminal state lock")
            .clone()
    }

    fn terminal_error(&self) -> Result<(), UpstreamWebSocketError> {
        match self.terminal_state() {
            UpstreamTerminalState::InboundBufferOverflow(_) => {
                Err(UpstreamWebSocketError::InboundBufferOverflow)
            }
            UpstreamTerminalState::LivenessTimeout(_) => {
                Err(UpstreamWebSocketError::LivenessTimeout)
            }
            UpstreamTerminalState::Open | UpstreamTerminalState::Closed(_) => Ok(()),
        }
    }
}

impl Drop for LiveUpstreamWebSocket {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn drive_write_operation<S>(
    writer: &mut SplitSink<WebSocketStream<S>, Message>,
    reader: &mut SplitStream<WebSocketStream<S>>,
    pump_state: &PumpState<'_>,
    message: Message,
    outbound_kind: UpstreamOutboundKind,
    liveness_state: PumpLivenessState<'_>,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let operation_started = Instant::now();
    let send = async {
        match outbound_kind {
            UpstreamOutboundKind::ControlFlush => writer.flush().await,
            UpstreamOutboundKind::Text | UpstreamOutboundKind::Ping => writer.send(message).await,
        }
    };
    pin_mut!(send);

    loop {
        if !check_liveness_deadline(
            pump_state.terminal_state,
            liveness_state.pending_challenge.as_ref(),
            Some((operation_started, outbound_kind)),
            pump_state.watchdog_policy,
        ) {
            return false;
        }
        let pong_deadline = liveness_state
            .pending_challenge
            .as_ref()
            .and_then(|challenge| challenge.sent_at)
            .map(|sent_at| sent_at + pump_state.watchdog_policy.pong_timeout());
        let write_deadline = operation_started + pump_state.watchdog_policy.write_timeout();
        let earliest_deadline =
            pong_deadline.map_or(write_deadline, |deadline| deadline.min(write_deadline));
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(earliest_deadline) => {
                if !check_liveness_deadline(
                    pump_state.terminal_state,
                    liveness_state.pending_challenge.as_ref(),
                    Some((operation_started, outbound_kind)),
                    pump_state.watchdog_policy,
                ) {
                    return false;
                }
            }
            result = &mut send => match result {
                Ok(()) => return true,
                Err(error) => {
                    record_error(pump_state.terminal_state, error.to_string());
                    return false;
                }
            },
            inbound = reader.next() => {
                let challenge_was_pending = liveness_state.pending_challenge.is_some();
                if !handle_inbound_message(
                    inbound,
                    pump_state,
                    liveness_state.pending_challenge,
                    liveness_state.control_flush_needed,
                ) {
                    return false;
                }
                if challenge_was_pending && liveness_state.pending_challenge.is_none() {
                    *liveness_state.next_ping_due = Instant::now() + liveness_state.ping_interval;
                }
            }
        }
    }
}

fn next_watchdog_nonce(counter: &mut u64) -> Vec<u8> {
    let nonce = format!("threadline-{}", *counter).into_bytes();
    *counter = counter.wrapping_add(1);
    nonce
}

fn handle_inbound_message(
    inbound: Option<Result<Message, TungsteniteError>>,
    pump_state: &PumpState<'_>,
    pending_challenge: &mut Option<PendingPongChallenge>,
    control_flush_needed: &mut bool,
) -> bool {
    match inbound {
        Some(Ok(Message::Text(text))) => try_enqueue_inbound(
            pump_state.inbound_tx,
            pump_state.byte_budget,
            pump_state.limits,
            text.to_string(),
            pump_state.terminal_state,
        ),
        Some(Ok(Message::Binary(bytes))) => try_enqueue_inbound(
            pump_state.inbound_tx,
            pump_state.byte_budget,
            pump_state.limits,
            String::from_utf8_lossy(bytes.as_ref()).into_owned(),
            pump_state.terminal_state,
        ),
        Some(Ok(Message::Ping(payload))) => {
            *control_flush_needed = true;
            debug!(payload_len = payload.len(), "ws_pump_ping_received");
            true
        }
        Some(Ok(Message::Pong(payload))) => {
            if pending_challenge.as_mut().is_some_and(|challenge| {
                challenge.nonce == payload
                    && challenge.sent_at.is_none_or(|sent_at| {
                        Instant::now() < sent_at + pump_state.watchdog_policy.pong_timeout()
                    })
            }) {
                let challenge = pending_challenge
                    .as_mut()
                    .expect("matching challenge remains present");
                if challenge.sent_at.is_some() {
                    *pending_challenge = None;
                } else {
                    challenge.acknowledged_early = true;
                }
            }
            debug!(payload_len = payload.len(), "ws_pump_pong_received");
            true
        }
        Some(Ok(Message::Close(frame))) => {
            record_close(
                pump_state.terminal_state,
                UpstreamCloseMetadata {
                    code: frame.as_ref().map(|frame| u16::from(frame.code)),
                    reason: frame.as_ref().map(|frame| frame.reason.to_string()),
                    error: None,
                },
            );
            false
        }
        Some(Ok(Message::Frame(_))) => true,
        Some(Err(error)) => {
            if !record_transport_overflow(
                pump_state.terminal_state,
                pump_state.inbound_tx,
                pump_state.byte_budget,
                pump_state.limits,
                &error,
            ) {
                record_error(pump_state.terminal_state, error.to_string());
            }
            false
        }
        None => false,
    }
}

fn check_liveness_deadline(
    terminal_state: &Arc<StdMutex<UpstreamTerminalState>>,
    pending_challenge: Option<&PendingPongChallenge>,
    operation: Option<(Instant, UpstreamOutboundKind)>,
    watchdog_policy: UpstreamWatchdogPolicy,
) -> bool {
    let now = Instant::now();
    let pong_expired = pending_challenge
        .and_then(|challenge| challenge.sent_at)
        .map(|sent_at| (sent_at + watchdog_policy.pong_timeout(), sent_at))
        .filter(|(deadline, _)| *deadline <= now);
    let write_expired = operation
        .map(|(started_at, kind)| {
            (
                started_at + watchdog_policy.write_timeout(),
                started_at,
                kind,
            )
        })
        .filter(|(deadline, _, _)| *deadline <= now);

    match (pong_expired, write_expired) {
        (None, None) => true,
        (Some((pong_deadline, sent_at)), Some((write_deadline, _, _)))
            if pong_deadline <= write_deadline =>
        {
            record_liveness_timeout(
                terminal_state,
                UpstreamLivenessTimeoutCause::PongDeadline,
                watchdog_policy.pong_timeout(),
                now.saturating_duration_since(sent_at),
                None,
            );
            false
        }
        (Some(_), Some((_, started_at, kind))) | (None, Some((_, started_at, kind))) => {
            record_liveness_timeout(
                terminal_state,
                UpstreamLivenessTimeoutCause::WriteDeadline,
                watchdog_policy.write_timeout(),
                now.saturating_duration_since(started_at),
                Some(kind),
            );
            false
        }
        (Some((_, sent_at)), None) => {
            record_liveness_timeout(
                terminal_state,
                UpstreamLivenessTimeoutCause::PongDeadline,
                watchdog_policy.pong_timeout(),
                now.saturating_duration_since(sent_at),
                None,
            );
            false
        }
    }
}

fn record_liveness_timeout(
    terminal_state: &Arc<StdMutex<UpstreamTerminalState>>,
    cause: UpstreamLivenessTimeoutCause,
    timeout: Duration,
    elapsed: Duration,
    outbound_kind: Option<UpstreamOutboundKind>,
) {
    let metadata = UpstreamLivenessTimeout {
        cause,
        timeout,
        elapsed,
        outbound_kind,
    };
    let mut state = terminal_state.lock().expect("terminal state lock");
    if !matches!(*state, UpstreamTerminalState::Open) {
        return;
    }
    *state = UpstreamTerminalState::LivenessTimeout(metadata.clone());
    drop(state);
    tracing::warn!(
        cause = ?metadata.cause,
        timeout_ms = metadata.timeout.as_millis() as u64,
        elapsed_ms = metadata.elapsed.as_millis() as u64,
        outbound_kind = ?metadata.outbound_kind,
        "ws_pump_liveness_timeout"
    );
}

fn liveness_timeout_close_metadata() -> UpstreamCloseMetadata {
    UpstreamCloseMetadata {
        code: None,
        reason: None,
        error: Some("upstream websocket liveness timeout".to_string()),
    }
}

fn try_enqueue_inbound(
    sender: &mpsc::Sender<InboundEnvelope>,
    byte_budget: &Arc<Semaphore>,
    limits: UpstreamInboundLimits,
    payload: String,
    terminal_state: &Arc<StdMutex<UpstreamTerminalState>>,
) -> bool {
    let incoming_bytes = payload.len();
    if !payload_fits_inbound_byte_limit(incoming_bytes, limits) {
        record_inbound_overflow(
            terminal_state,
            inbound_overflow(
                InboundBufferOverflowCause::PayloadBytes,
                sender,
                byte_budget,
                limits,
                incoming_bytes,
            ),
        );
        return false;
    }
    let permit_count = u32::try_from(incoming_bytes)
        .expect("validated inbound byte limit always fits the semaphore permit count");
    let byte_permit = byte_budget.clone().try_acquire_many_owned(permit_count);
    let cause = match byte_permit {
        Ok(byte_permit) => match sender.try_send(InboundEnvelope {
            payload: payload.into_boxed_str(),
            _byte_permit: byte_permit,
        }) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Full(_)) => InboundBufferOverflowCause::MessageCount,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        },
        Err(_) => InboundBufferOverflowCause::PayloadBytes,
    };
    record_inbound_overflow(
        terminal_state,
        inbound_overflow(cause, sender, byte_budget, limits, incoming_bytes),
    );
    false
}

fn payload_fits_inbound_byte_limit(incoming_bytes: usize, limits: UpstreamInboundLimits) -> bool {
    incoming_bytes <= limits.max_bytes()
}

fn inbound_overflow(
    cause: InboundBufferOverflowCause,
    sender: &mpsc::Sender<InboundEnvelope>,
    byte_budget: &Arc<Semaphore>,
    limits: UpstreamInboundLimits,
    incoming_bytes: usize,
) -> InboundBufferOverflow {
    InboundBufferOverflow {
        cause,
        queued_messages: limits.max_messages().saturating_sub(sender.capacity()),
        queued_bytes: limits
            .max_bytes()
            .saturating_sub(byte_budget.available_permits()),
        incoming_bytes,
        max_messages: limits.max_messages(),
        max_bytes: limits.max_bytes(),
    }
}

fn record_inbound_overflow(
    terminal_state: &Arc<StdMutex<UpstreamTerminalState>>,
    overflow: InboundBufferOverflow,
) {
    let mut state = terminal_state.lock().expect("terminal state lock");
    if !matches!(*state, UpstreamTerminalState::Open) {
        return;
    }
    *state = UpstreamTerminalState::InboundBufferOverflow(overflow.clone());
    drop(state);
    tracing::warn!(
        overflow_cause = ?overflow.cause,
        queued_messages = overflow.queued_messages,
        queued_bytes = overflow.queued_bytes,
        incoming_bytes = overflow.incoming_bytes,
        max_messages = overflow.max_messages,
        max_bytes = overflow.max_bytes,
        recoverable = false,
        "ws_pump_inbound_overflow"
    );
}

fn record_close(target: &Arc<StdMutex<UpstreamTerminalState>>, metadata: UpstreamCloseMetadata) {
    let mut state = target.lock().expect("terminal state lock");
    if matches!(*state, UpstreamTerminalState::Open) {
        *state = UpstreamTerminalState::Closed(metadata);
    }
}

fn record_error(target: &Arc<StdMutex<UpstreamTerminalState>>, error: String) {
    record_close(
        target,
        UpstreamCloseMetadata {
            code: None,
            reason: None,
            error: Some(error),
        },
    );
}

fn record_transport_overflow(
    target: &Arc<StdMutex<UpstreamTerminalState>>,
    sender: &mpsc::Sender<InboundEnvelope>,
    byte_budget: &Arc<Semaphore>,
    limits: UpstreamInboundLimits,
    error: &TungsteniteError,
) -> bool {
    let TungsteniteError::Capacity(CapacityError::MessageTooLong { size, .. }) = error else {
        return false;
    };
    record_inbound_overflow(
        target,
        inbound_overflow(
            InboundBufferOverflowCause::TransportSize,
            sender,
            byte_budget,
            limits,
            *size,
        ),
    );
    true
}

fn outbound_channel_closed_metadata() -> UpstreamCloseMetadata {
    UpstreamCloseMetadata {
        code: None,
        reason: Some("outbound channel closed".to_string()),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context as TaskContext, Poll};

    use super::*;
    use futures_util::task::AtomicWaker;
    use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, oneshot};
    use tokio::time::{advance, timeout};
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    async fn connect_test_pump() -> LiveUpstreamWebSocket {
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

    struct WriteGateIo {
        inner: DuplexStream,
        write_started: Arc<Notify>,
        write_open: Arc<AtomicBool>,
        write_waker: Arc<AtomicWaker>,
        _release_token: Option<Arc<()>>,
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

    struct FlushGateIo {
        inner: DuplexStream,
        flush_started: Arc<Notify>,
        flush_open: Arc<AtomicBool>,
        flush_waker: Arc<AtomicWaker>,
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

    #[tokio::test]
    async fn websocket_pump_receives_text_while_outbound_write_is_pending() {
        let (client_io, server_io) = duplex(64);
        let write_started = Arc::new(Notify::new());
        let write_open = Arc::new(AtomicBool::new(false));
        let write_waker = Arc::new(AtomicWaker::new());
        let client_io = WriteGateIo {
            inner: client_io,
            write_started: Arc::clone(&write_started),
            write_open: Arc::clone(&write_open),
            write_waker: Arc::clone(&write_waker),
            _release_token: None,
        };
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let pump = LiveUpstreamWebSocket::from_stream(client);

        pump.send_text("outbound")
            .await
            .expect("queue outbound text");
        timeout(Duration::from_secs(1), write_started.notified())
            .await
            .expect("writer should become pending at the gate");

        server_writer
            .send(Message::Text("reader-progress".to_string()))
            .await
            .expect("send inbound text");
        assert_eq!(
            timeout(Duration::from_secs(1), pump.recv_text())
                .await
                .expect("reader should progress while writer is pending")
                .expect("receive inbound text"),
            Some("reader-progress".to_string())
        );

        write_open.store(true, Ordering::SeqCst);
        write_waker.wake();
        assert!(matches!(
            timeout(Duration::from_secs(1), server_reader.next())
                .await
                .expect("outbound text should finish after the gate opens"),
            Some(Ok(Message::Text(text))) if text == "outbound"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_reads_text_while_automatic_control_flush_is_pending() {
        let (client_io, server_io) = duplex(64);
        let write_started = Arc::new(Notify::new());
        let write_open = Arc::new(AtomicBool::new(false));
        let write_waker = Arc::new(AtomicWaker::new());
        let client_io = WriteGateIo {
            inner: client_io,
            write_started: Arc::clone(&write_started),
            write_open,
            write_waker,
            _release_token: None,
        };
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, _server_reader) = server.split();
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(8), Duration::from_secs(5))
            .expect("valid watchdog policy");
        let pump = Arc::new(
            LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                client,
                Duration::from_secs(60),
                policy,
            ),
        );

        server_writer
            .send(Message::Ping(b"server-ping".to_vec()))
            .await
            .expect("send server Ping");
        timeout(Duration::from_secs(1), write_started.notified())
            .await
            .expect("automatic control flush should reach the controlled gate");
        server_writer
            .send(Message::Text("reader-progress".to_string()))
            .await
            .expect("send inbound text while flush is pending");
        assert_eq!(
            pump.recv_text().await.expect("receive inbound text"),
            Some("reader-progress".to_string())
        );

        advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            pump.recv_text().await,
            Err(UpstreamWebSocketError::LivenessTimeout)
        );
        assert!(matches!(
            pump.terminal_state(),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::WriteDeadline,
                outbound_kind: Some(UpstreamOutboundKind::ControlFlush),
                ..
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_reads_matching_pong_and_text_during_sustained_outbound_traffic() {
        let (client_io, server_io) = duplex(4096);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let policy = UpstreamWatchdogPolicy::new(Duration::from_millis(1), Duration::from_secs(1))
            .expect("valid watchdog policy");
        let pump = Arc::new(
            LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                client,
                Duration::from_millis(1),
                policy,
            ),
        );
        let (challenge_sent_tx, challenge_sent_rx) = oneshot::channel();

        tokio::task::yield_now().await;

        let server_task = tokio::spawn(async move {
            let mut challenge_sent_tx = Some(challenge_sent_tx);
            while let Some(message) = server_reader.next().await {
                match message.expect("read client frame") {
                    Message::Ping(nonce) => {
                        server_writer
                            .send(Message::Pong(nonce))
                            .await
                            .expect("send matching Pong");
                        server_writer
                            .send(Message::Text("ready-inbound".to_string()))
                            .await
                            .expect("send ready inbound Text");
                        if let Some(challenge_sent_tx) = challenge_sent_tx.take() {
                            challenge_sent_tx
                                .send(())
                                .expect("report challenge response");
                        }
                    }
                    Message::Text(_)
                    | Message::Pong(_)
                    | Message::Binary(_)
                    | Message::Close(_) => {}
                    Message::Frame(_) => {}
                }
            }
        });
        let producer_pump = Arc::clone(&pump);
        let producer = tokio::spawn(async move {
            for _ in 0..256 {
                if producer_pump.send_text("outbound").await.is_err() {
                    break;
                }
            }
        });

        advance(Duration::from_millis(1)).await;
        challenge_sent_rx
            .await
            .expect("server should receive challenge");
        advance(Duration::from_millis(1)).await;
        assert_eq!(
            pump.recv_text()
                .await
                .expect("healthy pump should receive Text"),
            Some("ready-inbound".to_string())
        );
        assert!(
            matches!(pump.terminal_state(), UpstreamTerminalState::Open),
            "{:?}",
            pump.terminal_state()
        );

        producer.abort();
        server_task.abort();
    }

    #[tokio::test]
    async fn websocket_pump_sends_active_ping_after_interval() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("local addr");
        let (ping_seen_tx, ping_seen_rx) = oneshot::channel();

        let accept_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept client");
            let websocket = accept_async(stream).await.expect("accept websocket");
            let (_writer, mut reader) = websocket.split();

            while let Some(message) = reader.next().await {
                match message.expect("read websocket message") {
                    Message::Ping(payload) => {
                        assert!(!payload.is_empty());
                        assert!(payload.len() <= 125);
                        let _ = ping_seen_tx.send(());
                        break;
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        });

        let (stream, _) = connect_async(format!("ws://{address}"))
            .await
            .expect("connect websocket");
        let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval(
            stream,
            Duration::from_millis(20),
        );

        timeout(Duration::from_secs(2), ping_seen_rx)
            .await
            .expect("active ping should be sent")
            .expect("server should report active ping");

        drop(pump);
        accept_task.await.expect("accept task");
    }

    #[tokio::test]
    async fn websocket_pump_times_out_when_a_sent_challenge_never_receives_a_matching_pong() {
        let (client_io, _server_io) = duplex(1024);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let policy =
            UpstreamWatchdogPolicy::new(Duration::from_millis(20), Duration::from_millis(100))
                .expect("valid short policy");
        let pump = Arc::new(
            LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                client,
                Duration::from_millis(1),
                policy,
            ),
        );
        let waiting_recv = {
            let pump = Arc::clone(&pump);
            tokio::spawn(async move { pump.recv_text().await })
        };

        assert_eq!(
            timeout(Duration::from_secs(1), waiting_recv)
                .await
                .expect("Pong timeout should release the receiver")
                .expect("receiver task should not panic"),
            Err(UpstreamWebSocketError::LivenessTimeout)
        );
        assert!(matches!(
            pump.terminal_state(),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::PongDeadline,
                timeout,
                outbound_kind: None,
                ..
            }) if timeout == Duration::from_millis(20)
        ));
        assert_eq!(
            pump.close_metadata().await,
            Some(liveness_timeout_close_metadata())
        );
    }

    #[tokio::test]
    async fn websocket_pump_times_out_a_stalled_ping_write() {
        let (client_io, _server_io) = duplex(1024);
        let write_started = Arc::new(Notify::new());
        let write_open = Arc::new(AtomicBool::new(false));
        let write_waker = Arc::new(AtomicWaker::new());
        let client_io = WriteGateIo {
            inner: client_io,
            write_started: Arc::clone(&write_started),
            write_open,
            write_waker,
            _release_token: None,
        };
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let policy =
            UpstreamWatchdogPolicy::new(Duration::from_millis(100), Duration::from_millis(20))
                .expect("valid short policy");
        let pump = Arc::new(
            LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                client,
                Duration::from_millis(1),
                policy,
            ),
        );
        timeout(Duration::from_secs(1), write_started.notified())
            .await
            .expect("Ping write should reach the controlled gate");

        assert_eq!(
            timeout(Duration::from_secs(1), pump.recv_text())
                .await
                .expect("write timeout should release the receiver"),
            Err(UpstreamWebSocketError::LivenessTimeout)
        );
        assert!(matches!(
            pump.terminal_state(),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::WriteDeadline,
                timeout,
                outbound_kind: Some(UpstreamOutboundKind::Ping),
                ..
            }) if timeout == Duration::from_millis(20)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_processes_delayed_matching_pong_before_deadline_and_schedules_from_ack()
    {
        let (client_io, server_io) = duplex(1024);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let (first_ping_tx, first_ping_rx) = oneshot::channel();
        let (ack_tx, ack_rx) = oneshot::channel();
        let (next_ping_tx, mut next_ping_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let nonce = match server_reader
                .next()
                .await
                .expect("first client frame")
                .expect("valid first client frame")
            {
                Message::Ping(nonce) => nonce,
                message => panic!("expected first client Ping, got {message:?}"),
            };
            first_ping_tx.send(()).expect("report first Ping");
            ack_rx.await.expect("release delayed matching Pong");
            server_writer
                .send(Message::Pong(nonce))
                .await
                .expect("send matching Pong");
            server_writer
                .send(Message::Text("after-delayed-ack".to_string()))
                .await
                .expect("send inbound Text");
            match server_reader
                .next()
                .await
                .expect("next client frame")
                .expect("valid next client frame")
            {
                Message::Ping(_) => next_ping_tx.send(()).expect("report next Ping"),
                message => panic!("expected next client Ping, got {message:?}"),
            }
        });
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(5))
            .expect("valid watchdog policy");
        let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(1),
            policy,
        );

        advance(Duration::from_secs(1)).await;
        first_ping_rx.await.expect("first Ping should be sent");
        advance(Duration::from_secs(2)).await;
        ack_tx.send(()).expect("allow matching Pong");
        assert_eq!(
            pump.recv_text().await.expect("receive inbound Text"),
            Some("after-delayed-ack".to_string())
        );
        assert!(matches!(pump.terminal_state(), UpstreamTerminalState::Open));
        assert!(next_ping_rx.try_recv().is_err());

        advance(Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert!(next_ping_rx.try_recv().is_err());
        advance(Duration::from_millis(1)).await;
        next_ping_rx
            .await
            .expect("next Ping should be scheduled from ACK time");

        drop(pump);
        peer.await.expect("peer task should complete");
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_stays_open_across_multiple_matching_challenges_without_text() {
        let (client_io, server_io) = duplex(1024);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let (completed_tx, completed_rx) = oneshot::channel();
        let (challenge_tx, mut challenge_rx) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            let mut challenges = Vec::new();
            while let Some(message) = server_reader.next().await {
                match message.expect("read client websocket frame") {
                    Message::Ping(payload) => {
                        challenges.push(payload.clone());
                        server_writer
                            .send(Message::Pong(payload))
                            .await
                            .expect("reply with matching Pong");
                        challenge_tx.send(()).expect("report matching Pong write");
                        if challenges.len() == 3 {
                            server_writer
                                .send(Message::Text("response-completed".to_string()))
                                .await
                                .expect("send completion text");
                            let _ = completed_tx.send(challenges);
                            while server_reader.next().await.is_some() {}
                            return;
                        }
                    }
                    Message::Close(_) => return,
                    _ => {}
                }
            }
        });
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(5))
            .expect("valid watchdog policy");
        let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(1),
            policy,
        );

        for _ in 0..3 {
            advance(Duration::from_secs(6)).await;
            challenge_rx
                .recv()
                .await
                .expect("peer should reply to the current challenge");
            tokio::task::yield_now().await;
        }

        let challenges = completed_rx
            .await
            .expect("peer should observe three challenges");
        assert_eq!(challenges.len(), 3);
        assert!(
            challenges
                .iter()
                .all(|nonce| !nonce.is_empty() && nonce.len() <= 125)
        );
        assert_ne!(challenges[0], challenges[1]);
        assert_ne!(challenges[1], challenges[2]);
        assert!(!pump.is_closed(), "{:?}", pump.terminal_state());
        assert_eq!(
            pump.recv_text().await.expect("receive completion text"),
            Some("response-completed".to_string())
        );
        drop(pump);
        peer.await.expect("peer task should complete");
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_ignores_stale_and_unsolicited_pong_until_the_original_deadline() {
        let limits = UpstreamInboundLimits::new(2, 16).expect("valid limits");
        let (inbound_tx, mut inbound_rx) = mpsc::channel(limits.max_messages());
        let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(8))
            .expect("valid watchdog policy");
        let pump_state = PumpState {
            inbound_tx: &inbound_tx,
            byte_budget: &byte_budget,
            limits,
            terminal_state: &terminal_state,
            watchdog_policy: policy,
        };
        let sent_at = Instant::now();
        let mut challenge = Some(PendingPongChallenge {
            nonce: b"current-challenge".to_vec(),
            sent_at: Some(sent_at),
            acknowledged_early: false,
        });
        let mut control_flush_needed = false;

        assert!(handle_inbound_message(
            Some(Ok(Message::Pong(b"stale-challenge".to_vec()))),
            &pump_state,
            &mut challenge,
            &mut control_flush_needed,
        ));
        assert!(handle_inbound_message(
            Some(Ok(Message::Pong(b"unsolicited-pong".to_vec()))),
            &pump_state,
            &mut challenge,
            &mut control_flush_needed,
        ));
        assert!(handle_inbound_message(
            Some(Ok(Message::Text("traffic".to_string()))),
            &pump_state,
            &mut challenge,
            &mut control_flush_needed,
        ));
        assert!(handle_inbound_message(
            Some(Ok(Message::Ping(b"server-ping".to_vec()))),
            &pump_state,
            &mut challenge,
            &mut control_flush_needed,
        ));
        assert!(control_flush_needed);
        assert_eq!(
            inbound_rx
                .recv()
                .await
                .map(|envelope| envelope.payload.into_string()),
            Some("traffic".to_string())
        );

        advance(Duration::from_secs(5)).await;
        assert!(!check_liveness_deadline(
            &terminal_state,
            challenge.as_ref(),
            None,
            policy,
        ));
        assert!(matches!(
            *terminal_state.lock().expect("terminal state lock"),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::PongDeadline,
                timeout,
                outbound_kind: None,
                ..
            }) if timeout == Duration::from_secs(5)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_times_out_at_original_deadline_despite_irrelevant_peer_traffic() {
        let (client_io, server_io) = duplex(1024);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let (traffic_tx, traffic_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            match server_reader
                .next()
                .await
                .expect("client Ping")
                .expect("valid client Ping")
            {
                Message::Ping(_) => {}
                message => panic!("expected client Ping, got {message:?}"),
            }
            for payload in [
                b"mismatch".as_slice(),
                b"stale".as_slice(),
                b"unsolicited".as_slice(),
            ] {
                server_writer
                    .send(Message::Pong(payload.to_vec()))
                    .await
                    .expect("send irrelevant Pong");
            }
            server_writer
                .send(Message::Text("irrelevant-data".to_string()))
                .await
                .expect("send data traffic");
            server_writer
                .send(Message::Ping(b"server-ping".to_vec()))
                .await
                .expect("send server Ping");
            traffic_tx.send(()).expect("report peer traffic");
            while server_reader.next().await.is_some() {}
        });
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(8))
            .expect("valid watchdog policy");
        let pump = Arc::new(
            LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                client,
                Duration::from_secs(1),
                policy,
            ),
        );

        advance(Duration::from_secs(1)).await;
        traffic_rx.await.expect("peer should receive client Ping");
        assert_eq!(
            pump.recv_text()
                .await
                .expect("data traffic should be delivered"),
            Some("irrelevant-data".to_string())
        );
        advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            pump.terminal_state(),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::PongDeadline,
                timeout,
                outbound_kind: None,
                ..
            }) if timeout == Duration::from_secs(5)
        ));

        drop(pump);
        peer.await.expect("peer task should complete");
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_dispatches_due_ping_before_queued_text_under_ready_traffic() {
        let (client_io, server_io) = duplex(4096);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(5))
            .expect("valid watchdog policy");
        let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(1),
            policy,
        );
        tokio::task::yield_now().await;

        for index in 0..4 {
            server_writer
                .send(Message::Text(format!("ready-{index}")))
                .await
                .expect("send ready inbound text");
        }

        advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        pump.send_text("queued-text")
            .await
            .expect("queue outbound text after Ping is due");
        let first = server_reader
            .next()
            .await
            .expect("due Ping frame")
            .expect("valid due Ping frame");
        let nonce = match first {
            Message::Ping(nonce) => nonce,
            message => panic!("expected due Ping before queued Text, got {message:?}"),
        };
        server_writer
            .send(Message::Pong(nonce))
            .await
            .expect("acknowledge due Ping");
        assert!(matches!(
            server_reader
                .next()
                .await
                .expect("queued Text frame")
                .expect("valid queued Text frame"),
            Message::Text(text) if text == "queued-text"
        ));

        drop(pump);
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_keeps_a_held_text_deadline_while_reading_server_traffic() {
        let (client_io, server_io) = duplex(1024);
        let write_started = Arc::new(Notify::new());
        let write_open = Arc::new(AtomicBool::new(false));
        let write_waker = Arc::new(AtomicWaker::new());
        let client_io = WriteGateIo {
            inner: client_io,
            write_started: Arc::clone(&write_started),
            write_open,
            write_waker,
            _release_token: None,
        };
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, _server_reader) = server.split();
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5))
            .expect("valid watchdog policy");
        let pump = Arc::new(
            LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                client,
                Duration::from_secs(60),
                policy,
            ),
        );

        pump.send_text("held-text").await.expect("queue held text");
        timeout(Duration::from_secs(1), write_started.notified())
            .await
            .expect("Text write should reach the controlled gate");
        for payload in [b"server-ping-one".as_slice(), b"server-ping-two".as_slice()] {
            server_writer
                .send(Message::Ping(payload.to_vec()))
                .await
                .expect("send server Ping while Text write is pending");
        }
        server_writer
            .send(Message::Text("reader-progress".to_string()))
            .await
            .expect("send inbound Text while write is pending");
        assert_eq!(
            pump.recv_text().await.expect("receive inbound Text"),
            Some("reader-progress".to_string())
        );

        advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            pump.recv_text().await,
            Err(UpstreamWebSocketError::LivenessTimeout)
        );
        assert!(matches!(
            pump.terminal_state(),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::WriteDeadline,
                timeout,
                elapsed,
                outbound_kind: Some(UpstreamOutboundKind::Text),
            }) if timeout == Duration::from_secs(5) && elapsed >= timeout
        ));
    }

    #[tokio::test]
    async fn websocket_pump_flushes_automatic_pong_and_one_held_text_after_the_gate_opens() {
        let (client_io, server_io) = duplex(1024);
        let write_started = Arc::new(Notify::new());
        let write_open = Arc::new(AtomicBool::new(false));
        let write_waker = Arc::new(AtomicWaker::new());
        let client_io = WriteGateIo {
            inner: client_io,
            write_started: Arc::clone(&write_started),
            write_open: Arc::clone(&write_open),
            write_waker: Arc::clone(&write_waker),
            _release_token: None,
        };
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5))
            .expect("valid watchdog policy");
        let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(60),
            policy,
        );

        pump.send_text("held-text").await.expect("queue held text");
        timeout(Duration::from_secs(1), write_started.notified())
            .await
            .expect("Text write should reach the controlled gate");
        server_writer
            .send(Message::Ping(b"server-ping-one".to_vec()))
            .await
            .expect("send first server Ping");
        server_writer
            .send(Message::Ping(b"server-ping-two".to_vec()))
            .await
            .expect("send second server Ping");
        server_writer
            .send(Message::Text("reader-progress".to_string()))
            .await
            .expect("send inbound Text while write is pending");
        assert_eq!(
            timeout(Duration::from_secs(1), pump.recv_text())
                .await
                .expect("reader should progress while write is pending")
                .expect("receive inbound Text"),
            Some("reader-progress".to_string())
        );

        write_open.store(true, Ordering::SeqCst);
        write_waker.wake();
        let mut saw_text = 0;
        let mut saw_latest_automatic_pong = false;
        while let Ok(Some(Ok(message))) =
            timeout(Duration::from_millis(50), server_reader.next()).await
        {
            match message {
                Message::Text(text) if text == "held-text" => saw_text += 1,
                Message::Pong(payload) if payload.as_slice() == b"server-ping-two" => {
                    saw_latest_automatic_pong = true;
                }
                Message::Pong(payload) => assert!(
                    !payload.is_empty(),
                    "the pump must not add a manual empty Pong"
                ),
                _ => {}
            }
        }
        assert_eq!(
            saw_text, 1,
            "held Text should appear on the wire exactly once"
        );
        assert!(
            saw_latest_automatic_pong,
            "the most recent processed server Ping should receive an automatic Pong"
        );
        assert!(!pump.is_closed(), "{:?}", pump.terminal_state());
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_services_due_ping_and_queued_text_with_sustained_ready_traffic() {
        let (client_io, server_io) = duplex(4096);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5))
            .expect("valid watchdog policy");
        let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(1),
            policy,
        );

        pump.send_text("queued-text")
            .await
            .expect("queue outbound Text");
        for index in 0..128 {
            server_writer
                .send(Message::Text(format!("ready-{index}")))
                .await
                .expect("keep inbound traffic ready");
        }
        server_writer
            .send(Message::Ping(b"ready-server-ping".to_vec()))
            .await
            .expect("send server Ping while traffic is ready");
        advance(Duration::from_secs(1)).await;

        let mut saw_due_ping = None;
        let mut saw_queued_text = false;
        for _ in 0..8 {
            let message = timeout(Duration::from_secs(1), server_reader.next())
                .await
                .expect("ready traffic must not starve client progress")
                .expect("client should keep the connection open")
                .expect("client frame should be valid");
            match message {
                Message::Ping(nonce) => {
                    assert!(
                        saw_due_ping.is_none(),
                        "only one client challenge is in flight"
                    );
                    server_writer
                        .send(Message::Pong(nonce.clone()))
                        .await
                        .expect("acknowledge due client Ping");
                    saw_due_ping = Some(nonce);
                }
                Message::Text(text) if text == "queued-text" => saw_queued_text = true,
                Message::Pong(payload) => assert!(
                    !payload.is_empty(),
                    "the pump must not add a manual empty Pong"
                ),
                _ => {}
            }
            if saw_due_ping.is_some() && saw_queued_text {
                break;
            }
        }
        assert!(
            saw_due_ping.is_some(),
            "due client Ping should not be starved"
        );
        assert!(
            saw_queued_text,
            "queued Text should not be starved by sustained ready inbound traffic"
        );
        assert!(!pump.is_closed(), "{:?}", pump.terminal_state());
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_rejects_matching_pong_at_the_deadline() {
        let limits = UpstreamInboundLimits::new(1, 16).expect("valid limits");
        let (inbound_tx, _inbound_rx) = mpsc::channel(limits.max_messages());
        let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(8))
            .expect("valid watchdog policy");
        let pump_state = PumpState {
            inbound_tx: &inbound_tx,
            byte_budget: &byte_budget,
            limits,
            terminal_state: &terminal_state,
            watchdog_policy: policy,
        };
        let mut challenge = Some(PendingPongChallenge {
            nonce: b"current-challenge".to_vec(),
            sent_at: Some(Instant::now()),
            acknowledged_early: false,
        });
        let mut control_flush_needed = false;

        advance(Duration::from_secs(5)).await;
        assert!(handle_inbound_message(
            Some(Ok(Message::Pong(b"current-challenge".to_vec()))),
            &pump_state,
            &mut challenge,
            &mut control_flush_needed,
        ));
        assert!(challenge.is_some());
        assert!(!check_liveness_deadline(
            &terminal_state,
            challenge.as_ref(),
            None,
            policy,
        ));
        assert!(matches!(
            *terminal_state.lock().expect("terminal state lock"),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::PongDeadline,
                ..
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_latches_matching_pong_before_ping_send_completes() {
        let limits = UpstreamInboundLimits::new(1, 16).expect("valid limits");
        let (inbound_tx, _inbound_rx) = mpsc::channel(limits.max_messages());
        let byte_budget = Arc::new(Semaphore::new(limits.max_bytes()));
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(8))
            .expect("valid watchdog policy");
        let pump_state = PumpState {
            inbound_tx: &inbound_tx,
            byte_budget: &byte_budget,
            limits,
            terminal_state: &terminal_state,
            watchdog_policy: policy,
        };
        let mut challenge = Some(PendingPongChallenge {
            nonce: b"current-challenge".to_vec(),
            sent_at: None,
            acknowledged_early: false,
        });
        let mut control_flush_needed = false;

        assert!(handle_inbound_message(
            Some(Ok(Message::Pong(b"current-challenge".to_vec()))),
            &pump_state,
            &mut challenge,
            &mut control_flush_needed,
        ));
        assert!(
            challenge
                .as_ref()
                .expect("challenge remains until send completion")
                .acknowledged_early
        );
        assert!(matches!(
            *terminal_state.lock().expect("terminal state lock"),
            UpstreamTerminalState::Open
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_accepts_matching_pong_before_ping_flush_completes() {
        let (client_io, server_io) = duplex(1024);
        let flush_started = Arc::new(Notify::new());
        let flush_open = Arc::new(AtomicBool::new(false));
        let flush_waker = Arc::new(AtomicWaker::new());
        let client_io = FlushGateIo {
            inner: client_io,
            flush_started: Arc::clone(&flush_started),
            flush_open: Arc::clone(&flush_open),
            flush_waker: Arc::clone(&flush_waker),
        };
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(10))
            .expect("valid watchdog policy");
        let flush_waiter = {
            let flush_started = Arc::clone(&flush_started);
            tokio::spawn(async move { flush_started.notified().await })
        };
        let pump = LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
            client,
            Duration::from_secs(10),
            policy,
        );

        advance(Duration::from_secs(10)).await;
        let nonce = match server_reader
            .next()
            .await
            .expect("client Ping bytes")
            .expect("valid client Ping")
        {
            Message::Ping(nonce) => nonce,
            message => panic!("expected client Ping, got {message:?}"),
        };
        flush_waiter.await.expect("flush waiter should complete");
        server_writer
            .send(Message::Pong(nonce))
            .await
            .expect("send matching Pong before flush completion");
        server_writer
            .send(Message::Text("early-pong-observed".to_string()))
            .await
            .expect("send ordered read barrier after early Pong");
        assert_eq!(
            pump.recv_text()
                .await
                .expect("pump should process the early Pong before the read barrier"),
            Some("early-pong-observed".to_string())
        );

        flush_open.store(true, Ordering::SeqCst);
        flush_waker.wake();
        pump.send_text("flush-complete-barrier")
            .await
            .expect("queue outbound barrier after releasing flush gate");
        assert!(matches!(
            server_reader
                .next()
                .await
                .expect("outbound barrier frame")
                .expect("valid outbound barrier frame"),
            Message::Text(text) if text == "flush-complete-barrier"
        ));
        advance(Duration::from_secs(6)).await;
        assert!(matches!(pump.terminal_state(), UpstreamTerminalState::Open));
        server_writer
            .send(Message::Text("post-deadline-data".to_string()))
            .await
            .expect("send data after the original Pong deadline");
        assert_eq!(
            pump.recv_text()
                .await
                .expect("healthy pump should continue reading after the deadline window"),
            Some("post-deadline-data".to_string())
        );

        drop(pump);
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_rejects_matching_pong_at_exact_deadline_on_live_socket() {
        let (client_io, server_io) = duplex(1024);
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_writer, mut server_reader) = server.split();
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(5), Duration::from_secs(10))
            .expect("valid watchdog policy");
        let pump = Arc::new(
            LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                client,
                Duration::from_secs(1),
                policy,
            ),
        );

        advance(Duration::from_secs(1)).await;
        let nonce = match server_reader
            .next()
            .await
            .expect("client Ping")
            .expect("valid client Ping")
        {
            Message::Ping(nonce) => nonce,
            message => panic!("expected client Ping, got {message:?}"),
        };
        tokio::task::yield_now().await;
        let pong_at_deadline = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let _ = server_writer.send(Message::Pong(nonce)).await;
        });
        tokio::task::yield_now().await;
        advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;

        assert!(matches!(
            pump.terminal_state(),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::PongDeadline,
                timeout,
                outbound_kind: None,
                ..
            }) if timeout == Duration::from_secs(5)
        ));
        pong_at_deadline
            .await
            .expect("deadline Pong task should finish");
    }

    #[test]
    fn liveness_timeout_is_sticky_and_logs_once_without_nonce_contents() {
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let capture = OverflowLogCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());

        tracing::subscriber::with_default(subscriber, || {
            record_liveness_timeout(
                &terminal_state,
                UpstreamLivenessTimeoutCause::WriteDeadline,
                Duration::from_secs(7),
                Duration::from_secs(8),
                Some(UpstreamOutboundKind::Text),
            );
            record_liveness_timeout(
                &terminal_state,
                UpstreamLivenessTimeoutCause::PongDeadline,
                Duration::from_secs(1),
                Duration::from_secs(2),
                None,
            );
        });

        assert!(matches!(
            *terminal_state.lock().expect("terminal state lock"),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::WriteDeadline,
                timeout,
                elapsed,
                outbound_kind: Some(UpstreamOutboundKind::Text),
            }) if timeout == Duration::from_secs(7) && elapsed == Duration::from_secs(8)
        ));
        let events = capture.0.lock().expect("liveness log capture lock");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("timeout_ms"), Some(&"7000".to_string()));
        assert_eq!(events[0].get("elapsed_ms"), Some(&"8000".to_string()));
        assert!(
            events[0]
                .values()
                .all(|value| !value.contains("current-challenge"))
        );
    }

    #[test]
    fn liveness_timeout_does_not_replace_inbound_overflow() {
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::InboundBufferOverflow(
            InboundBufferOverflow {
                cause: InboundBufferOverflowCause::MessageCount,
                queued_messages: 1,
                queued_bytes: 1,
                incoming_bytes: 1,
                max_messages: 1,
                max_bytes: 1,
            },
        )));

        record_liveness_timeout(
            &terminal_state,
            UpstreamLivenessTimeoutCause::PongDeadline,
            Duration::from_secs(1),
            Duration::from_secs(1),
            None,
        );

        assert!(matches!(
            *terminal_state.lock().expect("terminal state lock"),
            UpstreamTerminalState::InboundBufferOverflow(_)
        ));
    }

    #[tokio::test]
    async fn websocket_pump_records_metadata_when_outbound_channel_closes() {
        let mut pump = connect_test_pump().await;
        let (replacement_tx, replacement_rx) = mpsc::channel(1);
        drop(replacement_rx);
        let original_tx = std::mem::replace(&mut pump.outbound_tx, replacement_tx);
        drop(original_tx);

        let metadata = timeout(Duration::from_secs(2), async {
            loop {
                if pump.is_closed()
                    && let Some(metadata) = pump.close_metadata().await
                {
                    break metadata;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pump should close after outbound sender drop");

        assert_eq!(metadata, outbound_channel_closed_metadata());
    }

    #[tokio::test]
    async fn websocket_pump_releases_cancelled_receivers_and_dropped_handles() {
        let limits = UpstreamInboundLimits::new(1, 4).expect("valid limits");
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let (inbound_tx, inbound_rx) = mpsc::channel(1);
        let budget = Arc::new(Semaphore::new(limits.max_bytes()));
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let pump = Arc::new(LiveUpstreamWebSocket {
            outbound_tx,
            inbound_rx: Mutex::new(inbound_rx),
            terminal_state: Arc::clone(&terminal_state),
            task: tokio::spawn(std::future::pending()),
            after_text_send: None,
            pending_text_send: None,
        });
        let (started_tx, started_rx) = oneshot::channel();
        let pending_recv = {
            let pump = Arc::clone(&pump);
            tokio::spawn(async move {
                let _ = started_tx.send(());
                pump.recv_text().await
            })
        };
        started_rx.await.expect("recv task should start");
        tokio::task::yield_now().await;
        pending_recv.abort();
        assert!(
            pending_recv
                .await
                .expect_err("recv task should be cancelled")
                .is_cancelled()
        );

        assert!(try_enqueue_inbound(
            &inbound_tx,
            &budget,
            limits,
            "four".to_string(),
            &terminal_state,
        ));
        assert_eq!(budget.available_permits(), 0);
        drop(inbound_tx);
        drop(pump);
        assert_eq!(budget.available_permits(), limits.max_bytes());
    }

    #[tokio::test]
    async fn websocket_pump_returns_overflow_when_a_waiting_send_is_released() {
        let (outbound_tx, outbound_rx) = mpsc::channel(1);
        outbound_tx
            .try_send(OutboundCommand::Text("queued".to_string()))
            .expect("fill outbound queue");
        let (_inbound_tx, inbound_rx) = mpsc::channel(1);
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let pump = Arc::new(LiveUpstreamWebSocket {
            outbound_tx,
            inbound_rx: Mutex::new(inbound_rx),
            terminal_state: Arc::clone(&terminal_state),
            task: tokio::spawn(std::future::pending()),
            after_text_send: None,
            pending_text_send: None,
        });
        let (started_tx, started_rx) = oneshot::channel();
        let waiting_send = {
            let pump = Arc::clone(&pump);
            tokio::spawn(async move {
                let _ = started_tx.send(());
                pump.send_text("waiting").await
            })
        };
        started_rx.await.expect("send task should start");
        tokio::task::yield_now().await;
        *terminal_state.lock().expect("terminal state lock") =
            UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
                cause: InboundBufferOverflowCause::MessageCount,
                queued_messages: 1,
                queued_bytes: 0,
                incoming_bytes: 1,
                max_messages: 1,
                max_bytes: 1,
            });
        drop(outbound_rx);

        assert_eq!(
            waiting_send.await.expect("send task should complete"),
            Err(UpstreamWebSocketError::InboundBufferOverflow)
        );
    }

    #[tokio::test]
    async fn websocket_pump_returns_liveness_timeout_when_a_waiting_send_is_released() {
        let (outbound_tx, outbound_rx) = mpsc::channel(1);
        outbound_tx
            .try_send(OutboundCommand::Text("queued".to_string()))
            .expect("fill outbound queue");
        let (_inbound_tx, inbound_rx) = mpsc::channel(1);
        let terminal_state = Arc::new(StdMutex::new(UpstreamTerminalState::Open));
        let pump = Arc::new(LiveUpstreamWebSocket {
            outbound_tx,
            inbound_rx: Mutex::new(inbound_rx),
            terminal_state: Arc::clone(&terminal_state),
            task: tokio::spawn(std::future::pending()),
            after_text_send: None,
            pending_text_send: None,
        });
        let (started_tx, started_rx) = oneshot::channel();
        let waiting_send = {
            let pump = Arc::clone(&pump);
            tokio::spawn(async move {
                let _ = started_tx.send(());
                pump.send_text("waiting").await
            })
        };
        started_rx.await.expect("send task should start");
        tokio::task::yield_now().await;
        record_liveness_timeout(
            &terminal_state,
            UpstreamLivenessTimeoutCause::WriteDeadline,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Some(UpstreamOutboundKind::Text),
        );
        drop(outbound_rx);

        assert_eq!(
            waiting_send.await.expect("send task should complete"),
            Err(UpstreamWebSocketError::LivenessTimeout)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn websocket_pump_liveness_expiry_releases_gated_io_and_waiting_callers() {
        let (client_io, _server_io) = duplex(1024);
        let write_started = Arc::new(Notify::new());
        let write_open = Arc::new(AtomicBool::new(false));
        let write_waker = Arc::new(AtomicWaker::new());
        let io_token = Arc::new(());
        let io_released = Arc::downgrade(&io_token);
        let client_io = WriteGateIo {
            inner: client_io,
            write_started: Arc::clone(&write_started),
            write_open,
            write_waker,
            _release_token: Some(io_token),
        };
        let client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let policy = UpstreamWatchdogPolicy::new(Duration::from_secs(10), Duration::from_secs(5))
            .expect("valid watchdog policy");
        let pump = Arc::new(
            LiveUpstreamWebSocket::from_stream_with_ping_interval_and_watchdog_policy(
                client,
                Duration::from_secs(60),
                policy,
            ),
        );

        pump.send_text("held").await.expect("queue held Text");
        write_started.notified().await;
        for _ in 0..OUTBOUND_CHANNEL_CAPACITY {
            pump.outbound_tx
                .try_send(OutboundCommand::Text("queued".to_string()))
                .expect("fill outbound channel while writer is gated");
        }

        let waiting_send = {
            let pump = Arc::clone(&pump);
            tokio::spawn(async move { pump.send_text("waiting").await })
        };
        let waiting_recv = {
            let pump = Arc::clone(&pump);
            tokio::spawn(async move { pump.recv_text().await })
        };
        tokio::task::yield_now().await;

        advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;

        assert!(
            io_released.upgrade().is_none(),
            "expiry must drop gated socket IO"
        );
        assert_eq!(
            waiting_send
                .await
                .expect("waiting send task should complete"),
            Err(UpstreamWebSocketError::LivenessTimeout)
        );
        assert_eq!(
            waiting_recv
                .await
                .expect("waiting receive task should complete"),
            Err(UpstreamWebSocketError::LivenessTimeout)
        );
        assert!(matches!(
            pump.terminal_state(),
            UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
                cause: UpstreamLivenessTimeoutCause::WriteDeadline,
                outbound_kind: Some(UpstreamOutboundKind::Text),
                ..
            })
        ));
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
        ));
        assert_eq!(budget.available_permits(), 0);

        assert!(!try_enqueue_inbound(
            &sender,
            &budget,
            limits,
            "next".to_string(),
            &terminal_state,
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
            ));
            assert!(record_transport_overflow(
                &state,
                &sender,
                &budget,
                limits,
                &overflow_error,
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

    #[derive(Clone, Default)]
    struct OverflowLogCapture(Arc<StdMutex<Vec<BTreeMap<String, String>>>>);

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
    struct EventFields(BTreeMap<String, String>);

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
}
