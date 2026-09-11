use std::sync::{Arc, Mutex as StdMutex};

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, MissedTickBehavior};
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
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum UpstreamWebSocketError {
    #[error("Threadline could not queue an outbound upstream websocket message.")]
    OutboundQueueClosed,
    #[error("The upstream websocket inbound buffer overflowed.")]
    InboundBufferOverflow,
}

pub struct LiveUpstreamWebSocket {
    outbound_tx: mpsc::Sender<OutboundCommand>,
    inbound_rx: Mutex<mpsc::Receiver<InboundEnvelope>>,
    terminal_state: Arc<StdMutex<UpstreamTerminalState>>,
    task: JoinHandle<()>,
}

enum OutboundCommand {
    Text(String),
}

struct InboundEnvelope {
    payload: Box<str>,
    _byte_permit: OwnedSemaphorePermit,
}

const OUTBOUND_CHANNEL_CAPACITY: usize = 32;
const UPSTREAM_PING_INTERVAL: Duration = Duration::from_secs(30);

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
        Self::from_stream_with_ping_interval_and_limits(stream, UPSTREAM_PING_INTERVAL, limits)
    }

    #[cfg(test)]
    fn from_stream_with_ping_interval<S>(
        stream: WebSocketStream<S>,
        ping_interval: Duration,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_stream_with_ping_interval_and_limits(
            stream,
            ping_interval,
            UpstreamInboundLimits::DEFAULT,
        )
    }

    fn from_stream_with_ping_interval_and_limits<S>(
        stream: WebSocketStream<S>,
        ping_interval: Duration,
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
            let mut ping_timer =
                tokio::time::interval_at(Instant::now() + ping_interval, ping_interval);
            ping_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

            debug!(
                outbound_capacity = OUTBOUND_CHANNEL_CAPACITY,
                ping_interval_secs = ping_interval.as_secs_f64(),
                "ws_pump_started"
            );
            loop {
                tokio::select! {
                    _ = ping_timer.tick() => {
                        if let Err(error) = writer.send(Message::Ping(Vec::new())).await {
                            record_error(&task_terminal_state, error.to_string());
                            break;
                        }
                        debug!("ws_pump_ping_sent");
                    }
                    outbound = outbound_rx.recv() => match outbound {
                        Some(OutboundCommand::Text(text)) => {
                            if let Err(error) = writer.send(Message::Text(text)).await {
                                record_error(&task_terminal_state, error.to_string());
                                break;
                            }
                        }
                        None => {
                            record_close(&task_terminal_state, outbound_channel_closed_metadata());
                            break;
                        }
                    },
                    inbound = reader.next() => match inbound {
                        Some(Ok(Message::Text(text))) => {
                            if !try_enqueue_inbound(&inbound_tx, &byte_budget, limits, text.to_string(), &task_terminal_state) {
                                break;
                            }
                        }
                        Some(Ok(Message::Binary(bytes))) => {
                            if !try_enqueue_inbound(&inbound_tx, &byte_budget, limits, String::from_utf8_lossy(bytes.as_ref()).into_owned(), &task_terminal_state) {
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            let payload_len = payload.len();
                            debug!(payload_len, "ws_pump_ping_received");
                            if let Err(error) = writer.send(Message::Pong(payload)).await {
                                record_error(&task_terminal_state, error.to_string());
                                break;
                            }
                            debug!(payload_len, "ws_pump_pong_sent");
                        }
                        Some(Ok(Message::Pong(payload))) => {
                            debug!(payload_len = payload.len(), "ws_pump_pong_received");
                        }
                        Some(Ok(Message::Close(frame))) => {
                            let metadata = UpstreamCloseMetadata {
                                code: frame.as_ref().map(|frame| u16::from(frame.code)),
                                reason: frame.as_ref().map(|frame| frame.reason.to_string()),
                                error: None,
                            };
                            record_close(&task_terminal_state, metadata);
                            break;
                        }
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Err(error)) => {
                            if !record_transport_overflow(
                                &task_terminal_state,
                                &inbound_tx,
                                &byte_budget,
                                limits,
                                &error,
                            ) {
                                record_error(&task_terminal_state, error.to_string());
                            }
                            break;
                        }
                        None => break,
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
        }
    }

    pub async fn send_text(&self, text: impl Into<String>) -> Result<(), UpstreamWebSocketError> {
        self.terminal_error()?;
        self.outbound_tx
            .send(OutboundCommand::Text(text.into()))
            .await
            .map_err(|_| {
                self.terminal_error()
                    .err()
                    .unwrap_or(UpstreamWebSocketError::OutboundQueueClosed)
            })?;
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
            UpstreamTerminalState::Open | UpstreamTerminalState::Closed(_) => Ok(()),
        }
    }
}

impl Drop for LiveUpstreamWebSocket {
    fn drop(&mut self) {
        self.task.abort();
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

    use super::*;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::connect_async;
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
                        assert!(payload.is_empty());
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
