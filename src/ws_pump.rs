mod inbound;
use inbound::*;

mod liveness;
use liveness::*;

mod io;
use io::*;

mod driver;
use driver::*;

mod handle;

mod diagnostics;
use diagnostics::*;

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
    #[cfg(test)]
    diagnostics: CloseDiagnostics,
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
    diagnostics: &'a mut CloseDiagnostics,
}

struct PumpLivenessState<'a> {
    pending_challenge: &'a mut Option<PendingPongChallenge>,
    control_flush_needed: &'a mut bool,
    next_ping_due: &'a mut Instant,
    ping_interval: Duration,
}

fn outbound_channel_closed_metadata() -> UpstreamCloseMetadata {
    UpstreamCloseMetadata {
        code: None,
        reason: Some("outbound channel closed".to_string()),
        error: None,
    }
}

#[cfg(test)]
mod tests;
