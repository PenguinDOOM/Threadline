use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamCloseMetadata {
    pub code: Option<u16>,
    pub reason: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum UpstreamWebSocketError {
    #[error("Threadline could not queue an outbound upstream websocket message.")]
    OutboundQueueClosed,
}

pub struct LiveUpstreamWebSocket {
    outbound_tx: mpsc::Sender<OutboundCommand>,
    inbound_rx: Mutex<mpsc::UnboundedReceiver<String>>,
    close_metadata: Arc<Mutex<Option<UpstreamCloseMetadata>>>,
    is_closed: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

enum OutboundCommand {
    Text(String),
}

const OUTBOUND_CHANNEL_CAPACITY: usize = 32;

impl LiveUpstreamWebSocket {
    pub fn from_stream<S>(_stream: WebSocketStream<S>) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut writer, mut reader) = _stream.split();
        let (outbound_tx, mut outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let close_metadata = Arc::new(Mutex::new(None));
        let task_close_metadata = Arc::clone(&close_metadata);
        let is_closed = Arc::new(AtomicBool::new(false));
        let task_is_closed = Arc::clone(&is_closed);

        let task = tokio::spawn(async move {
            debug!(
                outbound_capacity = OUTBOUND_CHANNEL_CAPACITY,
                "ws_pump_started"
            );
            loop {
                tokio::select! {
                    outbound = outbound_rx.recv() => match outbound {
                        Some(OutboundCommand::Text(text)) => {
                            if let Err(error) = writer.send(Message::Text(text)).await {
                                record_error(&task_close_metadata, error.to_string()).await;
                                break;
                            }
                        }
                        None => {
                            record_close(
                                &task_close_metadata,
                                outbound_channel_closed_metadata(),
                            )
                            .await;
                            break;
                        }
                    },
                    inbound = reader.next() => match inbound {
                        Some(Ok(Message::Text(text))) => {
                            let _ = inbound_tx.send(text.to_string());
                        }
                        Some(Ok(Message::Binary(bytes))) => {
                            let _ = inbound_tx.send(String::from_utf8_lossy(bytes.as_ref()).into_owned());
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            let payload_len = payload.len();
                            debug!(payload_len, "ws_pump_ping_received");
                            if let Err(error) = writer.send(Message::Pong(payload)).await {
                                record_error(&task_close_metadata, error.to_string()).await;
                                break;
                            }
                            debug!(payload_len, "ws_pump_pong_sent");
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(frame))) => {
                            let metadata = UpstreamCloseMetadata {
                                code: frame.as_ref().map(|frame| u16::from(frame.code)),
                                reason: frame.as_ref().map(|frame| frame.reason.to_string()),
                                error: None,
                            };
                            record_close(&task_close_metadata, metadata).await;
                            break;
                        }
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Err(error)) => {
                            record_error(&task_close_metadata, error.to_string()).await;
                            break;
                        }
                        None => break,
                    }
                }
            }

            let mut guard = task_close_metadata.lock().await;
            if guard.is_none() {
                *guard = Some(UpstreamCloseMetadata {
                    code: None,
                    reason: None,
                    error: None,
                });
            }
            task_is_closed.store(true, Ordering::SeqCst);
        });

        Self {
            outbound_tx,
            inbound_rx: Mutex::new(inbound_rx),
            close_metadata,
            is_closed,
            task,
        }
    }

    pub async fn send_text(&self, text: impl Into<String>) -> Result<(), UpstreamWebSocketError> {
        self.outbound_tx
            .send(OutboundCommand::Text(text.into()))
            .await
            .map_err(|_| UpstreamWebSocketError::OutboundQueueClosed)
    }

    pub async fn recv_text(&self) -> Result<Option<String>, UpstreamWebSocketError> {
        Ok(self.inbound_rx.lock().await.recv().await)
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }

    pub async fn close_metadata(&self) -> Option<UpstreamCloseMetadata> {
        self.close_metadata.lock().await.clone()
    }
}

impl Drop for LiveUpstreamWebSocket {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn record_close(
    target: &Arc<Mutex<Option<UpstreamCloseMetadata>>>,
    metadata: UpstreamCloseMetadata,
) {
    *target.lock().await = Some(metadata);
}

async fn record_error(target: &Arc<Mutex<Option<UpstreamCloseMetadata>>>, error: String) {
    *target.lock().await = Some(UpstreamCloseMetadata {
        code: None,
        reason: None,
        error: Some(error),
    });
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
    use super::*;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::connect_async;

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
}
