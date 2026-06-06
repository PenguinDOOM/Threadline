use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

type ServerSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    Message,
>;

pub struct ScriptedWebSocketServer {
    url: String,
    writer: Arc<Mutex<Option<ServerSink>>>,
    incoming_rx: Mutex<Option<mpsc::UnboundedReceiver<Message>>>,
    connected: Arc<Notify>,
    is_connected: Arc<AtomicBool>,
    accept_task: JoinHandle<()>,
    reader_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[allow(dead_code)]
impl ScriptedWebSocketServer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("local addr");
        let url = format!("ws://{address}");
        let writer = Arc::new(Mutex::new(None));
        let reader_task = Arc::new(Mutex::new(None));
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let connected = Arc::new(Notify::new());
        let is_connected = Arc::new(AtomicBool::new(false));

        let accept_writer = Arc::clone(&writer);
        let accept_reader_task = Arc::clone(&reader_task);
        let accept_connected = Arc::clone(&connected);
        let accept_is_connected = Arc::clone(&is_connected);
        let accept_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept client");
            let websocket = accept_async(stream).await.expect("accept websocket");
            let (sink, mut stream) = websocket.split();
            *accept_writer.lock().await = Some(sink);
            accept_is_connected.store(true, Ordering::SeqCst);
            accept_connected.notify_waiters();

            let reader = tokio::spawn(async move {
                while let Some(message) = stream.next().await {
                    match message {
                        Ok(message) => {
                            if incoming_tx.send(message).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
            *accept_reader_task.lock().await = Some(reader);
        });

        Self {
            url,
            writer,
            incoming_rx: Mutex::new(Some(incoming_rx)),
            connected,
            is_connected,
            accept_task,
            reader_task,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub async fn send_text(&self, text: &str) {
        self.send(Message::Text(text.to_string())).await;
    }

    pub async fn send_binary(&self, payload: Vec<u8>) {
        self.send(Message::Binary(payload)).await;
    }

    pub async fn send_ping(&self, payload: &[u8]) {
        self.send(Message::Ping(payload.to_vec())).await;
    }

    pub async fn send_close(&self, code: u16, reason: &str) {
        self.send(Message::Close(Some(
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(
                    code,
                ),
                reason: reason.to_string().into(),
            },
        )))
        .await;
    }

    pub async fn recv_client_message(&self) -> Option<Message> {
        self.wait_until_connected().await;
        let mut incoming_rx = self.incoming_rx.lock().await;
        let receiver = incoming_rx
            .as_mut()
            .expect("incoming receiver should remain available");
        receiver.recv().await
    }

    pub async fn abort_connection(&self) {
        self.wait_until_connected().await;
        self.writer.lock().await.take();
        if let Some(task) = self.reader_task.lock().await.take() {
            task.abort();
        }
    }

    async fn send(&self, message: Message) {
        self.wait_until_connected().await;
        let mut writer = self.writer.lock().await;
        let sink = writer.as_mut().expect("client connected");
        sink.send(message).await.expect("send scripted message");
    }

    async fn wait_until_connected(&self) {
        while !self.is_connected.load(Ordering::SeqCst) {
            self.connected.notified().await;
        }
    }
}

impl Drop for ScriptedWebSocketServer {
    fn drop(&mut self) {
        self.accept_task.abort();
        if let Ok(mut guard) = self.reader_task.try_lock()
            && let Some(task) = guard.take()
        {
            task.abort();
        }
    }
}
