use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::Frame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::{Data, OpCode};

#[path = "scripted_ws/startup.rs"]
mod startup;

type ServerSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    Message,
>;

#[derive(Clone, Copy)]
enum StartupBehavior {
    KeepAlive,
    KeepAliveWithNodelay,
    KeepAliveWithoutReader,
    KeepAliveAfterFirstClientMessageWithoutReader,
    KeepAliveUntilReaderStopped,
    DisconnectAfterHandshake,
}

pub struct ScriptedWebSocketServer {
    url: String,
    writer: Arc<Mutex<Option<ServerSink>>>,
    incoming_rx: Mutex<Option<mpsc::UnboundedReceiver<Message>>>,
    connected: Arc<Notify>,
    is_connected: Arc<AtomicBool>,
    client_disconnected: Arc<Notify>,
    is_client_disconnected: Arc<AtomicBool>,
    reader_stop_requested: Arc<AtomicBool>,
    reader_stop: Arc<Notify>,
    accept_task: JoinHandle<()>,
    reader_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[allow(dead_code)]
impl ScriptedWebSocketServer {
    pub async fn start() -> Self {
        Self::start_with_behavior(StartupBehavior::KeepAlive).await
    }

    pub async fn start_with_nodelay() -> Self {
        Self::start_with_behavior(StartupBehavior::KeepAliveWithNodelay).await
    }

    pub async fn start_without_reader() -> Self {
        Self::start_with_behavior(StartupBehavior::KeepAliveWithoutReader).await
    }

    pub async fn start_after_first_client_message_without_reader() -> Self {
        Self::start_with_behavior(StartupBehavior::KeepAliveAfterFirstClientMessageWithoutReader)
            .await
    }

    pub async fn start_with_stoppable_reader() -> Self {
        Self::start_with_behavior(StartupBehavior::KeepAliveUntilReaderStopped).await
    }

    pub async fn start_disconnect_after_handshake() -> Self {
        Self::start_with_behavior(StartupBehavior::DisconnectAfterHandshake).await
    }

    async fn start_with_behavior(startup_behavior: StartupBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("local addr");
        let url = format!("ws://{address}");
        let (startup, incoming_rx) = startup::StartupState::new();
        let accept_state = startup.clone();
        let accept_task = tokio::spawn(accept_state.accept(listener, startup_behavior));
        Self {
            url,
            writer: startup.writer,
            incoming_rx: Mutex::new(Some(incoming_rx)),
            connected: startup.connected,
            is_connected: startup.is_connected,
            client_disconnected: startup.reader.client_disconnected,
            is_client_disconnected: startup.reader.is_client_disconnected,
            reader_stop_requested: startup.reader.reader_stop_requested,
            reader_stop: startup.reader.reader_stop,
            accept_task,
            reader_task: startup.reader_task,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub async fn send_text(&self, text: &str) {
        self.send(Message::Text(text.to_string())).await;
    }

    pub async fn send_text_burst(&self, texts: &[&str]) {
        self.wait_until_connected().await;
        let mut writer = self.writer.lock().await;
        let sink = writer.as_mut().expect("client connected");
        for text in texts {
            sink.send(Message::Text((*text).to_string()))
                .await
                .expect("send scripted text");
        }
    }

    pub async fn send_binary(&self, payload: Vec<u8>) {
        self.send(Message::Binary(payload)).await;
    }

    pub async fn send_text_frame(&self, payload: &[u8]) {
        self.send(Message::Frame(Frame::message(
            payload.to_vec(),
            OpCode::Data(Data::Text),
            true,
        )))
        .await;
    }

    pub async fn send_fragmented_text(&self, first: &[u8], final_fragment: &[u8]) {
        self.send(Message::Frame(Frame::message(
            first.to_vec(),
            OpCode::Data(Data::Text),
            false,
        )))
        .await;
        self.send(Message::Frame(Frame::message(
            final_fragment.to_vec(),
            OpCode::Data(Data::Continue),
            true,
        )))
        .await;
    }

    pub async fn send_ping(&self, payload: &[u8]) {
        self.send(Message::Ping(payload.to_vec())).await;
    }

    pub async fn send_pong(&self, payload: Vec<u8>) {
        self.send(Message::Pong(payload)).await;
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

    pub async fn take_pending_client_messages(&self) -> Vec<Message> {
        self.wait_until_connected().await;
        let mut incoming_rx = self.incoming_rx.lock().await;
        let receiver = incoming_rx
            .as_mut()
            .expect("incoming receiver should remain available");
        let mut messages = Vec::new();
        while let Ok(message) = receiver.try_recv() {
            messages.push(message);
        }
        messages
    }

    pub fn stop_reader(&self) {
        self.reader_stop_requested.store(true, Ordering::SeqCst);
        self.reader_stop.notify_waiters();
    }

    pub async fn abort_connection(&self) {
        self.wait_until_connected().await;
        self.writer.lock().await.take();
        if let Some(task) = self.reader_task.lock().await.take() {
            task.abort();
        }
    }

    pub async fn wait_for_client_disconnect(&self) {
        while !self.is_client_disconnected.load(Ordering::SeqCst) {
            self.client_disconnected.notified().await;
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

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use tokio::time::{Duration, timeout};
    use tokio_tungstenite::connect_async;

    use super::ScriptedWebSocketServer;

    #[tokio::test]
    async fn abort_connection_drops_unpolled_reader_and_closes_peer_socket() {
        let server = ScriptedWebSocketServer::start_without_reader().await;
        let (client, _) = connect_async(server.url()).await.expect("connect client");
        let (_writer, mut reader) = client.split();

        server.abort_connection().await;

        assert!(
            matches!(
                timeout(Duration::from_secs(1), reader.next())
                    .await
                    .expect("server abort should close the peer socket"),
                None | Some(Err(_))
            ),
            "the peer must observe socket closure rather than wait for a timeout"
        );
    }
}
