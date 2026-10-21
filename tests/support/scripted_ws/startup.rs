use super::*;
use tokio_tungstenite::accept_async;

type ServerStream =
    futures_util::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub(super) struct StartupState {
    pub(super) writer: Arc<Mutex<Option<ServerSink>>>,
    pub(super) reader_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(super) connected: Arc<Notify>,
    pub(super) is_connected: Arc<AtomicBool>,
    pub(super) reader: ReaderState,
}

#[derive(Clone)]
pub(super) struct ReaderState {
    incoming_tx: mpsc::UnboundedSender<Message>,
    pub(super) client_disconnected: Arc<Notify>,
    pub(super) is_client_disconnected: Arc<AtomicBool>,
    pub(super) reader_stop_requested: Arc<AtomicBool>,
    pub(super) reader_stop: Arc<Notify>,
}

impl StartupState {
    pub(super) fn new() -> (Self, mpsc::UnboundedReceiver<Message>) {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let state = Self {
            writer: Arc::new(Mutex::new(None)),
            reader_task: Arc::new(Mutex::new(None)),
            connected: Arc::new(Notify::new()),
            is_connected: Arc::new(AtomicBool::new(false)),
            reader: ReaderState {
                incoming_tx,
                client_disconnected: Arc::new(Notify::new()),
                is_client_disconnected: Arc::new(AtomicBool::new(false)),
                reader_stop_requested: Arc::new(AtomicBool::new(false)),
                reader_stop: Arc::new(Notify::new()),
            },
        };
        (state, incoming_rx)
    }

    pub(super) async fn accept(self, listener: TcpListener, behavior: StartupBehavior) {
        let (stream, _) = listener.accept().await.expect("accept client");
        if matches!(behavior, StartupBehavior::KeepAliveWithNodelay) {
            stream
                .set_nodelay(true)
                .expect("set accepted socket NODELAY");
        }
        let websocket = accept_async(stream).await.expect("accept websocket");
        if matches!(behavior, StartupBehavior::DisconnectAfterHandshake) {
            self.notify_connected();
            drop(websocket);
            return;
        }
        let (sink, stream) = websocket.split();
        *self.writer.lock().await = Some(sink);
        self.notify_connected();
        let reader = tokio::spawn(self.reader.read(stream, behavior));
        *self.reader_task.lock().await = Some(reader);
    }

    fn notify_connected(&self) {
        self.is_connected.store(true, Ordering::SeqCst);
        self.connected.notify_waiters();
    }
}

impl ReaderState {
    async fn read(self, mut stream: ServerStream, behavior: StartupBehavior) {
        match behavior {
            StartupBehavior::KeepAliveWithoutReader => {
                let _stream = stream;
                std::future::pending::<()>().await;
            }
            StartupBehavior::KeepAliveAfterFirstClientMessageWithoutReader => {
                if let Some(Ok(message)) = stream.next().await {
                    let _ = self.incoming_tx.send(message);
                }
                std::future::pending::<()>().await;
            }
            StartupBehavior::KeepAliveUntilReaderStopped => self.read_until_stopped(stream).await,
            _ => self.read_until_disconnected(stream).await,
        }
    }

    async fn read_until_stopped(self, mut stream: ServerStream) {
        loop {
            if self.reader_stop_requested.load(Ordering::SeqCst) {
                return;
            }
            tokio::select! {
                _ = self.reader_stop.notified() => {}
                message = stream.next() => match message {
                    Some(Ok(message)) => {
                        if self.incoming_tx.send(message).is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
        }
        self.notify_disconnected();
    }

    async fn read_until_disconnected(self, mut stream: ServerStream) {
        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => {
                    if self.incoming_tx.send(message).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        self.notify_disconnected();
    }

    fn notify_disconnected(&self) {
        self.is_client_disconnected.store(true, Ordering::SeqCst);
        self.client_disconnected.notify_waiters();
    }
}
