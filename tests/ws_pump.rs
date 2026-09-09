use std::time::Duration;

#[path = "support/scripted_ws.rs"]
mod scripted_ws;

use scripted_ws::ScriptedWebSocketServer;
use threadline::ws_pump::{LiveUpstreamWebSocket, UpstreamCloseMetadata};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

async fn connect_pump(server: &ScriptedWebSocketServer) -> LiveUpstreamWebSocket {
    let (stream, _) = connect_async(server.url())
        .await
        .expect("connect client websocket");
    LiveUpstreamWebSocket::from_stream(stream)
}

async fn wait_for_closed(pump: &LiveUpstreamWebSocket) -> UpstreamCloseMetadata {
    timeout(Duration::from_secs(2), async {
        loop {
            if pump.is_closed()
                && let Some(metadata) = pump.close_metadata().await
            {
                break metadata;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pump should close")
}

#[tokio::test]
async fn websocket_pump_replies_to_server_ping_while_idle() {
    let server = ScriptedWebSocketServer::start().await;
    let pump = connect_pump(&server).await;

    server.send_ping(b"idle-check").await;

    let message = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("pong timeout")
        .expect("client message");

    match message {
        Message::Pong(payload) => assert_eq!(payload.as_slice(), b"idle-check"),
        other => panic!("expected pong, got {other:?}"),
    }
    assert!(!pump.is_closed());
}

#[tokio::test]
async fn websocket_pump_replies_to_server_ping_when_inbound_messages_are_not_consumed() {
    let server = ScriptedWebSocketServer::start().await;
    let pump = connect_pump(&server).await;

    server.send_text("queued-text").await;
    server.send_binary(b"queued-binary".to_vec()).await;
    server.send_ping(b"still-alive").await;

    let message = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("pong timeout")
        .expect("client message");
    match message {
        Message::Pong(payload) => assert_eq!(payload.as_slice(), b"still-alive"),
        other => panic!("expected pong, got {other:?}"),
    }

    assert_eq!(
        pump.recv_text().await.expect("recv text"),
        Some("queued-text".to_string())
    );
    assert_eq!(
        pump.recv_text().await.expect("recv binary as text"),
        Some("queued-binary".to_string())
    );
}

#[tokio::test]
async fn websocket_pump_send_text_only_queues_outbound_messages() {
    let server = ScriptedWebSocketServer::start().await;
    let pump = connect_pump(&server).await;

    pump.send_text("from-threadline").await.expect("send text");

    let message = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("text timeout")
        .expect("client message");
    match message {
        Message::Text(text) => assert_eq!(text.as_str(), "from-threadline"),
        other => panic!("expected text, got {other:?}"),
    }

    assert!(
        timeout(Duration::from_millis(100), pump.recv_text())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn websocket_pump_records_close_metadata_without_panicking() {
    let server = ScriptedWebSocketServer::start().await;
    let pump = connect_pump(&server).await;

    server.send_close(1000, "done").await;

    let metadata = wait_for_closed(&pump).await;

    assert_eq!(metadata.code, Some(1000));
    assert_eq!(metadata.reason.as_deref(), Some("done"));
    assert_eq!(metadata.error, None);
}

#[tokio::test]
async fn websocket_pump_records_error_metadata_when_connection_drops() {
    let server = ScriptedWebSocketServer::start().await;
    let pump = connect_pump(&server).await;

    server.abort_connection().await;

    let metadata = wait_for_closed(&pump).await;

    assert_eq!(metadata.code, None);
    assert!(metadata.reason.is_none());
    assert!(metadata.error.is_some());
}

#[tokio::test]
async fn websocket_pump_replies_to_server_ping_after_a_retained_idle_gap() {
    let server = ScriptedWebSocketServer::start().await;
    let pump = connect_pump(&server).await;

    server.send_text("response-completed").await;
    assert_eq!(
        pump.recv_text().await.expect("recv completed event"),
        Some("response-completed".to_string())
    );

    sleep(Duration::from_millis(100)).await;
    server.send_ping(b"retained-idle-check").await;

    let message = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("pong timeout")
        .expect("client message");

    match message {
        Message::Pong(payload) => assert_eq!(payload.as_slice(), b"retained-idle-check"),
        other => panic!("expected pong, got {other:?}"),
    }

    assert!(!pump.is_closed());
}
