use std::time::Duration;

#[path = "support/scripted_ws.rs"]
mod scripted_ws;

use scripted_ws::ScriptedWebSocketServer;
use threadline::ws_pump::{
    InboundBufferOverflow, InboundBufferOverflowCause, LiveUpstreamWebSocket,
    UpstreamCloseMetadata, UpstreamInboundLimits, UpstreamTerminalState, UpstreamWebSocketError,
};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{connect_async, connect_async_with_config};

async fn connect_pump(server: &ScriptedWebSocketServer) -> LiveUpstreamWebSocket {
    let (stream, _) = connect_async(server.url())
        .await
        .expect("connect client websocket");
    LiveUpstreamWebSocket::from_stream(stream)
}

async fn connect_pump_with_transport_limits(
    server: &ScriptedWebSocketServer,
    inbound_limits: UpstreamInboundLimits,
) -> LiveUpstreamWebSocket {
    let config = WebSocketConfig {
        max_message_size: Some(inbound_limits.max_bytes()),
        max_frame_size: Some(inbound_limits.max_bytes().max(125)),
        ..WebSocketConfig::default()
    };
    let (stream, _) = connect_async_with_config(server.url(), Some(config), false)
        .await
        .expect("connect client websocket");
    LiveUpstreamWebSocket::from_stream_with_limits(stream, inbound_limits)
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

async fn wait_for_transport_overflow(pump: &LiveUpstreamWebSocket) {
    timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                pump.terminal_state(),
                UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
                    cause: InboundBufferOverflowCause::TransportSize,
                    ..
                })
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("oversized frame should overflow the transport");
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
async fn websocket_pump_keeps_ping_alive_at_exact_bound_then_overflows_on_next_data() {
    let server = ScriptedWebSocketServer::start().await;
    let (stream, _) = connect_async(server.url())
        .await
        .expect("connect client websocket");
    let pump = LiveUpstreamWebSocket::from_stream_with_limits(
        stream,
        UpstreamInboundLimits::new(1, 2).expect("valid limits"),
    );

    server.send_text("ab").await;
    server.send_ping(b"exact-bound").await;
    let message = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("pong timeout")
        .expect("client message");
    assert!(matches!(message, Message::Pong(payload) if payload.as_slice() == b"exact-bound"));
    assert!(!pump.is_closed());

    server.send_text("b").await;
    timeout(Duration::from_secs(1), async {
        loop {
            if let UpstreamTerminalState::InboundBufferOverflow(overflow) = pump.terminal_state() {
                assert_eq!(overflow.cause, InboundBufferOverflowCause::PayloadBytes);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pump should overflow without consuming inbound messages");

    assert_eq!(
        pump.recv_text().await,
        Err(UpstreamWebSocketError::InboundBufferOverflow)
    );
}

#[tokio::test]
async fn websocket_pump_transport_limits_accept_exact_frames_and_reject_oversized_frames() {
    let exact_server = ScriptedWebSocketServer::start().await;
    let exact_pump = connect_pump_with_transport_limits(
        &exact_server,
        UpstreamInboundLimits::new(2, 125).expect("valid limits"),
    )
    .await;
    exact_server.send_text_frame(&[b'a'; 125]).await;
    assert_eq!(
        exact_pump
            .recv_text()
            .await
            .expect("exact frame is accepted"),
        Some("a".repeat(125))
    );

    let oversized_server = ScriptedWebSocketServer::start().await;
    let oversized_pump = connect_pump_with_transport_limits(
        &oversized_server,
        UpstreamInboundLimits::new(2, 125).expect("valid limits"),
    )
    .await;
    oversized_server.send_text_frame(&[b'b'; 126]).await;

    wait_for_transport_overflow(&oversized_pump).await;
    assert_eq!(
        oversized_pump.recv_text().await,
        Err(UpstreamWebSocketError::InboundBufferOverflow)
    );
}

#[tokio::test]
async fn websocket_pump_transport_limits_reject_fragmented_messages_and_allow_control_frames() {
    let fragmented_server = ScriptedWebSocketServer::start().await;
    let fragmented_pump = connect_pump_with_transport_limits(
        &fragmented_server,
        UpstreamInboundLimits::new(2, 3).expect("valid limits"),
    )
    .await;
    fragmented_server.send_fragmented_text(b"ab", b"cd").await;

    timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                fragmented_pump.terminal_state(),
                UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
                    cause: InboundBufferOverflowCause::TransportSize,
                    ..
                })
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fragmented oversized message should overflow the transport");

    let ping_server = ScriptedWebSocketServer::start().await;
    let ping_pump = connect_pump_with_transport_limits(
        &ping_server,
        UpstreamInboundLimits::new(1, 3).expect("valid limits"),
    )
    .await;
    let ping_payload = vec![b'p'; 125];
    ping_server.send_ping(&ping_payload).await;
    let response = timeout(Duration::from_secs(1), ping_server.recv_client_message())
        .await
        .expect("pong timeout")
        .expect("pong message");
    assert!(matches!(response, Message::Pong(payload) if payload.as_slice() == ping_payload));
    assert!(!ping_pump.is_closed());
}

#[tokio::test]
async fn websocket_pump_preserves_text_binary_fifo_and_releases_bytes_after_dequeue() {
    let server = ScriptedWebSocketServer::start().await;
    let (stream, _) = connect_async(server.url())
        .await
        .expect("connect client websocket");
    let pump = LiveUpstreamWebSocket::from_stream_with_limits(
        stream,
        UpstreamInboundLimits::new(3, 5).expect("valid limits"),
    );

    server.send_text("").await;
    server.send_binary(vec![0xFF]).await;
    server.send_text("a").await;

    assert_eq!(
        pump.recv_text().await.expect("recv empty text"),
        Some(String::new())
    );
    assert_eq!(
        pump.recv_text().await.expect("recv lossy binary"),
        Some("\u{fffd}".to_string())
    );
    assert_eq!(
        pump.recv_text().await.expect("recv text"),
        Some("a".to_string())
    );

    server.send_text("hello").await;
    assert_eq!(
        pump.recv_text()
            .await
            .expect("recv payload after permit release"),
        Some("hello".to_string())
    );
}

#[tokio::test]
async fn websocket_pump_reports_independent_count_byte_and_lossy_binary_overflows() {
    let count_server = ScriptedWebSocketServer::start().await;
    let (count_stream, _) = connect_async(count_server.url())
        .await
        .expect("connect count client websocket");
    let count_pump = LiveUpstreamWebSocket::from_stream_with_limits(
        count_stream,
        UpstreamInboundLimits::new(1, 16).expect("valid limits"),
    );
    count_server.send_text("one").await;
    count_server.send_text("two").await;

    let byte_server = ScriptedWebSocketServer::start().await;
    let (byte_stream, _) = connect_async(byte_server.url())
        .await
        .expect("connect byte client websocket");
    let byte_pump = LiveUpstreamWebSocket::from_stream_with_limits(
        byte_stream,
        UpstreamInboundLimits::new(2, 2).expect("valid limits"),
    );
    byte_server.send_text("abc").await;

    let binary_server = ScriptedWebSocketServer::start().await;
    let (binary_stream, _) = connect_async(binary_server.url())
        .await
        .expect("connect binary client websocket");
    let binary_pump = LiveUpstreamWebSocket::from_stream_with_limits(
        binary_stream,
        UpstreamInboundLimits::new(2, 2).expect("valid limits"),
    );
    binary_server.send_binary(vec![0xFF]).await;

    for (pump, expected_cause) in [
        (&count_pump, InboundBufferOverflowCause::MessageCount),
        (&byte_pump, InboundBufferOverflowCause::PayloadBytes),
        (&binary_pump, InboundBufferOverflowCause::PayloadBytes),
    ] {
        timeout(Duration::from_secs(1), async {
            loop {
                if let UpstreamTerminalState::InboundBufferOverflow(overflow) =
                    pump.terminal_state()
                {
                    assert_eq!(overflow.cause, expected_cause);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pump should overflow");
        assert!(pump.is_closed());
        assert_eq!(
            pump.recv_text().await,
            Err(UpstreamWebSocketError::InboundBufferOverflow)
        );
    }
}

#[tokio::test]
async fn websocket_pump_makes_single_oversized_payload_a_sticky_terminal_error() {
    let server = ScriptedWebSocketServer::start().await;
    let (stream, _) = connect_async(server.url())
        .await
        .expect("connect client websocket");
    let pump = LiveUpstreamWebSocket::from_stream_with_limits(
        stream,
        UpstreamInboundLimits::new(2, 10).expect("valid limits"),
    );

    server.send_text("completion").await;
    server.send_text("oversized-message").await;

    timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                pump.terminal_state(),
                UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
                    cause: InboundBufferOverflowCause::PayloadBytes,
                    ..
                })
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pump should overflow on the oversized payload");

    assert_eq!(
        pump.recv_text().await,
        Err(UpstreamWebSocketError::InboundBufferOverflow)
    );
    assert_eq!(
        pump.recv_text().await,
        Err(UpstreamWebSocketError::InboundBufferOverflow)
    );
    assert_eq!(
        pump.send_text("after-overflow").await,
        Err(UpstreamWebSocketError::InboundBufferOverflow)
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
async fn websocket_pump_releases_waiting_recv_with_normal_close_metadata() {
    let server = ScriptedWebSocketServer::start().await;
    let pump = std::sync::Arc::new(connect_pump(&server).await);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let waiting_recv = {
        let pump = std::sync::Arc::clone(&pump);
        tokio::spawn(async move {
            let _ = started_tx.send(());
            pump.recv_text().await
        })
    };
    started_rx.await.expect("recv task should start");
    tokio::task::yield_now().await;
    server.send_close(1000, "normal-close").await;

    assert_eq!(
        timeout(Duration::from_secs(1), waiting_recv)
            .await
            .expect("waiting recv should be released")
            .expect("recv task should not panic"),
        Ok(None)
    );
    assert_eq!(
        wait_for_closed(&pump).await,
        UpstreamCloseMetadata {
            code: Some(1000),
            reason: Some("normal-close".to_string()),
            error: None,
        }
    );
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
