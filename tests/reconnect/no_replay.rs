use super::*;

#[tokio::test]
async fn reconnect_fallback_is_not_attempted_for_non_continuation_requests() {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
    let app = build_test_router(Arc::new(connector.clone()));

    let response = post_responses(app, json!({"model":"gpt-6-sol","input":"first"})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("initial request timeout")
        .expect("initial request");
    server.send_close(1000, "closed-before-event").await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (event, data) = sse_event_and_data(frames.first().expect("failed frame"));
    let payload: Value = serde_json::from_str(data).expect("failed json");

    assert_eq!(frames.len(), 2);
    assert_eq!(event, "response.failed");
    assert_response_failed_payload(&payload, "upstream_websocket_closed");
    assert!(
        !body_text.contains("event: error\n"),
        "expected terminal websocket close to use the downstream response.failed contract: {body_text}"
    );
    assert_done_frame(frames[1]);

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn reconnect_fallback_is_not_attempted_after_any_upstream_event() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![planned_connection(
        &seed_server,
        Some("turn-state-1"),
        false,
        None,
    )]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;

    let response = post_responses(
        app,
        json!({
            "model":"gpt-6-sol",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let _ = timeout(Duration::from_secs(1), seed_server.recv_client_message())
        .await
        .expect("continuation request timeout")
        .expect("continuation request");
    seed_server
        .send_text(r#"{"type":"response.created","response":{"id":"response-created"}}"#)
        .await;
    seed_server.send_close(1000, "closed-after-event").await;

    assert_created_then_closed_body(response).await;

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn stale_continuation_with_spare_reconnect_plans_returns_previous_response_not_found_without_reconnect()
 {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let first_attempt_server = Arc::new(ScriptedWebSocketServer::start().await);
    let reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(&seed_server, Some("turn-state-1"), false, None),
        planned_connection(&first_attempt_server, None, false, None),
        planned_connection(&reconnect_server, None, false, None),
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;
    seed_server.send_close(1000, "seed complete").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = post_responses(
        app,
        json!({
            "model":"gpt-6-sol",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let no_first_attempt = timeout(
        Duration::from_millis(250),
        first_attempt_server.recv_client_message(),
    )
    .await;
    assert!(no_first_attempt.is_err());

    assert_no_reconnect(&reconnect_server).await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn summary_request_first_send_failure_does_not_reconnect_as_continuation() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let first_attempt_server =
        Arc::new(ScriptedWebSocketServer::start_disconnect_after_handshake().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(&seed_server, Some("turn-state-1"), false, None),
        planned_connection(&first_attempt_server, None, true, None),
        planned_connection(&unexpected_reconnect_server, None, false, None),
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;
    seed_server.send_close(1000, "seed complete").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = post_responses(app, auxiliary_summary_request(Some("response-1"))).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    assert_no_reconnect(&unexpected_reconnect_server).await;

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 2);
}

async fn assert_created_then_closed_body(response: Response<Body>) {
    let body = timeout(
        Duration::from_secs(1),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("body timeout")
    .expect("body");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (created_event, created_data) = sse_event_and_data(frames.first().expect("created frame"));
    let (failed_event, failed_data) = sse_event_and_data(frames.get(1).expect("failed frame"));
    let created_payload: Value = serde_json::from_str(created_data).expect("created json");
    let failed_payload: Value = serde_json::from_str(failed_data).expect("failed json");

    assert_eq!(frames.len(), 3);
    assert_eq!(created_event, "response.created");
    assert_eq!(
        created_payload,
        json!({"type":"response.created","response":{"id":"response-created"}})
    );
    assert_eq!(failed_event, "response.failed");
    assert_response_failed_payload(&failed_payload, "upstream_websocket_closed");
    assert!(
        !body_text.contains("event: error\n"),
        "expected terminal websocket close to use the downstream response.failed contract: {body_text}"
    );
    assert_done_frame(frames[2]);
}
