use super::*;

#[tokio::test]
async fn live_retained_continuation_close_before_first_send_returns_previous_response_not_found_without_reconnect_or_resend()
 {
    let retained_server = Arc::new(ScriptedWebSocketServer::start().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(&retained_server, Some("turn-state-1"), false, None),
        planned_connection(&unexpected_reconnect_server, None, false, None),
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &retained_server, "response-1").await;

    let response_task = tokio::spawn({
        let app = app.clone();
        async move {
            post_responses(
                app,
                json!({
                    "model":"gpt-6-sol",
                    "input":"followup",
                    "previous_response_id":"response-1"
                }),
            )
            .await
        }
    });

    retained_server.abort_connection().await;

    let response = timeout(Duration::from_secs(1), response_task)
        .await
        .expect("continuation response timeout")
        .expect("continuation response task");
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "retained close before first send should fail before SSE starts"
    );

    assert_retained_close_without_resend(&retained_server).await;

    assert_no_reconnect(&unexpected_reconnect_server).await;

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

async fn assert_retained_close_without_resend(retained_server: &ScriptedWebSocketServer) {
    let retained_message = timeout(
        Duration::from_millis(250),
        retained_server.recv_client_message(),
    )
    .await
    .expect("retained close should resolve the pending client receive");
    assert!(
        retained_message.is_none(),
        "expected the retained upstream to close before resending the same previous_response_id"
    );
}

#[tokio::test]
async fn retained_continuation_close_after_send_before_first_upstream_event_replays_stale_marker_without_reconnect()
 {
    let retained_server = Arc::new(ScriptedWebSocketServer::start().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(&retained_server, Some("turn-state-1"), false, None),
        planned_connection(&unexpected_reconnect_server, None, false, None),
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &retained_server, "response-1").await;

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

    let body_task = tokio::spawn(async move {
        timeout(
            Duration::from_secs(1),
            to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("body timeout")
        .expect("body bytes")
    });

    let retained_message = timeout(
        Duration::from_secs(1),
        retained_server.recv_client_message(),
    )
    .await
    .expect("continuation request timeout")
    .expect("continuation request");
    let retained_message = retained_message.into_text().expect("text request");
    let retained_payload: Value = serde_json::from_str(&retained_message).expect("request json");
    assert_eq!(retained_payload["previous_response_id"], "response-1");

    retained_server
        .send_close(1000, "closed-before-first-event")
        .await;

    assert_stale_close_body(body_task).await;

    assert_no_reconnect(&unexpected_reconnect_server).await;

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}

async fn assert_stale_close_body(body_task: tokio::task::JoinHandle<axum::body::Bytes>) {
    let body = body_task.await.expect("body task");
    let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
    let frames = split_sse_frames(&body_text);
    let (failed_event, failed_data) = sse_event_and_data(frames.first().expect("failed frame"));
    let failed_payload: Value = serde_json::from_str(failed_data).expect("failed json");

    assert_eq!(frames.len(), 2);
    assert_eq!(failed_event, "response.failed");
    assert_response_failed_payload(&failed_payload, "previous_response_not_found");
    assert_done_frame(frames[1]);
}
