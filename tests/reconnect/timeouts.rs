use super::*;

#[tokio::test]
async fn retained_continuation_liveness_timeout_before_preflight_returns_previous_response_not_found_without_reconnect()
 {
    let retained_server = Arc::new(ScriptedWebSocketServer::start_without_reader().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(
            &retained_server,
            Some("retained-turn-state"),
            false,
            Some(short_watchdog_policy()),
        ),
        planned_connection(&unexpected_reconnect_server, None, false, None),
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker_without_reader(app.clone(), &retained_server, "response-1").await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = post_responses(
        app.clone(),
        json!({
            "model":"gpt-6-sol",
            "input":"followup",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let payload: Value = serde_json::from_slice(&body).expect("error json");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");

    assert_no_reconnect(&unexpected_reconnect_server).await;
    assert_eq!(connector.recorded_sessions().await.len(), 1);
}

#[tokio::test]
async fn retained_continuation_liveness_timeout_before_first_upstream_event_returns_http_not_found_and_invalidates_aliases()
 {
    let retained_server = Arc::new(ScriptedWebSocketServer::start_with_stoppable_reader().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(
            &retained_server,
            Some("retained-turn-state"),
            false,
            Some(short_watchdog_policy()),
        ),
        planned_connection(&unexpected_reconnect_server, None, false, None),
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker_without_reader(app.clone(), &retained_server, "response-1").await;
    let _ = retained_server
        .recv_client_message()
        .await
        .expect("seed response.create");

    let response = tokio::spawn(post_responses(
        app.clone(),
        json!({
            "model":"gpt-6-sol", "input":"followup", "previous_response_id":"response-1"
        }),
    ));

    let _ = retained_server
        .recv_client_message()
        .await
        .expect("continuation response.create");
    retained_server.stop_reader();
    let retained_websocket = connector
        .recorded_websockets()
        .await
        .into_iter()
        .next()
        .expect("retained websocket");
    let response = timeout(Duration::from_secs(1), response)
        .await
        .expect("timeout headers")
        .expect("response task");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let error: Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(error["error"]["code"], "previous_response_not_found");
    assert!(retained_websocket.upgrade().is_none());

    assert_retry_marker_invalidated(app).await;

    assert_no_reconnect(&unexpected_reconnect_server).await;
    assert_eq!(connector.recorded_sessions().await.len(), 1);
}

async fn assert_retry_marker_invalidated(app: axum::Router) {
    let retry = post_responses(
        app,
        json!({
            "model":"gpt-6-sol",
            "input":"retry",
            "previous_response_id":"response-1"
        }),
    )
    .await;
    assert_eq!(retry.status(), StatusCode::BAD_REQUEST);
    let retry_body = to_bytes(retry.into_body(), usize::MAX)
        .await
        .expect("retry body");
    let retry_payload: Value = serde_json::from_slice(&retry_body).expect("retry error json");
    assert_eq!(
        retry_payload["error"]["code"],
        "previous_response_not_found"
    );
}
