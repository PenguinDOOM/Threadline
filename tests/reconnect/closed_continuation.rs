use super::no_replay::ProbeHttpState;
use super::*;

pub(super) async fn read_probe_payload(
    state: &mut ProbeHttpState,
    request: Request<Body>,
) -> Result<(axum::http::request::Parts, Value), StatusCode> {
    let (parts, body) = request.into_parts();
    let bytes = tokio::select! {
        bytes = to_bytes(body, 1024 * 1024) => bytes,
        _ = state.shutdown.changed() => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };
    let Ok(bytes) = bytes else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let Ok(payload) = serde_json::from_slice::<Value>(&bytes) else {
        return Err(StatusCode::BAD_REQUEST);
    };
    Ok((parts, payload))
}

use super::timeouts::{start_probe_fixture, start_probe_listener, stop_probe_fixture};

#[tokio::test]
async fn lifetime_probe_fixture_smoke_without_external_client() {
    timeout(Duration::from_secs(15), async {
        for trial in ["success", "retry-failure", "wait", "cancel"] {
            let mut fixture = start_probe_fixture(trial).await;
            let (_, mut listener) = start_probe_listener(&fixture).await;
            let seed = post_responses(fixture.app.clone(), json!({"model":PROBE_MODEL,"input":PROBE_SEED})).await;
            assert_eq!(seed.status(), StatusCode::OK);
            let _ = to_bytes(seed.into_body(), usize::MAX).await.expect("seed body");
            let app = fixture.app.clone();
            let continuation = tokio::spawn(post_responses(app, json!({"model":PROBE_MODEL,"input":PROBE_CONTINUE,"previous_response_id":"resp_probe_seed"})));
            if trial == "cancel" {
                timeout(Duration::from_secs(1), fixture.hold_started.notified()).await.expect("cancel held request");
                continuation.abort();
                let _ = continuation.await;
            } else {
                let response = continuation.await.expect("continuation task");
                if trial == "wait" {
                    assert_eq!(response.status(), StatusCode::OK);
                    let _ = to_bytes(response.into_body(), usize::MAX).await.expect("delayed body");
                } else {
                    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
                    let full = json!([{"role":"user","content":PROBE_SEED}, {"role":"assistant","content":PROBE_SEED_ANSWER}, {"role":"user","content":PROBE_CONTINUE}]);
                    let retry = post_responses(fixture.app.clone(), json!({"model":PROBE_MODEL,"input":full})).await;
                    assert_eq!(retry.status(), StatusCode::OK);
                    let bytes = to_bytes(retry.into_body(), usize::MAX).await.expect("final body");
                    let text = String::from_utf8(bytes.to_vec()).expect("UTF8");
                    assert_eq!(text.matches("data: [DONE]").count(), 1);
                }
            }
            (&mut fixture.worker).await.expect("fixture script");
            assert_eq!(fixture.executor.attempts.load(Ordering::SeqCst), 0);
            stop_probe_fixture(&mut fixture, &mut listener).await;
        }
    }).await.expect("finite offline smoke");
}

#[tokio::test]
async fn recovery_native_failure_and_hosted_tools_use_safe_header_decisions() {
    for (tools, output, conflicting, expected_status) in [
        (None, json!([]), false, StatusCode::BAD_GATEWAY),
        (
            Some(json!([{"type":"web_search"}])),
            json!([]),
            false,
            StatusCode::OK,
        ),
        (
            None,
            json!([{"type":"message","content":[]}]),
            false,
            StatusCode::BAD_GATEWAY,
        ),
        (None, json!([]), true, StatusCode::OK),
    ] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector =
            RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
        let app = build_test_router(Arc::new(connector.clone()));
        seed_marker(app.clone(), &server, "seed-marker").await;
        let response = begin_continuation(app, &server, "seed-marker", tools).await;
        let mut event = json!({"type":"response.failed","response":{"id":"failed-id","output":output,"error":{"code":"websocket_connection_limit_reached","message":"upstream text"}}});
        if conflicting {
            event["error"] = json!({"code":"rate_limit_exceeded","message":"other"});
        }
        server.send_text(&event.to_string()).await;
        let response = timeout(Duration::from_secs(1), response)
            .await
            .expect("headers")
            .expect("response task");
        assert_eq!(response.status(), expected_status);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        if expected_status == StatusCode::OK {
            let body = String::from_utf8(bytes.to_vec()).expect("utf8");
            let frames = split_sse_frames(&body);
            assert_eq!(frames.len(), 2);
            let failed: Value =
                serde_json::from_str(sse_event_and_data(frames[0]).1).expect("failure");
            assert_response_failed_payload(
                &failed,
                if conflicting {
                    "rate_limit_exceeded"
                } else {
                    "websocket_connection_limit_reached"
                },
            );
            assert_done_frame(frames[1]);
        } else {
            let body: Value = serde_json::from_slice(&bytes).expect("JSON failure");
            assert_eq!(
                body["error"]["code"],
                if expected_status == StatusCode::BAD_REQUEST {
                    "previous_response_not_found"
                } else {
                    "websocket_connection_limit_reached"
                }
            );
        }
        assert_eq!(connector.recorded_sessions().await.len(), 1);
    }
}

#[tokio::test]
async fn recovery_created_limit_is_http502_but_text_limit_is_terminal_http200() {
    for boundary_event in [
        None,
        Some(json!({"type":"response.output_text.delta","delta":"visible"})),
        Some(json!({"type":"response.output_item.added","item":{"type":"message","content":[]}})),
        Some(json!({"type":"response.reasoning_summary_text.delta","delta":"reasoning"})),
        Some(
            json!({"type":"response.output_item.added","item":{"type":"function_call","name":"external","call_id":"external-call","arguments":""}}),
        ),
        Some(
            json!({"type":"response.output_item.done","item":{"type":"compaction","id":"state-item","encrypted_content":"opaque-fixture"}}),
        ),
    ] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector =
            RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
        let (app, executor) = build_counting_test_router(connector.clone());
        seed_marker(app.clone(), &server, "seed-marker").await;
        let response = begin_continuation(app.clone(), &server, "seed-marker", None).await;
        server
            .send_text(r#"{"type":"response.created","response":{"id":"active","output":[]}}"#)
            .await;
        if let Some(event) = &boundary_event {
            server.send_text(&event.to_string()).await;
        }
        server.send_text(r#"{"type":"error","error":{"code":"websocket_connection_limit_reached","message":"untrusted upstream message"}}"#).await;
        let response = timeout(Duration::from_secs(1), response)
            .await
            .expect("headers deadline")
            .expect("response task");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        if let Some(expected_event) = &boundary_event {
            assert_eq!(status, StatusCode::OK);
            let body = String::from_utf8(body.to_vec()).expect("utf8");
            assert_boundary_limit_terminal(&body, expected_event);
        } else {
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            let body: Value = serde_json::from_slice(&body).expect("json error");
            assert_eq!(
                body,
                json!({"error":{"code":"websocket_connection_limit_reached","message":"The upstream websocket connection limit was reached.","type":"server_error"}})
            );
        }
        assert_eq!(executor.executions.load(Ordering::SeqCst), 0);
        assert_eq!(connector.recorded_sessions().await.len(), 1);
        assert!(connector.recorded_websockets().await[0].upgrade().is_none());
        assert!(
            timeout(Duration::from_secs(1), server.recv_client_message())
                .await
                .expect("pump released")
                .is_none()
        );
        let retry = post_responses(
            app,
            json!({"model":"gpt-6-sol","input":"retry","previous_response_id":"seed-marker"}),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::BAD_REQUEST);
    }
}

fn assert_boundary_limit_terminal(body: &str, expected_event: &Value) {
    let frames = split_sse_frames(body);
    assert_eq!(frames.len(), 4);
    assert_eq!(sse_event_and_data(frames[0]).0, "response.created");
    let boundary: Value =
        serde_json::from_str(sse_event_and_data(frames[1]).1).expect("boundary event");
    assert_eq!(&boundary, expected_event);
    let failed: Value = serde_json::from_str(sse_event_and_data(frames[2]).1).expect("failure");
    assert_response_failed_payload(&failed, "websocket_connection_limit_reached");
    assert_eq!(
        failed["response"]["error"]["message"],
        "The upstream websocket connection limit was reached."
    );
    assert_done_frame(frames[3]);
}

#[tokio::test]
async fn recovery_nested_exact_errors_fail_http502_without_message_heuristic_replay() {
    for (event, recovery) in [
        (
            json!({"type":"error","error":{"error":{"code":"websocket_connection_limit_reached"}}}),
            true,
        ),
        (
            json!({"type":"error","error":{"code":"previous_response_not_found"}}),
            true,
        ),
        (
            json!({"type":"error","error":{"message":"Previous response with id fixture not found"}}),
            false,
        ),
        (
            json!({"type":"error","error":{"code":"unknown_error","message":"60 minutes websocket_connection_limit_reached"}}),
            false,
        ),
    ] {
        assert_nested_recovery_case(event, recovery).await;
    }
}

async fn assert_nested_recovery_case(event: Value, recovery: bool) {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
    let app = build_test_router(Arc::new(connector.clone()));
    seed_marker(app.clone(), &server, "seed-marker").await;
    let response = begin_continuation(app, &server, "seed-marker", None).await;
    server.send_text(&event.to_string()).await;
    let response = timeout(Duration::from_secs(1), response)
        .await
        .expect("headers")
        .expect("task");
    assert_eq!(
        response.status(),
        if recovery {
            StatusCode::BAD_GATEWAY
        } else {
            StatusCode::OK
        }
    );
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    if recovery {
        let error: Value = serde_json::from_slice(&bytes).expect("JSON");
        let limit = event.pointer("/error/error/code").and_then(Value::as_str)
            == Some("websocket_connection_limit_reached");
        assert_eq!(
            error["error"]["code"],
            if limit {
                "websocket_connection_limit_reached"
            } else {
                "upstream_error_event"
            }
        );
        assert_eq!(
            error["error"]["message"],
            if limit {
                "The upstream websocket connection limit was reached."
            } else {
                "The upstream websocket emitted an error event."
            }
        );
    } else {
        let text = String::from_utf8(bytes.to_vec()).expect("UTF8");
        assert_eq!(text.matches("event: response.failed").count(), 1);
        assert_eq!(text.matches("data: [DONE]").count(), 1);
    }
    assert_eq!(connector.recorded_sessions().await.len(), 1);
}

#[tokio::test]
async fn recovery_unknown_and_payload_created_handoff_then_synthetic_final_once() {
    for boundary in [
        json!({"type":"response.future_state","state":"fixture"}),
        json!({"type":"response.created","response":{"id":"active","output":[{"type":"message","content":[]}]}}),
    ] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector =
            RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
        let app = build_test_router(Arc::new(connector));
        seed_marker(app.clone(), &server, "seed-marker").await;
        let response = begin_continuation(app, &server, "seed-marker", None).await;
        server.send_text(&boundary.to_string()).await;
        let response = timeout(Duration::from_secs(1), response)
            .await
            .expect("boundary headers")
            .expect("task");
        assert_eq!(response.status(), StatusCode::OK);
        server
            .send_text(
                &assistant_text_completed_event("final-marker", "synthetic once").to_string(),
            )
            .await;
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(bytes.to_vec()).expect("UTF8");
        let frames = split_sse_frames(&text);
        assert_eq!(frames.len(), 4);
        let forwarded: Value =
            serde_json::from_str(sse_event_and_data(frames[0]).1).expect("event");
        assert_eq!(forwarded, boundary);
        assert_eq!(
            sse_event_and_data(frames[1]).0,
            "response.output_text.delta"
        );
        assert_eq!(sse_event_and_data(frames[2]).0, "response.completed");
        assert_done_frame(frames[3]);
    }
}

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

    retained_server.abort_connection().await;
    let upstream = connector.recorded_websockets().await[0]
        .upgrade()
        .expect("retained pump");
    timeout(Duration::from_secs(1), async {
        while !upstream.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pre-send close observed");
    drop(upstream);

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

#[tokio::test]
async fn recovery_real_tcp_disconnect_releases_pre_header_lease_without_upstream_events() {
    use tokio::net::TcpListener;

    for lifecycle_only in [false, true] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector =
            RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
        let executor = Arc::new(CountingToolExecutor::default());
        let app = build_router_with_services(
            ThreadlineConfig::default(),
            ThreadlineServices::with_internal_tool_executor(
                Arc::new(StaticAuthProvider),
                Arc::new(connector.clone()),
                Arc::clone(&executor) as Arc<dyn InternalToolExecutor>,
            ),
        );
        seed_continuation_alias(app.clone(), &server).await;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback bind");
        let address = listener.local_addr().expect("loopback address");
        let http_server = tokio::spawn(axum::serve(listener, app.clone()).into_future());
        let downstream = send_tcp_continuation(address, &server).await;
        let weak = connector.recorded_websockets().await[0].clone();
        assert!(weak.upgrade().is_some_and(|upstream| matches!(
            upstream.terminal_state(),
            threadline::ws_pump::UpstreamTerminalState::Open
        )));
        if lifecycle_only {
            server
                .send_text(r#"{"type":"response.created","response":{"id":"active","output":[]}}"#)
                .await;
        }
        drop(downstream);
        let released = timeout(Duration::from_secs(1), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        http_server.abort();
        let _ = http_server.await;
        released.expect("real TCP disconnect must cancel pre-header handler with healthy upstream");
        assert_retained_close_without_resend(&server).await;
        for marker in ["seed-marker", "alias-marker"] {
            let response = post_responses(
                app.clone(),
                json!({"model":"gpt-6-sol","input":"retry","previous_response_id":marker}),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        assert_eq!(connector.recorded_sessions().await.len(), 1);
        assert_eq!(executor.executions.load(Ordering::SeqCst), 0);
    }
}

async fn seed_continuation_alias(app: axum::Router, server: &ScriptedWebSocketServer) {
    seed_marker(app.clone(), server, "seed-marker").await;
    let alias = begin_continuation(app, server, "seed-marker", None).await;
    server
        .send_text(&assistant_text_completed_event("alias-marker", "alias completion").to_string())
        .await;
    let alias = timeout(Duration::from_secs(1), alias)
        .await
        .expect("alias headers")
        .expect("alias task");
    let _ = to_bytes(alias.into_body(), usize::MAX)
        .await
        .expect("alias body");
}

async fn send_tcp_continuation(
    address: std::net::SocketAddr,
    server: &ScriptedWebSocketServer,
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;

    let mut downstream = tokio::net::TcpStream::connect(address)
        .await
        .expect("downstream TCP");
    let payload =
        json!({"model":"gpt-6-sol","input":"continue","previous_response_id":"alias-marker"})
            .to_string();
    let request = format!(
        "POST /v1/responses HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    );
    downstream
        .write_all(request.as_bytes())
        .await
        .expect("send HTTP request");
    let create = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("TCP continuation received")
        .expect("create");
    let create: Value =
        serde_json::from_str(&create.into_text().expect("create text")).expect("create json");
    assert_eq!(create["previous_response_id"], "alias-marker");
    downstream
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
async fn retained_continuation_close_after_send_before_first_upstream_event_returns_http502_without_reconnect()
 {
    let retained_server = Arc::new(ScriptedWebSocketServer::start().await);
    let unexpected_reconnect_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(&retained_server, Some("turn-state-1"), false, None),
        planned_connection(&unexpected_reconnect_server, None, false, None),
    ]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &retained_server, "response-1").await;

    let response = tokio::spawn(post_responses(
        app,
        json!({
            "model":"gpt-6-sol", "input":"followup", "previous_response_id":"response-1"
        }),
    ));

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

    let response = timeout(Duration::from_secs(1), response)
        .await
        .expect("headers")
        .expect("response task");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let error: Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(error["error"]["code"], "upstream_websocket_closed");

    assert_no_reconnect(&unexpected_reconnect_server).await;

    let sessions = connector.recorded_sessions().await;
    assert_eq!(sessions.len(), 1);
}
