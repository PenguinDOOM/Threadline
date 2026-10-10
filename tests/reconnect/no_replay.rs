use super::*;

#[derive(Clone)]
pub(super) struct ProbeHttpState {
    pub(super) posts: Arc<AtomicUsize>,
    pub(super) shutdown: tokio::sync::watch::Receiver<bool>,
}

pub(super) async fn probe_http_observation(
    axum::extract::State(mut state): axum::extract::State<ProbeHttpState>,
    request: Request<Body>,
    next: axum::middleware::Next,
) -> Response<Body> {
    if request.method() != axum::http::Method::POST || request.uri().path() != "/v1/responses" {
        return next.run(request).await;
    }
    let post = state.posts.fetch_add(1, Ordering::SeqCst) + 1;
    let (parts, mut payload) =
        match super::closed_continuation::read_probe_payload(&mut state, request).await {
            Ok(payload) => payload,
            Err(status) => {
                return Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .expect("probe error response");
            }
        };
    log_probe_post(post, &payload);
    if payload["model"] != PROBE_MODEL || post > 3 {
        probe_log(json!({"event":"probe_http_status","post":post,"status":400}));
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::empty())
            .expect("response");
    }
    payload["model"] = json!("threadline-main-gpt-6-sol");
    let response = tokio::select! {
        response = next.run(Request::from_parts(parts, Body::from(payload.to_string()))) => response,
        _ = state.shutdown.changed() => Response::builder().status(StatusCode::SERVICE_UNAVAILABLE).body(Body::empty()).expect("shutdown response"),
    };
    probe_log(json!({"event":"probe_http_status","post":post,"status":response.status().as_u16()}));
    super::timeouts::observe_probe_response(response, state.shutdown)
}

fn log_probe_post(post: usize, payload: &Value) {
    probe_log(json!({"event":"probe_post","post":post,
        "marker_present":payload.get("previous_response_id").is_some(),
        "seed_included":probe_input_contains(&payload["input"], PROBE_SEED),
        "seed_answer_included":probe_input_contains(&payload["input"], PROBE_SEED_ANSWER),
        "continuation_included":probe_input_contains(&payload["input"], PROBE_CONTINUE)}));
}

#[tokio::test]
async fn client_emulated_full_resend_uses_new_session_and_only_registers_success() {
    for retry_fails in [false, true] {
        let old_server = Arc::new(ScriptedWebSocketServer::start().await);
        let new_server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector = RecordingConnector::new(vec![
            planned_connection(&old_server, Some("old-turn-state"), false, None),
            planned_connection(&new_server, None, false, None),
        ]);
        let app = build_test_router(Arc::new(connector.clone()));
        seed_marker(app.clone(), &old_server, "seed-marker").await;
        let alias = begin_continuation(app.clone(), &old_server, "seed-marker", None).await;
        old_server
            .send_text(
                &assistant_text_completed_event("alias-marker", "alias completion").to_string(),
            )
            .await;
        let _ = to_bytes(alias.await.expect("alias headers").into_body(), usize::MAX)
            .await
            .expect("alias body");
        let continuation = begin_continuation(app.clone(), &old_server, "alias-marker", None).await;
        old_server
            .send_text(r#"{"type":"error","error":{"code":"websocket_connection_limit_reached"}}"#)
            .await;
        let response = timeout(Duration::from_secs(1), continuation)
            .await
            .expect("limit headers")
            .expect("task");
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let error: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("error body"),
        )
        .expect("error JSON");
        assert_eq!(error["error"]["code"], "websocket_connection_limit_reached");
        assert_eq!(connector.recorded_sessions().await.len(), 1);
        assert!(connector.recorded_websockets().await[0].upgrade().is_none());
        assert_old_aliases_missing(app.clone()).await;
        let full_input = json!([
            {"role":"user","content":"seed"}, {"role":"assistant","content":"seed completion"},
            {"role":"user","content":"continue"}, {"role":"assistant","content":"alias completion"},
            {"role":"user","content":"continue"}
        ]);
        let retry =
            post_responses(app.clone(), json!({"model":"gpt-6-sol","input":full_input})).await;
        assert_eq!(retry.status(), StatusCode::OK);
        let create = timeout(Duration::from_secs(1), new_server.recv_client_message())
            .await
            .expect("new create deadline")
            .expect("new create");
        let create: Value = serde_json::from_str(&create.into_text().expect("text")).expect("JSON");
        assert_eq!(create["input"], full_input);
        assert!(create.get("previous_response_id").is_none());
        let sessions = connector.recorded_sessions().await;
        assert_eq!(sessions.len(), 2);
        assert_ne!(sessions[0].session_id, sessions[1].session_id);
        assert_ne!(sessions[0].window_id, sessions[1].window_id);
        assert_eq!(sessions[1].turn_state, None);
        assert_client_retry_result(app, &new_server, &connector, retry, retry_fails).await;
    }
}

async fn assert_old_aliases_missing(app: axum::Router) {
    for marker in ["seed-marker", "alias-marker", "new-failed-marker"] {
        let stale = post_responses(
            app.clone(),
            json!({"model":"gpt-6-sol","input":"lookup","previous_response_id":marker}),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::BAD_REQUEST);
    }
}

async fn assert_client_retry_result(
    app: axum::Router,
    server: &ScriptedWebSocketServer,
    connector: &RecordingConnector,
    retry: Response<Body>,
    retry_fails: bool,
) {
    if retry_fails {
        server.send_text(r#"{"type":"response.failed","response":{"id":"new-failed-marker","error":{"code":"websocket_connection_limit_reached"}}}"#).await;
    } else {
        server
            .send_text(&assistant_text_completed_event("new-marker", "final once").to_string())
            .await;
    }
    let bytes = to_bytes(retry.into_body(), usize::MAX)
        .await
        .expect("retry body");
    let text = String::from_utf8(bytes.to_vec()).expect("UTF8");
    assert_eq!(text.matches("data: [DONE]").count(), 1);
    assert_eq!(
        text.matches("event: response.failed").count(),
        usize::from(retry_fails)
    );
    assert_eq!(
        text.matches("event: response.completed").count(),
        usize::from(!retry_fails)
    );
    assert_eq!(
        text.matches("event: response.output_text.delta").count(),
        usize::from(!retry_fails)
    );
    assert_eq!(connector.recorded_sessions().await.len(), 2);
    assert_old_aliases_missing(app.clone()).await;
    if !retry_fails {
        let next = begin_continuation(app, server, "new-marker", None).await;
        next.abort();
        assert!(next.await.expect_err("cancelled task").is_cancelled());
    }
}

#[tokio::test]
async fn transient_limit_leaves_main_marker_usable_then_visible_close_is_terminal() {
    let retained = Arc::new(ScriptedWebSocketServer::start().await);
    let auxiliary = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(&retained, None, false, None),
        planned_connection(&auxiliary, None, false, None),
    ]);
    let app = build_test_router(Arc::new(connector.clone()));
    seed_marker(app.clone(), &retained, "seed-marker").await;
    let summary = post_responses(app.clone(), auxiliary_summary_request(Some("seed-marker"))).await;
    assert_eq!(summary.status(), StatusCode::OK);
    let create = auxiliary
        .recv_client_message()
        .await
        .expect("auxiliary create");
    let create: Value = serde_json::from_str(&create.into_text().expect("text")).expect("JSON");
    assert!(create.get("previous_response_id").is_none());
    auxiliary
        .send_text(r#"{"type":"error","error":{"code":"websocket_connection_limit_reached"}}"#)
        .await;
    let bytes = to_bytes(summary.into_body(), usize::MAX)
        .await
        .expect("summary body");
    let text = String::from_utf8(bytes.to_vec()).expect("UTF8");
    assert_eq!(text.matches("event: response.failed").count(), 1);
    assert!(text.contains("websocket_connection_limit_reached"));
    assert!(connector.recorded_websockets().await[1].upgrade().is_none());
    let continued = begin_continuation(app, &retained, "seed-marker", None).await;
    retained
        .send_text(r#"{"type":"response.output_text.delta","delta":"visible once"}"#)
        .await;
    let response = timeout(Duration::from_secs(1), continued)
        .await
        .expect("headers")
        .expect("task");
    assert_eq!(response.status(), StatusCode::OK);
    retained.send_close(1000, "scripted visible close").await;
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(bytes.to_vec()).expect("UTF8");
    assert_eq!(text.matches("event: response.output_text.delta").count(), 1);
    assert_eq!(text.matches("event: response.failed").count(), 1);
    assert_eq!(text.matches("data: [DONE]").count(), 1);
    assert!(text.contains("upstream_websocket_closed"));
    assert_eq!(connector.recorded_sessions().await.len(), 2);
}

#[tokio::test]
async fn continuation_close_after_send_never_replays_regardless_of_tools() {
    for (tools, recovery, abort_transport) in [
        (
            json!([{"type":"function","name":"external","parameters":{}}]),
            true,
            false,
        ),
        (
            json!([{"type":"function","name":"external","parameters":{}}]),
            true,
            true,
        ),
        (json!([{"type":"web_search"}]), false, false),
        (json!([{"type":"future_hosted_tool"}]), false, false),
    ] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector =
            RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
        let app = build_test_router(Arc::new(connector.clone()));
        seed_marker(app.clone(), &server, "seed-marker").await;
        let response = begin_continuation(app, &server, "seed-marker", Some(tools)).await;
        server
            .send_text(r#"{"type":"response.in_progress","response":{"id":"active","output":[]}}"#)
            .await;
        if abort_transport {
            server.abort_connection().await;
        } else {
            server.send_close(1000, "scripted close").await;
        }
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
            assert_eq!(error["error"]["code"], "upstream_websocket_closed");
        } else {
            let text = String::from_utf8(bytes.to_vec()).expect("UTF8");
            assert!(text.contains("upstream_websocket_closed"));
            assert_eq!(text.matches("event: response.failed").count(), 1);
            assert_eq!(text.matches("data: [DONE]").count(), 1);
        }
        assert_eq!(connector.recorded_sessions().await.len(), 1);
        assert!(connector.recorded_websockets().await[0].upgrade().is_none());
    }
}

#[tokio::test]
async fn recovery_internal_followup_limit_is_terminal_without_duplicate_tool_execution() {
    for failure in ["limit", "internal-close", "followup-close"] {
        assert_internal_progress_failure_without_duplicate_execution(failure).await;
    }
}

async fn assert_internal_progress_failure_without_duplicate_execution(failure: &str) {
    let server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
    let (app, executor) = build_counting_test_router(connector.clone());
    seed_marker(app.clone(), &server, "seed-marker").await;
    let response = begin_continuation(app.clone(), &server, "seed-marker", None).await;
    let body = tokio::spawn(async move {
        let response = response.await.expect("headers");
        assert_eq!(response.status(), StatusCode::OK);
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body")
    });
    let tool_event = json!({"type":"response.output_item.done","item":{
        "type":"function_call","call_id":"call-1","name":"threadline_echo",
        "arguments":json!({"value":"tool-output"}).to_string()
    }});
    server.send_text(&tool_event.to_string()).await;
    timeout(Duration::from_secs(1), async {
        while executor.executions.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("internal execution started");
    if failure != "internal-close" {
        assert_internal_followup(&server).await;
    }
    if failure == "limit" {
        server.send_text(r#"{"type":"response.failed","response":{"id":"failed-id","error":{"code":"websocket_connection_limit_reached"}}}"#).await;
    } else {
        server.send_close(1000, "scripted close").await;
    }
    let bytes = timeout(Duration::from_secs(1), body)
        .await
        .expect("terminal body")
        .expect("body task");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    let frames = split_sse_frames(&text);
    assert_eq!(frames.len(), 2);
    let failed: Value = serde_json::from_str(sse_event_and_data(frames[0]).1).expect("failure");
    assert_response_failed_payload(
        &failed,
        if failure == "limit" {
            "websocket_connection_limit_reached"
        } else {
            "upstream_websocket_closed"
        },
    );
    assert_done_frame(frames[1]);
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert_eq!(connector.recorded_sessions().await.len(), 1);
    assert!(
        timeout(Duration::from_secs(1), server.recv_client_message())
            .await
            .expect("no additional create")
            .is_none()
    );
    for marker in ["seed-marker", "intermediate", "failed-id"] {
        let retry = post_responses(
            app.clone(),
            json!({"model":"gpt-6-sol","input":"retry","previous_response_id":marker}),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::BAD_REQUEST);
    }
}

async fn assert_internal_followup(server: &ScriptedWebSocketServer) {
    server
        .send_text(r#"{"type":"response.completed","response":{"id":"intermediate"}}"#)
        .await;
    let followup = timeout(Duration::from_secs(1), server.recv_client_message())
        .await
        .expect("followup")
        .expect("followup create");
    let followup: Value = serde_json::from_str(&followup.into_text().expect("text")).expect("JSON");
    assert_eq!(followup["previous_response_id"], "intermediate");
    assert_eq!(followup["input"][0]["type"], "function_call_output");
}

#[tokio::test]
async fn reconnect_fallback_is_not_attempted_for_non_continuation_requests() {
    for connection_limit in [false, true] {
        let server = Arc::new(ScriptedWebSocketServer::start().await);
        let connector =
            RecordingConnector::new(vec![planned_connection(&server, None, false, None)]);
        let app = build_test_router(Arc::new(connector.clone()));

        let response = post_responses(app, json!({"model":"gpt-6-sol","input":"first"})).await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = timeout(Duration::from_secs(1), server.recv_client_message())
            .await
            .expect("initial request timeout")
            .expect("initial request");
        if connection_limit {
            server
                .send_text(
                    r#"{"type":"error","error":{"code":"websocket_connection_limit_reached"}}"#,
                )
                .await;
        } else {
            server.send_close(1000, "closed-before-event").await;
        }

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body_text = String::from_utf8(body.to_vec()).expect("utf8 body");
        let frames = split_sse_frames(&body_text);
        let (event, data) = sse_event_and_data(frames.first().expect("failed frame"));
        let payload: Value = serde_json::from_str(data).expect("failed json");

        assert_eq!(frames.len(), 2);
        assert_eq!(event, "response.failed");
        assert_response_failed_payload(
            &payload,
            if connection_limit {
                "websocket_connection_limit_reached"
            } else {
                "upstream_websocket_closed"
            },
        );
        assert!(
            !body_text.contains("event: error\n"),
            "expected terminal websocket close to use the downstream response.failed contract: {body_text}"
        );
        assert_done_frame(frames[1]);

        let sessions = connector.recorded_sessions().await;
        assert_eq!(sessions.len(), 1);
    }
}

#[tokio::test]
async fn lifecycle_only_close_returns_http502_without_reconnect() {
    let seed_server = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![planned_connection(
        &seed_server,
        Some("turn-state-1"),
        false,
        None,
    )]);
    let app = build_test_router(Arc::new(connector.clone()));

    seed_marker(app.clone(), &seed_server, "response-1").await;

    let response = tokio::spawn(post_responses(
        app,
        json!({
            "model":"gpt-6-sol", "input":"followup", "previous_response_id":"response-1"
        }),
    ));

    let _ = timeout(Duration::from_secs(1), seed_server.recv_client_message())
        .await
        .expect("continuation request timeout")
        .expect("continuation request");
    seed_server
        .send_text(r#"{"type":"response.created","response":{"id":"response-created"}}"#)
        .await;
    seed_server.send_close(1000, "closed-after-event").await;

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
