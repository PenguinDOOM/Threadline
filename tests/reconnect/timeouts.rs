use super::no_replay::{ProbeHttpState, probe_http_observation};
use super::*;

pub(super) fn observe_probe_response(
    response: Response<Body>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Response<Body> {
    use futures_util::StreamExt;

    response.map(|body| {
        Body::from_stream(futures_util::stream::unfold(
            (body.into_data_stream(), shutdown),
            |(mut body, mut shutdown)| async move {
                if *shutdown.borrow() {
                    return None;
                }
                tokio::select! {
                    chunk = body.next() => chunk.map(|chunk| (chunk, (body, shutdown))),
                    _ = shutdown.changed() => None,
                }
            },
        ))
    })
}

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
    let upstream = connector.recorded_websockets().await[0]
        .upgrade()
        .expect("retained pump");
    timeout(Duration::from_secs(1), async {
        while !upstream.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pre-send timeout observed");
    drop(upstream);

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
async fn retained_continuation_liveness_timeout_after_send_returns_http_timeout_and_invalidates_aliases()
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
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let error: Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(error["error"]["code"], "upstream_liveness_timeout");
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

pub(super) struct ProbeFixture {
    pub(super) app: axum::Router,
    connector: RecordingConnector,
    pub(super) executor: Arc<ProbeToolExecutor>,
    posts: Arc<AtomicUsize>,
    pub(super) worker: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::watch::Sender<bool>,
    pub(super) hold_started: Arc<tokio::sync::Notify>,
}

impl Drop for ProbeFixture {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        self.worker.abort();
    }
}

pub(super) struct ProbeListenerTask(tokio::task::JoinHandle<()>);

impl Drop for ProbeListenerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn start_probe_listener(
    fixture: &ProbeFixture,
) -> (std::net::SocketAddr, ProbeListenerTask) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe listener");
    let address = listener.local_addr().expect("probe port");
    let app = fixture.app.clone();
    let mut shutdown = fixture.shutdown.subscribe();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
            .expect("probe server");
    });
    (address, ProbeListenerTask(task))
}

pub(super) async fn stop_probe_fixture(
    fixture: &mut ProbeFixture,
    listener: &mut ProbeListenerTask,
) {
    fixture.shutdown.send(true).expect("probe shutdown");
    fixture.worker.abort();
    if !fixture.worker.is_finished() {
        let _ = (&mut fixture.worker).await;
    }
    (&mut listener.0).await.expect("probe listener stopped");
    fixture.app = axum::Router::new();
    assert!(
        fixture
            .connector
            .recorded_websockets()
            .await
            .iter()
            .all(|pump| pump.upgrade().is_none())
    );
}

async fn probe_models() -> axum::Json<Value> {
    axum::Json(json!({"object":"list","data":[{
        "id":PROBE_MODEL,"object":"model","created":0,"owned_by":"threadline"
    }]}))
}

pub(super) async fn start_probe_fixture(trial: &'static str) -> ProbeFixture {
    let old = Arc::new(ScriptedWebSocketServer::start().await);
    let fresh = Arc::new(ScriptedWebSocketServer::start().await);
    let connector = RecordingConnector::new(vec![
        planned_connection(&old, Some("probe-old-turn-state"), false, None),
        planned_connection(&fresh, None, false, None),
    ]);
    let executor = Arc::new(ProbeToolExecutor::default());
    let posts = Arc::new(AtomicUsize::new(0));
    let hold_started = Arc::new(tokio::sync::Notify::new());
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let bridge = build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::with_internal_tool_executor(
            Arc::new(StaticAuthProvider),
            Arc::new(connector.clone()),
            Arc::clone(&executor) as Arc<dyn InternalToolExecutor>,
        ),
    );
    let app = axum::Router::new()
        .route("/v1/models", axum::routing::get(probe_models))
        .fallback_service(bridge.clone())
        .layer(axum::middleware::from_fn_with_state(
            ProbeHttpState {
                posts: Arc::clone(&posts),
                shutdown: receiver,
            },
            probe_http_observation,
        ));
    let worker = tokio::spawn(run_probe_script(
        trial,
        old,
        fresh,
        connector.clone(),
        bridge,
        Arc::clone(&hold_started),
    ));
    ProbeFixture {
        app,
        connector,
        executor,
        posts,
        worker,
        shutdown,
        hold_started,
    }
}

async fn probe_receive_create(server: &ScriptedWebSocketServer) -> Value {
    loop {
        let message = server
            .recv_client_message()
            .await
            .expect("probe create expected");
        if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
            return serde_json::from_str(&text).expect("probe create JSON");
        }
    }
}

async fn probe_send_final(server: &ScriptedWebSocketServer, marker: &str, text: &str) {
    let mut event = assistant_text_completed_event(marker, text);
    event["response"]["status"] = json!("completed");
    event["response"]["output"][0]["id"] = json!(format!("message-{marker}"));
    event["response"]["output"][0]["status"] = json!("completed");
    server.send_text(&event.to_string()).await;
}

async fn run_probe_script(
    trial: &str,
    old: Arc<ScriptedWebSocketServer>,
    fresh: Arc<ScriptedWebSocketServer>,
    connector: RecordingConnector,
    app: axum::Router,
    hold_started: Arc<tokio::sync::Notify>,
) {
    let seed = probe_receive_create(&old).await;
    let seed_matches = probe_input_contains(&seed["input"], PROBE_SEED);
    probe_log(json!({"event":"probe_seed","known_input":seed_matches}));
    assert!(seed_matches, "use the fixed probe seed input");
    probe_send_final(&old, "resp_probe_seed", PROBE_SEED_ANSWER).await;
    let continuation = probe_receive_create(&old).await;
    let continuation_matches = probe_input_contains(&continuation["input"], PROBE_CONTINUE);
    let marker_matches = continuation["previous_response_id"] == "resp_probe_seed";
    probe_log(
        json!({"event":"probe_continuation","known_input":continuation_matches,"marker_matches":marker_matches}),
    );
    assert!(
        continuation_matches && marker_matches,
        "use fixed continuation in the same chat"
    );
    run_probe_recovery(trial, &old, &fresh, &connector, app, &hold_started).await;
}

async fn run_probe_recovery(
    trial: &str,
    old: &ScriptedWebSocketServer,
    fresh: &ScriptedWebSocketServer,
    connector: &RecordingConnector,
    app: axum::Router,
    hold_started: &tokio::sync::Notify,
) {
    if matches!(trial, "success" | "retry-failure") {
        old.send_text(r#"{"type":"error","error":{"code":"websocket_connection_limit_reached"}}"#)
            .await;
        let resend = probe_receive_create(fresh).await;
        let full_history = [PROBE_SEED, PROBE_SEED_ANSWER, PROBE_CONTINUE]
            .iter()
            .all(|token| probe_input_contains(&resend["input"], token));
        let marker_absent = resend.get("previous_response_id").is_none();
        let sessions = connector.recorded_sessions().await;
        let fresh_state = sessions.len() == 2
            && sessions[1].turn_state.is_none()
            && sessions[0].session_id != sessions[1].session_id;
        probe_log(
            json!({"event":"probe_full_resend","full_fixture_history":full_history,
            "marker_absent":marker_absent,"fresh_state":fresh_state}),
        );
        assert!(
            full_history && marker_absent && fresh_state,
            "full fixture history and fresh markerless session required"
        );
        if trial == "retry-failure" {
            fresh
                .send_text(
                    r#"{"type":"error","error":{"code":"websocket_connection_limit_reached"}}"#,
                )
                .await;
        } else {
            probe_send_final(fresh, "resp_probe_final", PROBE_FINAL).await;
        }
    } else {
        probe_wait_or_cancel(trial, old, connector, app, hold_started).await;
    }
}

async fn probe_wait_or_cancel(
    trial: &str,
    server: &ScriptedWebSocketServer,
    connector: &RecordingConnector,
    app: axum::Router,
    hold_started: &tokio::sync::Notify,
) {
    server
        .send_text(r#"{"type":"response.created","response":{"id":"resp_probe_held","output":[]}}"#)
        .await;
    probe_log(json!({"event":"probe_hold","seconds":if trial == "wait" { 5 } else { 15 }}));
    hold_started.notify_one();
    let disconnected = tokio::select! {
        _ = server.wait_for_client_disconnect() => true,
        _ = tokio::time::sleep(Duration::from_secs(if trial == "wait" { 5 } else { 15 })) => false,
    };
    if disconnected {
        let released = connector.recorded_websockets().await[0].upgrade().is_none();
        let pending = server.take_pending_client_messages().await;
        let extra_creates = pending.iter().filter(|message| message.is_text()).count();
        let alias = post_responses(app, json!({"model":"gpt-6-sol","input":PROBE_CONTINUE,"previous_response_id":"resp_probe_seed"})).await;
        probe_log(json!({"event":"probe_cancel","pump_released":released,
            "alias_invalid":alias.status() == StatusCode::BAD_REQUEST,"extra_creates":extra_creates}));
        assert!(
            trial == "cancel"
                && released
                && extra_creates == 0
                && alias.status() == StatusCode::BAD_REQUEST,
            "cancel contract not observed"
        );
    } else {
        probe_log(json!({"event":"probe_delay_finished","stop_observed":false}));
        probe_send_final(server, "resp_probe_final", PROBE_FINAL).await;
        assert_eq!(trial, "wait", "Stop was not observed within the hold");
    }
}

pub(super) async fn run_manual_probe(trial: &'static str) {
    let mut fixture = start_probe_fixture(trial).await;
    let (address, mut listener) = start_probe_listener(&fixture).await;
    probe_log(
        json!({"event":"probe_ready","trial":trial,"endpoint":format!("http://{address}/v1"),
        "model":PROBE_MODEL,"deadline_seconds":120}),
    );
    let result = timeout(Duration::from_secs(117), async {
        let script_passed = (&mut fixture.worker).await.is_ok();
        if !script_passed {
            probe_log(json!({"event":"probe_incomplete","script_passed":false}));
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        probe_log(
            json!({"event":"probe_end","posts":fixture.posts.load(Ordering::SeqCst),
            "connections":fixture.connector.recorded_sessions().await.len(),
            "tool_attempts":fixture.executor.attempts.load(Ordering::SeqCst),
            "manual_ui_observation_required":true}),
        );
        fixture.executor.attempts.load(Ordering::SeqCst) == 0
            && fixture.posts.load(Ordering::SeqCst)
                == if matches!(trial, "success" | "retry-failure") {
                    3
                } else {
                    2
                }
    })
    .await;
    timeout(
        Duration::from_secs(2),
        stop_probe_fixture(&mut fixture, &mut listener),
    )
    .await
    .expect("finite probe cleanup");
    probe_log(
        json!({"event":"probe_cleanup","deadline_expired":result.is_err(),
        "script_passed":matches!(result, Ok(true)),"listener_stopped":true,"pumps_released":true}),
    );
    assert!(
        matches!(result, Ok(true)),
        "probe incomplete; manual observation required"
    );
}

#[tokio::test]
async fn lifetime_probe_cleanup_releases_idle_partial_body_and_pre_header_connections() {
    use tokio::io::AsyncReadExt;

    for state in ["idle", "partial-body", "pre-header"] {
        let mut fixture = start_probe_fixture("cancel").await;
        let (address, mut listener) = start_probe_listener(&fixture).await;
        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("probe TCP");
        prepare_probe_cleanup_request(state, &mut fixture, &mut client, address).await;
        timeout(
            Duration::from_secs(2),
            stop_probe_fixture(&mut fixture, &mut listener),
        )
        .await
        .expect("cleanup deadline");
        let mut bytes = Vec::new();
        timeout(Duration::from_secs(1), client.read_to_end(&mut bytes))
            .await
            .expect("downstream closed")
            .expect("TCP read");
        let rebound = tokio::net::TcpListener::bind(address)
            .await
            .expect("listener port released");
        drop(rebound);
        assert_eq!(fixture.executor.attempts.load(Ordering::SeqCst), 0);
        assert_eq!(
            fixture.connector.recorded_sessions().await.len(),
            usize::from(state == "pre-header")
        );
    }
}

async fn prepare_probe_cleanup_request(
    state: &str,
    fixture: &mut ProbeFixture,
    client: &mut tokio::net::TcpStream,
    address: std::net::SocketAddr,
) {
    use tokio::io::AsyncWriteExt;

    match state {
        "partial-body" => {
            client
                .write_all(
                    format!("POST /v1/responses HTTP/1.1\r\nHost: {address}\r\nContent-Length: 100\r\n\r\n{{")
                        .as_bytes(),
                )
                .await
                .expect("partial body");
            timeout(Duration::from_secs(1), async {
                while fixture.posts.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("body wait started");
        }
        "pre-header" => {
            let seed = post_responses(
                fixture.app.clone(),
                json!({"model":PROBE_MODEL,"input":PROBE_SEED}),
            )
            .await;
            assert_eq!(seed.status(), StatusCode::OK);
            let _ = to_bytes(seed.into_body(), usize::MAX)
                .await
                .expect("seed body");
            let payload = json!({"model":PROBE_MODEL,"input":PROBE_CONTINUE,"previous_response_id":"resp_probe_seed"}).to_string();
            client
                .write_all(
                    format!("POST /v1/responses HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}", payload.len())
                        .as_bytes(),
                )
                .await
                .expect("held request");
            timeout(Duration::from_secs(1), fixture.hold_started.notified())
                .await
                .expect("pre-header wait started");
        }
        "idle" => {}
        _ => unreachable!("known probe cleanup state"),
    }
}
