use axum::Router;
use axum::body::{Body, Bytes, HttpBody, to_bytes};
use axum::extract::Json;
use axum::http::{HeaderValue, Request, StatusCode};
use axum::routing::post;
use futures_util::future::BoxFuture;
use futures_util::stream;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tower::ServiceExt;

use threadline::auth::LoadedUpstreamAuth;
use threadline::codex_ws::UpstreamSessionDescriptor;
use threadline::config::ThreadlineConfig;
use threadline::errors::ThreadlineError;
use threadline::http::build_router;
use threadline::http::build_router_with_services;
use threadline::models::RouteProfile;
use threadline::responses::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};

#[derive(Clone)]
struct MissingAuthProvider;

impl UpstreamAuthProvider for MissingAuthProvider {
    fn load(&self) -> Result<LoadedUpstreamAuth, ThreadlineError> {
        Err(ThreadlineError::UpstreamCredentialsUnavailable)
    }
}

#[derive(Clone)]
struct CountingMissingAuthProvider {
    loads: Arc<AtomicUsize>,
}

impl CountingMissingAuthProvider {
    fn new() -> Self {
        Self {
            loads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn load_count(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }
}

impl UpstreamAuthProvider for CountingMissingAuthProvider {
    fn load(&self) -> Result<LoadedUpstreamAuth, ThreadlineError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Err(ThreadlineError::UpstreamCredentialsUnavailable)
    }
}

#[derive(Clone)]
struct UnusedConnector;

impl UpstreamConnector for UnusedConnector {
    fn connect(
        &self,
        _auth: LoadedUpstreamAuth,
        _session: Option<UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
        Box::pin(async { panic!("connector should not be called when auth loading fails") })
    }
}

const NEW_MAIN_VISIBLE_MODEL_IDS: [&str; 4] = [
    "threadline-main-gpt-6-astra",
    "threadline-main-gpt-5.6-sol",
    "threadline-main-gpt-5.6-terra",
    "threadline-main-gpt-5.6-luna",
];

const NEW_MAIN_RAW_COMPATIBILITY_MODEL_IDS: [&str; 3] =
    ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"];

const ASTRA_MAIN_MODEL_IDS: [&str; 2] = ["threadline-main-gpt-6-astra", "gpt-6-astra"];

const ADVERTISED_MAIN_MODEL_IDS: [&str; 6] = [
    "threadline-main-gpt-6-astra",
    "threadline-main-gpt-5.6-sol",
    "threadline-main-gpt-5.6-terra",
    "threadline-main-gpt-5.6-luna",
    "threadline-main-gpt-5.5",
    "threadline-main-gpt-5.4",
];

const ADVERTISED_UTILITY_MODEL_IDS: [&str; 3] = [
    "threadline-utility-gpt-5.6-luna",
    "threadline-utility-gpt-5.4-mini",
    "threadline-utility-gpt-5.3-codex-spark",
];

const ACCEPTED_MAIN_MODEL_IDS: [&str; 14] = [
    "threadline-main-gpt-6-astra",
    "threadline-main-gpt-5.6-sol",
    "threadline-main-gpt-5.6-terra",
    "threadline-main-gpt-5.6-luna",
    "threadline-main-gpt-5.5",
    "threadline-main-gpt-5.4",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-6-astra",
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.3-codex-spark",
];

const UNSUPPORTED_MODEL_IDS: [&str; 5] = [
    "threadline-utility-gpt-5.6-luna",
    "threadline-utility-gpt-5.4-mini",
    "threadline-utility-gpt-5.3-codex-spark",
    "codex-mini-latest",
    "threadline-test-unsupported",
];

const HIDDEN_MAIN_COMPATIBILITY_MODEL_IDS: [&str; 7] = [
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.3-codex-spark",
];

const REQUEST_BODY_WHITESPACE_CHUNK_BYTES: usize = 64 * 1024;

async fn read_json_body(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn invalid_model_payload(model: Value) -> Value {
    json!({ "model": model })
}

async fn post_responses_json(app: axum::Router, payload: Value) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn post_responses_request_body(app: axum::Router, body: String) -> axum::response::Response {
    post_responses_body(app, Some("application/json"), Body::from(body)).await
}

async fn post_responses_body(
    app: axum::Router,
    content_type: Option<&str>,
    body: Body,
) -> axum::response::Response {
    let mut request = Request::builder().method("POST").uri("/v1/responses");
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    app.oneshot(request.body(body).unwrap()).await.unwrap()
}

fn json_body_with_trailing_whitespace(prefix: &'static str, total_bytes: usize) -> Body {
    assert!(total_bytes >= prefix.len());

    let whitespace_chunk = Bytes::from(vec![b' '; REQUEST_BODY_WHITESPACE_CHUNK_BYTES]);
    let remaining_bytes = total_bytes - prefix.len();
    Body::from_stream(stream::unfold(
        (Some(Bytes::from_static(prefix.as_bytes())), remaining_bytes),
        move |(prefix, remaining_bytes)| {
            let whitespace_chunk = whitespace_chunk.clone();
            async move {
                if let Some(prefix) = prefix {
                    return Some((Ok::<Bytes, std::io::Error>(prefix), (None, remaining_bytes)));
                }
                if remaining_bytes == 0 {
                    return None;
                }

                let chunk_bytes = remaining_bytes.min(whitespace_chunk.len());
                Some((
                    Ok(whitespace_chunk.slice(..chunk_bytes)),
                    (None, remaining_bytes - chunk_bytes),
                ))
            }
        },
    ))
}

async fn post_responses_stream_request_body(
    app: axum::Router,
    chunks: Vec<Result<Bytes, std::io::Error>>,
) -> axum::response::Response {
    let body = Body::from_stream(stream::iter(chunks));
    assert_eq!(body.size_hint().upper(), None);
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    assert!(request.headers().get("content-length").is_none());
    app.oneshot(request).await.unwrap()
}

async fn post_baseline_json_body(
    content_type: Option<&str>,
    body: Body,
) -> axum::response::Response {
    async fn baseline(Json(_): Json<Value>) -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let mut request = Request::builder().method("POST").uri("/");
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    Router::new()
        .route("/", post(baseline))
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap()
}

async fn response_parts(
    response: axum::response::Response,
) -> (StatusCode, Option<HeaderValue>, Bytes) {
    let status = response.status();
    let content_type = response.headers().get("content-type").cloned();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, content_type, body)
}

fn assert_request_body_too_large(payload: Value) {
    assert_eq!(
        payload,
        json!({
            "error": {
                "code": "request_body_too_large",
                "message": "The /v1/responses request body exceeds the configured byte limit.",
                "type": "invalid_request_error"
            }
        })
    );
}

fn assert_invalid_model_error(payload: &Value) {
    assert_eq!(payload["error"]["type"], "invalid_request_error");
    assert_eq!(payload["error"]["code"], "invalid_model");
}

fn assert_unsupported_reasoning_context_error(payload: &Value) {
    assert_eq!(payload["error"]["type"], "invalid_request_error");
    assert_eq!(payload["error"]["code"], "unsupported_reasoning_context");
    assert_eq!(
        payload["error"]["message"],
        "reasoning.context=all_turns is not supported for this model. The model metadata has use_responses_lite=false."
    );
}

fn utility_config() -> ThreadlineConfig {
    ThreadlineConfig {
        profile: RouteProfile::Utility,
        ..ThreadlineConfig::default()
    }
}

#[tokio::test]
async fn request_body_limit_accepts_exact_utf8_bytes_and_rejects_one_byte_over() {
    for (profile, model) in [
        (RouteProfile::Main, "gpt-5.4"),
        (RouteProfile::Utility, "threadline-utility-gpt-5.4-mini"),
    ] {
        let body = json!({ "model": model, "input": "cafe\u{301}" }).to_string();
        let body_bytes = body.len();

        for limit in [body_bytes + 1, body_bytes] {
            let app = build_router_with_services(
                ThreadlineConfig {
                    profile,
                    max_request_body_bytes: limit,
                    ..ThreadlineConfig::default()
                },
                ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
            );
            let response = post_responses_request_body(app, body.clone()).await;
            assert_eq!(
                response.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "profile={profile:?}, limit={limit}"
            );
            assert_eq!(
                read_json_body(response).await["error"]["code"],
                "upstream_credentials_unavailable",
                "profile={profile:?}, limit={limit}"
            );
        }

        let auth = CountingMissingAuthProvider::new();
        let app = build_router_with_services(
            ThreadlineConfig {
                profile,
                max_request_body_bytes: body_bytes - 1,
                ..ThreadlineConfig::default()
            },
            ThreadlineServices::new(Arc::new(auth.clone()), Arc::new(UnusedConnector)),
        );
        let response = post_responses_request_body(app, body).await;
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "profile={profile:?}"
        );
        assert_request_body_too_large(read_json_body(response).await);
        assert_eq!(auth.load_count(), 0, "profile={profile:?}");
    }
}

#[tokio::test]
async fn request_body_limit_counts_unknown_length_chunks_cumulatively_without_auth() {
    let body = br#"{"model":"gpt-5.4"}"#;
    let limit = body.len();
    let chunks = [
        Bytes::copy_from_slice(&body[..8]),
        Bytes::copy_from_slice(&body[8..]),
    ];
    assert!(chunks.iter().all(|chunk| chunk.len() <= limit));
    let exact_app = build_router_with_services(
        ThreadlineConfig {
            max_request_body_bytes: limit,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );
    let exact = post_responses_stream_request_body(
        exact_app,
        vec![Ok(chunks[0].clone()), Ok(chunks[1].clone())],
    )
    .await;
    assert_eq!(exact.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        read_json_body(exact).await["error"]["code"],
        "upstream_credentials_unavailable"
    );

    let over_auth = CountingMissingAuthProvider::new();
    let over_app = build_router_with_services(
        ThreadlineConfig {
            max_request_body_bytes: limit - 1,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::new(Arc::new(over_auth.clone()), Arc::new(UnusedConnector)),
    );
    let over = post_responses_stream_request_body(
        over_app,
        vec![Ok(chunks[0].clone()), Ok(chunks[1].clone())],
    )
    .await;
    assert_eq!(over.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_request_body_too_large(read_json_body(over).await);
    assert_eq!(over_auth.load_count(), 0);
}

#[tokio::test]
async fn request_body_limit_preserves_non_413_json_rejections() {
    for (content_type, body) in [
        (Some("application/json"), "{".to_string()),
        (None, "{}".to_string()),
        (Some("text/plain"), " ".repeat(2048)),
    ] {
        let baseline =
            response_parts(post_baseline_json_body(content_type, Body::from(body.clone())).await)
                .await;
        let auth = CountingMissingAuthProvider::new();
        let app = build_router_with_services(
            ThreadlineConfig {
                max_request_body_bytes: 1,
                ..ThreadlineConfig::default()
            },
            ThreadlineServices::new(Arc::new(auth.clone()), Arc::new(UnusedConnector)),
        );
        let actual =
            response_parts(post_responses_body(app, content_type, Body::from(body)).await).await;
        assert_eq!(actual, baseline, "content_type={content_type:?}");
        assert_eq!(auth.load_count(), 0, "content_type={content_type:?}");
    }
}

#[tokio::test]
async fn request_body_limit_preserves_io_read_error_rejection() {
    let baseline = response_parts(
        post_baseline_json_body(
            Some("application/json"),
            Body::from_stream(stream::iter(vec![Err::<Bytes, _>(std::io::Error::other(
                "read failed",
            ))])),
        )
        .await,
    )
    .await;
    let auth = CountingMissingAuthProvider::new();
    let app = build_router_with_services(
        ThreadlineConfig {
            max_request_body_bytes: 1024,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::new(Arc::new(auth.clone()), Arc::new(UnusedConnector)),
    );
    let actual = response_parts(
        post_responses_body(
            app,
            Some("application/json"),
            Body::from_stream(stream::iter(vec![Err::<Bytes, _>(std::io::Error::other(
                "read failed",
            ))])),
        )
        .await,
    )
    .await;
    assert_eq!(actual, baseline);
    assert_eq!(actual.0, StatusCode::BAD_REQUEST);
    assert_ne!(actual.0, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(auth.load_count(), 0);
}

#[tokio::test]
async fn request_body_limit_accepts_large_json_above_axum_default_with_default_and_custom_limits() {
    let input = "x".repeat(2 * 1024 * 1024 + 1);
    let serialized_payload = serde_json::to_vec(&json!({"model":"gpt-5.4","input":input}))
        .expect("serialized request payload");
    let body_bytes = serialized_payload.len();
    let empty_payload_bytes = serde_json::to_vec(&json!({"model":"gpt-5.4","input":""}))
        .expect("serialized empty request payload")
        .len();
    assert_eq!(body_bytes, empty_payload_bytes + input.len());
    assert!(body_bytes > 2 * 1024 * 1024);
    assert!(body_bytes < 4 * 1024 * 1024);

    for config in [
        ThreadlineConfig::default(),
        ThreadlineConfig {
            max_request_body_bytes: 4 * 1024 * 1024,
            ..ThreadlineConfig::default()
        },
    ] {
        let app = build_router_with_services(
            config,
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );
        let response = post_responses_body(
            app,
            Some("application/json"),
            Body::from(serialized_payload.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            read_json_body(response).await["error"]["code"],
            "upstream_credentials_unavailable"
        );
    }

    let auth = CountingMissingAuthProvider::new();
    let app = build_router_with_services(
        ThreadlineConfig {
            max_request_body_bytes: body_bytes - 1,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::new(Arc::new(auth.clone()), Arc::new(UnusedConnector)),
    );
    let response = post_responses_body(
        app,
        Some("application/json"),
        Body::from(serialized_payload),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_request_body_too_large(read_json_body(response).await);
    assert_eq!(auth.load_count(), 0);
}

#[tokio::test]
async fn request_body_limit_preserves_json_semantic_validation() {
    let app = build_router_with_services(
        ThreadlineConfig {
            max_request_body_bytes: 1024,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );
    let scalar = post_responses_json(app, json!("not an object")).await;
    assert_eq!(scalar.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json_body(scalar).await["error"]["code"],
        "invalid_request_error"
    );

    let app = build_router_with_services(
        ThreadlineConfig {
            max_request_body_bytes: 1024,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );
    let array = post_responses_json(app, json!(["not an object"])).await;
    assert_eq!(array.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json_body(array).await["error"]["code"],
        "invalid_request_error"
    );
}

#[tokio::test]
async fn request_body_limit_preserves_invalid_model_validation_and_prioritizes_oversize() {
    let app = build_router_with_services(
        ThreadlineConfig {
            max_request_body_bytes: 1024,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );
    let invalid_model = post_responses_json(app, json!({"model":"not-supported"})).await;
    assert_eq!(invalid_model.status(), StatusCode::BAD_REQUEST);
    assert_invalid_model_error(&read_json_body(invalid_model).await);

    let auth = CountingMissingAuthProvider::new();
    let app = build_router_with_services(
        ThreadlineConfig {
            max_request_body_bytes: 1,
            ..ThreadlineConfig::default()
        },
        ThreadlineServices::new(Arc::new(auth.clone()), Arc::new(UnusedConnector)),
    );
    let over_malformed =
        post_responses_body(app, Some("application/json"), Body::from("{".repeat(2))).await;
    assert_eq!(over_malformed.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_request_body_too_large(read_json_body(over_malformed).await);
    assert_eq!(auth.load_count(), 0);
}

#[tokio::test]
async fn request_body_limit_enforces_default_boundary_without_auth() {
    let prefix = r#"{"model":"gpt-5.4"}"#;
    let exact_body = json_body_with_trailing_whitespace(prefix, 32 * 1024 * 1024);
    assert_eq!(exact_body.size_hint().upper(), None);
    let exact_app = build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );
    let exact = post_responses_body(exact_app, Some("application/json"), exact_body).await;
    assert_eq!(exact.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        read_json_body(exact).await["error"]["code"],
        "upstream_credentials_unavailable"
    );

    let over_body = json_body_with_trailing_whitespace(prefix, 32 * 1024 * 1024 + 1);
    assert_eq!(over_body.size_hint().upper(), None);
    let over_auth = CountingMissingAuthProvider::new();
    let over_app = build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::new(Arc::new(over_auth.clone()), Arc::new(UnusedConnector)),
    );
    let over = post_responses_body(over_app, Some("application/json"), over_body).await;
    assert_eq!(over.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        read_json_body(over).await,
        json!({
            "error": {
                "code": "request_body_too_large",
                "message": "The /v1/responses request body exceeds the configured byte limit.",
                "type": "invalid_request_error"
            }
        })
    );
    assert_eq!(over_auth.load_count(), 0);
}

#[tokio::test]
async fn request_body_limit_leaves_non_response_routes_unchanged_for_both_profiles() {
    for profile in [RouteProfile::Main, RouteProfile::Utility] {
        for uri in ["/health", "/v1/models"] {
            let baseline = response_parts(
                build_router(ThreadlineConfig {
                    profile,
                    max_request_body_bytes: 1,
                    ..ThreadlineConfig::default()
                })
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
            )
            .await;
            let oversized = response_parts(
                build_router(ThreadlineConfig {
                    profile,
                    max_request_body_bytes: 1,
                    ..ThreadlineConfig::default()
                })
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(uri)
                        .body(Body::from("oversized unused request body"))
                        .unwrap(),
                )
                .await
                .unwrap(),
            )
            .await;
            assert_eq!(baseline.0, StatusCode::OK, "profile={profile:?}, uri={uri}");
            assert_eq!(oversized, baseline, "profile={profile:?}, uri={uri}");
        }
    }
}

#[tokio::test]
async fn health_endpoint_reports_ok() {
    let app = build_router(ThreadlineConfig::default());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["status"], "ok");
    assert_eq!(payload["service"], "threadline");
}

#[tokio::test]
async fn models_endpoint_returns_supported_models() {
    let app = build_router(ThreadlineConfig::default());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let payload = read_json_body(response).await;

    assert_eq!(payload["object"], "list");
    let models = payload["data"].as_array().expect("models list");
    assert_eq!(models.len(), ADVERTISED_MAIN_MODEL_IDS.len());

    for (model, expected_id) in models.iter().zip(ADVERTISED_MAIN_MODEL_IDS) {
        assert_eq!(model["id"], expected_id);
        assert_eq!(model["object"], "model");
        assert_eq!(model["created"], 0);
        assert_eq!(model["owned_by"], "threadline");
    }
}

#[tokio::test]
async fn models_endpoint_returns_only_utility_profile_models() {
    let app = build_router(ThreadlineConfig {
        profile: RouteProfile::Utility,
        ..ThreadlineConfig::default()
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let payload = read_json_body(response).await;

    assert_eq!(payload["object"], "list");
    let models = payload["data"].as_array().expect("models list");
    assert_eq!(models.len(), ADVERTISED_UTILITY_MODEL_IDS.len());

    for (model, expected_id) in models.iter().zip(ADVERTISED_UTILITY_MODEL_IDS) {
        assert_eq!(model["id"], expected_id);
        assert_eq!(model["object"], "model");
        assert_eq!(model["created"], 0);
        assert_eq!(model["owned_by"], "threadline");
    }
}

#[tokio::test]
async fn responses_endpoint_rejects_missing_model() {
    let app = build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );

    let response = post_responses_json(app, json!({})).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload = read_json_body(response).await;
    assert_invalid_model_error(&payload);
}

#[tokio::test]
async fn responses_endpoint_rejects_non_string_model() {
    let app = build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );

    let response =
        post_responses_json(app, invalid_model_payload(json!({ "id": "gpt-5.4" }))).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload = read_json_body(response).await;
    assert_invalid_model_error(&payload);
}

#[tokio::test]
async fn responses_model_rejects_missing_non_string_unknown_and_profile_mismatch_cases() {
    let cases = [
        (ThreadlineConfig::default(), json!({})),
        (
            ThreadlineConfig::default(),
            invalid_model_payload(json!({ "id": "gpt-5.4" })),
        ),
        (
            ThreadlineConfig::default(),
            invalid_model_payload(json!("codex-mini-latest")),
        ),
        (
            ThreadlineConfig::default(),
            invalid_model_payload(json!("threadline-utility-gpt-5.4-mini")),
        ),
        (
            utility_config(),
            invalid_model_payload(json!("threadline-main-gpt-5.4")),
        ),
    ];

    for (config, payload) in cases {
        let app = build_router_with_services(
            config,
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, payload).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = read_json_body(response).await;
        assert_invalid_model_error(&body);
    }
}

#[tokio::test]
async fn responses_model_accepts_main_compatibility_ids_on_main() {
    for model_id in HIDDEN_MAIN_COMPATIBILITY_MODEL_IDS {
        let app = build_router_with_services(
            ThreadlineConfig::default(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, json!({ "model": model_id })).await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let payload = read_json_body(response).await;
        assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
        assert_eq!(payload["error"]["type"], "configuration_error");
    }
}

#[tokio::test]
async fn responses_utility_profile_rejects_new_main_visible_aliases_before_auth_loading() {
    for model_id in NEW_MAIN_VISIBLE_MODEL_IDS {
        let app = build_router_with_services(
            utility_config(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, json!({ "model": model_id })).await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_invalid_model_error(&payload);
    }
}

#[tokio::test]
async fn responses_utility_profile_rejects_new_main_compatibility_ids_before_auth_loading() {
    for model_id in NEW_MAIN_RAW_COMPATIBILITY_MODEL_IDS {
        let app = build_router_with_services(
            utility_config(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, json!({ "model": model_id })).await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_invalid_model_error(&payload);
    }
}

#[tokio::test]
async fn responses_utility_profile_rejects_astra_ids_before_auth_loading() {
    for model_id in ASTRA_MAIN_MODEL_IDS {
        let app = build_router_with_services(
            utility_config(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, json!({ "model": model_id })).await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_invalid_model_error(&payload);
    }
}

#[tokio::test]
async fn responses_endpoint_rejects_each_unsupported_model() {
    for model_id in UNSUPPORTED_MODEL_IDS {
        let app = build_router_with_services(
            ThreadlineConfig::default(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, invalid_model_payload(json!(model_id))).await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_invalid_model_error(&payload);
    }
}

#[tokio::test]
async fn responses_endpoint_rejects_unsupported_model_before_lease_acquisition() {
    for model_id in UNSUPPORTED_MODEL_IDS {
        let app = build_router(ThreadlineConfig {
            retained_session_capacity: 0,
            ..ThreadlineConfig::default()
        });

        let response = post_responses_json(
            app,
            json!({
                "model": model_id,
                "previous_response_id": "response-missing"
            }),
        )
        .await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_invalid_model_error(&payload);
    }
}

#[tokio::test]
async fn responses_endpoint_rejects_unsupported_model_before_auth_loading_and_upstream_connection()
{
    for model_id in UNSUPPORTED_MODEL_IDS {
        let app = build_router_with_services(
            ThreadlineConfig::default(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, invalid_model_payload(json!(model_id))).await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_invalid_model_error(&payload);
    }
}

#[tokio::test]
async fn responses_endpoint_rejects_reasoning_all_turns_for_unsupported_model_before_auth_or_upstream()
 {
    let app = build_router_with_services(
        utility_config(),
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );

    let response = post_responses_json(
        app,
        json!({
            "model": "threadline-utility-gpt-5.3-codex-spark",
            "input": "utility-all-turns",
            "reasoning": {
                "context": "all_turns"
            }
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload = read_json_body(response).await;
    assert_unsupported_reasoning_context_error(&payload);
}

#[tokio::test]
async fn responses_endpoint_allows_non_persistent_request_for_reasoning_all_turns_unsupported_model_to_reach_existing_auth_path()
 {
    let app = build_router_with_services(
        utility_config(),
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );

    let response = post_responses_json(
        app,
        json!({
            "model": "threadline-utility-gpt-5.3-codex-spark",
            "input": "utility-non-persistent",
            "reasoning": {
                "effort": "high"
            }
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload = read_json_body(response).await;
    assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
    assert_eq!(payload["error"]["type"], "configuration_error");
    assert_ne!(payload["error"]["code"], "unsupported_reasoning_context");
}

#[tokio::test]
async fn responses_endpoint_rejects_unsupported_reasoning_all_turns_before_retained_session_lease()
{
    let app = build_router(ThreadlineConfig {
        retained_session_capacity: 0,
        ..ThreadlineConfig::default()
    });

    let response = post_responses_json(
        app,
        json!({
            "model": "gpt-5.4",
            "input": "main-all-turns",
            "previous_response_id": "response-lease",
            "reasoning": {
                "context": "all_turns"
            }
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload = read_json_body(response).await;
    assert_unsupported_reasoning_context_error(&payload);
}

#[tokio::test]
async fn responses_endpoint_allows_raw_gpt_5_6_main_compatibility_ids_for_reasoning_all_turns_to_reach_existing_auth_path()
 {
    for model_id in NEW_MAIN_RAW_COMPATIBILITY_MODEL_IDS {
        let app = build_router_with_services(
            ThreadlineConfig::default(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(
            app,
            json!({
                "model": model_id,
                "input": "main-all-turns-next-model",
                "reasoning": {
                    "context": "all_turns"
                }
            }),
        )
        .await;

        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
        assert_eq!(payload["error"]["type"], "configuration_error");
        assert_ne!(payload["error"]["code"], "unsupported_reasoning_context");
    }
}

#[tokio::test]
async fn responses_endpoint_allows_astra_main_reasoning_all_turns_to_reach_existing_auth_path() {
    for model_id in ASTRA_MAIN_MODEL_IDS {
        let app = build_router_with_services(
            ThreadlineConfig::default(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(
            app,
            json!({
                "model": model_id,
                "input": "astra-all-turns",
                "reasoning": {
                    "context": "all_turns"
                }
            }),
        )
        .await;

        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
        assert_eq!(payload["error"]["type"], "configuration_error");
        assert_ne!(payload["error"]["code"], "unsupported_reasoning_context");
    }
}

#[tokio::test]
async fn responses_endpoint_rejects_legacy_raw_main_compatibility_ids_for_reasoning_all_turns_before_auth_or_upstream()
 {
    for model_id in ["gpt-5.5", "gpt-5.4"] {
        let app = build_router_with_services(
            ThreadlineConfig::default(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(
            app,
            json!({
                "model": model_id,
                "input": "main-all-turns-legacy-model",
                "reasoning": {
                    "context": "all_turns"
                }
            }),
        )
        .await;

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_unsupported_reasoning_context_error(&payload);
    }
}

#[tokio::test]
async fn responses_endpoint_accepts_each_supported_model_before_missing_auth_error() {
    for model_id in ACCEPTED_MAIN_MODEL_IDS {
        let app = build_router_with_services(
            ThreadlineConfig::default(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, json!({ "model": model_id })).await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let payload = read_json_body(response).await;
        assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
        assert_eq!(payload["error"]["type"], "configuration_error");
    }
}

#[tokio::test]
async fn responses_utility_profile_accepts_each_advertised_model_before_missing_auth_error() {
    for model_id in ADVERTISED_UTILITY_MODEL_IDS {
        let app = build_router_with_services(
            utility_config(),
            ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
        );

        let response = post_responses_json(app, json!({ "model": model_id })).await;

        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "model_id={model_id}"
        );

        let payload = read_json_body(response).await;
        assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
        assert_eq!(payload["error"]["type"], "configuration_error");
    }
}

#[tokio::test]
async fn responses_endpoint_reports_configuration_error_for_allowed_model_when_upstream_credentials_are_unavailable()
 {
    let app = build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );

    let response = post_responses_json(app, json!({ "model": "gpt-5.4" })).await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload = read_json_body(response).await;

    assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
    assert_eq!(payload["error"]["type"], "configuration_error");
    assert_eq!(
        payload["error"]["message"],
        "Threadline could not load upstream credentials."
    );
}
