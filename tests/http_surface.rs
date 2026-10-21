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

#[derive(Clone)]
struct TimeoutConnector {
    attempts: Arc<AtomicUsize>,
}

impl UpstreamConnector for TimeoutConnector {
    fn connect(
        &self,
        _auth: LoadedUpstreamAuth,
        _session: Option<UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ThreadlineError::UpstreamWebSocketConnectTimeout) })
    }
}

#[derive(Clone)]
struct AvailableAuthProvider;

impl UpstreamAuthProvider for AvailableAuthProvider {
    fn load(&self) -> Result<LoadedUpstreamAuth, ThreadlineError> {
        Ok(LoadedUpstreamAuth {
            bearer_token: "test-token".to_string(),
            source: threadline::auth::AuthSource::CodexHomeAuth,
            refresh_boundary: threadline::auth::RefreshBoundary::NotAvailable,
        })
    }
}

const CURRENT_MAIN_VISIBLE_MODEL_IDS: [&str; 4] = [
    "threadline-main-gpt-6.1-sol",
    "threadline-main-gpt-6-astra",
    "threadline-main-gpt-6-sol",
    "threadline-main-gpt-6-luna",
];

const CURRENT_MAIN_RAW_MODEL_IDS: [&str; 4] =
    ["gpt-6.1-sol", "gpt-6-astra", "gpt-6-sol", "gpt-6-luna"];

const ASTRA_MAIN_MODEL_IDS: [&str; 2] = ["threadline-main-gpt-6-astra", "gpt-6-astra"];

const ADVERTISED_MAIN_MODEL_IDS: [&str; 4] = [
    "threadline-main-gpt-6.1-sol",
    "threadline-main-gpt-6-astra",
    "threadline-main-gpt-6-sol",
    "threadline-main-gpt-6-luna",
];

const ADVERTISED_UTILITY_MODEL_IDS: [&str; 1] = ["threadline-utility-gpt-6-luna"];

const ACCEPTED_MAIN_MODEL_IDS: [&str; 8] = [
    "threadline-main-gpt-6.1-sol",
    "threadline-main-gpt-6-astra",
    "threadline-main-gpt-6-sol",
    "threadline-main-gpt-6-luna",
    "gpt-6.1-sol",
    "gpt-6-astra",
    "gpt-6-sol",
    "gpt-6-luna",
];

const RETIRED_MODEL_IDS: [&str; 15] = [
    "threadline-main-gpt-5.6-sol",
    "threadline-main-gpt-5.6-terra",
    "threadline-main-gpt-5.6-luna",
    "threadline-main-gpt-5.5",
    "threadline-main-gpt-5.4",
    "threadline-utility-gpt-5.6-luna",
    "threadline-utility-gpt-5.4-mini",
    "threadline-utility-gpt-5.3-codex-spark",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.3-codex-spark",
];

const UNSUPPORTED_MODEL_IDS: [&str; 5] = [
    "threadline-utility-gpt-6-luna",
    "threadline-main-gpt-6-terra",
    "gpt-6-terra",
    "codex-mini-latest",
    "threadline-test-unsupported",
];

const HIDDEN_MAIN_COMPATIBILITY_MODEL_IDS: [&str; 4] =
    ["gpt-6.1-sol", "gpt-6-astra", "gpt-6-sol", "gpt-6-luna"];

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

fn utility_config() -> ThreadlineConfig {
    ThreadlineConfig {
        profile: RouteProfile::Utility,
        ..ThreadlineConfig::default()
    }
}

#[path = "http_surface/body_limits.rs"]
mod body_limits;

#[path = "http_surface/body_validation.rs"]
mod body_validation;

#[path = "http_surface/catalog.rs"]
mod catalog;

#[path = "http_surface/model_validation.rs"]
mod model_validation;

#[path = "http_surface/profiles.rs"]
mod profiles;
