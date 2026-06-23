use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;
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

const ADVERTISED_MAIN_MODEL_IDS: [&str; 2] = ["threadline-main-gpt-5.5", "threadline-main-gpt-5.4"];

const ADVERTISED_UTILITY_MODEL_IDS: [&str; 2] = [
    "threadline-utility-gpt-5.4-mini",
    "threadline-utility-gpt-5.3-codex-spark",
];

const ACCEPTED_MAIN_MODEL_IDS: [&str; 6] = [
    "threadline-main-gpt-5.5",
    "threadline-main-gpt-5.4",
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.3-codex-spark",
];

const UNSUPPORTED_MODEL_IDS: [&str; 4] = [
    "threadline-utility-gpt-5.4-mini",
    "threadline-utility-gpt-5.3-codex-spark",
    "codex-mini-latest",
    "threadline-test-unsupported",
];

const HIDDEN_MAIN_COMPATIBILITY_MODEL_IDS: [&str; 4] =
    ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex-spark"];

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
