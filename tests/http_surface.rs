use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use threadline::config::ThreadlineConfig;
use threadline::http::build_router;

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
async fn models_endpoint_returns_configured_model() {
    let config = ThreadlineConfig {
        model_id: "codex-threadline-preview".to_string(),
        ..ThreadlineConfig::default()
    };
    let app = build_router(config);

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

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["object"], "list");
    assert_eq!(payload["data"][0]["id"], "codex-threadline-preview");
    assert_eq!(payload["data"][0]["created"], 0);
    assert_eq!(payload["data"][0]["owned_by"], "threadline");
}

#[tokio::test]
async fn responses_endpoint_returns_stable_placeholder_error() {
    let app = build_router(ThreadlineConfig::default());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"model":"ignored"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["error"]["code"], "responses_not_ready");
    assert_eq!(payload["error"]["type"], "not_implemented_error");
    assert_eq!(
        payload["error"]["message"],
        "The /v1/responses bridge is not available yet."
    );
}
