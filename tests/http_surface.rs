use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use futures_util::future::BoxFuture;
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

use threadline::auth::LoadedUpstreamAuth;
use threadline::codex_ws::UpstreamSessionDescriptor;
use threadline::config::ThreadlineConfig;
use threadline::errors::ThreadlineError;
use threadline::http::build_router;
use threadline::http::build_router_with_services;
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
async fn responses_endpoint_reports_configuration_error_when_upstream_credentials_are_unavailable()
{
    let app = build_router_with_services(
        ThreadlineConfig::default(),
        ThreadlineServices::new(Arc::new(MissingAuthProvider), Arc::new(UnusedConnector)),
    );

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

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
    assert_eq!(payload["error"]["type"], "configuration_error");
    assert_eq!(
        payload["error"]["message"],
        "Threadline could not load upstream credentials."
    );
}
