use super::*;

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
