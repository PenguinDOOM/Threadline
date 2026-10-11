use super::*;

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
