use super::*;

#[tokio::test]
async fn request_body_limit_accepts_exact_utf8_bytes_and_rejects_one_byte_over() {
    for (profile, model) in [
        (RouteProfile::Main, "gpt-6-sol"),
        (RouteProfile::Utility, "threadline-utility-gpt-6-luna"),
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
    let body = br#"{"model":"gpt-6-sol"}"#;
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
async fn request_body_limit_accepts_large_json_above_axum_default_with_default_and_custom_limits() {
    let input = "x".repeat(2 * 1024 * 1024 + 1);
    let serialized_payload = serde_json::to_vec(&json!({"model":"gpt-6-sol","input":input}))
        .expect("serialized request payload");
    let body_bytes = serialized_payload.len();
    let empty_payload_bytes = serde_json::to_vec(&json!({"model":"gpt-6-sol","input":""}))
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

    assert_large_body_rejected_before_auth(serialized_payload, body_bytes).await;
}

#[tokio::test]
async fn request_body_limit_enforces_default_boundary_without_auth() {
    let prefix = r#"{"model":"gpt-6-sol"}"#;
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

async fn assert_large_body_rejected_before_auth(serialized_payload: Vec<u8>, body_bytes: usize) {
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
