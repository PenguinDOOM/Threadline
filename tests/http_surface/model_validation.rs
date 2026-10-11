use super::*;

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
        post_responses_json(app, invalid_model_payload(json!({ "id": "gpt-6-sol" }))).await;

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
            invalid_model_payload(json!({ "id": "gpt-6-sol" })),
        ),
        (
            ThreadlineConfig::default(),
            invalid_model_payload(json!("codex-mini-latest")),
        ),
        (
            ThreadlineConfig::default(),
            invalid_model_payload(json!("threadline-utility-gpt-6-luna")),
        ),
        (
            utility_config(),
            invalid_model_payload(json!("threadline-main-gpt-6-sol")),
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
    for profile in [RouteProfile::Main, RouteProfile::Utility] {
        for model_id in RETIRED_MODEL_IDS {
            for reasoning in [None, Some(json!({ "context": "all_turns" }))] {
                let config = ThreadlineConfig {
                    profile,
                    ..ThreadlineConfig::default()
                };
                let app = build_router_with_services(
                    config,
                    ThreadlineServices::new(
                        Arc::new(MissingAuthProvider),
                        Arc::new(UnusedConnector),
                    ),
                );
                let mut request = json!({ "model": model_id });
                if let Some(reasoning) = reasoning {
                    request["reasoning"] = reasoning;
                }

                let response = post_responses_json(app, request).await;

                assert_eq!(
                    response.status(),
                    StatusCode::BAD_REQUEST,
                    "profile={profile:?}, model_id={model_id}"
                );

                let payload = read_json_body(response).await;
                assert_invalid_model_error(&payload);
            }
        }
    }
}

#[tokio::test]
async fn responses_endpoint_rejects_retired_model_before_retained_session_lease() {
    for model_id in RETIRED_MODEL_IDS {
        for reasoning in [None, Some(json!({ "context": "all_turns" }))] {
            let app = build_router(ThreadlineConfig {
                retained_session_capacity: 0,
                ..ThreadlineConfig::default()
            });
            let mut request = json!({
                "model": model_id,
                "previous_response_id": "response-lease"
            });
            if let Some(reasoning) = reasoning {
                request["reasoning"] = reasoning;
            }
            let response = post_responses_json(app, request).await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "model_id={model_id}"
            );
            assert_invalid_model_error(&read_json_body(response).await);
        }
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

    let response = post_responses_json(app, json!({ "model": "gpt-6-sol" })).await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload = read_json_body(response).await;

    assert_eq!(payload["error"]["code"], "upstream_credentials_unavailable");
    assert_eq!(payload["error"]["type"], "configuration_error");
    assert_eq!(
        payload["error"]["message"],
        "Threadline could not load upstream credentials."
    );
}
