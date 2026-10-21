use super::*;

#[tokio::test]
async fn responses_utility_profile_rejects_new_main_visible_aliases_before_auth_loading() {
    for model_id in CURRENT_MAIN_VISIBLE_MODEL_IDS {
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
    for model_id in CURRENT_MAIN_RAW_MODEL_IDS {
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
async fn responses_endpoint_allows_reasoning_capable_raw_main_compatibility_ids_for_reasoning_all_turns_to_reach_existing_auth_path()
 {
    for model_id in CURRENT_MAIN_RAW_MODEL_IDS {
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
async fn responses_endpoint_maps_connect_timeout_to_fixed_server_error_for_main_and_utility() {
    for (profile, model) in [
        (RouteProfile::Main, "gpt-6-sol"),
        (RouteProfile::Utility, "threadline-utility-gpt-6-luna"),
    ] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let app = build_router_with_services(
            ThreadlineConfig {
                profile,
                retained_session_capacity: 1,
                ..ThreadlineConfig::default()
            },
            ThreadlineServices::new(
                Arc::new(AvailableAuthProvider),
                Arc::new(TimeoutConnector {
                    attempts: attempts.clone(),
                }),
            ),
        );
        let second_app = app.clone();
        let response = post_responses_json(app, json!({ "model": model })).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            read_json_body(response).await,
            json!({
                "error": {
                    "code": "upstream_websocket_connect_timeout",
                    "message": "The upstream websocket connection timed out.",
                    "type": "server_error"
                }
            })
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let second_response = post_responses_json(second_app, json!({ "model": model })).await;
        assert_eq!(second_response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
