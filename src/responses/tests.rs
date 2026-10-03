use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{
    ResponsesRouteState, continuation_terminal_error, normalize_persistent_reasoning_context,
    responses_handler, rewrite_stale_continuation_first_send_error,
    thread_id_from_prompt_cache_key,
};
use crate::auth::{AuthSource, LoadedUpstreamAuth, RefreshBoundary};
use crate::codex_ws::UpstreamSessionDescriptor;
use crate::errors::ThreadlineError;
use crate::models::{ModelAlias, RouteProfile, resolve_request_model_for_profile};
use crate::registry::{RegistryAcquireError, RetainedSessionRegistry};
use crate::responses::{
    ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
};
use crate::ws_pump::{
    InboundBufferOverflow, InboundBufferOverflowCause, LiveUpstreamWebSocket, UpstreamTerminalState,
};
use axum::Json;
use axum::body::to_bytes;
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::future::BoxFuture;
use serde_json::json;

#[derive(Clone)]
struct StaticAuthProvider;

impl UpstreamAuthProvider for StaticAuthProvider {
    fn load(&self) -> Result<LoadedUpstreamAuth, ThreadlineError> {
        Ok(LoadedUpstreamAuth {
            bearer_token: "test-token".to_string(),
            source: AuthSource::CodexKeyring,
            refresh_boundary: RefreshBoundary::NotAvailable,
        })
    }
}

#[derive(Clone)]
struct CountingConnector {
    calls: Arc<AtomicUsize>,
}

impl UpstreamConnector for CountingConnector {
    fn connect(
        &self,
        _auth: LoadedUpstreamAuth,
        _session: Option<UpstreamSessionDescriptor>,
    ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(ThreadlineError::UpstreamWebSocketConnectFailed)
        })
    }
}

async fn assert_previous_response_not_found(response: axum::response::Response) {
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("stale continuation error body");
    let payload: serde_json::Value =
        serde_json::from_slice(&body).expect("stale continuation error json");
    assert_eq!(payload["error"]["code"], "previous_response_not_found");
}

async fn assert_markers_removed(registry: &RetainedSessionRegistry, markers: &[&str]) {
    for marker in markers {
        assert_eq!(
            registry
                .acquire_previous(marker)
                .await
                .expect_err("armed first-send failure must remove every alias"),
            RegistryAcquireError::PreviousResponseNotFound,
            "marker {marker}"
        );
    }
}

async fn seed_retained_aliases_for_first_send_timeout() -> (
    Arc<RetainedSessionRegistry>,
    Arc<LiveUpstreamWebSocket>,
    Arc<AtomicUsize>,
) {
    let registry = Arc::new(RetainedSessionRegistry::new(1));
    let send_attempts = Arc::new(AtomicUsize::new(0));
    let upstream = Arc::new(
        LiveUpstreamWebSocket::test_liveness_timeout_after_first_text_send(Arc::clone(
            &send_attempts,
        )),
    );
    assert!(matches!(
        upstream.terminal_state(),
        UpstreamTerminalState::Open
    ));

    let mut lease = registry.acquire_new().await.expect("seed retained session");
    lease.replace_upstream(Some(Arc::clone(&upstream))).await;
    lease.record_completed_marker("response-accepted").await;
    lease.record_completed_marker("response-alias").await;
    lease.release();
    (registry, upstream, send_attempts)
}

fn persistent_reasoning_alias() -> &'static ModelAlias {
    resolve_request_model_for_profile(
        json!({ "model": "threadline-main-gpt-6-sol" })
            .as_object()
            .expect("model payload"),
        RouteProfile::Main,
    )
    .expect("persistent reasoning alias")
}

fn utility_reasoning_alias() -> &'static ModelAlias {
    resolve_request_model_for_profile(
        json!({ "model": "threadline-utility-gpt-6-luna" })
            .as_object()
            .expect("model payload"),
        RouteProfile::Utility,
    )
    .expect("utility reasoning alias")
}

#[test]
fn prompt_cache_key_exposes_vscode_conversation_as_thread_identity() {
    let payload = json!({
        "prompt_cache_key": "48a65359-981b-47c2-9612-e1c64ae07e22:gpt-6-sol"
    });

    assert_eq!(
        thread_id_from_prompt_cache_key(payload.as_object().expect("payload object")).as_deref(),
        Some("48a65359-981b-47c2-9612-e1c64ae07e22")
    );
}

#[test]
fn invalid_prompt_cache_key_does_not_supply_thread_identity() {
    for payload in [
        json!({}),
        json!({ "prompt_cache_key": "not-a-uuid:gpt-6-sol" }),
        json!({ "prompt_cache_key": "48a65359-981b-47c2-9612-e1c64ae07e22" }),
        json!({ "prompt_cache_key": 12 }),
    ] {
        assert!(
            thread_id_from_prompt_cache_key(payload.as_object().expect("payload object")).is_none()
        );
    }
}

#[test]
fn persistent_reasoning_normalization_inserts_context_for_missing_or_null_reasoning() {
    for mut payload in [json!({}), json!({ "reasoning": null })] {
        let applied = normalize_persistent_reasoning_context(
            payload.as_object_mut().expect("request object"),
            true,
            persistent_reasoning_alias(),
        );

        assert!(applied);
        assert_eq!(payload["reasoning"], json!({ "context": "all_turns" }));
    }
}

#[test]
fn persistent_reasoning_normalization_preserves_existing_reasoning_fields_and_context() {
    let mut missing_context = json!({
        "reasoning": {
            "effort": "high",
            "summary": "detailed"
        }
    });
    let applied = normalize_persistent_reasoning_context(
        missing_context.as_object_mut().expect("request object"),
        true,
        persistent_reasoning_alias(),
    );
    assert!(applied);
    assert_eq!(
        missing_context["reasoning"],
        json!({
            "context": "all_turns",
            "effort": "high",
            "summary": "detailed"
        })
    );

    for mut explicit_context in [
        json!({ "reasoning": { "context": "client_context" } }),
        json!({ "reasoning": { "context": 12 } }),
    ] {
        let original = explicit_context.clone();
        let applied = normalize_persistent_reasoning_context(
            explicit_context.as_object_mut().expect("request object"),
            true,
            persistent_reasoning_alias(),
        );

        assert!(!applied);
        assert_eq!(explicit_context, original);
    }
}

#[test]
fn persistent_reasoning_normalization_is_noop_when_disabled_for_main_or_utility_and_non_object() {
    for (enabled, alias, mut payload) in [
        (false, persistent_reasoning_alias(), json!({})),
        (true, utility_reasoning_alias(), json!({})),
        (
            true,
            persistent_reasoning_alias(),
            json!({ "reasoning": "manual" }),
        ),
    ] {
        let original = payload.clone();
        let applied = normalize_persistent_reasoning_context(
            payload.as_object_mut().expect("request object"),
            enabled,
            alias,
        );

        assert!(!applied);
        assert_eq!(payload, original);
    }
}

#[test]
fn stale_continuation_first_send_rewrites_closed_upstream_to_previous_response_not_found() {
    let rewritten =
        rewrite_stale_continuation_first_send_error(ThreadlineError::UpstreamWebSocketClosed);

    assert!(matches!(
        rewritten,
        ThreadlineError::PreviousResponseNotFound
    ));
}

#[test]
fn stale_continuation_first_send_rewrites_liveness_timeout_to_previous_response_not_found() {
    let rewritten =
        rewrite_stale_continuation_first_send_error(ThreadlineError::UpstreamLivenessTimeout);

    assert!(matches!(
        rewritten,
        ThreadlineError::PreviousResponseNotFound
    ));
}

#[tokio::test]
async fn retained_continuation_first_send_liveness_timeout_invalidates_all_aliases_without_reconnect()
 {
    let (registry, upstream, send_attempts) = seed_retained_aliases_for_first_send_timeout().await;

    let connector_calls = Arc::new(AtomicUsize::new(0));
    let state = ResponsesRouteState {
        profile: RouteProfile::Main,
        persistent_reasoning_enabled: false,
        registry: Arc::clone(&registry),
        services: ThreadlineServices::new(
            Arc::new(StaticAuthProvider),
            Arc::new(CountingConnector {
                calls: Arc::clone(&connector_calls),
            }),
        ),
    };

    let response = match responses_handler(
        State(state),
        Json(json!({
            "model": "gpt-6-sol",
            "input": "continue",
            "previous_response_id": "response-accepted"
        })),
        Default::default(),
    )
    .await
    {
        Ok(_) => panic!("first-send liveness timeout must not begin an SSE response"),
        Err(error) => error.into_response(),
    };
    assert_previous_response_not_found(response).await;
    assert_eq!(send_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(connector_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        upstream.terminal_state(),
        UpstreamTerminalState::LivenessTimeout(_)
    ));

    assert_markers_removed(&registry, &["response-accepted", "response-alias"]).await;
}

#[test]
fn stale_continuation_first_send_preserves_inbound_overflow_error() {
    let preserved =
        rewrite_stale_continuation_first_send_error(ThreadlineError::UpstreamInboundBufferOverflow);

    assert!(matches!(
        preserved,
        ThreadlineError::UpstreamInboundBufferOverflow
    ));
}

#[test]
fn stale_continuation_first_send_preserves_non_transport_errors() {
    let preserved =
        rewrite_stale_continuation_first_send_error(ThreadlineError::InvalidResponsesRequest);

    assert!(matches!(
        preserved,
        ThreadlineError::InvalidResponsesRequest
    ));
}

#[test]
fn continuation_terminal_snapshot_preserves_overflow_over_closed_stale_mapping() {
    let overflow = UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
        cause: InboundBufferOverflowCause::MessageCount,
        queued_messages: 1,
        queued_bytes: 1,
        incoming_bytes: 1,
        max_messages: 1,
        max_bytes: 1,
    });

    assert!(matches!(
        continuation_terminal_error(overflow),
        Some(ThreadlineError::UpstreamInboundBufferOverflow)
    ));
}
