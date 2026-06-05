use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;

use crate::config::ThreadlineConfig;
use crate::errors::ThreadlineError;

const MODEL_CREATED_UNSPECIFIED: u64 = 0;

#[derive(Clone)]
struct AppState {
    config: ThreadlineConfig,
}

#[derive(Serialize)]
struct HealthPayload {
    status: &'static str,
    service: &'static str,
}

#[derive(Serialize)]
struct ModelListPayload {
    object: &'static str,
    data: Vec<ModelEntry>,
}

#[derive(Serialize)]
struct ModelEntry {
    id: String,
    object: &'static str,
    created: u64,
    owned_by: &'static str,
}

pub fn build_router(config: ThreadlineConfig) -> Router {
    let state = AppState { config };

    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(models))
        .route("/v1/responses", post(responses_placeholder))
        .with_state(state)
}

async fn health() -> Json<HealthPayload> {
    Json(HealthPayload {
        status: "ok",
        service: "threadline",
    })
}

async fn models(State(state): State<AppState>) -> Json<ModelListPayload> {
    Json(ModelListPayload {
        object: "list",
        data: vec![ModelEntry {
            id: state.config.model_id,
            object: "model",
            created: MODEL_CREATED_UNSPECIFIED,
            owned_by: "threadline",
        }],
    })
}

async fn responses_placeholder() -> Result<Json<serde_json::Value>, ThreadlineError> {
    Err(ThreadlineError::ResponsesNotReady)
}
