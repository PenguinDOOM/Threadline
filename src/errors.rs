use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Serialize)]
pub struct PublicErrorDocument {
    pub error: PublicErrorPayload,
}

#[derive(Debug, Serialize)]
pub struct PublicErrorPayload {
    pub code: &'static str,
    pub message: &'static str,
    #[serde(rename = "type")]
    pub error_type: &'static str,
}

#[derive(Debug, Error)]
pub enum ThreadlineError {
    #[error("The /v1/responses bridge is not available yet.")]
    ResponsesNotReady,

    #[error("Invalid bind host: {0}")]
    InvalidBindHost(String),
}

impl ThreadlineError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::ResponsesNotReady => StatusCode::NOT_IMPLEMENTED,
            Self::InvalidBindHost(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn public_error(&self) -> PublicErrorPayload {
        match self {
            Self::ResponsesNotReady => PublicErrorPayload {
                code: "responses_not_ready",
                message: "The /v1/responses bridge is not available yet.",
                error_type: "not_implemented_error",
            },
            Self::InvalidBindHost(_) => PublicErrorPayload {
                code: "configuration_error",
                message: "Threadline failed to resolve its configured bind address.",
                error_type: "configuration_error",
            },
        }
    }
}

impl IntoResponse for ThreadlineError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let payload = PublicErrorDocument {
            error: self.public_error(),
        };

        (status, Json(payload)).into_response()
    }
}
