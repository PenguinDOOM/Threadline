use std::borrow::Cow;

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
    pub code: Cow<'static, str>,
    pub message: Cow<'static, str>,
    #[serde(rename = "type")]
    pub error_type: Cow<'static, str>,
}

#[derive(Debug, Error)]
pub enum ThreadlineError {
    #[error("The /v1/responses bridge is not available yet.")]
    ResponsesNotReady,

    #[error("The /v1/responses request body was not a valid JSON object.")]
    InvalidResponsesRequest,

    #[error("The /v1/responses request must include a supported string model.")]
    InvalidModel,

    #[error(
        "reasoning.context=all_turns is not supported for this model. The model metadata has use_responses_lite=false."
    )]
    UnsupportedReasoningContext,

    #[error(
        "Threadline could not find the retained session for the supplied previous_response_id."
    )]
    PreviousResponseNotFound,

    #[error("The retained session for this previous_response_id is already in use.")]
    RetainedSessionConflict,

    #[error("Threadline has no free retained session capacity for another active response.")]
    RetainedSessionCapacityExceeded,

    #[error("Threadline could not connect to the upstream Codex websocket.")]
    UpstreamWebSocketConnectFailed,

    #[error("The upstream Codex websocket handshake was rejected with HTTP {status}.")]
    UpstreamWebSocketHandshakeRejected { status: StatusCode },

    #[error(
        "The upstream Codex websocket closed before Threadline finished streaming the response."
    )]
    UpstreamWebSocketClosed,

    #[error(
        "The upstream response.failed event cannot be streamed as a successful downstream response."
    )]
    UpstreamResponseFailed,

    #[error("The upstream websocket emitted an error event.")]
    UpstreamErrorEvent,

    #[error("The upstream websocket emitted malformed JSON.")]
    UpstreamInvalidJson,

    #[error("Threadline failed while executing an internal tool.")]
    InternalToolFailed,

    #[error("Threadline could not find a job with that job_id.")]
    JobNotFound,

    #[error("Threadline jobs are disabled.")]
    JobsDisabled,

    #[error("The requested job command is not allowed by Threadline policy.")]
    JobCommandNotAllowed,

    #[error("The Threadline job command failed.")]
    JobCommandFailed,

    #[error("The Threadline job was cancelled.")]
    JobCancelled,

    #[error("Threadline could not load upstream credentials.")]
    UpstreamCredentialsUnavailable,

    #[error("Threadline is missing THREADLINE_UPSTREAM_URL for upstream websocket connections.")]
    UpstreamUrlMissing,

    #[error("{0}")]
    InvalidServerConfiguration(String),

    #[error("Invalid bind host: {0}")]
    InvalidBindHost(String),
}

impl ThreadlineError {
    pub fn is_upstream_recoverable_close(&self) -> bool {
        matches!(
            self,
            Self::UpstreamWebSocketClosed | Self::UpstreamWebSocketConnectFailed
        )
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::ResponsesNotReady => StatusCode::NOT_IMPLEMENTED,
            Self::InvalidResponsesRequest => StatusCode::BAD_REQUEST,
            Self::InvalidModel => StatusCode::BAD_REQUEST,
            Self::UnsupportedReasoningContext => StatusCode::BAD_REQUEST,
            Self::PreviousResponseNotFound => StatusCode::BAD_REQUEST,
            Self::RetainedSessionConflict => StatusCode::CONFLICT,
            Self::RetainedSessionCapacityExceeded => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamWebSocketConnectFailed => StatusCode::BAD_GATEWAY,
            Self::UpstreamWebSocketHandshakeRejected { status } => *status,
            Self::UpstreamWebSocketClosed => StatusCode::BAD_GATEWAY,
            Self::UpstreamResponseFailed => StatusCode::BAD_GATEWAY,
            Self::UpstreamErrorEvent => StatusCode::BAD_GATEWAY,
            Self::UpstreamInvalidJson => StatusCode::BAD_GATEWAY,
            Self::InternalToolFailed => StatusCode::INTERNAL_SERVER_ERROR,
            Self::JobNotFound => StatusCode::NOT_FOUND,
            Self::JobsDisabled => StatusCode::FORBIDDEN,
            Self::JobCommandNotAllowed => StatusCode::FORBIDDEN,
            Self::JobCommandFailed => StatusCode::INTERNAL_SERVER_ERROR,
            Self::JobCancelled => StatusCode::CONFLICT,
            Self::UpstreamCredentialsUnavailable => StatusCode::INTERNAL_SERVER_ERROR,
            Self::UpstreamUrlMissing => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidServerConfiguration(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidBindHost(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn public_error(&self) -> PublicErrorPayload {
        match self {
            Self::ResponsesNotReady => borrowed_public_error(
                "responses_not_ready",
                "The /v1/responses bridge is not available yet.",
                "not_implemented_error",
            ),
            Self::InvalidResponsesRequest => borrowed_public_error(
                "invalid_request_error",
                "The /v1/responses request body must be a JSON object.",
                "invalid_request_error",
            ),
            Self::InvalidModel => borrowed_public_error(
                "invalid_model",
                "The /v1/responses request must include a supported string model.",
                "invalid_request_error",
            ),
            Self::UnsupportedReasoningContext => borrowed_public_error(
                "unsupported_reasoning_context",
                "reasoning.context=all_turns is not supported for this model. The model metadata has use_responses_lite=false.",
                "invalid_request_error",
            ),
            Self::PreviousResponseNotFound => borrowed_public_error(
                "previous_response_not_found",
                "Threadline could not find the retained session for that previous_response_id.",
                "invalid_request_error",
            ),
            Self::RetainedSessionConflict => borrowed_public_error(
                "retained_session_conflict",
                "The retained session for that previous_response_id is already active.",
                "conflict_error",
            ),
            Self::RetainedSessionCapacityExceeded => borrowed_public_error(
                "retained_session_capacity_exceeded",
                "Threadline has no free retained session capacity for another active response.",
                "service_unavailable_error",
            ),
            Self::UpstreamWebSocketConnectFailed => borrowed_public_error(
                "upstream_websocket_connect_failed",
                "Threadline could not connect to the upstream Codex websocket.",
                "bad_gateway_error",
            ),
            Self::UpstreamWebSocketHandshakeRejected { status } => PublicErrorPayload {
                code: Cow::Borrowed("upstream_websocket_handshake_rejected"),
                message: Cow::Owned(format_upstream_websocket_handshake_rejected_message(
                    *status,
                )),
                error_type: Cow::Borrowed("bad_gateway_error"),
            },
            Self::UpstreamWebSocketClosed => borrowed_public_error(
                "upstream_websocket_closed",
                "The upstream Codex websocket closed before Threadline finished streaming the response.",
                "bad_gateway_error",
            ),
            Self::UpstreamResponseFailed => borrowed_public_error(
                "upstream_response_failed",
                "The upstream response.failed event cannot be streamed as a successful downstream response.",
                "bad_gateway_error",
            ),
            Self::UpstreamErrorEvent => borrowed_public_error(
                "upstream_error_event",
                "The upstream websocket emitted an error event.",
                "bad_gateway_error",
            ),
            Self::UpstreamInvalidJson => borrowed_public_error(
                "upstream_invalid_json",
                "The upstream websocket emitted malformed JSON.",
                "bad_gateway_error",
            ),
            Self::InternalToolFailed => borrowed_public_error(
                "internal_tool_failed",
                "Threadline failed while executing an internal tool.",
                "internal_server_error",
            ),
            Self::JobNotFound => borrowed_public_error(
                "job_not_found",
                "Threadline could not find a job with that job_id.",
                "invalid_request_error",
            ),
            Self::JobsDisabled => borrowed_public_error(
                "jobs_disabled",
                "Threadline jobs are disabled.",
                "forbidden_error",
            ),
            Self::JobCommandNotAllowed => borrowed_public_error(
                "job_command_not_allowed",
                "The requested job command is not allowed by Threadline policy.",
                "forbidden_error",
            ),
            Self::JobCommandFailed => borrowed_public_error(
                "job_command_failed",
                "The Threadline job command failed.",
                "internal_server_error",
            ),
            Self::JobCancelled => borrowed_public_error(
                "job_cancelled",
                "The Threadline job was cancelled.",
                "conflict_error",
            ),
            Self::UpstreamCredentialsUnavailable => borrowed_public_error(
                "upstream_credentials_unavailable",
                "Threadline could not load upstream credentials.",
                "configuration_error",
            ),
            Self::UpstreamUrlMissing => borrowed_public_error(
                "configuration_error",
                "Threadline is missing THREADLINE_UPSTREAM_URL for upstream websocket connections.",
                "configuration_error",
            ),
            Self::InvalidServerConfiguration(message) => PublicErrorPayload {
                code: Cow::Borrowed("configuration_error"),
                message: Cow::Owned(message.clone()),
                error_type: Cow::Borrowed("configuration_error"),
            },
            Self::InvalidBindHost(_) => borrowed_public_error(
                "configuration_error",
                "Threadline failed to resolve its configured bind address.",
                "configuration_error",
            ),
        }
    }

    pub fn public_error_document(&self) -> PublicErrorDocument {
        PublicErrorDocument {
            error: self.public_error(),
        }
    }
}

fn borrowed_public_error(
    code: &'static str,
    message: &'static str,
    error_type: &'static str,
) -> PublicErrorPayload {
    PublicErrorPayload {
        code: Cow::Borrowed(code),
        message: Cow::Borrowed(message),
        error_type: Cow::Borrowed(error_type),
    }
}

fn format_upstream_websocket_handshake_rejected_message(status: StatusCode) -> String {
    match status.canonical_reason() {
        Some(reason) => format!(
            "The upstream Codex websocket handshake was rejected with HTTP {} {}.",
            status.as_u16(),
            reason
        ),
        None => format!(
            "The upstream Codex websocket handshake was rejected with HTTP {}.",
            status.as_u16()
        ),
    }
}

impl IntoResponse for ThreadlineError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let payload = self.public_error_document();

        (status, Json(payload)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_websocket_handshake_rejected_uses_upstream_status() {
        let error = ThreadlineError::UpstreamWebSocketHandshakeRejected {
            status: StatusCode::FORBIDDEN,
        };

        assert_eq!(error.status_code(), StatusCode::FORBIDDEN);

        let document = error.public_error_document();

        assert_eq!(
            document.error.code.as_ref(),
            "upstream_websocket_handshake_rejected"
        );
        assert_eq!(
            document.error.message.as_ref(),
            "The upstream Codex websocket handshake was rejected with HTTP 403 Forbidden."
        );
        assert_eq!(document.error.error_type.as_ref(), "bad_gateway_error");
    }

    #[test]
    fn upstream_websocket_handshake_rejected_propagates_exact_server_error_status() {
        let error = ThreadlineError::UpstreamWebSocketHandshakeRejected {
            status: StatusCode::SERVICE_UNAVAILABLE,
        };

        assert_eq!(error.status_code(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            error.public_error_document().error.message.as_ref(),
            "The upstream Codex websocket handshake was rejected with HTTP 503 Service Unavailable."
        );
    }

    #[test]
    fn invalid_server_configuration_maps_to_configuration_error_with_original_message() {
        let error = ThreadlineError::InvalidServerConfiguration(
            "--utility-port must differ from --port".to_string(),
        );

        assert_eq!(error.status_code(), StatusCode::INTERNAL_SERVER_ERROR);

        let document = error.public_error_document();

        assert_eq!(document.error.code.as_ref(), "configuration_error");
        assert_eq!(
            document.error.message.as_ref(),
            "--utility-port must differ from --port"
        );
        assert_eq!(document.error.error_type.as_ref(), "configuration_error");
    }

    #[test]
    fn unsupported_reasoning_context_maps_to_stable_invalid_request_error() {
        let error = ThreadlineError::UnsupportedReasoningContext;

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);

        let document = error.public_error_document();

        assert_eq!(
            document.error.code.as_ref(),
            "unsupported_reasoning_context"
        );
        assert_eq!(
            document.error.message.as_ref(),
            "reasoning.context=all_turns is not supported for this model. The model metadata has use_responses_lite=false."
        );
        assert_eq!(document.error.error_type.as_ref(), "invalid_request_error");
    }
}
