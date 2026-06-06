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

    #[error("The /v1/responses request body was not a valid JSON object.")]
    InvalidResponsesRequest,

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
            Self::PreviousResponseNotFound => StatusCode::NOT_FOUND,
            Self::RetainedSessionConflict => StatusCode::CONFLICT,
            Self::RetainedSessionCapacityExceeded => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamWebSocketConnectFailed => StatusCode::BAD_GATEWAY,
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
            Self::InvalidResponsesRequest => PublicErrorPayload {
                code: "invalid_request_error",
                message: "The /v1/responses request body must be a JSON object.",
                error_type: "invalid_request_error",
            },
            Self::PreviousResponseNotFound => PublicErrorPayload {
                code: "previous_response_not_found",
                message: "Threadline could not find the retained session for that previous_response_id.",
                error_type: "invalid_request_error",
            },
            Self::RetainedSessionConflict => PublicErrorPayload {
                code: "retained_session_conflict",
                message: "The retained session for that previous_response_id is already active.",
                error_type: "conflict_error",
            },
            Self::RetainedSessionCapacityExceeded => PublicErrorPayload {
                code: "retained_session_capacity_exceeded",
                message: "Threadline has no free retained session capacity for another active response.",
                error_type: "service_unavailable_error",
            },
            Self::UpstreamWebSocketConnectFailed => PublicErrorPayload {
                code: "upstream_websocket_connect_failed",
                message: "Threadline could not connect to the upstream Codex websocket.",
                error_type: "bad_gateway_error",
            },
            Self::UpstreamWebSocketClosed => PublicErrorPayload {
                code: "upstream_websocket_closed",
                message: "The upstream Codex websocket closed before Threadline finished streaming the response.",
                error_type: "bad_gateway_error",
            },
            Self::UpstreamResponseFailed => PublicErrorPayload {
                code: "upstream_response_failed",
                message: "The upstream response.failed event cannot be streamed as a successful downstream response.",
                error_type: "bad_gateway_error",
            },
            Self::UpstreamErrorEvent => PublicErrorPayload {
                code: "upstream_error_event",
                message: "The upstream websocket emitted an error event.",
                error_type: "bad_gateway_error",
            },
            Self::UpstreamInvalidJson => PublicErrorPayload {
                code: "upstream_invalid_json",
                message: "The upstream websocket emitted malformed JSON.",
                error_type: "bad_gateway_error",
            },
            Self::InternalToolFailed => PublicErrorPayload {
                code: "internal_tool_failed",
                message: "Threadline failed while executing an internal tool.",
                error_type: "internal_server_error",
            },
            Self::JobNotFound => PublicErrorPayload {
                code: "job_not_found",
                message: "Threadline could not find a job with that job_id.",
                error_type: "invalid_request_error",
            },
            Self::JobsDisabled => PublicErrorPayload {
                code: "jobs_disabled",
                message: "Threadline jobs are disabled.",
                error_type: "forbidden_error",
            },
            Self::JobCommandNotAllowed => PublicErrorPayload {
                code: "job_command_not_allowed",
                message: "The requested job command is not allowed by Threadline policy.",
                error_type: "forbidden_error",
            },
            Self::JobCommandFailed => PublicErrorPayload {
                code: "job_command_failed",
                message: "The Threadline job command failed.",
                error_type: "internal_server_error",
            },
            Self::JobCancelled => PublicErrorPayload {
                code: "job_cancelled",
                message: "The Threadline job was cancelled.",
                error_type: "conflict_error",
            },
            Self::UpstreamCredentialsUnavailable => PublicErrorPayload {
                code: "upstream_credentials_unavailable",
                message: "Threadline could not load upstream credentials.",
                error_type: "configuration_error",
            },
            Self::UpstreamUrlMissing => PublicErrorPayload {
                code: "configuration_error",
                message: "Threadline is missing THREADLINE_UPSTREAM_URL for upstream websocket connections.",
                error_type: "configuration_error",
            },
            Self::InvalidBindHost(_) => PublicErrorPayload {
                code: "configuration_error",
                message: "Threadline failed to resolve its configured bind address.",
                error_type: "configuration_error",
            },
        }
    }

    pub fn public_error_document(&self) -> PublicErrorDocument {
        PublicErrorDocument {
            error: self.public_error(),
        }
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

        assert_eq!(document.error.code, "upstream_websocket_handshake_rejected");
        assert_eq!(
            document.error.message,
            "The upstream Codex websocket handshake was rejected with HTTP 403 Forbidden."
        );
    }

    #[test]
    fn upstream_websocket_handshake_rejected_propagates_exact_server_error_status() {
        let error = ThreadlineError::UpstreamWebSocketHandshakeRejected {
            status: StatusCode::SERVICE_UNAVAILABLE,
        };

        assert_eq!(error.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
