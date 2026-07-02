//! Error types for nox-ingestor-replayer

use async_nats::jetstream::context::PublishErrorKind;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use thiserror::Error;
use tracing::warn;

/// RPC related errors
#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Invalid RPC endpoint: {0}")]
    InvalidEndpoint(String),

    #[error("Provider error: {0}")]
    Provider(String),
}

/// Errors returned by the replay API endpoints.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("invalid block range: from must be <= to")]
    InvalidRange,
    #[error("range exceeds maximum of {max} blocks")]
    RangeTooLarge { max: u64 },
    #[error("to_block {to} exceeds chain head {latest}")]
    RangeBeyondHead { to: u64, latest: u64 },
    #[error("chain is busy")]
    ChainBusy,
    #[error("rpc error")]
    Rpc {
        #[source]
        source: RpcError,
    },
    #[error("nats error")]
    Nats {
        #[source]
        source: NatsError,
    },
}

impl ReplayError {
    fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::InvalidRange | Self::RangeTooLarge { .. } | Self::RangeBeyondHead { .. } => {
                StatusCode::BAD_REQUEST
            }
            Self::ChainBusy => StatusCode::CONFLICT,
            Self::Nats { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Rpc { .. } => StatusCode::BAD_GATEWAY,
        }
    }

    fn retryable(&self) -> bool {
        matches!(self, Self::ChainBusy | Self::Nats { .. } | Self::Rpc { .. })
    }
}

impl IntoResponse for ReplayError {
    fn into_response(self) -> Response {
        let status = self.status();
        if status.is_server_error() {
            warn!(error = %self, detail = ?std::error::Error::source(&self), "replay request failed");
        }
        let error_obj = json!({
            "message": self.to_string(),
            "retryable": self.retryable(),
        });
        (status, Json(json!({ "error": error_obj }))).into_response()
    }
}

/// NATS related errors
#[derive(Error, Debug)]
pub enum NatsError {
    #[error("TLS configuration error: {0}")]
    Tls(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Disconnected")]
    Disconnected,

    #[error("NATS unavailable")]
    Unavailable,

    #[error("Publish error: {0}")]
    Publish(String),

    #[error("Publish failed ({kind:?}): {message}")]
    PublishFailed {
        kind: PublishErrorKind,
        message: String,
    },

    #[error("Stream setup error: {0}")]
    StreamSetup(String),
}

impl NatsError {
    /// Whether a JetStream publish error is worth retrying with the same `Nats-Msg-Id`.
    pub fn is_transient(kind: PublishErrorKind) -> bool {
        matches!(
            kind,
            PublishErrorKind::TimedOut
                | PublishErrorKind::BrokenPipe
                | PublishErrorKind::MaxAckPending
                | PublishErrorKind::StreamNotFound
        )
    }
}
