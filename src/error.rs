//! Error types for nox-ingestor-replayer

use async_nats::jetstream::context::PublishErrorKind;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use thiserror::Error;
use tracing::warn;

/// Chain/RPC related errors
#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Invalid RPC endpoint: {0}")]
    InvalidEndpoint(String),

    #[error("Provider error: {0}")]
    Provider(String),
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

    #[error("Publish error: {0}")]
    Publish(String),

    #[error("Stream setup error: {0}")]
    StreamSetup(String),

    #[error("NATS unavailable")]
    Unavailable,

    #[error("Publish failed ({kind:?}): {message}")]
    PublishFailed {
        kind: PublishErrorKind,
        message: String,
        transient: bool,
    },
}

impl From<async_nats::jetstream::context::PublishError> for NatsError {
    fn from(e: async_nats::jetstream::context::PublishError) -> Self {
        let kind = e.kind();
        NatsError::PublishFailed {
            kind,
            message: e.to_string(),
            transient: is_transient_publish_error(kind),
        }
    }
}

fn is_transient_publish_error(kind: PublishErrorKind) -> bool {
    matches!(
        kind,
        PublishErrorKind::TimedOut
            | PublishErrorKind::BrokenPipe
            | PublishErrorKind::MaxAckPending
            | PublishErrorKind::StreamNotFound
    )
}

/// Errors surfaced by the on-demand replay endpoint
#[derive(Error, Debug)]
pub enum ReplayError {
    #[error("Unauthorized")]
    Unauthorized,

    #[error("Invalid range")]
    InvalidRange,

    #[error("Requested range beyond chain head (to: {to}, latest: {latest})")]
    RangeBeyondHead { to: u64, latest: u64 },

    #[error("Chain {chain_id} busy")]
    ChainBusy { chain_id: u32 },

    #[error("Chain {chain_id} not configured")]
    ChainNotConfigured { chain_id: u32 },

    #[error("Missing required query parameter: chain_id")]
    MissingChainId,

    #[error("Invalid chain_id query parameter: {value:?}")]
    InvalidChainId { value: String },

    #[error("At capacity (max {max} concurrent replay jobs)")]
    AtCapacity { max: usize },

    #[error("RPC error on chain {chain_id}: {source}")]
    Rpc {
        chain_id: u32,
        #[source]
        source: RpcError,
    },

    #[error("NATS error on chain {chain_id}: {source}")]
    Nats {
        chain_id: u32,
        #[source]
        source: NatsError,
    },
}

impl ReplayError {
    fn status(&self) -> StatusCode {
        match self {
            ReplayError::Unauthorized => StatusCode::UNAUTHORIZED,
            ReplayError::InvalidRange | ReplayError::RangeBeyondHead { .. } => {
                StatusCode::BAD_REQUEST
            }
            ReplayError::ChainBusy { .. } => StatusCode::CONFLICT,
            ReplayError::ChainNotConfigured { .. } => StatusCode::BAD_REQUEST,
            ReplayError::MissingChainId | ReplayError::InvalidChainId { .. } => {
                StatusCode::BAD_REQUEST
            }
            ReplayError::AtCapacity { .. } => StatusCode::SERVICE_UNAVAILABLE,
            ReplayError::Nats { .. } => StatusCode::SERVICE_UNAVAILABLE,
            ReplayError::Rpc { .. } => StatusCode::BAD_GATEWAY,
        }
    }

    fn retryable(&self) -> bool {
        matches!(
            self,
            ReplayError::ChainBusy { .. }
                | ReplayError::AtCapacity { .. }
                | ReplayError::Nats { .. }
                | ReplayError::Rpc { .. }
        )
    }
}

impl ReplayError {
    /// Render the JSON error envelope (`{"error": {message, retryable}}`) for
    /// reuse by handlers that choose their own status code.
    pub(crate) fn body(&self) -> serde_json::Value {
        json!({
            "error": {
                "message": self.to_string(),
                "retryable": self.retryable(),
            }
        })
    }
}

impl IntoResponse for ReplayError {
    fn into_response(self) -> Response {
        let status = self.status();

        if status.is_server_error() {
            warn!(error = %self, status = %status, "replay request failed");
        }

        let body = self.body();

        (status, Json(body)).into_response()
    }
}
