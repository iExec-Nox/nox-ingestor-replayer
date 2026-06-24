//! Error types for nox-ingestor-replayer

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use thiserror::Error;

/// Chain/RPC related errors
#[derive(Error, Debug)]
pub enum ChainError {
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
    #[error("chain {chain_id} not configured")]
    ChainNotConfigured { chain_id: u32 },
    #[error("invalid block range: from must be <= to")]
    InvalidRange,
    #[error("range exceeds maximum of {max} blocks")]
    RangeTooLarge { max: u64 },
    #[error("chain {chain_id} is busy")]
    ChainBusy { chain_id: u32 },
    #[allow(dead_code)]
    #[error("at capacity (max {max} concurrent chains)")]
    AtCapacity { max: usize },
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("nats error: {0}")]
    Nats(String),
    #[error("nats unavailable")]
    NatsUnavailable,
    #[error("replay cancelled")]
    Cancelled,
}

impl ReplayError {
    fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::ChainNotConfigured { .. } | Self::InvalidRange | Self::RangeTooLarge { .. } => {
                StatusCode::BAD_REQUEST
            }
            Self::ChainBusy { .. } => StatusCode::CONFLICT,
            Self::AtCapacity { .. } | Self::NatsUnavailable | Self::Nats(_) | Self::Cancelled => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::Rpc(_) => StatusCode::BAD_GATEWAY,
        }
    }

    fn kind(&self) -> &str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::ChainNotConfigured { .. } => "chain_not_configured",
            Self::InvalidRange => "invalid_range",
            Self::RangeTooLarge { .. } => "range_too_large",
            Self::ChainBusy { .. } => "chain_busy",
            Self::AtCapacity { .. } => "at_capacity",
            Self::Rpc(_) => "rpc_error",
            Self::Nats(_) => "nats_error",
            Self::NatsUnavailable => "nats_unavailable",
            Self::Cancelled => "cancelled",
        }
    }

    fn retryable(&self) -> bool {
        matches!(
            self,
            Self::ChainBusy { .. }
                | Self::AtCapacity { .. }
                | Self::NatsUnavailable
                | Self::Nats(_)
                | Self::Rpc(_)
                | Self::Cancelled
        )
    }
}

impl IntoResponse for ReplayError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = Json(json!({
            "error": {
                "kind": self.kind(),
                "message": self.to_string(),
                "retryable": self.retryable(),
            }
        }));
        (status, body).into_response()
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

    #[error("Publish error: {0}")]
    Publish(String),

    #[error("Stream setup error: {0}")]
    StreamSetup(String),
}
