//! Error types for nox-ingestor-replayer

use thiserror::Error;

/// Chain/RPC related errors
#[derive(Error, Debug)]
pub enum ChainError {
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
}
