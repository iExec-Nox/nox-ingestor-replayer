//! Simple NATS JetStream publisher

use async_nats::HeaderMap;
use async_nats::jetstream::Context as JetStreamContext;
use std::sync::Arc;
use tracing::debug;

use crate::config::NatsConfig;
use crate::error::NatsError;
use crate::events::TransactionMessage;

use super::client::NatsClient;

#[derive(Debug, Clone, Copy)]
/// Outcome of a single NATS JetStream publish.
pub struct PublishOutcome {
    /// `true` if JetStream deduplicated this message (already seen by message ID).
    pub duplicate: bool,
}

/// Stateless NATS JetStream publisher. One instance per chain pipeline.
pub struct Publisher {
    jetstream: Arc<JetStreamContext>,
    nats: Arc<NatsClient>,
    subject_prefix: String,
}

impl Publisher {
    /// Create a publisher attached to `nats`, writing to the stream configured in `config`.
    pub fn new(nats: Arc<NatsClient>, config: &NatsConfig) -> Self {
        Self {
            jetstream: nats.jetstream(),
            nats,
            subject_prefix: config.subject.clone(),
        }
    }

    /// Returns `true` if the underlying NATS connection is currently healthy.
    pub fn is_connected(&self) -> bool {
        self.nats.is_connected()
    }

    /// Publish `message` to JetStream, returning whether it was a duplicate.
    pub async fn publish(&self, message: &TransactionMessage) -> Result<PublishOutcome, NatsError> {
        let subject = message.subject(&self.subject_prefix);
        let payload = message
            .to_bytes()
            .map_err(|e| NatsError::Publish(format!("Serialization error: {e}")))?;
        let checksum = message.compute_checksum();
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", checksum.as_str());

        let ack = self
            .jetstream
            .publish_with_headers(subject.clone(), headers, payload.into())
            .await
            .map_err(|e| NatsError::Publish(format!("Publish error: {e}")))?
            .await
            .map_err(|e| NatsError::Publish(format!("Ack error: {e}")))?;

        debug!(
            subject,
            checksum,
            event_count = message.events.len(),
            seq = ack.sequence,
            duplicate = ack.duplicate,
            "published transaction message"
        );
        Ok(PublishOutcome {
            duplicate: ack.duplicate,
        })
    }
}
