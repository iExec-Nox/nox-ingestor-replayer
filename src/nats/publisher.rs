//! Simple NATS JetStream publisher

use async_nats::HeaderMap;
use async_nats::jetstream::Context as JetStreamContext;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::config::NatsConfig;
use crate::error::{NatsError, is_transient};
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
    publish_max_retries: u32,
    publish_retry_delay: Duration,
}

impl Publisher {
    /// Create a publisher attached to `nats`, writing to the stream configured in `config`.
    pub fn new(nats: Arc<NatsClient>, config: &NatsConfig) -> Self {
        Self {
            jetstream: nats.jetstream(),
            nats,
            subject_prefix: config.subject.clone(),
            publish_max_retries: config.publish_max_retries,
            publish_retry_delay: config.publish_retry_delay,
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
            .map_err(|e| NatsError::PublishFailed {
                kind: e.kind(),
                message: e.to_string(),
            })?
            .await
            .map_err(|e| NatsError::PublishFailed {
                kind: e.kind(),
                message: e.to_string(),
            })?;

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

    /// Publish `message` with bounded retries on transient failures.
    ///
    /// Retries reuse the same deterministic `Nats-Msg-Id`, so a retry within the
    /// stream's `duplicate_window` is deduplicated (idempotent). Only transient
    /// publish errors are retried; fatal publish errors and serialization errors
    /// return immediately.
    pub async fn publish_with_retry(
        &self,
        message: &TransactionMessage,
    ) -> Result<PublishOutcome, NatsError> {
        let mut attempt = 0u32;
        loop {
            match self.publish(message).await {
                Ok(outcome) => return Ok(outcome),
                Err(NatsError::PublishFailed { kind, message: msg })
                    if is_transient(kind) && attempt < self.publish_max_retries =>
                {
                    attempt += 1;
                    warn!(
                        error = %msg,
                        ?kind,
                        attempt,
                        max_retries = self.publish_max_retries,
                        retry_delay_ms = self.publish_retry_delay.as_millis(),
                        "publish failed, retrying"
                    );
                    sleep(self.publish_retry_delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
