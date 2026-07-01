//! Simple NATS JetStream publisher

use async_nats::HeaderMap;
use async_nats::jetstream::Context as JetStreamContext;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::config::NatsConfig;
use crate::error::NatsError;
use crate::events::TransactionMessage;

use super::client::NatsClient;

#[derive(Debug, Clone, Copy)]
pub struct PublishOutcome {
    pub duplicate: bool,
}

pub struct Publisher {
    jetstream: Arc<JetStreamContext>,
    nats: Arc<NatsClient>,
    subject_prefix: String,
    publish_max_retries: u32,
    publish_retry_delay: Duration,
}

impl Publisher {
    pub fn new(nats: Arc<NatsClient>, config: &NatsConfig) -> Self {
        Self {
            jetstream: nats.jetstream(),
            nats,
            subject_prefix: config.subject.clone(),
            publish_max_retries: config.publish_max_retries,
            publish_retry_delay: config.publish_retry_delay,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.nats.is_connected()
    }

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
    pub async fn publish_with_retry(
        &self,
        message: &TransactionMessage,
    ) -> Result<PublishOutcome, NatsError> {
        let mut attempt = 0u32;
        loop {
            match self.publish(message).await {
                Ok(outcome) => return Ok(outcome),
                Err(NatsError::PublishFailed { kind, message: msg })
                    if NatsError::is_transient(kind) && attempt < self.publish_max_retries =>
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
