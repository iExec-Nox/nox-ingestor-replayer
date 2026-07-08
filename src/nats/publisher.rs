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
            .await?
            .await?;

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

    /// Publishes with bounded retry on transient failures.
    pub async fn publish_with_retry(
        &self,
        message: &TransactionMessage,
    ) -> Result<PublishOutcome, NatsError> {
        let mut attempt = 0;
        loop {
            match self.publish(message).await {
                Ok(outcome) => return Ok(outcome),
                Err(e) if Self::should_retry(&e, attempt, self.publish_max_retries) => {
                    attempt += 1;
                    warn!(
                        attempt,
                        max_retries = self.publish_max_retries,
                        error = %e,
                        "transient publish failure, retrying"
                    );
                    sleep(self.publish_retry_delay).await;
                }
                Err(e) => {
                    warn!(
                        attempt,
                        max_retries = self.publish_max_retries,
                        error = %e,
                        exhausted = attempt >= self.publish_max_retries,
                        "publish failed, giving up"
                    );
                    return Err(e);
                }
            }
        }
    }

    fn should_retry(err: &NatsError, attempt: u32, max_retries: u32) -> bool {
        matches!(
            err,
            NatsError::PublishFailed {
                transient: true,
                ..
            }
        ) && attempt < max_retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::jetstream::context::PublishErrorKind;

    fn transient_err() -> NatsError {
        NatsError::PublishFailed {
            kind: PublishErrorKind::TimedOut,
            message: "timed out".to_string(),
            transient: true,
        }
    }

    fn permanent_err() -> NatsError {
        NatsError::PublishFailed {
            kind: PublishErrorKind::Other,
            message: "invalid subject".to_string(),
            transient: false,
        }
    }

    #[test]
    fn should_retry_returns_true_when_transient_and_below_limit() {
        assert!(Publisher::should_retry(&transient_err(), 0, 5));
        assert!(Publisher::should_retry(&transient_err(), 4, 5));
    }

    #[test]
    fn should_retry_returns_false_when_retries_exhausted() {
        assert!(!Publisher::should_retry(&transient_err(), 5, 5));
    }

    #[test]
    fn should_retry_returns_false_when_failure_is_not_transient() {
        assert!(!Publisher::should_retry(&permanent_err(), 0, 5));
    }

    #[test]
    fn should_retry_returns_false_for_non_publish_failed_variants() {
        assert!(!Publisher::should_retry(&NatsError::Disconnected, 0, 5));
    }
}
