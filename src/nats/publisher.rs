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
pub struct PublishOutcome {
    pub duplicate: bool,
}

pub struct Publisher {
    jetstream: Arc<JetStreamContext>,
    nats: Arc<NatsClient>,
    subject_prefix: String,
}

impl Publisher {
    pub fn new(nats: Arc<NatsClient>, config: &NatsConfig) -> Self {
        Self {
            jetstream: nats.jetstream(),
            nats,
            subject_prefix: config.subject.clone(),
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
