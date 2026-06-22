//! Simple NATS JetStream publisher

use async_nats::HeaderMap;
use async_nats::jetstream::Context as JetStreamContext;
use axum_prometheus::metrics::counter;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::NatsConfig;
use crate::error::NatsError;
use crate::events::TransactionMessage;

use super::MessageBuffer;
use super::client::ConnectionState;

/// Publisher for sending transaction messages to NATS JetStream
pub struct Publisher {
    jetstream: Arc<JetStreamContext>,
    state_rx: watch::Receiver<ConnectionState>,
    pause_tx: watch::Sender<bool>,
    buffer: MessageBuffer,
    subject_prefix: String,
}

impl Publisher {
    /// Create a new publisher
    pub fn new(
        jetstream: Arc<JetStreamContext>,
        config: &NatsConfig,
        state_rx: watch::Receiver<ConnectionState>,
        pause_tx: watch::Sender<bool>,
    ) -> Self {
        Self {
            jetstream,
            state_rx,
            pause_tx,
            buffer: MessageBuffer::new(config.buffer_capacity),
            subject_prefix: config.subject.clone(),
        }
    }

    /// Publish a transaction message
    ///
    /// If connected, publishes immediately.
    /// If disconnected, buffers the message and signals pause.
    /// If buffer is full, returns error.
    pub async fn publish(&mut self, message: TransactionMessage) -> Result<(), NatsError> {
        let state = *self.state_rx.borrow();

        match state {
            ConnectionState::Connected => {
                match self.do_publish(&message).await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        // Connection may have dropped between state check and publish
                        // Buffer the message and pause to prevent message loss
                        warn!(error = %e, "Publish failed while connected, buffering message");
                        let _ = self.pause_tx.send(true);
                        self.buffer.push(message)
                    }
                }
            }
            ConnectionState::Disconnected => {
                // Signal pause to reader
                let _ = self.pause_tx.send(true);
                // Buffer the message
                self.buffer.push(message)?;

                debug!(
                    buffer_len = self.buffer.len(),
                    "Message buffered while disconnected"
                );
                Ok(())
            }
        }
    }

    /// Flush buffered messages
    ///
    /// Called when connection is restored. Publishes all buffered messages in FIFO order.
    /// On failure, re-buffers the failed message and all remaining messages to maintain
    /// FIFO order and prevent message loss.
    pub async fn flush_buffer(&mut self) -> Result<usize, NatsError> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let total_count = self.buffer.len();
        info!(count = total_count, "Flushing buffered messages");

        let messages: Vec<_> = self.buffer.drain().collect();
        let mut published_count = 0;

        for (index, message) in messages.iter().enumerate() {
            if let Err(e) = self.do_publish(message).await {
                // Re-buffer the failed message and all remaining messages
                let remaining_count = total_count - index;
                error!(
                    error = %e,
                    published = published_count,
                    remaining = remaining_count,
                    "Failed to flush message, re-buffering remaining messages"
                );

                // Re-buffer remaining messages (including the failed one) at the front
                // to maintain FIFO order. We clone the remaining messages since we
                // borrowed them from the iterator.
                self.buffer.extend_front(messages.into_iter().skip(index));

                return Err(e);
            }
            published_count += 1;
        }

        info!(count = published_count, "Buffer flushed successfully");
        Ok(published_count)
    }

    /// Handle connection state change
    ///
    /// Should be called when state_rx signals a change.
    pub async fn handle_state_change(&mut self) -> Result<(), NatsError> {
        let state = *self.state_rx.borrow();

        match state {
            ConnectionState::Connected => {
                info!("NATS connected, flushing buffer and resuming");

                // Flush any buffered messages
                if let Err(e) = self.flush_buffer().await {
                    warn!(error = %e, "Error flushing buffer");
                }

                // Signal resume to reader
                let _ = self.pause_tx.send(false);
            }
            ConnectionState::Disconnected => {
                warn!("NATS disconnected, pausing reader");
                let _ = self.pause_tx.send(true);
            }
        }

        Ok(())
    }

    /// Get the number of buffered messages
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if buffer is full
    pub fn is_buffer_full(&self) -> bool {
        self.buffer.is_full()
    }

    /// Check if buffer is empty (all messages confirmed by NATS)
    pub fn is_buffer_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get current connection state
    pub fn connection_state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// Attempt to drain the buffer if currently connected.
    ///
    /// Covers the case where JetStream quorum was lost without a TCP disconnect:
    /// `ConnectionState` never transitions, so `handle_state_change` is never
    /// triggered. The periodic tick calls this to drain the buffer on recovery.
    ///
    /// Returns `Ok(true)` if buffer was non-empty and fully drained.
    /// Returns `Ok(false)` if buffer was already empty or state is not Connected.
    /// Returns `Err` if the flush attempt failed.
    pub async fn try_resume_if_connected(&mut self) -> Result<bool, NatsError> {
        if *self.state_rx.borrow() != ConnectionState::Connected {
            return Ok(false);
        }
        if self.buffer.is_empty() {
            return Ok(false);
        }
        self.flush_buffer().await?;
        let _ = self.pause_tx.send(false);
        Ok(true)
    }

    /// Publish a transaction message to JetStream
    async fn do_publish(&self, message: &TransactionMessage) -> Result<(), NatsError> {
        let subject = message.subject(&self.subject_prefix);
        let payload = message
            .to_bytes()
            .map_err(|e| NatsError::Publish(format!("Serialization error: {}", e)))?;

        // Use checksum as Nats-Msg-Id for deduplication
        let checksum = message.compute_checksum();
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", checksum.as_str());

        let publish_future = self
            .jetstream
            .publish_with_headers(subject.clone(), headers, payload.into())
            .await;

        let ack_future = match publish_future {
            Ok(fut) => fut,
            Err(e) => {
                counter!("nox_replayer.nats.publishes_total", "outcome" => "err").increment(1);
                return Err(NatsError::Publish(format!("Publish error: {}", e)));
            }
        };

        let ack = match ack_future.await {
            Ok(ack) => ack,
            Err(e) => {
                counter!("nox_replayer.nats.publishes_total", "outcome" => "err").increment(1);
                return Err(NatsError::Publish(format!("Ack error: {}", e)));
            }
        };

        counter!("nox_replayer.nats.publishes_total", "outcome" => "ok").increment(1);

        debug!(
            subject,
            checksum,
            event_count = message.events.len(),
            seq = ack.sequence,
            duplicate = ack.duplicate,
            "Published transaction message"
        );

        Ok(())
    }
}
