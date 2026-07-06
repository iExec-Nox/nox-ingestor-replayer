//! Async replay job execution and status tracking.

use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, RwLock, watch};
use tracing::warn;

use crate::chain::{BatchResult, BlockReader};
use crate::error::{NatsError, RpcError};
use crate::events::{TransactionMessage, log_event};
use crate::nats::{PublishOutcome, Publisher};

/// Lifecycle state of the single replay job slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Idle,
    Running,
    Completed,
    Stopped,
}

/// Snapshot of the current (or most recent) replay job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayJobStatus {
    pub state: JobState,
    pub from_block: u64,
    pub to_block: u64,
    /// Resume position -- same semantics as the old `next_block`.
    /// Running: next unprocessed block. Stopped: block to resume from.
    /// Completed: to_block.saturating_add(1).
    pub current_block: u64,
    pub transactions_published: u64,
    pub events_total: u64,
    pub duplicates: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: u64,
    pub stopped_reason: Option<String>,
}

impl Default for ReplayJobStatus {
    fn default() -> Self {
        Self {
            state: JobState::Idle,
            from_block: 0,
            to_block: 0,
            current_block: 0,
            transactions_published: 0,
            events_total: 0,
            duplicates: 0,
            started_at: None,
            finished_at: None,
            duration_ms: 0,
            stopped_reason: None,
        }
    }
}

impl ReplayJobStatus {
    /// Fresh `Running` status; used by the POST handler before spawning the job.
    pub fn running(from_block: u64, to_block: u64) -> Self {
        Self {
            state: JobState::Running,
            from_block,
            to_block,
            current_block: from_block,
            started_at: Some(Utc::now()),
            ..Default::default()
        }
    }
}

/// Source of block batches for a replay job. Exists as a seam so
/// `run_replay_job`'s failure paths are testable without a live RPC endpoint.
pub(crate) trait BatchSource {
    fn batch_size(&self) -> u64;
    async fn read_batch_bounded(&self, from: u64, to: u64) -> Result<BatchResult, RpcError>;
}

impl BatchSource for BlockReader {
    fn batch_size(&self) -> u64 {
        BlockReader::batch_size(self)
    }

    async fn read_batch_bounded(&self, from: u64, to: u64) -> Result<BatchResult, RpcError> {
        BlockReader::read_batch_bounded(self, from, to).await
    }
}

/// Sink for publishing replayed transactions. Exists as a seam so
/// `run_replay_job`'s failure paths are testable without a live NATS broker.
pub(crate) trait TxSink {
    async fn publish_with_retry(
        &self,
        tx: &TransactionMessage,
    ) -> Result<PublishOutcome, NatsError>;
}

impl TxSink for Publisher {
    async fn publish_with_retry(
        &self,
        tx: &TransactionMessage,
    ) -> Result<PublishOutcome, NatsError> {
        Publisher::publish_with_retry(self, tx).await
    }
}

/// Run a replay job to completion, writing progress into `slot` as it goes.
///
/// `permit` is held for the lifetime of the job and released (freeing the
/// slot for a subsequent `POST /replay`) when this function returns.
///
/// On panic the permit still drops (409 recovers) but the slot is left in its
/// last `Running` state until the next job overwrites it — a panic here is a
/// bug, not a modeled state.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn run_replay_job<S: BatchSource, K: TxSink>(
    source: Arc<S>,
    sink: Arc<K>,
    shutdown: watch::Receiver<bool>,
    permit: OwnedSemaphorePermit,
    slot: Arc<RwLock<ReplayJobStatus>>,
    from_block: u64,
    to_block: u64,
) {
    let start = Instant::now();
    let mut transactions_published = 0u64;
    let mut events_total = 0u64;
    let mut duplicates = 0u64;
    let mut current = from_block;
    let mut resume_from = from_block;
    let mut stopped_reason: Option<String> = None;

    'outer: while current <= to_block {
        if *shutdown.borrow() {
            warn!(from = current, "replay stopped: shutdown signal received");
            stopped_reason = Some("sigterm received".to_string());
            resume_from = current;
            break;
        }

        let batch_to = current
            .saturating_add(source.batch_size().saturating_sub(1))
            .min(to_block);

        let batch = match source.read_batch_bounded(current, batch_to).await {
            Ok(batch) => batch,
            Err(e) => {
                warn!(from = current, to = batch_to, error = %e, "replay stopped: batch read failed");
                stopped_reason = Some("rpc error".to_string());
                resume_from = current;
                break;
            }
        };

        for tx in &batch.transactions {
            if *shutdown.borrow() {
                warn!(
                    block = tx.block_number,
                    "replay stopped: shutdown signal received"
                );
                stopped_reason = Some("sigterm received".to_string());
                resume_from = tx.block_number;
                break 'outer;
            }

            match sink.publish_with_retry(tx).await {
                Ok(outcome) => {
                    for event in &tx.events {
                        log_event(event);
                    }
                    transactions_published += 1;
                    events_total += tx.events.len() as u64;
                    if outcome.duplicate {
                        duplicates += 1;
                    }
                }
                Err(e) => {
                    warn!(block = tx.block_number, error = %e, "replay stopped: publish failed");
                    stopped_reason = Some("nats error".to_string());
                    // Resume from the failed block; already-acked blocks are not re-read.
                    resume_from = tx.block_number;
                    break 'outer;
                }
            }
        }

        current = batch_to.saturating_add(1);

        let mut s = slot.write().await;
        s.current_block = current;
        s.transactions_published = transactions_published;
        s.events_total = events_total;
        s.duplicates = duplicates;
        s.duration_ms = start.elapsed().as_millis() as u64;
    }

    let mut s = slot.write().await;
    s.transactions_published = transactions_published;
    s.events_total = events_total;
    s.duplicates = duplicates;
    s.duration_ms = start.elapsed().as_millis() as u64;
    s.finished_at = Some(Utc::now());
    match stopped_reason {
        None => {
            s.state = JobState::Completed;
            s.current_block = to_block.saturating_add(1);
            s.stopped_reason = None;
        }
        Some(reason) => {
            s.state = JobState::Stopped;
            s.current_block = resume_from;
            s.stopped_reason = Some(reason);
        }
    }
    drop(s);

    // Release the permit now that the terminal status has been written to the slot.
    drop(permit);
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Semaphore;

    use crate::events::{BooleanOperation, Operator, TransactionEvent};

    #[test]
    fn default_status_json_shape_is_idle_with_nulls_and_zeros() {
        let status = ReplayJobStatus::default();
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "state": "Idle",
                "from_block": 0,
                "to_block": 0,
                "current_block": 0,
                "transactions_published": 0,
                "events_total": 0,
                "duplicates": 0,
                "started_at": null,
                "finished_at": null,
                "duration_ms": 0,
                "stopped_reason": null,
            })
        );
    }

    #[test]
    fn running_status_json_shape_has_started_at_and_null_terminal_fields() {
        let status = ReplayJobStatus::running(100, 500);
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], "Running");
        assert_eq!(value["from_block"], 100);
        assert_eq!(value["to_block"], 500);
        assert_eq!(value["current_block"], 100);
        assert!(value["finished_at"].is_null());
        assert!(value["stopped_reason"].is_null());
        let started_at = value["started_at"].as_str().unwrap();
        assert!(DateTime::parse_from_rfc3339(started_at).is_ok());
    }

    #[test]
    fn completed_status_json_shape_has_finished_at_and_current_block_past_to() {
        let status = ReplayJobStatus {
            state: JobState::Completed,
            from_block: 10,
            to_block: 20,
            current_block: 21,
            transactions_published: 5,
            events_total: 8,
            duplicates: 1,
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            duration_ms: 1234,
            stopped_reason: None,
        };
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], "Completed");
        assert_eq!(value["current_block"], 21);
        assert!(value["stopped_reason"].is_null());
        let finished_at = value["finished_at"].as_str().unwrap();
        assert!(DateTime::parse_from_rfc3339(finished_at).is_ok());
    }

    #[test]
    fn stopped_status_json_shape_has_stopped_reason_and_resume_block() {
        let status = ReplayJobStatus {
            state: JobState::Stopped,
            from_block: 10,
            to_block: 500,
            current_block: 42,
            transactions_published: 3,
            events_total: 4,
            duplicates: 0,
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            duration_ms: 500,
            stopped_reason: Some("rpc error".to_string()),
        };
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], "Stopped");
        assert_eq!(value["stopped_reason"], "rpc error");
        assert_eq!(value["current_block"], 42);
    }

    #[test]
    fn default_is_idle_with_zeroed_fields() {
        let status = ReplayJobStatus::default();
        assert_eq!(status.state, JobState::Idle);
        assert_eq!(status.from_block, 0);
        assert_eq!(status.to_block, 0);
        assert_eq!(status.current_block, 0);
        assert_eq!(status.transactions_published, 0);
        assert_eq!(status.events_total, 0);
        assert_eq!(status.duplicates, 0);
        assert!(status.started_at.is_none());
        assert!(status.finished_at.is_none());
        assert_eq!(status.duration_ms, 0);
        assert!(status.stopped_reason.is_none());
    }

    #[test]
    fn running_sets_state_and_current_block() {
        let status = ReplayJobStatus::running(100, 500);
        assert_eq!(status.state, JobState::Running);
        assert_eq!(status.from_block, 100);
        assert_eq!(status.to_block, 500);
        assert_eq!(status.current_block, 100);
        assert!(status.started_at.is_some());
    }

    fn make_tx(block_number: u64, event_count: usize) -> TransactionMessage {
        let events = (0..event_count)
            .map(|i| TransactionEvent {
                log_index: i as u64,
                caller: Address::ZERO,
                operator: Operator::Eq(BooleanOperation {
                    left_hand_operand: "0x1".to_string(),
                    right_hand_operand: "0x2".to_string(),
                    result: "0x0".to_string(),
                }),
            })
            .collect();
        TransactionMessage::new(
            1,
            Address::ZERO,
            block_number,
            0,
            format!("0x{block_number:x}"),
            events,
        )
    }

    fn make_batch(
        start_block: u64,
        end_block: u64,
        transactions: Vec<TransactionMessage>,
    ) -> BatchResult {
        BatchResult {
            transactions,
            start_block,
            end_block,
        }
    }

    fn permit(sem: &Arc<Semaphore>) -> OwnedSemaphorePermit {
        sem.clone().try_acquire_owned().unwrap()
    }

    struct StubSource {
        batch_size: u64,
        batches: StdMutex<VecDeque<Result<BatchResult, RpcError>>>,
    }

    impl StubSource {
        fn new(batch_size: u64, batches: Vec<Result<BatchResult, RpcError>>) -> Self {
            Self {
                batch_size,
                batches: StdMutex::new(batches.into()),
            }
        }
    }

    impl BatchSource for StubSource {
        fn batch_size(&self) -> u64 {
            self.batch_size
        }

        async fn read_batch_bounded(&self, _from: u64, _to: u64) -> Result<BatchResult, RpcError> {
            self.batches
                .lock()
                .unwrap()
                .pop_front()
                .expect("StubSource exhausted")
        }
    }

    struct StubSink {
        fail_at_block: Option<u64>,
        duplicate_at_block: Option<u64>,
    }

    impl StubSink {
        fn ok() -> Self {
            Self {
                fail_at_block: None,
                duplicate_at_block: None,
            }
        }

        fn failing_at(block: u64) -> Self {
            Self {
                fail_at_block: Some(block),
                duplicate_at_block: None,
            }
        }

        fn duplicate_at(block: u64) -> Self {
            Self {
                fail_at_block: None,
                duplicate_at_block: Some(block),
            }
        }
    }

    impl TxSink for StubSink {
        async fn publish_with_retry(
            &self,
            tx: &TransactionMessage,
        ) -> Result<PublishOutcome, NatsError> {
            if self.fail_at_block == Some(tx.block_number) {
                return Err(NatsError::Publish("stub failure".to_string()));
            }
            Ok(PublishOutcome {
                duplicate: self.duplicate_at_block == Some(tx.block_number),
            })
        }
    }

    #[tokio::test]
    async fn run_replay_job_completes_when_all_batches_succeed() {
        let source = Arc::new(StubSource::new(
            2,
            vec![
                Ok(make_batch(10, 11, vec![make_tx(10, 1)])),
                Ok(make_batch(12, 13, vec![make_tx(12, 2)])),
            ],
        ));
        let sink = Arc::new(StubSink::ok());
        let (_tx, rx) = watch::channel(false);
        let semaphore = Arc::new(Semaphore::new(1));
        let owned_permit = permit(&semaphore);
        let slot = Arc::new(RwLock::new(ReplayJobStatus::running(10, 13)));

        run_replay_job(source, sink, rx, owned_permit, slot.clone(), 10, 13).await;

        let status = slot.read().await.clone();
        assert_eq!(status.state, JobState::Completed);
        assert_eq!(status.current_block, 14);
        assert_eq!(status.transactions_published, 2);
        assert_eq!(status.events_total, 3);
        assert_eq!(status.duplicates, 0);
        assert!(status.finished_at.is_some());
        assert!(status.stopped_reason.is_none());
    }

    #[tokio::test]
    async fn run_replay_job_stops_on_rpc_error() {
        let source = Arc::new(StubSource::new(
            2,
            vec![
                Ok(make_batch(10, 11, vec![make_tx(10, 1)])),
                Err(RpcError::Provider("boom".to_string())),
            ],
        ));
        let sink = Arc::new(StubSink::ok());
        let (_tx, rx) = watch::channel(false);
        let semaphore = Arc::new(Semaphore::new(1));
        let owned_permit = permit(&semaphore);
        let slot = Arc::new(RwLock::new(ReplayJobStatus::running(10, 15)));

        run_replay_job(source, sink, rx, owned_permit, slot.clone(), 10, 15).await;

        let status = slot.read().await.clone();
        assert_eq!(status.state, JobState::Stopped);
        assert_eq!(status.stopped_reason, Some("rpc error".to_string()));
        assert_eq!(status.current_block, 12);
        assert_eq!(status.transactions_published, 1);
        assert!(status.finished_at.is_some());
    }

    #[tokio::test]
    async fn run_replay_job_stops_on_publish_error() {
        let source = Arc::new(StubSource::new(
            1,
            vec![Ok(make_batch(10, 11, vec![make_tx(10, 1), make_tx(11, 1)]))],
        ));
        let sink = Arc::new(StubSink::failing_at(11));
        let (_tx, rx) = watch::channel(false);
        let semaphore = Arc::new(Semaphore::new(1));
        let owned_permit = permit(&semaphore);
        let slot = Arc::new(RwLock::new(ReplayJobStatus::running(10, 20)));

        run_replay_job(source, sink, rx, owned_permit, slot.clone(), 10, 20).await;

        let status = slot.read().await.clone();
        assert_eq!(status.state, JobState::Stopped);
        assert_eq!(status.stopped_reason, Some("nats error".to_string()));
        assert_eq!(status.current_block, 11);
        assert_eq!(status.transactions_published, 1);
    }

    #[tokio::test]
    async fn run_replay_job_counts_duplicate_publishes() {
        let source = Arc::new(StubSource::new(
            2,
            vec![Ok(make_batch(10, 11, vec![make_tx(10, 1), make_tx(11, 1)]))],
        ));
        let sink = Arc::new(StubSink::duplicate_at(11));
        let (_tx, rx) = watch::channel(false);
        let semaphore = Arc::new(Semaphore::new(1));
        let owned_permit = permit(&semaphore);
        let slot = Arc::new(RwLock::new(ReplayJobStatus::running(10, 11)));

        run_replay_job(source, sink, rx, owned_permit, slot.clone(), 10, 11).await;

        let status = slot.read().await.clone();
        assert_eq!(status.state, JobState::Completed);
        assert_eq!(status.transactions_published, 2);
        assert_eq!(status.duplicates, 1);
    }

    #[tokio::test]
    async fn run_replay_job_stops_on_shutdown_signal_between_batches() {
        struct ShutdownFlippingSink {
            shutdown_tx: watch::Sender<bool>,
        }

        impl TxSink for ShutdownFlippingSink {
            async fn publish_with_retry(
                &self,
                _tx: &TransactionMessage,
            ) -> Result<PublishOutcome, NatsError> {
                // Flip shutdown after the first batch's tx is published, so the
                // job observes it on the next top-of-loop check before batch 2.
                let _ = self.shutdown_tx.send(true);
                Ok(PublishOutcome { duplicate: false })
            }
        }

        let source = Arc::new(StubSource::new(
            2,
            vec![
                Ok(make_batch(10, 11, vec![make_tx(10, 1)])),
                Ok(make_batch(12, 13, vec![make_tx(12, 1)])),
            ],
        ));
        let (shutdown_tx, rx) = watch::channel(false);
        let sink = Arc::new(ShutdownFlippingSink { shutdown_tx });
        let semaphore = Arc::new(Semaphore::new(1));
        let owned_permit = permit(&semaphore);
        let slot = Arc::new(RwLock::new(ReplayJobStatus::running(10, 15)));

        run_replay_job(source, sink, rx, owned_permit, slot.clone(), 10, 15).await;

        let status = slot.read().await.clone();
        assert_eq!(status.state, JobState::Stopped);
        assert_eq!(status.stopped_reason, Some("sigterm received".to_string()));
        assert_eq!(status.current_block, 12);
        assert!(status.finished_at.is_some());
    }

    #[tokio::test]
    async fn run_replay_job_holds_slot_running_and_permit_taken_while_source_gated() {
        use tokio::sync::Notify;

        struct GatedSource {
            batch_size: u64,
            notify: Arc<Notify>,
        }

        impl BatchSource for GatedSource {
            fn batch_size(&self) -> u64 {
                self.batch_size
            }

            async fn read_batch_bounded(
                &self,
                from: u64,
                to: u64,
            ) -> Result<BatchResult, RpcError> {
                self.notify.notified().await;
                Ok(make_batch(from, to, vec![]))
            }
        }

        let notify = Arc::new(Notify::new());
        let source = Arc::new(GatedSource {
            batch_size: 10,
            notify: notify.clone(),
        });
        let sink = Arc::new(StubSink::ok());
        let (_tx, rx) = watch::channel(false);
        let semaphore = Arc::new(Semaphore::new(1));
        let owned_permit = semaphore.clone().try_acquire_owned().unwrap();
        let slot = Arc::new(RwLock::new(ReplayJobStatus::running(10, 15)));

        let handle = tokio::spawn(run_replay_job(
            source,
            sink,
            rx,
            owned_permit,
            slot.clone(),
            10,
            15,
        ));

        assert!(semaphore.clone().try_acquire_owned().is_err());
        assert_eq!(slot.read().await.state, JobState::Running);

        notify.notify_one();
        handle.await.unwrap();

        assert!(semaphore.clone().try_acquire_owned().is_ok());
        let status = slot.read().await.clone();
        assert_ne!(status.state, JobState::Running);
    }
}
