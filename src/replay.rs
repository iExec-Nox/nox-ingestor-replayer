//! Async replay job execution and status tracking.

use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, RwLock, watch};
use tracing::warn;

use crate::chain::BlockReader;
use crate::events::log_event;
use crate::nats::Publisher;

/// Lifecycle state of the single replay job slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum JobState {
    Idle,
    Running,
    Completed,
    Stopped,
}

/// Snapshot of the current (or most recent) replay job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplayJobStatus {
    pub(crate) state: JobState,
    pub(crate) from_block: u64,
    pub(crate) to_block: u64,
    pub(crate) current_block: u64,
    pub(crate) transactions_published: u64,
    pub(crate) events_total: u64,
    pub(crate) duplicates: u64,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
    pub(crate) duration_ms: u64,
    pub(crate) stopped_reason: Option<String>,
}

/// Run a replay job to completion, writing progress into `status` as it goes.
///
/// `permit` is held for the lifetime of the job and dropped when this
/// function returns, freeing the concurrency slot for another job.
#[allow(dead_code)]
pub(crate) async fn run_replay_job(
    source: Arc<BlockReader>,
    sink: Arc<Publisher>,
    shutdown: watch::Receiver<bool>,
    permit: OwnedSemaphorePermit,
    status: Arc<RwLock<ReplayJobStatus>>,
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

        let mut slot = status.write().await;
        slot.current_block = current;
        slot.transactions_published = transactions_published;
        slot.events_total = events_total;
        slot.duplicates = duplicates;
        slot.duration_ms = start.elapsed().as_millis() as u64;
    }

    let mut slot = status.write().await;
    slot.transactions_published = transactions_published;
    slot.events_total = events_total;
    slot.duplicates = duplicates;
    slot.duration_ms = start.elapsed().as_millis() as u64;
    slot.finished_at = Some(Utc::now());
    match stopped_reason {
        None => {
            slot.state = JobState::Completed;
            slot.current_block = to_block.saturating_add(1);
            slot.stopped_reason = None;
        }
        Some(reason) => {
            slot.state = JobState::Stopped;
            slot.current_block = resume_from;
            slot.stopped_reason = Some(reason);
        }
    }
    drop(slot);

    // Release the permit now that the terminal status has been written to the slot.
    drop(permit);
}
