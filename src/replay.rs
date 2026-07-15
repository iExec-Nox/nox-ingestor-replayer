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

/// Why a replay job is no longer running. Defaults to `Completed`, the
/// non-failure case, and is overwritten with the specific cause on failure.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StopReason {
    #[default]
    Completed,
    ShutdownSignal,
    RpcError,
    NatsError,
}

/// Snapshot of the current (or most recent) replay job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplayJobStatus {
    pub(crate) state: JobState,
    pub(crate) from_block: u64,
    pub(crate) to_block: u64,
    #[serde(flatten)]
    pub(crate) progress: ReplayProgress,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
    pub(crate) stop_reason: StopReason,
}

/// Mutable progress counters accumulated while a replay job runs.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct ReplayProgress {
    pub(crate) current_block: u64,
    pub(crate) transactions_published: u64,
    pub(crate) events_total: u64,
    pub(crate) duplicates: u64,
    pub(crate) duration_ms: u64,
}

impl ReplayProgress {
    /// Copy the running counters into the shared status slot.
    fn apply_to(&self, slot: &mut ReplayJobStatus) {
        slot.progress = *self;
    }
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
    let mut progress = ReplayProgress {
        current_block: from_block,
        ..Default::default()
    };
    let mut current = from_block;
    let mut resume_from = from_block;
    let mut stop_reason = StopReason::Completed;

    'outer: while current <= to_block {
        if *shutdown.borrow() {
            warn!(from = current, "replay stopped: shutdown signal received");
            stop_reason = StopReason::ShutdownSignal;
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
                stop_reason = StopReason::RpcError;
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
                stop_reason = StopReason::ShutdownSignal;
                resume_from = tx.block_number;
                break 'outer;
            }

            match sink.publish_with_retry(tx).await {
                Ok(outcome) => {
                    for event in &tx.events {
                        log_event(event);
                    }
                    progress.transactions_published += 1;
                    progress.events_total += tx.events.len() as u64;
                    if outcome.duplicate {
                        progress.duplicates += 1;
                    }
                }
                Err(e) => {
                    warn!(block = tx.block_number, error = %e, "replay stopped: publish failed");
                    stop_reason = StopReason::NatsError;
                    // Resume from the failed block; already-acked blocks are not re-read.
                    resume_from = tx.block_number;
                    break 'outer;
                }
            }
        }

        current = batch_to.saturating_add(1);
        progress.current_block = current;
        progress.duration_ms = start.elapsed().as_millis() as u64;

        let mut slot = status.write().await;
        progress.apply_to(&mut slot);
    }

    progress.duration_ms = start.elapsed().as_millis() as u64;

    let mut slot = status.write().await;
    progress.apply_to(&mut slot);
    slot.finished_at = Some(Utc::now());
    slot.stop_reason = stop_reason;
    if stop_reason == StopReason::Completed {
        slot.state = JobState::Completed;
        slot.progress.current_block = to_block.saturating_add(1);
    } else {
        slot.state = JobState::Stopped;
        slot.progress.current_block = resume_from;
    }
    drop(slot);

    // Release the permit now that the terminal status has been written to the slot.
    drop(permit);
}
