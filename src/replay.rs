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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) enum JobState {
    #[default]
    Idle,
    Running,
    Completed,
    Stopped,
}

/// Why a replay job reached its terminal state. `None` only while the job is
/// idle or running; `Some(_)` for every terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StopReason {
    Completed,
    ShutdownSignal,
    RpcError,
    NatsError,
}

/// Snapshot of the current (or most recent) replay job.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct ReplayJobStatus {
    pub(crate) state: JobState,
    pub(crate) from_block: u64,
    pub(crate) to_block: u64,
    #[serde(flatten)]
    pub(crate) progress: ReplayProgress,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
    pub(crate) stop_reason: Option<StopReason>,
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

impl ReplayJobStatus {
    /// Fresh `Running` status; used by the POST handler before spawning the job.
    pub(crate) fn running(from_block: u64, to_block: u64) -> Self {
        Self {
            state: JobState::Running,
            from_block,
            to_block,
            progress: ReplayProgress {
                current_block: from_block,
                ..Default::default()
            },
            started_at: Some(Utc::now()),
            ..Default::default()
        }
    }
}

/// Query parameter carrying the target chain id. `POST /replay` requires it
/// (the handler rejects `None` with `ReplayError::MissingChainId`);
/// `GET /replay/status` treats `None` as "every configured chain".
#[derive(Debug, Deserialize)]
pub(crate) struct ChainQuery {
    pub(crate) chain_id: Option<u32>,
}

/// JSON body for `POST /replay`: the inclusive block range to replay.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReplayBody {
    pub(crate) from_block: u64,
    pub(crate) to_block: u64,
}

/// Echoed back in `ReplayAccepted`: the chain and block range that were accepted.
/// The range fields are flattened, so this still serializes as
/// `{chain_id, from_block, to_block}`.
#[derive(Debug, Serialize)]
pub(crate) struct ReplayRequest {
    pub(crate) chain_id: u32,
    #[serde(flatten)]
    pub(crate) range: ReplayBody,
}

/// `202` body for `POST /replay`: the replay has been accepted and is running in the background.
#[derive(Debug, Serialize)]
pub(crate) struct ReplayAccepted {
    pub(crate) request: ReplayRequest,
    pub(crate) accepted_at: DateTime<Utc>,
}

/// Run a replay job to completion, writing progress into `status` as it goes.
///
/// `hold` is held for the lifetime of the job and dropped (releasing the
/// per-chain and global permits it wraps) when this function returns.
///
/// On panic `hold` still drops (409/503 recovers) but the slot is left in its
/// last `Running` state until the next job overwrites it — a panic here is a
/// bug, not a modeled state.
pub(crate) async fn run_replay_job(
    source: Arc<BlockReader>,
    sink: Arc<Publisher>,
    shutdown: watch::Receiver<bool>,
    hold: (OwnedSemaphorePermit, OwnedSemaphorePermit),
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
    let mut stop_reason: Option<StopReason> = None;

    'outer: while current <= to_block {
        if *shutdown.borrow() {
            warn!(from = current, "replay stopped: shutdown signal received");
            stop_reason = Some(StopReason::ShutdownSignal);
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
                stop_reason = Some(StopReason::RpcError);
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
                stop_reason = Some(StopReason::ShutdownSignal);
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
                    stop_reason = Some(StopReason::NatsError);
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
    match stop_reason {
        None => {
            slot.state = JobState::Completed;
            slot.stop_reason = Some(StopReason::Completed);
            slot.progress.current_block = to_block.saturating_add(1);
        }
        Some(reason) => {
            slot.state = JobState::Stopped;
            slot.stop_reason = Some(reason);
            slot.progress.current_block = resume_from;
        }
    }

    drop(slot);

    // Release the held permit(s) now that the terminal status has been written to the slot.
    drop(hold);
}
