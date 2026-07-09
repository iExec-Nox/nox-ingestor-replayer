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

/// Run a replay job to completion, writing progress into `slot` as it goes.
///
/// `permit` is held for the lifetime of the job and released (freeing the
/// slot for a subsequent `POST /replay`) when this function returns.
///
/// On panic the permit still drops (409 recovers) but the slot is left in its
/// last `Running` state until the next job overwrites it — a panic here is a
/// bug, not a modeled state.
#[allow(dead_code)]
pub(crate) async fn run_replay_job(
    source: Arc<BlockReader>,
    sink: Arc<Publisher>,
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
