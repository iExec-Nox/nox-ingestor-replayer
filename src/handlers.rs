//! Axum server handlers for health checks and metrics.

use axum::{
    Json,
    extract::State,
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Instant;
use subtle::ConstantTimeEq;
use tracing::warn;

use crate::application::AppState;
use crate::error::{NatsError, ReplayError};
use crate::events::log_event;

/// Health check endpoint handler.
///
/// Returns a simple "OK" response to indicate that the service is running.
/// This endpoint is typically used for health checks and service monitoring.
///
/// # Returns
///
/// JSON response containing:
/// - `status`: The status of the service ("ok")
pub async fn health_check() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// `GET /metrics` renders Prometheus metrics as plain text.
pub async fn metrics(State(metrics_handle): State<PrometheusHandle>) -> String {
    metrics_handle.render()
}

/// Fallback handler for non-existing routes.
///
/// Returns 404 NOT_FOUND to indicate the requested route does not exist.
pub async fn not_found(uri: Uri) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": {
                "message": format!("Route not found {}", uri.path()),
                "retryable": false,
            }
        })),
    )
}

/// `GET /` returns service name and current UTC timestamp.
pub async fn root() -> Json<Value> {
    Json(json!({ "service": "Ingestor Replayer", "timestamp": Utc::now().to_rfc3339() }))
}

/// Request body for `POST /replay`: the inclusive block range to replay.
#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    pub from_block: u64,
    pub to_block: u64,
}

/// Response from a successful replay.
#[derive(Debug, Serialize)]
pub struct ReplayResponse {
    pub from_block: u64,
    pub to_block: u64,
    pub transactions_published: u64,
    pub events_total: u64,
    pub duplicates: u64,
    pub duration_ms: u64,
    pub completed: bool,
    pub next_block: Option<u64>,
    pub stopped_reason: Option<String>,
}

/// Map a stop state to the `(completed, next_block)` pair for the response.
fn replay_outcome(completed: bool, next_block_if_incomplete: u64) -> (bool, Option<u64>) {
    if completed {
        (true, None)
    } else {
        (false, Some(next_block_if_incomplete))
    }
}

/// Validate that `[from, to]` is a non-empty range not exceeding `max` blocks.
fn validate_span(from: u64, to: u64, max: u64) -> Result<u64, ReplayError> {
    if from > to {
        return Err(ReplayError::InvalidRange);
    }
    let span = to
        .checked_sub(from)
        .and_then(|s| s.checked_add(1))
        .ok_or(ReplayError::InvalidRange)?;
    if span > max {
        return Err(ReplayError::RangeTooLarge { max });
    }
    Ok(span)
}

/// Reject a `to_block` that lies beyond the current chain head.
fn check_within_head(to: u64, latest: u64) -> Result<(), ReplayError> {
    if to > latest {
        return Err(ReplayError::RangeBeyondHead { to, latest });
    }
    Ok(())
}

/// Verify the `X-Api-Key` header matches `expected` in constant time.
fn check_api_key(headers: &axum::http::HeaderMap, expected: &str) -> Result<(), ReplayError> {
    if expected.is_empty() {
        return Err(ReplayError::Unauthorized);
    }
    let provided = headers
        .get("X-Api-Key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let ok =
        provided.len() == expected.len() && provided.as_bytes().ct_eq(expected.as_bytes()).into();
    if ok {
        Ok(())
    } else {
        Err(ReplayError::Unauthorized)
    }
}

/// `POST /replay` replays a block range, publishing each transaction to NATS.
///
/// Pre-flight failures return an error status. Mid-range failures stop without rollback
/// and return `200` with `completed: false` and `next_block` to resume from. Retries and
/// resumes reuse the deterministic `Nats-Msg-Id`, so JetStream dedups them.
pub async fn replay(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<ReplayRequest>,
) -> Result<Json<ReplayResponse>, ReplayError> {
    check_api_key(&headers, &state.replay.api_key)?;
    validate_span(
        req.from_block,
        req.to_block,
        state.replay.max_blocks_per_request,
    )?;

    let _permit = state
        .lock
        .clone()
        .try_acquire_owned()
        .map_err(|_| ReplayError::ChainBusy)?;

    if !state.publisher.is_connected() {
        return Err(ReplayError::Nats {
            source: NatsError::Unavailable,
        });
    }
    let latest = state
        .reader
        .latest_block()
        .await
        .map_err(|source| ReplayError::Rpc { source })?;
    check_within_head(req.to_block, latest)?;

    let start = Instant::now();
    let mut transactions_published = 0u64;
    let mut events_total = 0u64;
    let mut duplicates = 0u64;
    let mut current = req.from_block;
    let mut resume_from = req.from_block;
    let mut stopped_reason: Option<String> = None;

    'outer: while current <= req.to_block {
        if *state.shutdown.borrow() {
            warn!(from = current, "replay stopped: shutdown signal received");
            stopped_reason = Some("sigterm received".to_string());
            resume_from = current;
            break;
        }

        let batch_to = current
            .saturating_add(state.reader.batch_size().saturating_sub(1))
            .min(req.to_block);

        let batch = match state.reader.read_batch_bounded(current, batch_to).await {
            Ok(batch) => batch,
            Err(e) => {
                warn!(from = current, to = batch_to, error = %e, "replay stopped: batch read failed");
                stopped_reason = Some("rpc error".to_string());
                resume_from = current;
                break;
            }
        };

        for tx in &batch.transactions {
            if *state.shutdown.borrow() {
                warn!(
                    block = tx.block_number,
                    "replay stopped: shutdown signal received"
                );
                stopped_reason = Some("sigterm received".to_string());
                resume_from = tx.block_number;
                break 'outer;
            }

            match state.publisher.publish_with_retry(tx).await {
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
    }

    let (completed, next_block) = replay_outcome(stopped_reason.is_none(), resume_from);

    Ok(Json(ReplayResponse {
        from_block: req.from_block,
        to_block: req.to_block,
        transactions_published,
        events_total,
        duplicates,
        duration_ms: start.elapsed().as_millis() as u64,
        completed,
        next_block,
        stopped_reason,
    }))
}

/// `GET /replay/status` returns whether a replay is currently running.
pub async fn replay_status(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ReplayError> {
    let busy = state.lock.available_permits() == 0;
    Ok(Json(json!({ "busy": busy })))
}
