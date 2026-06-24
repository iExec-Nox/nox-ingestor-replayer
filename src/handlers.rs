//! Axum server handlers for health checks and metrics.

use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Instant;
use subtle::ConstantTimeEq;

use crate::application::AppState;
use crate::error::ReplayError;
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

/// `GET /metrics` — renders Prometheus metrics as plain text.
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
                "kind": "not_found",
                "message": format!("Route not found {}", uri.path()),
                "retryable": false,
            }
        })),
    )
}

/// `GET /` — returns service name and current UTC timestamp.
pub async fn root() -> Json<Value> {
    Json(json!({ "service": "Ingestor Replayer", "timestamp": Utc::now().to_rfc3339() }))
}

/// `POST /replay` — replays a block range for a given chain.
#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    pub chain_id: u32,
    pub from_block: u64,
    pub to_block: u64,
}

/// Response from a successful replay.
#[derive(Debug, Serialize)]
pub struct ReplayResponse {
    pub chain_id: u32,
    pub from_block: u64,
    pub to_block: u64,
    pub transactions_published: u64,
    pub events_total: u64,
    pub duplicates: u64,
    pub duration_ms: u64,
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

/// Verify the `X-Api-Key` header matches `expected` in constant time.
fn check_api_key(headers: &axum::http::HeaderMap, expected: &str) -> Result<(), ReplayError> {
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

    if req.chain_id != state.chain_id {
        return Err(ReplayError::ChainNotConfigured {
            chain_id: req.chain_id,
        });
    }
    if !state.publisher.is_connected() {
        return Err(ReplayError::NatsUnavailable);
    }
    let _permit = state
        .lock
        .clone()
        .try_acquire_owned()
        .map_err(|_| ReplayError::ChainBusy {
            chain_id: req.chain_id,
        })?;

    let start = Instant::now();
    let mut transactions_published = 0u64;
    let mut events_total = 0u64;
    let mut duplicates = 0u64;
    let mut current = req.from_block;

    while current <= req.to_block {
        let batch_to = current
            .saturating_add(state.reader.batch_size() - 1)
            .min(req.to_block);

        let batch = state
            .reader
            .read_batch_bounded(current, batch_to)
            .await
            .map_err(|e| ReplayError::Rpc(e.to_string()))?;

        for tx in &batch.transactions {
            let outcome = state
                .publisher
                .publish(tx)
                .await
                .map_err(|e| ReplayError::Nats(e.to_string()))?;
            for event in &tx.events {
                log_event(event);
            }
            transactions_published += 1;
            events_total += tx.events.len() as u64;
            if outcome.duplicate {
                duplicates += 1;
            }
        }
        current = batch_to + 1;
    }

    Ok(Json(ReplayResponse {
        chain_id: req.chain_id,
        from_block: req.from_block,
        to_block: req.to_block,
        transactions_published,
        events_total,
        duplicates,
        duration_ms: start.elapsed().as_millis() as u64,
    }))
}

/// `GET /replay/{chain_id}` — returns whether the chain is currently replaying.
pub async fn replay_status(
    State(state): State<AppState>,
    Path(chain_id): Path<u32>,
) -> Result<Json<serde_json::Value>, ReplayError> {
    if chain_id != state.chain_id {
        return Err(ReplayError::ChainNotConfigured { chain_id });
    }
    let busy = state.lock.available_permits() == 0;
    Ok(Json(json!({ "chain_id": chain_id, "busy": busy })))
}
