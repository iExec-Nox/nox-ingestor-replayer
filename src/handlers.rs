//! Axum server handlers for health checks and metrics.

use axum::{
    Json,
    extract::State,
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;

use crate::application::AppState;
use crate::error::{NatsError, ReplayError};
use crate::replay::{ReplayJobStatus, run_replay_job};

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

/// Request body for `POST /replay`: the inclusive block range to replay.
#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    pub from_block: u64,
    pub to_block: u64,
}

/// `202` body for `POST /replay`: the replay has been accepted and is running in the background.
#[derive(Debug, Serialize)]
pub struct ReplayAccepted {
    pub from_block: u64,
    pub to_block: u64,
    pub accepted_at: DateTime<Utc>,
}

/// Validate that `from <= to`.
fn validate_span(from: u64, to: u64) -> Result<(), ReplayError> {
    if from > to {
        return Err(ReplayError::InvalidRange);
    }
    Ok(())
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

/// `POST /replay` accepts a block range and replays it in a background task,
/// publishing each transaction to NATS.
///
/// Pre-flight failures (auth, validation, busy, NATS/RPC pre-checks) return an
/// error status synchronously. Once accepted, the replay runs asynchronously;
/// progress and the terminal outcome are available via `GET /replay/status`.
/// Retries and resumes reuse the deterministic `Nats-Msg-Id`, so JetStream dedups them.
pub async fn replay(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<ReplayRequest>,
) -> Result<(StatusCode, Json<ReplayAccepted>), ReplayError> {
    check_api_key(&headers, &state.replay.api_key)?;
    validate_span(req.from_block, req.to_block)?;

    let permit = state
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

    *state.job_status.write().await = ReplayJobStatus::running(req.from_block, req.to_block);

    let handle = tokio::spawn(run_replay_job(
        state.reader.clone(),
        state.publisher.clone(),
        state.shutdown.clone(),
        permit,
        state.job_status.clone(),
        req.from_block,
        req.to_block,
    ));
    *state.replay_task.lock().unwrap() = Some(handle);

    Ok((
        StatusCode::ACCEPTED,
        Json(ReplayAccepted {
            from_block: req.from_block,
            to_block: req.to_block,
            accepted_at: Utc::now(),
        }),
    ))
}

/// `GET /replay/status` returns the current (or most recent) replay job status.
pub async fn replay_status(
    State(state): State<AppState>,
) -> Result<Json<ReplayJobStatus>, ReplayError> {
    let status = state.job_status.read().await.clone();
    Ok(Json(status))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_span_rejects_from_greater_than_to() {
        let result = validate_span(5, 3);
        assert!(matches!(result, Err(ReplayError::InvalidRange)));
    }

    #[test]
    fn validate_span_accepts_from_equal_to() {
        assert!(validate_span(5, 5).is_ok());
    }

    #[test]
    fn validate_span_accepts_full_range_without_cap() {
        assert!(validate_span(0, u64::MAX).is_ok());
    }

    #[test]
    fn replay_accepted_serializes_accepted_at_as_rfc3339() {
        let body = ReplayAccepted {
            from_block: 10,
            to_block: 20,
            accepted_at: Utc::now(),
        };
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["from_block"], 10);
        assert_eq!(value["to_block"], 20);
        let accepted_at = value["accepted_at"].as_str().unwrap();
        assert!(DateTime::parse_from_rfc3339(accepted_at).is_ok());
    }
}
