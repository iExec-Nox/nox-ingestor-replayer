//! Axum server handlers for health checks and metrics.

use axum::{
    Json,
    extract::{Query, State},
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use chrono::Utc;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use subtle::ConstantTimeEq;

use crate::application::AppState;
use crate::error::{NatsError, ReplayError};
use crate::replay::{
    ChainQuery, ReplayAccepted, ReplayBody, ReplayJobStatus, ReplayRequest, StatusResponse,
    run_replay_job,
};

/// Health check endpoint handler.
///
/// Returns a simple "OK" response to indicate that the service is running.
/// This endpoint is typically used for health checks and service monitoring.
///
/// # Returns
///
/// JSON response containing:
/// - `status`: The status of the service ("ok")
pub(crate) async fn health_check() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// `GET /metrics` — renders Prometheus metrics as plain text.
pub(crate) async fn metrics(State(metrics_handle): State<PrometheusHandle>) -> String {
    metrics_handle.render()
}

/// Fallback handler for non-existing routes.
///
/// Returns 404 NOT_FOUND to indicate the requested route does not exist.
pub(crate) async fn not_found(uri: Uri) -> impl IntoResponse {
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
pub(crate) async fn root() -> Json<Value> {
    Json(json!({ "service": "Ingestor Replayer", "timestamp": Utc::now().to_rfc3339() }))
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
    // Length is compared in the clear before `ct_eq`: a deliberate, accepted
    // leak (internal fixed-length key, so its length carries negligible info).
    if provided.len() == expected.len() && provided.as_bytes().ct_eq(expected.as_bytes()).into() {
        Ok(())
    } else {
        Err(ReplayError::Unauthorized)
    }
}

/// `POST /replay?chain_id=<id>` accepts a chain ID (query parameter) and a
/// block range (body) and replays it in a background task, publishing each
/// transaction to NATS.
///
/// Pre-flight failures (auth, validation, unknown chain, busy, at-capacity,
/// NATS/RPC pre-checks) return an error status synchronously. Once accepted,
/// the replay runs asynchronously; progress and the terminal outcome are
/// available via `GET /replay/status?chain_id=<id>`. Retries and resumes
/// reuse the deterministic `Nats-Msg-Id`, so JetStream dedups them.
pub(crate) async fn replay(
    State(state): State<AppState>,
    Query(query): Query<ChainQuery>,
    headers: axum::http::HeaderMap,
    Json(body): Json<ReplayBody>,
) -> Result<(StatusCode, Json<ReplayAccepted>), ReplayError> {
    check_api_key(&headers, &state.replay.api_key)?;
    let chain_id = query.chain_id.ok_or(ReplayError::MissingChainId)?;
    validate_span(body.from_block, body.to_block)?;

    let req = ReplayRequest {
        chain_id,
        range: body,
    };

    let pipeline = state
        .registry
        .get(req.chain_id)
        .ok_or(ReplayError::ChainNotConfigured {
            chain_id: req.chain_id,
        })?;

    let chain_permit =
        pipeline
            .lock
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReplayError::ChainBusy {
                chain_id: req.chain_id,
            })?;
    let global_permit = state
        .registry
        .global
        .clone()
        .try_acquire_owned()
        .map_err(|_| ReplayError::AtCapacity {
            max: state.replay.max_concurrent_chains,
        })?;

    if !pipeline.publisher.is_connected() {
        return Err(ReplayError::Nats {
            source: NatsError::Unavailable,
        });
    }
    let latest = pipeline
        .reader
        .latest_block()
        .await
        .map_err(|source| ReplayError::Rpc { source })?;
    check_within_head(req.range.to_block, latest)?;

    *pipeline.job_status.write().await =
        ReplayJobStatus::running(req.range.from_block, req.range.to_block);

    let handle = tokio::spawn(run_replay_job(
        pipeline.reader.clone(),
        pipeline.publisher.clone(),
        state.shutdown.clone(),
        (chain_permit, global_permit),
        pipeline.job_status.clone(),
        req.range.from_block,
        req.range.to_block,
    ));
    // Poisoning is all but impossible: the guard is never held across an `.await` and no holder panics.
    *pipeline
        .replay_task
        .lock()
        .expect("replay_task mutex poisoned") = Some(handle);

    Ok((
        StatusCode::ACCEPTED,
        Json(ReplayAccepted {
            request: req,
            accepted_at: Utc::now(),
        }),
    ))
}

/// `GET /replay/status` returns the current (or most recent) replay job
/// status. With `?chain_id=<id>`, returns just that chain's status
/// (deliberately NOT a `ReplayError` — an unknown chain here is a plain 404,
/// not a request outcome we want folded into replay request metrics).
/// Without it, returns every configured chain's status as a JSON object
/// keyed by chain_id.
pub(crate) async fn replay_status(
    State(state): State<AppState>,
    Query(query): Query<ChainQuery>,
) -> Result<Json<StatusResponse>, (StatusCode, Json<Value>)> {
    match query.chain_id {
        Some(chain_id) => {
            let pipeline = state.registry.get(chain_id).ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({
                        "error": {
                            "kind": "chain_not_configured",
                            "message": format!("chain {chain_id} not configured"),
                            "retryable": false,
                        }
                    })),
                )
            })?;
            let status = pipeline.job_status.read().await.clone();
            Ok(Json(StatusResponse::Single(status)))
        }
        None => {
            let mut out = BTreeMap::new();
            for (&chain_id, pipeline) in &state.registry.pipelines {
                out.insert(chain_id, pipeline.job_status.read().await.clone());
            }
            Ok(Json(StatusResponse::All(out)))
        }
    }
}
