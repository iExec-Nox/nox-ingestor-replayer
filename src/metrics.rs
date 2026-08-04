//! Centralized metric name and shared label-key constants.
//!
//! Every pre-registration and emit site references these, so a name typo fails
//! to compile instead of silently creating a divergent, un-pre-registered series.

/// Label key shared by all per-chain metrics.
pub(crate) const CHAIN_ID: &str = "chain_id";

// NATS
pub(crate) const NATS_CONNECTION_STATE: &str = "nox_replayer.nats.connection_state";
pub(crate) const NATS_PUBLISH_RETRIES_TOTAL: &str = "nox_replayer.nats.publish_retries_total";
pub(crate) const NATS_RECONNECTS_TOTAL: &str = "nox_replayer.nats.reconnects_total";

// Build
pub(crate) const BUILD_INFO: &str = "nox_replayer.build_info";

// Replay
pub(crate) const REPLAY_BLOCKS_READ_TOTAL: &str = "nox_replayer.replay.blocks_read_total";
pub(crate) const REPLAY_DUPLICATES_TOTAL: &str = "nox_replayer.replay.duplicates_total";
pub(crate) const REPLAY_EVENTS_TOTAL: &str = "nox_replayer.replay.events_total";
pub(crate) const REPLAY_JOBS_IN_FLIGHT: &str = "nox_replayer.replay.jobs_in_flight";
pub(crate) const REPLAY_JOB_DURATION_SECONDS: &str = "nox_replayer.replay.job_duration_seconds";
pub(crate) const REPLAY_LAST_PUBLISHED_BLOCK: &str = "nox_replayer.replay.last_published_block";
pub(crate) const REPLAY_PUBLISH_ERRORS_TOTAL: &str = "nox_replayer.replay.publish_errors_total";
pub(crate) const REPLAY_REQUESTS_TOTAL: &str = "nox_replayer.replay.requests_total";
pub(crate) const REPLAY_RPC_ERRORS_TOTAL: &str = "nox_replayer.replay.rpc_errors_total";
pub(crate) const REPLAY_RPC_READ_SECONDS: &str = "nox_replayer.replay.rpc_read_seconds";
pub(crate) const REPLAY_TRANSACTIONS_PUBLISHED_TOTAL: &str =
    "nox_replayer.replay.transactions_published_total";
