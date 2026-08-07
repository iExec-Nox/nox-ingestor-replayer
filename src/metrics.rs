//! Centralized metric name and shared label-key constants.
//!
//! Every pre-registration and emit site references these, so a name typo fails
//! to compile instead of silently creating a divergent, un-pre-registered series.

/// Label key shared by all per-chain metrics.
pub(crate) const CHAIN_ID: &str = "chain_id";

// NATS
pub(crate) const NATS_CONNECTION_STATE: &str = "nox_replayer_nats_connection_state";
pub(crate) const NATS_PUBLISH_RETRIES_TOTAL: &str = "nox_replayer_nats_publish_retries_total";
pub(crate) const NATS_RECONNECTS_TOTAL: &str = "nox_replayer_nats_reconnects_total";

// Build
pub(crate) const BUILD_INFO: &str = "nox_replayer_build_info";

// Replay
pub(crate) const REPLAY_BLOCKS_READ_TOTAL: &str = "nox_replayer_replay_blocks_read_total";
pub(crate) const REPLAY_EVENTS_TOTAL: &str = "nox_replayer_replay_events_total";
pub(crate) const REPLAY_JOBS_IN_FLIGHT: &str = "nox_replayer_replay_jobs_in_flight";
pub(crate) const REPLAY_JOBS_DURATION_SECONDS: &str = "nox_replayer_replay_jobs_duration_seconds";
pub(crate) const REPLAY_LAST_PUBLISHED_BLOCK: &str = "nox_replayer_replay_last_published_block";
pub(crate) const REPLAY_PUBLISH_REQUESTS_TOTAL: &str = "nox_replayer_replay_publish_requests_total";
pub(crate) const REPLAY_REQUESTS_TOTAL: &str = "nox_replayer_replay_requests_total";
pub(crate) const REPLAY_RPC_ERRORS_TOTAL: &str = "nox_replayer_replay_rpc_errors_total";
pub(crate) const REPLAY_RPC_READS_SECONDS: &str = "nox_replayer_replay_rpc_reads_seconds";
