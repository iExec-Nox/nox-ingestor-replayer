use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use alloy::primitives::Address;
use config::{Config as ConfigBuilder, ConfigError, Environment};
use config_secret::EnvironmentSecretFile;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};

/// TLS certificate configuration for mTLS client authentication.
#[derive(Clone, Deserialize, Validate)]
#[validate(schema(function = "validate_tls_certs"))]
pub(crate) struct TlsConfig {
    /// Whether mTLS is enabled (`NOX_REPLAYER_NATS__TLS__ENABLED`, default `true`).
    /// Set to `false` for dev / Tenderly VM to connect to a plain NATS server.
    pub(crate) enabled: bool,
    /// CA certificate PEM content (`NOX_REPLAYER_NATS__TLS__CA`). Required when `enabled`.
    #[serde(default)]
    pub(crate) ca: String,
    /// Client certificate PEM content (`NOX_REPLAYER_NATS__TLS__CERT`). Required when `enabled`.
    #[serde(default)]
    pub(crate) cert: String,
    /// Client private key PEM content (`NOX_REPLAYER_NATS__TLS__KEY`). Required when `enabled`.
    #[serde(default)]
    pub(crate) key: String,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("enabled", &self.enabled)
            .field("ca", &format_args!("<{} bytes>", self.ca.len()))
            .field("cert", &format_args!("<{} bytes>", self.cert.len()))
            .field("key", &"<redacted>")
            .finish()
    }
}

fn validate_tls_certs(cfg: &TlsConfig) -> Result<(), ValidationError> {
    if cfg.enabled && (cfg.ca.is_empty() || cfg.cert.is_empty() || cfg.key.is_empty()) {
        return Err(
            ValidationError::new("tls_certs_required").with_message(Cow::from(
                "tls.ca, tls.cert, and tls.key must all be set when tls.enabled is true",
            )),
        );
    }
    Ok(())
}

fn validate_chains_not_empty(chains: &HashMap<u32, ChainConfig>) -> Result<(), ValidationError> {
    if chains.is_empty() {
        return Err(ValidationError::new("chains_empty").with_message(Cow::from(
            "at least one chain must be configured via NOX_REPLAYER_CHAINS__<chain_id>__*",
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize, Validate)]
pub(crate) struct Config {
    #[validate(nested)]
    #[validate(custom(function = "validate_chains_not_empty"))]
    pub(crate) chains: HashMap<u32, ChainConfig>,
    #[validate(nested)]
    pub(crate) nats: NatsConfig,
    #[validate(nested)]
    pub(crate) server: ServerConfig,
    #[validate(nested)]
    pub(crate) replay: ReplayConfig,
}

/// Configuration for the on-demand `POST /replay` endpoint.
#[derive(Clone, Deserialize, Validate)]
pub(crate) struct ReplayConfig {
    #[validate(length(min = 1))]
    pub(crate) api_key: String,
    /// Global cap on concurrently-running replay jobs across all chains (default: 20).
    #[validate(range(min = 1))]
    pub(crate) max_concurrent_replay_jobs: usize,
}

impl std::fmt::Debug for ReplayConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayConfig")
            .field("api_key", &"<redacted>")
            .field(
                "max_concurrent_replay_jobs",
                &self.max_concurrent_replay_jobs,
            )
            .finish()
    }
}

/// Chain/RPC configuration. The chain ID is the `chains` map key, not a field here.
#[derive(Debug, Clone, Deserialize, Serialize, Validate)]
#[validate(schema(function = "validate_chain_timeouts"))]
pub(crate) struct ChainConfig {
    /// RPC endpoint URL
    #[validate(url)]
    pub(crate) rpc_endpoint: String,

    /// Contract address to monitor
    #[validate(custom(function = "validate_non_zero_address"))]
    pub(crate) contract_address: Address,

    /// Number of blocks to fetch per batch. Required per chain (no default).
    /// Upper bound is provider-dependent (`eth_getLogs` range/result caps).
    #[validate(range(min = 1, max = 10000))]
    pub(crate) batch_size: u64,

    /// Delay between retries. Required per chain (no default).
    #[serde(with = "humantime_serde")]
    #[validate(custom(function = "validate_duration_non_zero"))]
    pub(crate) retry_delay: Duration,

    /// Bounded retry attempts for a failing batch read
    #[validate(range(max = 10))]
    pub(crate) max_retries: u32,

    /// TCP connection timeout. Default `5s`.
    #[serde(with = "humantime_serde", default = "default_connect_timeout")]
    #[validate(custom(function = "validate_duration_non_zero"))]
    pub(crate) connect_timeout: Duration,

    /// Total per-request RPC timeout (connect + read). Default `8s`.
    #[serde(with = "humantime_serde", default = "default_rpc_timeout")]
    #[validate(custom(function = "validate_duration_non_zero"))]
    pub(crate) rpc_timeout: Duration,
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_rpc_timeout() -> Duration {
    Duration::from_secs(8)
}

fn validate_non_zero_address(address: &Address) -> Result<(), ValidationError> {
    if *address == Address::ZERO {
        return Err(ValidationError::new("address_is_zero")
            .with_message(Cow::from("contract address must not be the zero address")));
    }
    Ok(())
}

fn validate_duration_non_zero(value: &Duration) -> Result<(), ValidationError> {
    if *value == Duration::ZERO {
        return Err(ValidationError::new("duration_zero")
            .with_message(Cow::from("duration must be greater than zero")));
    }
    Ok(())
}

fn validate_chain_timeouts(cfg: &ChainConfig) -> Result<(), ValidationError> {
    if cfg.connect_timeout > cfg.rpc_timeout {
        return Err(ValidationError::new("connect_timeout_gt_rpc_timeout")
            .with_message(Cow::from("connect_timeout must not exceed rpc_timeout")));
    }
    Ok(())
}

fn validate_nats_delays(cfg: &NatsConfig) -> Result<(), ValidationError> {
    if cfg.reconnect_delay > cfg.max_reconnect_delay {
        return Err(
            ValidationError::new("reconnect_delay_gt_max").with_message(Cow::from(
                "reconnect_delay must not exceed max_reconnect_delay",
            )),
        );
    }
    if cfg.duplicate_window > cfg.retention {
        return Err(ValidationError::new("duplicate_window_gt_retention")
            .with_message(Cow::from("duplicate_window must not exceed retention")));
    }
    Ok(())
}

#[allow(clippy::ptr_arg)]
fn validate_nats_urls(urls: &Vec<String>) -> Result<(), ValidationError> {
    if urls.is_empty() {
        return Err(ValidationError::new("nats_urls_empty")
            .with_message(Cow::from("nats.urls must contain at least one URL")));
    }
    for u in urls {
        if !u.starts_with("nats://") && !u.starts_with("tls://") {
            return Err(ValidationError::new("nats_url_invalid_scheme")
                .with_message(Cow::from("each nats url must start with nats:// or tls://")));
        }
    }
    Ok(())
}

/// NATS JetStream configuration
#[derive(Debug, Clone, Deserialize, Validate)]
#[validate(schema(function = "validate_nats_delays"))]
pub(crate) struct NatsConfig {
    /// NATS server URLs (`NOX_REPLAYER_NATS__URLS`, comma-separated)
    #[validate(custom(function = "validate_nats_urls"))]
    pub(crate) urls: Vec<String>,

    /// TLS client certificate configuration
    #[validate(nested)]
    pub(crate) tls: TlsConfig,

    /// JetStream stream replica count (`NOX_REPLAYER_NATS__NUM_REPLICAS`, default `3`)
    #[validate(range(min = 1, max = 5))]
    pub(crate) num_replicas: u32,

    /// JetStream stream name
    #[validate(length(min = 1))]
    pub(crate) stream_name: String,

    /// Subject prefix for events
    #[validate(length(min = 1))]
    pub(crate) subject: String,

    /// Stream retention (default: "1d")
    #[serde(with = "humantime_serde")]
    #[validate(custom(function = "validate_duration_non_zero"))]
    pub(crate) retention: Duration,

    /// Duplicate detection window (default: "10m")
    #[serde(with = "humantime_serde")]
    #[validate(custom(function = "validate_duration_non_zero"))]
    pub(crate) duplicate_window: Duration,

    /// Initial reconnect delay (default: 1s)
    #[serde(with = "humantime_serde")]
    #[validate(custom(function = "validate_duration_non_zero"))]
    pub(crate) reconnect_delay: Duration,

    /// Max reconnect delay (default: 30s)
    #[serde(with = "humantime_serde")]
    #[validate(custom(function = "validate_duration_non_zero"))]
    pub(crate) max_reconnect_delay: Duration,

    /// Delay between publish retries (default: "250ms")
    #[serde(with = "humantime_serde")]
    #[validate(custom(function = "validate_duration_non_zero"))]
    pub(crate) publish_retry_delay: Duration,

    /// Bounded retry attempts for a transient publish failure
    #[validate(range(max = 10))]
    pub(crate) publish_max_retries: u32,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub(crate) struct ServerConfig {
    #[validate(length(min = 1))]
    pub(crate) host: String,
    #[validate(range(min = 1))]
    pub(crate) port: u16,
}

impl Config {
    pub(crate) fn load() -> Result<Self, ConfigError> {
        let config = ConfigBuilder::builder()
            .set_default("server.host", "127.0.0.1")?
            .set_default("server.port", "8080")?
            .set_default("replay.max_concurrent_replay_jobs", 20)?
            .set_default(
                "nats.urls",
                vec![
                    "nats://localhost:4221",
                    "nats://localhost:4222",
                    "nats://localhost:4223",
                ],
            )?
            .set_default("nats.tls.enabled", true)?
            .set_default("nats.tls.ca", "")?
            .set_default("nats.tls.cert", "")?
            .set_default("nats.tls.key", "")?
            .set_default("nats.num_replicas", 3)?
            .set_default("nats.stream_name", "nox_ingestor")?
            .set_default("nats.subject", "nox_ingestor")?
            .set_default("nats.retention", "1d")?
            .set_default("nats.duplicate_window", "10m")?
            .set_default("nats.reconnect_delay", "1s")?
            .set_default("nats.max_reconnect_delay", "30s")?
            .set_default("nats.publish_retry_delay", "250ms")?
            .set_default("nats.publish_max_retries", 5)?
            .add_source(
                Environment::with_prefix("NOX_REPLAYER")
                    .prefix_separator("_")
                    .separator("__")
                    .list_separator(",")
                    .with_list_parse_key("nats.urls")
                    .try_parsing(true),
            )
            .add_source(EnvironmentSecretFile::with_prefix("NOX_REPLAYER").separator("_"))
            .build()?;

        let cfg: Config = config.try_deserialize()?;
        Ok(cfg)
    }

    /// Returns the `host:port` string used to bind the HTTP listener.
    pub(crate) fn binding_address(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }
}
