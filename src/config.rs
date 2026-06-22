use std::collections::HashMap;
use std::time::Duration;

use alloy::primitives::Address;
use config::{Config as ConfigBuilder, ConfigError, Environment};
use config_secret::EnvironmentSecretFile;
use serde::Deserialize;

/// TLS certificate configuration for mTLS client authentication.
#[derive(Clone, Deserialize)]
pub struct TlsConfig {
    /// Whether mTLS is enabled (`NOX_REPLAYER_NATS__TLS__ENABLED`, default `true`).
    /// Set to `false` for dev / Tenderly VM to connect to a plain NATS server.
    pub enabled: bool,
    /// CA certificate PEM content (`NOX_REPLAYER_NATS__TLS__CA`). Required when `enabled`.
    #[serde(default)]
    pub ca: String,
    /// Client certificate PEM content (`NOX_REPLAYER_NATS__TLS__CERT`). Required when `enabled`.
    #[serde(default)]
    pub cert: String,
    /// Client private key PEM content (`NOX_REPLAYER_NATS__TLS__KEY`). Required when `enabled`.
    #[serde(default)]
    pub key: String,
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

#[derive(Debug, Deserialize)]
pub struct Config {
    pub chains: HashMap<u32, ChainConfig>,
    pub nats: NatsConfig,
    pub replay: ReplayConfig,
    pub server: ServerConfig,
}

#[derive(Clone, Deserialize)]
pub struct ReplayConfig {
    /// Shared API key for X-Api-Key auth
    pub api_key: String,
    /// Max blocks a single /replay request may span (to - from + 1). Default 5000.
    pub max_blocks_per_request: u64,
    /// Max chains replaying concurrently across the process. Default 20.
    pub max_concurrent_chains: usize,
}

impl std::fmt::Debug for ReplayConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayConfig")
            .field("api_key", &"<redacted>")
            .field("max_blocks_per_request", &self.max_blocks_per_request)
            .field("max_concurrent_chains", &self.max_concurrent_chains)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    /// RPC endpoint URL
    pub rpc_endpoint: String,

    /// Contract address to monitor
    pub contract_address: Address,

    /// Number of blocks to fetch per batch
    pub batch_size: u64,

    /// Delay between retries
    #[serde(with = "humantime_serde")]
    pub retry_delay: Duration,

    /// Bounded retry attempts for a failing batch read
    pub max_retries: u32,

    /// TCP connect timeout (handshake only). Default `10s`.
    #[serde(with = "humantime_serde", default = "default_connect_timeout")]
    pub connect_timeout: Duration,

    /// Total per-request RPC timeout (connect + read). Default `30s`.
    #[serde(with = "humantime_serde", default = "default_rpc_timeout")]
    pub rpc_timeout: Duration,
}

fn default_rpc_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

/// NATS JetStream configuration
#[derive(Debug, Clone, Deserialize)]
pub struct NatsConfig {
    /// NATS server URLs (`NOX_REPLAYER_NATS__URLS`, comma-separated)
    pub urls: Vec<String>,

    /// TLS client certificate configuration
    pub tls: TlsConfig,

    /// JetStream stream replica count (`NOX_REPLAYER_NATS__NUM_REPLICAS`, default `3`)
    pub num_replicas: u32,

    /// JetStream stream name
    pub stream_name: String,

    /// Subject prefix for events
    pub subject: String,

    /// Stream retention (default: "1d")
    #[serde(with = "humantime_serde")]
    pub retention: Duration,

    /// Duplicate detection window (default: "10m")
    #[serde(with = "humantime_serde")]
    pub duplicate_window: Duration,

    /// Initial reconnect delay (default: 1s)
    #[serde(with = "humantime_serde")]
    pub reconnect_delay: Duration,

    /// Max reconnect delay (default: 30s)
    #[serde(with = "humantime_serde")]
    pub max_reconnect_delay: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let config = ConfigBuilder::builder()
            .set_default("server.host", "127.0.0.1")?
            .set_default("server.port", "8080")?
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
            .set_default("replay.max_blocks_per_request", 5000)?
            .set_default("replay.max_concurrent_chains", 20)?
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
    pub fn binding_address(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }
}
