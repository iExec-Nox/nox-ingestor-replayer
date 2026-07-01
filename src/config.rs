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
    pub chain: ChainConfig,
    pub nats: NatsConfig,
    pub server: ServerConfig,
    pub replay: ReplayConfig,
}

/// Chain/RPC configuration
#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    /// Chain ID (default: 421614 for Arbitrum Sepolia)
    pub chain_id: u32,

    /// RPC endpoint URL
    pub rpc_endpoint: String,

    /// Contract address to monitor
    pub contract_address: Address,

    /// Number of blocks to fetch per batch (default: 50)
    pub batch_size: u64,

    /// Delay between retries (default: "250ms")
    #[serde(with = "humantime_serde")]
    pub retry_delay: Duration,

    /// Bounded retry attempts for a failing batch read
    pub max_retries: u32,

    /// TCP connection timeout (default: `8s`)
    #[serde(with = "humantime_serde", default = "default_connect_timeout")]
    pub connect_timeout: Duration,

    /// Total per-request RPC timeout (default: `8s`)
    #[serde(with = "humantime_serde", default = "default_rpc_timeout")]
    pub rpc_timeout: Duration,
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_rpc_timeout() -> Duration {
    Duration::from_secs(8)
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

    /// Delay between bounded publish retries on a transient NATS failure (default: 250ms).
    #[serde(with = "humantime_serde")]
    pub publish_retry_delay: Duration,

    /// Bounded retry attempts for a transient publish failure (default: 5).
    pub publish_max_retries: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Deserialize)]
pub struct ReplayConfig {
    pub api_key: String,
    pub max_blocks_per_request: u64,
}

impl std::fmt::Debug for ReplayConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayConfig")
            .field("api_key", &"<redacted>")
            .field("max_blocks_per_request", &self.max_blocks_per_request)
            .finish()
    }
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let config = ConfigBuilder::builder()
            .set_default("server.host", "127.0.0.1")?
            .set_default("server.port", "8080")?
            .set_default("chain.chain_id", 421614)?
            .set_default(
                "chain.rpc_endpoint",
                "https://arbitrum-sepolia-rpc.publicnode.com",
            )?
            .set_default(
                "chain.contract_address",
                "0x0000000000000000000000000000000000000000",
            )?
            .set_default("chain.batch_size", 50)?
            .set_default("chain.retry_delay", "250ms")?
            .set_default("chain.max_retries", 5)?
            .set_default("chain.connect_timeout", "10s")?
            .set_default("chain.rpc_timeout", "30s")?
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
            .set_default("replay.max_blocks_per_request", 5000)?
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
