use std::path::PathBuf;
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
    pub app: AppConfig,
    pub chain: ChainConfig,
    pub nats: NatsConfig,
    pub server: ServerConfig,
}

/// Chain/RPC configuration
#[derive(Debug, Deserialize)]
pub struct ChainConfig {
    /// Chain ID (default: 421614 for Arbitrum Sepolia)
    pub chain_id: u32,

    /// RPC endpoint URL
    pub rpc_endpoint: String,

    /// Contract address to monitor
    pub contract_address: Address,

    /// Initial block to start from (0 = require state file)
    pub initial_block: u64,

    /// Number of blocks to fetch per batch (default: 50)
    pub batch_size: u64,

    /// Delay between polls (default: "500ms")
    #[serde(with = "humantime_serde")]
    pub poll_delay: Duration,

    /// Delay between retries (default: "250ms")
    #[serde(with = "humantime_serde")]
    pub retry_delay: Duration,
}

/// Application configuration
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    /// State file path (default: nox_replayer_state_421614.json)
    pub state_path: String,

    /// Flush interval (default: "5s")
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
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

    /// Wait interval (default: 1s)
    #[serde(with = "humantime_serde")]
    pub wait_interval: Duration,

    /// Message buffer capacity (default: 1000)
    pub buffer_capacity: usize,
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
            .set_default("chain.chain_id", 421614)?
            .set_default(
                "chain.rpc_endpoint",
                "https://arbitrum-sepolia-rpc.publicnode.com",
            )?
            .set_default(
                "chain.contract_address",
                "0x0000000000000000000000000000000000000000",
            )?
            .set_default("chain.initial_block", 0)?
            .set_default("chain.batch_size", 50)?
            .set_default("chain.poll_delay", "500ms")?
            .set_default("chain.retry_delay", "250ms")?
            .set_default("app.flush_interval", "5s")?
            .set_default("app.state_path", "nox_replayer_state_421614.json")?
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
            .set_default("nats.buffer_capacity", 1000)?
            .set_default("nats.wait_interval", "1s")?
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

        config.try_deserialize()
    }

    /// Returns the `host:port` string used to bind the HTTP listener.
    pub fn binding_address(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    /// Get the state file path, using default if not specified
    pub fn state_file_path(&self) -> PathBuf {
        if self.app.state_path.is_empty() {
            PathBuf::from(format!("./nox_replayer_state_{}.json", self.chain.chain_id))
        } else {
            PathBuf::from(&self.app.state_path)
        }
    }
}
