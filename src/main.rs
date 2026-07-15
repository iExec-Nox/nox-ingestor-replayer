use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::application::Application;
use crate::config::Config;

pub mod application;
pub mod chain;
pub mod config;
pub mod error;
pub mod events;
pub mod handlers;
pub mod nats;
pub mod replay;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    async_nats::rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls ring crypto provider");

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::load().map_err(|e| {
        error!("Failed to load configuration: {e}");
        e
    })?;

    let app = Application::new(config)?;
    app.run().await?;

    Ok(())
}
