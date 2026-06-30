//! Application wiring: state assembly, route configuration, and server lifecycle.

use std::sync::Arc;

use anyhow::Result;
use axum::{Router, extract::FromRef, routing::get};
use axum_prometheus::{
    Handle, MakeDefaultHandle, PrometheusMetricLayerBuilder,
    metrics_exporter_prometheus::PrometheusHandle,
};
use tokio::signal;
use tracing::{debug, info, warn};

use crate::chain::{BlockReader, ChainClient, NoxEventParser};
use crate::config::{Config, ReplayConfig};
use crate::handlers;
use crate::nats::{NatsClient, Publisher};
use tokio::sync::Semaphore;

/// Shared state injected into every Axum handler.
#[derive(Clone)]
pub struct AppState {
    /// Prometheus metrics handle for the `/metrics` route.
    pub metrics_handle: PrometheusHandle,
    /// Blockchain block reader for the configured chain.
    pub reader: Arc<BlockReader>,
    /// NATS JetStream publisher.
    pub publisher: Arc<Publisher>,
    /// Semaphore (1 permit) guarding against concurrent replays on the same chain.
    pub lock: Arc<Semaphore>,
    /// Replay API configuration (API key, block limits).
    pub replay: ReplayConfig,
}

impl FromRef<AppState> for PrometheusHandle {
    fn from_ref(state: &AppState) -> Self {
        state.metrics_handle.clone()
    }
}

/// Owns configuration and drives the service startup sequence.
pub struct Application {
    config: Config,
}

impl Application {
    /// Create an `Application` from loaded configuration.
    pub fn new(config: Config) -> Result<Self> {
        Ok(Self { config })
    }

    /// Initialise dependencies, wire routes, and serve until shutdown signal.
    pub async fn run(self) -> Result<()> {
        debug!("Starting ingestor replayer");
        debug!("Config: {:?}", self.config);

        let nats_client = Arc::new(NatsClient::connect(&self.config.nats).await?);
        nats_client.setup_stream(&self.config.nats).await?;

        let parser = NoxEventParser::new(self.config.chain.contract_address);
        let client = ChainClient::new(
            &self.config.chain.rpc_endpoint,
            parser.contract_address(),
            parser.event_signatures(),
            self.config.chain.connect_timeout,
            self.config.chain.rpc_timeout,
        )?;
        let reader = BlockReader::new(client, parser, &self.config.chain);

        let publisher = Publisher::new(nats_client.clone(), &self.config.nats);

        let prometheus_layer = PrometheusMetricLayerBuilder::new()
            .with_allow_patterns(&["/", "/health", "/metrics", "/replay"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());

        let app_state = AppState {
            metrics_handle,
            reader: Arc::new(reader),
            publisher: Arc::new(publisher),
            lock: Arc::new(Semaphore::new(1)),
            replay: self.config.replay.clone(),
        };

        let app = Router::new()
            .route("/", get(handlers::root))
            .route("/health", get(handlers::health_check))
            .route("/metrics", get(handlers::metrics))
            .route("/replay", axum::routing::post(handlers::replay))
            .route("/replay/status", get(handlers::replay_status))
            .fallback(handlers::not_found)
            .layer(prometheus_layer)
            .with_state(app_state);

        let binding_address = self.config.binding_address();
        info!("starting TCP server listening on {binding_address}");
        let listener = tokio::net::TcpListener::bind(binding_address).await?;

        axum::serve(listener, app)
            .with_graceful_shutdown(Self::shutdown_signal())
            .await?;

        Ok(())
    }

    /// Resolves when `SIGTERM` or `Ctrl+C` is received.
    async fn shutdown_signal() {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("Received Ctrl+C, shutting down gracefully...");
            },
            _ = terminate => {
                info!("Received SIGTERM, shutting down gracefully...");
            },
        }

        warn!("Shutdown signal received, cleaning up...");
    }
}
