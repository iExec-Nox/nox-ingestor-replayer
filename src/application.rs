use std::collections::HashMap;
use std::sync::Arc;

use axum::{Router, routing::get};
use axum_prometheus::{Handle, MakeDefaultHandle, PrometheusMetricLayerBuilder};
use tokio::signal;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::chain::{BlockReader, ChainPipeline, ChainRegistry, NoxEventParser};
use crate::config::{Config, ReplayConfig};
use crate::handlers;
use crate::nats::{NatsClient, Publisher};

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<ChainRegistry>,
    pub replay: ReplayConfig,
}

pub struct Application {
    config: Config,
}

impl Application {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        debug!("Starting ingestor replayer");
        debug!("Config: {:?}", self.config);

        // 1. Connect to NATS
        let nats_client = Arc::new(NatsClient::connect(&self.config.nats).await?);
        nats_client.setup_stream(&self.config.nats).await?;

        // 2. Create parser, block reader and publisher
        let mut pipelines = HashMap::new();
        for (chain_id, chain_cfg) in &self.config.chains {
            let parser = NoxEventParser::new(chain_cfg.contract_address);
            let reader = BlockReader::new(
                &chain_cfg.rpc_endpoint,
                parser,
                chain_cfg.batch_size,
                chain_cfg.retry_delay,
                chain_cfg.max_retries,
                chain_cfg.connect_timeout,
                chain_cfg.rpc_timeout,
                *chain_id,
            )?;
            let publisher = Publisher::new(nats_client.clone(), &self.config.nats);
            pipelines.insert(
                *chain_id,
                ChainPipeline::new(Arc::new(reader), Arc::new(publisher)),
            );
        }

        let registry = Arc::new(ChainRegistry {
            pipelines,
            global: Arc::new(Semaphore::new(self.config.replay.max_concurrent_chains)),
        });

        let app_state = AppState {
            registry,
            replay: self.config.replay.clone(),
        };

        // 4. TCP server
        let prometheus_layer = PrometheusMetricLayerBuilder::new()
            .with_allow_patterns(&["/", "/health", "/metrics", "/replay"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());
        let metrics_handle_for_route = metrics_handle.clone();

        let app = Router::new()
            .route("/", get(handlers::root))
            .route("/health", get(handlers::health_check))
            .route(
                "/metrics",
                get(move || {
                    let h = metrics_handle_for_route.clone();
                    async move { h.render() }
                }),
            )
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
            _ = ctrl_c => info!("received Ctrl+C, shutting down"),
            _ = terminate => info!("received SIGTERM, shutting down"),
        }

        warn!("shutdown signal received, cleaning up");
    }
}
