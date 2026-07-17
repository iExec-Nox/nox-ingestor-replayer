use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use axum::{
    Router,
    extract::FromRef,
    routing::{get, post},
};
use axum_prometheus::{
    Handle, MakeDefaultHandle, PrometheusMetricLayerBuilder,
    metrics::{counter, gauge},
    metrics_exporter_prometheus::PrometheusHandle,
};
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::chain::{BlockReader, ChainClient, ChainPipeline, ChainRegistry, NoxEventParser};
use crate::config::{Config, ReplayConfig};
use crate::handlers;
use crate::nats::{NatsClient, Publisher};

/// Grace period to await an in-flight replay job before the NATS connection
/// drops on shutdown. Hardcoded, not a config knob (deliberate).
const REPLAY_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) metrics_handle: PrometheusHandle,
    pub(crate) registry: Arc<ChainRegistry>,
    pub(crate) replay: ReplayConfig,
    pub(crate) shutdown: watch::Receiver<bool>,
}

impl FromRef<AppState> for PrometheusHandle {
    fn from_ref(state: &AppState) -> Self {
        state.metrics_handle.clone()
    }
}

pub(crate) struct Application {
    config: Config,
}

impl Application {
    pub(crate) fn new(config: Config) -> Result<Self> {
        Ok(Self { config })
    }

    pub(crate) async fn run(self) -> Result<()> {
        debug!("Starting ingestor replayer");
        debug!("Config: {:?}", self.config);

        let prometheus_layer = PrometheusMetricLayerBuilder::new()
            .with_allow_patterns(&["/", "/health", "/metrics", "/replay", "/replay/status"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());

        gauge!("nox_replayer.nats.connection_state").set(0.0);
        counter!("nox_replayer.nats.reconnects_total").absolute(0);
        counter!("nox_replayer.nats.publish_retries_total").absolute(0);
        gauge!("nox_replayer.build_info", "version" => env!("CARGO_PKG_VERSION")).set(1.0);
        for chain_id in self.config.chains.keys() {
            let cid = chain_id.to_string();
            counter!("nox_replayer.replay.transactions_published_total", "chain_id" => cid.clone())
                .absolute(0);
            counter!("nox_replayer.replay.events_total", "chain_id" => cid.clone()).absolute(0);
            counter!("nox_replayer.replay.duplicates_total", "chain_id" => cid.clone()).absolute(0);
            counter!("nox_replayer.replay.blocks_read_total", "chain_id" => cid.clone())
                .absolute(0);
            counter!("nox_replayer.replay.rpc_errors_total", "chain_id" => cid.clone()).absolute(0);
            counter!("nox_replayer.replay.publish_errors_total", "chain_id" => cid.clone())
                .absolute(0);
            gauge!("nox_replayer.replay.jobs_in_flight", "chain_id" => cid).set(0.0);
        }

        let nats_client = Arc::new(NatsClient::connect(&self.config.nats).await?);
        nats_client.setup_stream(&self.config.nats).await?;

        let mut pipelines = HashMap::with_capacity(self.config.chains.len());
        for (chain_id, chain_config) in &self.config.chains {
            let parser = NoxEventParser::new(chain_config.contract_address);
            let client = ChainClient::new(
                &chain_config.rpc_endpoint,
                parser.contract_address(),
                parser.event_signatures(),
                chain_config.connect_timeout,
                chain_config.rpc_timeout,
            )?;
            let reader = BlockReader::new(client, parser, chain_config, *chain_id);
            let publisher = Publisher::new(nats_client.clone(), &self.config.nats);
            pipelines.insert(*chain_id, ChainPipeline::new(reader, publisher));
        }
        let registry = Arc::new(ChainRegistry::new(
            pipelines,
            self.config.replay.max_concurrent_replay_jobs,
        ));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let app_state = AppState {
            metrics_handle,
            registry: registry.clone(),
            replay: self.config.replay.clone(),
            shutdown: shutdown_rx,
        };

        let app = Router::new()
            .route("/", get(handlers::root))
            .route("/health", get(handlers::health_check))
            .route("/metrics", get(handlers::metrics))
            .route("/replay", post(handlers::replay))
            .route("/replay/status", get(handlers::replay_status))
            .fallback(handlers::not_found)
            .layer(prometheus_layer)
            .with_state(app_state);

        let binding_address = self.config.binding_address();
        info!("starting TCP server listening on {binding_address}");
        let listener = tokio::net::TcpListener::bind(binding_address).await?;

        axum::serve(listener, app)
            .with_graceful_shutdown(Self::shutdown_signal(shutdown_tx))
            .await?;

        // Await every chain's in-flight replay task concurrently
        let drains: Vec<_> = registry
            .pipelines
            .values()
            .map(|pipeline| {
                let replay_task = pipeline.replay_task.clone();
                tokio::spawn(async move {
                    await_replay_task(&replay_task, REPLAY_SHUTDOWN_GRACE).await;
                })
            })
            .collect();
        for drain in drains {
            let _ = drain.await;
        }

        Ok(())
    }

    async fn shutdown_signal(tx: watch::Sender<bool>) {
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

        let _ = tx.send(true);
        warn!("Shutdown signal received, cleaning up...");
    }
}

/// Await the in-flight replay task (if any) with a bounded grace period,
/// aborting it if it hasn't finished by then. No-op if no task is stored.
async fn await_replay_task(slot: &Mutex<Option<JoinHandle<()>>>, grace: Duration) {
    // Poisoning is all but impossible: the guard is never held across an `.await` and no holder panics.
    let handle = slot.lock().expect("replay_task mutex poisoned").take();
    let Some(handle) = handle else {
        return;
    };
    let abort_handle = handle.abort_handle();

    match tokio::time::timeout(grace, handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "replay task panicked during shutdown"),
        Err(_) => {
            warn!(
                grace_secs = grace.as_secs(),
                "replay task did not finish within shutdown grace period, aborting"
            );
            abort_handle.abort();
        }
    }
}
