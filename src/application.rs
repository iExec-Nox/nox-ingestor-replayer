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
    metrics_exporter_prometheus::PrometheusHandle,
};
use tokio::signal;
use tokio::sync::{RwLock, Semaphore, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::chain::{BlockReader, ChainClient, NoxEventParser};
use crate::config::Config;
use crate::handlers;
use crate::nats::{NatsClient, Publisher};
use crate::replay::ReplayJobStatus;

/// Grace period to await an in-flight replay job before the NATS connection
/// drops on shutdown. Hardcoded, not a config knob (deliberate).
const REPLAY_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) metrics_handle: PrometheusHandle,
    pub(crate) reader: Arc<BlockReader>,
    pub(crate) publisher: Arc<Publisher>,
    pub(crate) lock: Arc<Semaphore>,
    pub(crate) api_key: String,
    pub(crate) shutdown: watch::Receiver<bool>,
    pub(crate) job_status: Arc<RwLock<ReplayJobStatus>>,
    pub(crate) replay_task: Arc<Mutex<Option<JoinHandle<()>>>>,
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
            .with_allow_patterns(&["/", "/health", "/metrics", "/replay", "/replay/status"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let app_state = AppState {
            metrics_handle,
            reader: Arc::new(reader),
            publisher: Arc::new(publisher),
            lock: Arc::new(Semaphore::new(1)),
            api_key: self.config.api_key.clone(),
            shutdown: shutdown_rx,
            job_status: Arc::new(RwLock::new(ReplayJobStatus::default())),
            replay_task: Arc::new(Mutex::new(None)),
        };
        let replay_task = app_state.replay_task.clone();

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

        await_replay_task(&replay_task, REPLAY_SHUTDOWN_GRACE).await;

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
