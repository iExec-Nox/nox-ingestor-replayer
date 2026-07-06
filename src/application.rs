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
use crate::config::Config;
use crate::events::{Operator, TransactionEvent};
use crate::handlers;
use crate::nats::{NatsClient, Publisher};

#[derive(Clone)]
pub struct AppState {
    pub metrics_handle: PrometheusHandle,
    pub reader: Arc<BlockReader>,
    pub publisher: Arc<Publisher>,
}

impl FromRef<AppState> for PrometheusHandle {
    fn from_ref(state: &AppState) -> Self {
        state.metrics_handle.clone()
    }
}

pub struct Application {
    config: Config,
}

impl Application {
    pub fn new(config: Config) -> Result<Self> {
        Ok(Self { config })
    }

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
            .with_allow_patterns(&["/", "/health", "/metrics"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());

        let app_state = AppState {
            metrics_handle,
            reader: Arc::new(reader),
            publisher: Arc::new(publisher),
        };

        let app = Router::new()
            .route("/", get(handlers::root))
            .route("/health", get(handlers::health_check))
            .route("/metrics", get(handlers::metrics))
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

#[allow(dead_code)] // will be used in a later PR
fn log_event(event: &TransactionEvent) {
    match &event.operator {
        Operator::Add(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Add"
            );
        }
        Operator::Sub(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Sub"
            );
        }
        Operator::Mul(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Mul"
            );
        }
        Operator::Div(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Div"
            );
        }
        Operator::SafeAdd(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                success = op.success,
                result = op.result,
                "SafeAdd"
            );
        }
        Operator::SafeSub(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                success = op.success,
                result = op.result,
                "SafeSub"
            );
        }
        Operator::SafeMul(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                success = op.success,
                result = op.result,
                "SafeMul"
            );
        }
        Operator::SafeDiv(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                success = op.success,
                result = op.result,
                "SafeDiv"
            );
        }
        Operator::Eq(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Eq"
            );
        }
        Operator::Ne(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Ne"
            );
        }
        Operator::Ge(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Ge"
            );
        }
        Operator::Gt(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Gt"
            );
        }
        Operator::Le(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Le"
            );
        }
        Operator::Lt(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                leftHandOperand = op.left_hand_operand,
                rightHandOperand = op.right_hand_operand,
                result = op.result,
                "Lt"
            );
        }
        Operator::Select(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                condition = op.condition,
                if_true = op.if_true,
                if_false = op.if_false,
                result = op.result,
                "Select"
            );
        }
        Operator::Transfer(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                balanceFrom = op.balance_from,
                balanceTo = op.balance_to,
                amount = op.amount,
                success = op.success,
                newBalanceFrom = op.new_balance_from,
                newBalanceTo = op.new_balance_to,
                "Transfer"
            );
        }
        Operator::Mint(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                balanceTo = op.balance_to,
                amount = op.amount,
                totalSupply = op.total_supply,
                success = op.success,
                newBalanceTo = op.new_balance_to,
                newTotalSupply = op.new_total_supply,
                "Mint"
            );
        }
        Operator::Burn(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                balanceFrom = op.balance_from,
                amount = op.amount,
                totalSupply = op.total_supply,
                success = op.success,
                newBalanceFrom = op.new_balance_from,
                newTotalSupply = op.new_total_supply,
                "Burn"
            );
        }
        Operator::WrapAsPublicHandle(op) => {
            info!(
                log_index = event.log_index,
                caller = format!("{:#x}", event.caller),
                value = op.value,
                tee_type = op.tee_type,
                handle = op.handle,
                "WrapAsPublicHandle"
            )
        }
    }
}
