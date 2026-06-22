use std::time::Duration;

use anyhow::Result;
use axum::{Router, routing::get};
use axum_prometheus::{
    Handle, MakeDefaultHandle, PrometheusMetricLayerBuilder,
    metrics::{counter, gauge},
};
use tokio::sync::watch;
use tokio::time::{interval, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, info_span, warn};

/// Timeout for final state persistence on shutdown
const SHUTDOWN_PERSIST_TIMEOUT: Duration = Duration::from_secs(5);

use crate::chain::{BlockReader, NoxEventParser};
use crate::config::Config;
use crate::error::NoxError;
use crate::events::{Operator, TransactionEvent};
use crate::handlers;
use crate::nats::{ConnectionState, NatsClient, Publisher};
use crate::state::StateStore;

pub struct Application {
    config: Config,
}

impl Application {
    pub fn new(config: Config) -> Result<Self> {
        Ok(Self { config })
    }

    pub async fn run(self) -> Result<()> {
        debug!("Starting ingestor");
        debug!("Config: {:?}", self.config);

        // 1. Setup shutdown handler
        let shutdown_token = self.setup_signal_handler();

        // 2. Connect to NATS
        let nats_client = NatsClient::connect(&self.config.nats).await?;
        nats_client.setup_stream(&self.config.nats).await?;

        // 3. Load state store
        let state_store = self.load_state_store().await?;

        // 4. Setup pause signal for reader/publisher coordination
        let (pause_tx, pause_rx) = watch::channel(false);

        // 5. Create parser and block reader
        let parser = NoxEventParser::new(self.config.chain.contract_address);
        let mut block_reader = BlockReader::new(
            &self.config.chain.rpc_endpoint,
            parser,
            self.config.chain.batch_size,
            self.config.chain.poll_delay,
            self.config.chain.retry_delay,
            self.config.chain.chain_id,
            pause_rx,
        )?;

        // 6. Create publisher
        let mut publisher = Publisher::new(
            nats_client.jetstream(),
            &self.config.nats,
            nats_client.state_receiver(),
            pause_tx,
        );

        // 7. Get NATS state receiver
        let mut nats_state_rx = nats_client.state_receiver();

        // 8. Determine starting block
        let mut next_block = self.determine_start_block(&state_store)?;

        // 9. TCP server
        let prometheus_layer = PrometheusMetricLayerBuilder::new()
            .with_allow_patterns(&["/", "/health", "/metrics"])
            .build();
        let metrics_handle = Handle::make_default_handle(Handle::default());

        let app = Router::new()
            .route("/", get(handlers::root))
            .route("/health", get(handlers::health_check))
            .route("/metrics", get(handlers::metrics))
            .fallback(handlers::not_found)
            .layer(prometheus_layer)
            .with_state(metrics_handle);
        let binding_address = self.config.binding_address();
        info!("starting TCP server listening on {binding_address}");
        let listener = tokio::net::TcpListener::bind(binding_address).await?;
        tokio::spawn(async move { axum::serve(listener, app).await });

        // Initialize NATS metrics to zero so they appear in /metrics on startup
        gauge!("nox_replayer.nats.connection_state").set(0.0);
        counter!("nox_replayer.nats.reconnects_total").absolute(0);
        gauge!("nox_replayer.nats.buffer_len").set(0.0);
        counter!("nox_replayer.nats.publishes_total", "outcome" => "ok").absolute(0);
        counter!("nox_replayer.nats.publishes_total", "outcome" => "err").absolute(0);

        // 10. Main loop
        let mut flush_interval = interval(self.config.app.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first tick to avoid flushing before any work is done
        flush_interval.tick().await;

        let mut needs_delay = false;
        let mut was_connected = *nats_state_rx.borrow() == ConnectionState::Connected;

        loop {
            // Handle delay outside of select! to allow cancellation during sleep
            if needs_delay {
                tokio::select! {
                    biased;
                    _ = shutdown_token.cancelled() => {
                        info!("Shutdown signal received");
                        break;
                    }
                    _ = sleep(block_reader.poll_delay()) => {
                        needs_delay = false;
                    }
                }
                if shutdown_token.is_cancelled() {
                    break;
                }
                continue;
            }

            tokio::select! {
                biased;

                // 1. Shutdown (highest priority)
                _ = shutdown_token.cancelled() => {
                    info!("Shutdown signal received");
                    break;
                }

                // 2. NATS state change
                result = nats_state_rx.changed() => {
                    if result.is_ok() {
                        let state = *nats_state_rx.borrow();
                        info!(state = %state, "NATS state changed");

                        let connected = state == ConnectionState::Connected;
                        gauge!("nox_replayer.nats.connection_state").set(if connected { 1.0 } else { 0.0 });
                        if connected && !was_connected {
                            counter!("nox_replayer.nats.reconnects_total").increment(1);
                        }
                        was_connected = connected;

                        if let Err(e) = publisher.handle_state_change().await {
                            warn!(error = %e, "Error handling NATS state change");
                        }

                        gauge!("nox_replayer.nats.buffer_len").set(publisher.buffer_len() as f64);

                        // After flush, if buffer is fully drained, advance persisted state
                        // to catch up with next_block (all messages now confirmed by NATS)
                        if publisher.is_buffer_empty() && next_block > 0 {
                            state_store.update(next_block - 1);
                        }
                    }
                }

                // 3. Periodic state flush
                _ = flush_interval.tick() => {
                    if let Err(e) = state_store.persist().await {
                        warn!(error = %e, "Failed to persist state");
                    }
                    // Buffer-drain retry: if events buffered while NATS was unhealthy,
                    // the state-change watcher won't trigger drain on recovery.
                    match publisher.try_resume_if_connected().await {
                        Ok(true) => {
                            info!("Buffer drained via periodic retry; chain reader resumed");
                            gauge!("nox_replayer.nats.buffer_len").set(publisher.buffer_len() as f64);
                            if publisher.is_buffer_empty() && next_block > 0 {
                                state_store.update(next_block - 1);
                            }
                        }
                        Ok(false) => {}
                        Err(e) => {
                            warn!(error = %e, "Periodic buffer drain attempt failed");
                        }
                    }
                }

                // 4. Block reading and publishing
                _ = async {

                    // Wait if paused (NATS disconnected and buffer full)
                    block_reader.wait_until_unpaused().await;

                    // Get latest block
                    let latest = match block_reader.get_latest_block().await {
                        Ok(b) => b,
                        Err(e) => {
                            error!(error = %e, "Failed to get latest block");
                            needs_delay = true;
                            return;
                        }
                    };
                    counter!("nox_replayer.chain.block_number", "chain_id" => self.config.chain.chain_id.to_string(), "type" => "latest").absolute(latest);

                    // Check if we need to wait for new blocks
                    if next_block > latest {
                        needs_delay = true;
                        return;
                    }
                    // Read batch (now returns transactions grouped by transaction)
                    let batch = block_reader
                        .read_batch_with_retry(next_block, latest)
                        .await;
                    counter!("nox_replayer.chain.block_number", "chain_id" => self.config.chain.chain_id.to_string(), "type" => "parsed").absolute(batch.end_block);

                    // Publish transaction messages
                    // Each transaction becomes one NATS message
                    for transaction in batch.transactions {

                        // If buffer is full, wait for NATS to reconnect before continuing
                        while publisher.is_buffer_full() {
                            // Wait for state change (NATS reconnect will flush buffer)
                            sleep(self.config.nats.wait_interval).await;
                            // Check for shutdown
                            if shutdown_token.is_cancelled() {
                                return;
                            }
                        }

                        let tx_block_number = transaction.block_number;
                        let span = info_span!(
                            "transaction",
                            tx_hash = transaction.transaction_hash,
                            block_number = transaction.block_number,
                            caller = format!("{:#x}", transaction.caller),
                            event_count = transaction.events.len()
                        );
                        let _guard = span.enter();

                        for event in &transaction.events {
                            log_event(event);
                        }
                        if let Err(e) = publisher.publish(transaction).await {
                            // Unexpected error (not buffer full), retry after delay
                            warn!(error = %e, "Failed to publish transaction");
                            sleep(self.config.nats.wait_interval).await;
                        }
                        gauge!("nox_replayer.nats.buffer_len").set(publisher.buffer_len() as f64);
                        counter!("nox_replayer.chain.block_number", "chain_id" => self.config.chain.chain_id.to_string(), "type" => "published").absolute(tx_block_number);
                    }
                    // Always advance next_block to avoid re-reading in this session
                    if batch.end_block >= batch.start_block {
                        next_block = batch.end_block + 1;

                        // Only update persisted state when buffer is empty (all messages ACK'd by NATS).
                        // If buffer has messages, state stays behind so blocks are re-processed on crash.
                        // NATS Nats-Msg-Id deduplication handles any re-delivery.
                        if publisher.is_buffer_empty() {
                            state_store.update(batch.end_block);
                        }
                    }

                    // Only delay if we're caught up (within 1 batch of head)
                    // Skip delay when catching up to maximize throughput
                    let gap = latest.saturating_sub(batch.end_block);
                    if gap <= 1 {
                        needs_delay = true;
                    } else {
                        debug!(gap, "Catching up, skipping poll delay");
                    }
                } => {}
            }
        }

        // Final cleanup with timeout to prevent hanging on shutdown
        info!("Persisting final state...");
        match timeout(SHUTDOWN_PERSIST_TIMEOUT, state_store.persist()).await {
            Ok(Ok(())) => debug!("Final state persisted successfully"),
            Ok(Err(e)) => error!(error = %e, "Failed to persist final state"),
            Err(_) => error!(
                "Timeout persisting final state after {:?}",
                SHUTDOWN_PERSIST_TIMEOUT
            ),
        }

        info!("Listener stopped gracefully");
        Ok(())
    }

    /// Load state store
    async fn load_state_store(&self) -> Result<StateStore, NoxError> {
        let state_file_path = self.config.state_file_path();
        let initial_block = self.config.chain.initial_block;

        info!(
            path = %state_file_path.display(),
            initial_block,
            "Loading state"
        );

        let state_store =
            StateStore::load(state_file_path, self.config.chain.chain_id, initial_block).await?;

        Ok(state_store)
    }

    /// Determine the starting block based on state
    fn determine_start_block(&self, state_store: &StateStore) -> Result<u64, NoxError> {
        if state_store.get() == 0 && !state_store.was_loaded_from_file() {
            // No state file and no INITIAL_BLOCK: fail fast
            return Err(NoxError::NoInitialBlock);
        }

        if state_store.was_loaded_from_file() {
            // Resume from last processed + 1
            let last = state_store.get();
            info!(
                resuming_from = last + 1,
                last_processed = last,
                "Resuming from persisted state"
            );
            Ok(last + 1)
        } else {
            // Fresh start with INITIAL_BLOCK
            let initial = state_store.get();
            info!(starting_from = initial, "Starting from initial block");
            Ok(initial)
        }
    }

    /// Setup signal handlers for graceful shutdown.
    ///
    /// Handles SIGINT (Ctrl+C) on all platforms.
    /// Additionally handles SIGTERM on Unix for container/systemd deployments.
    fn setup_signal_handler(&self) -> CancellationToken {
        let shutdown_token = CancellationToken::new();
        let token = shutdown_token.clone();

        tokio::spawn(async move {
            let shutdown_signal = Self::wait_for_shutdown_signal().await;
            info!(
                "{} received, initiating graceful shutdown...",
                shutdown_signal
            );
            token.cancel();
        });

        shutdown_token
    }

    /// Wait for a shutdown signal (SIGINT or SIGTERM on Unix, SIGINT only on other platforms).
    async fn wait_for_shutdown_signal() -> &'static str {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
            let mut sigint =
                signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");

            tokio::select! {
                _ = sigterm.recv() => "SIGTERM",
                _ = sigint.recv() => "SIGINT",
            }
        }

        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
            "Ctrl+C"
        }
    }
}

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
