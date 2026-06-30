//! RPC client wrapper using alloy

use alloy::{
    primitives::{Address, B256},
    providers::{Provider, ProviderBuilder},
    rpc::types::{BlockNumberOrTag, Filter, Log},
    transports::http::reqwest,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::error::ChainError;

/// Chain client wrapping alloy HTTP provider
pub struct ChainClient {
    primary_provider: Arc<dyn Provider + Send + Sync>,
    contract_address: Address,
    event_signatures: Vec<B256>,
}

impl ChainClient {
    /// Create a new chain client
    pub fn new(
        rpc_endpoint: &str,
        contract_address: Address,
        event_signatures: Vec<B256>,
        connect_timeout: Duration,
        rpc_timeout: Duration,
    ) -> Result<Self, ChainError> {
        let primary_url = rpc_endpoint
            .parse()
            .map_err(|e| ChainError::InvalidEndpoint(format!("{}: {}", rpc_endpoint, e)))?;

        let http_client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(rpc_timeout)
            .build()
            .map_err(|e| ChainError::InvalidEndpoint(format!("http client build: {e}")))?;

        let primary_provider = ProviderBuilder::new().connect_reqwest(http_client, primary_url);

        info!(
            primary = %rpc_endpoint,
            connect_timeout_ms = connect_timeout.as_millis(),
            rpc_timeout_ms = rpc_timeout.as_millis(),
            "ChainClient initialized"
        );

        Ok(Self {
            primary_provider: Arc::new(primary_provider),
            contract_address,
            event_signatures,
        })
    }

    /// Fetch the latest block number from the primary provider.
    pub async fn get_latest_block(&self) -> Result<u64, ChainError> {
        self.primary_provider
            .get_block_number()
            .await
            .map_err(|e| ChainError::Provider(e.to_string()))
    }

    /// Fetch event logs for a block range filtered by contract address and event signatures.
    pub async fn get_logs(&self, from_block: u64, to_block: u64) -> Result<Vec<Log>, ChainError> {
        let filter = Filter::new()
            .address(self.contract_address)
            .event_signature(self.event_signatures.clone())
            .from_block(BlockNumberOrTag::Number(from_block))
            .to_block(BlockNumberOrTag::Number(to_block));

        self.primary_provider
            .get_logs(&filter)
            .await
            .map_err(|e| ChainError::Provider(e.to_string()))
    }
}
