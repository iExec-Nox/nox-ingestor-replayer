//! Block reader with batch support

use std::collections::HashMap;
use std::time::Duration;

use alloy::{primitives::FixedBytes, rpc::types::Log};
use tokio::time::sleep;
use tracing::warn;

use crate::error::ChainError;
use crate::events::{
    ArithmeticOperation, BooleanOperation, BurnOperation, EncryptionOperation, MintOperation,
    Operator, SafeArithmeticOperation, SelectOperation, TransactionEvent, TransactionMessage,
    TransferOperation,
};

use super::NoxEventParser;
use super::client::ChainClient;
use super::parser::NoxEvent;

/// Result of reading a batch of blocks
#[derive(Debug)]
pub struct BatchResult {
    /// Transactions from the batch
    pub transactions: Vec<TransactionMessage>,
    /// First block in the batch
    pub start_block: u64,
    /// Last block in the batch (inclusive)
    pub end_block: u64,
}

/// Block reader with batch support
pub struct BlockReader {
    client: ChainClient,
    parser: NoxEventParser,
    batch_size: u64,
    retry_delay: Duration,
    max_retries: u32,
    chain_id: u32,
}

impl BlockReader {
    /// Create a new block reader
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_endpoint: &str,
        parser: NoxEventParser,
        batch_size: u64,
        retry_delay: Duration,
        max_retries: u32,
        connect_timeout: Duration,
        rpc_timeout: Duration,
        chain_id: u32,
    ) -> Result<Self, ChainError> {
        let client = ChainClient::new(
            rpc_endpoint,
            parser.contract_address(),
            parser.event_signatures(),
            connect_timeout,
            rpc_timeout,
        )?;

        Ok(Self {
            client,
            parser,
            batch_size,
            retry_delay,
            max_retries,
            chain_id,
        })
    }

    /// Get the latest block number
    pub async fn get_latest_block(&self) -> Result<u64, ChainError> {
        self.client.get_latest_block().await
    }

    /// Read a batch with bounded retries
    pub async fn read_batch_bounded(
        &self,
        from: u64,
        to: u64,
        max_retries: u32,
    ) -> Result<BatchResult, ChainError> {
        let mut attempt = 0u32;
        loop {
            match self.read_batch(from, to).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    attempt += 1;
                    if attempt > max_retries {
                        return Err(e);
                    }
                    warn!(
                        error = %e,
                        from,
                        to,
                        attempt,
                        max_retries,
                        retry_delay_ms = self.retry_delay.as_millis(),
                        "batch read failed, retrying"
                    );
                    sleep(self.retry_delay).await;
                }
            }
        }
    }

    /// Read a batch of blocks starting from `start_block`
    ///
    /// Returns transactions grouped from the batch.
    /// Events within each transaction are sorted by log_index.
    /// Transactions are sorted by (block_number, first_log_index).
    async fn read_batch(&self, start_block: u64, to: u64) -> Result<BatchResult, ChainError> {
        if start_block > to {
            // No blocks available yet
            return Ok(BatchResult {
                transactions: Vec::new(),
                start_block,
                end_block: start_block.saturating_sub(1),
            });
        }

        // Calculate end block (clamped to latest and batch_size)
        let end_block = (start_block + self.batch_size - 1).min(to);

        // Fetch logs for the entire range
        let logs = self.client.get_logs(start_block, end_block).await?;

        // Parse all logs and convert to TransactionEvent with metadata
        let mut events: Vec<(u64, u64, String, TransactionEvent)> = logs
            .iter()
            .filter_map(|log| {
                let nox_event = self.parser.parse(log)?;
                to_transaction_event(&nox_event, log)
            })
            .collect();

        // Sort by (block_number, log_index) for guaranteed order
        events.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Group events by transaction
        let transactions = self.group_by_transaction(events);

        Ok(BatchResult {
            transactions,
            start_block,
            end_block,
        })
    }

    /// Returns the configured batch size
    pub fn batch_size(&self) -> u64 {
        self.batch_size
    }

    /// Returns the configured max retries
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Group events by transaction hash
    ///
    /// Returns transactions sorted by (block_number, first_log_index).
    /// Events within each transaction are sorted by log_index.
    ///
    /// Input: Vec of (block_number, log_index, tx_hash, event)
    fn group_by_transaction(
        &self,
        events: Vec<(u64, u64, String, TransactionEvent)>,
    ) -> Vec<TransactionMessage> {
        let mut tx_map: HashMap<String, (u64, Vec<TransactionEvent>)> = HashMap::new();

        for (block_number, _log_index, tx_hash, event) in events {
            tx_map
                .entry(tx_hash)
                .or_insert_with(|| (block_number, Vec::new()))
                .1
                .push(event);
        }

        let mut messages: Vec<TransactionMessage> = tx_map
            .into_iter()
            .map(|(tx_hash, (block_number, events))| {
                let first_log_index = events.first().map(|e| e.log_index).unwrap_or(0);
                let caller = events.first().map(|e| e.caller).unwrap_or_default();
                TransactionMessage::new(
                    self.chain_id,
                    caller,
                    block_number,
                    first_log_index,
                    tx_hash,
                    events,
                )
            })
            .collect();

        messages.sort_by(|a, b| {
            a.block_number
                .cmp(&b.block_number)
                .then_with(|| a.first_log_index.cmp(&b.first_log_index))
        });

        messages
    }
}

/// Convert bytes32 to hex string
fn to_handle(bytes: FixedBytes<32>) -> String {
    format!("{:#x}", bytes)
}

/// Convert a parsed `NoxEvent` with its source `Log` metadata into a `TransactionEvent`.
/// Returns (block_number, log_index, tx_hash, event) or None if metadata is missing.
fn to_transaction_event(
    event: &NoxEvent,
    log: &Log,
) -> Option<(u64, u64, String, TransactionEvent)> {
    let tx_hash = format!("{:#x}", log.transaction_hash?);
    let block_number = log.block_number?;
    let log_index = log.log_index?;
    let caller = event.caller();

    let operator = match event {
        NoxEvent::WrapAsPublicHandle(e) => Operator::WrapAsPublicHandle(EncryptionOperation {
            value: to_handle(e.value),
            tee_type: e.teeType,
            handle: to_handle(e.handle),
        }),
        NoxEvent::Add(e) => Operator::Add(ArithmeticOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Sub(e) => Operator::Sub(ArithmeticOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Mul(e) => Operator::Mul(ArithmeticOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Div(e) => Operator::Div(ArithmeticOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::SafeAdd(e) => Operator::SafeAdd(SafeArithmeticOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            success: to_handle(e.success),
            result: to_handle(e.result),
        }),
        NoxEvent::SafeSub(e) => Operator::SafeSub(SafeArithmeticOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            success: to_handle(e.success),
            result: to_handle(e.result),
        }),
        NoxEvent::SafeMul(e) => Operator::SafeMul(SafeArithmeticOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            success: to_handle(e.success),
            result: to_handle(e.result),
        }),
        NoxEvent::SafeDiv(e) => Operator::SafeDiv(SafeArithmeticOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            success: to_handle(e.success),
            result: to_handle(e.result),
        }),
        NoxEvent::Eq(e) => Operator::Eq(BooleanOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Ne(e) => Operator::Ne(BooleanOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Ge(e) => Operator::Ge(BooleanOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Gt(e) => Operator::Gt(BooleanOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Le(e) => Operator::Le(BooleanOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Lt(e) => Operator::Lt(BooleanOperation {
            left_hand_operand: to_handle(e.leftHandOperand),
            right_hand_operand: to_handle(e.rightHandOperand),
            result: to_handle(e.result),
        }),
        NoxEvent::Select(e) => Operator::Select(SelectOperation {
            condition: to_handle(e.condition),
            if_true: to_handle(e.ifTrue),
            if_false: to_handle(e.ifFalse),
            result: to_handle(e.result),
        }),
        NoxEvent::Transfer(e) => Operator::Transfer(TransferOperation {
            balance_from: to_handle(e.balanceFrom),
            balance_to: to_handle(e.balanceTo),
            amount: to_handle(e.amount),
            success: to_handle(e.success),
            new_balance_from: to_handle(e.newBalanceFrom),
            new_balance_to: to_handle(e.newBalanceTo),
        }),
        NoxEvent::Mint(e) => Operator::Mint(MintOperation {
            balance_to: to_handle(e.balanceTo),
            amount: to_handle(e.amount),
            total_supply: to_handle(e.totalSupply),
            success: to_handle(e.success),
            new_balance_to: to_handle(e.newBalanceTo),
            new_total_supply: to_handle(e.newTotalSupply),
        }),
        NoxEvent::Burn(e) => Operator::Burn(BurnOperation {
            balance_from: to_handle(e.balanceFrom),
            amount: to_handle(e.amount),
            total_supply: to_handle(e.totalSupply),
            success: to_handle(e.success),
            new_balance_from: to_handle(e.newBalanceFrom),
            new_total_supply: to_handle(e.newTotalSupply),
        }),
    };

    let event = TransactionEvent {
        log_index,
        caller,
        operator,
    };

    Some((block_number, log_index, tx_hash, event))
}
