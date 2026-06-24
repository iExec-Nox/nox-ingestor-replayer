//! NOX event types and message format
//!
//! Transaction-based message grouping: one message per transaction
//! containing all NOX events from that transaction.

use alloy::primitives::{Address, keccak256};
use serde::{Deserialize, Serialize};

/// Handle type for encrypted values (hex-encoded bytes32)
pub type Handle = String;

/// Binary operation (add, sub, mul, div)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArithmeticOperation {
    pub left_hand_operand: Handle,
    pub right_hand_operand: Handle,
    pub result: Handle,
}

/// Safe binary operation (safe_add, safe_sub, safe_mul, safe_div)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SafeArithmeticOperation {
    pub left_hand_operand: Handle,
    pub right_hand_operand: Handle,
    pub success: Handle,
    pub result: Handle,
}

/// Boolean operation (eq, ne, ge, gt, le, lt)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BooleanOperation {
    pub left_hand_operand: Handle,
    pub right_hand_operand: Handle,
    pub result: Handle,
}

/// Select operation (conditional)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectOperation {
    pub condition: Handle,
    pub if_true: Handle,
    pub if_false: Handle,
    pub result: Handle,
}

/// Encryption operation (plaintext to encrypted)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionOperation {
    pub value: String,
    pub tee_type: u8,
    pub handle: Handle,
}

/// Transfer operation (confidential transfer between addresses)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferOperation {
    pub balance_from: Handle,
    pub balance_to: Handle,
    pub amount: Handle,
    pub success: Handle,
    pub new_balance_from: Handle,
    pub new_balance_to: Handle,
}

/// Mint operation (confidential minting)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintOperation {
    pub balance_to: Handle,
    pub amount: Handle,
    pub total_supply: Handle,
    pub success: Handle,
    pub new_balance_to: Handle,
    pub new_total_supply: Handle,
}

/// Burn operation (confidential burning)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BurnOperation {
    pub balance_from: Handle,
    pub amount: Handle,
    pub total_supply: Handle,
    pub success: Handle,
    pub new_balance_from: Handle,
    pub new_total_supply: Handle,
}

/// Event payload with typed variants
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Operator {
    WrapAsPublicHandle(EncryptionOperation),
    Add(ArithmeticOperation),
    Sub(ArithmeticOperation),
    Mul(ArithmeticOperation),
    Div(ArithmeticOperation),
    SafeAdd(SafeArithmeticOperation),
    SafeSub(SafeArithmeticOperation),
    SafeMul(SafeArithmeticOperation),
    SafeDiv(SafeArithmeticOperation),
    Eq(BooleanOperation),
    Ne(BooleanOperation),
    Ge(BooleanOperation),
    Gt(BooleanOperation),
    Le(BooleanOperation),
    Lt(BooleanOperation),
    Select(SelectOperation),
    Transfer(TransferOperation),
    Mint(MintOperation),
    Burn(BurnOperation),
}

/// Individual event within a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionEvent {
    pub log_index: u64,
    /// Caller address
    pub caller: Address,
    #[serde(flatten)]
    pub operator: Operator,
}

/// Message format grouping events by transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionMessage {
    /// Chain ID where the events occurred
    pub chain_id: u32,
    /// Caller address
    pub caller: Address,
    /// Block number
    pub block_number: u64,
    /// First log index in this transaction (used for ordering)
    #[serde(skip)]
    pub first_log_index: u64,
    /// Transaction hash
    pub transaction_hash: String,
    /// Events in this transaction, ordered by log_index
    pub events: Vec<TransactionEvent>,
}

/// Logs a single transaction event via tracing.
pub fn log_event(event: &TransactionEvent) {
    use tracing::info;
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

impl TransactionMessage {
    /// Creates a new transaction message
    pub fn new(
        chain_id: u32,
        caller: Address,
        block_number: u64,
        first_log_index: u64,
        transaction_hash: String,
        events: Vec<TransactionEvent>,
    ) -> Self {
        Self {
            chain_id,
            caller,
            block_number,
            first_log_index,
            transaction_hash,
            events,
        }
    }

    /// Computes a unique checksum for deduplication
    /// Based on chain_id + tx_hash (no log_index since we group by tx)
    pub fn compute_checksum(&self) -> String {
        let input = format!("{}:{}", self.chain_id, self.transaction_hash);
        keccak256(input.as_bytes()).to_string()
    }

    /// Returns the subject for the transaction message
    pub fn subject(&self, base_subject: &str) -> String {
        format!("{}.{}", base_subject, self.transaction_hash)
    }

    /// Converts the transaction message to bytes for NATS
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}
