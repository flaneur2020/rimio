use crate::error::{AmberError, Result};
use crate::metadata_store::ObjectMeta;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use ulid::Ulid;

/// 2PC Transaction State
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionState {
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborted,
}

/// 2PC Transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub tx_id: String,
    pub state: TransactionState,
    pub participants: Vec<String>, // node_ids
    pub slot_id: u16,
    pub object_meta: ObjectMeta,
    pub votes: HashMap<String, Vote>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Vote {
    Yes,
    No(String), // reason
}

/// Coordinator for 2PC transactions
pub struct TwoPhaseCommit {
    node_id: String,
    transactions: Arc<RwLock<HashMap<String, Transaction>>>,
}

impl TwoPhaseCommit {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            transactions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a new 2PC transaction
    pub async fn begin_transaction(
        &self,
        participants: Vec<String>,
        slot_id: u16,
        object_meta: ObjectMeta,
    ) -> Result<String> {
        let tx_id = Ulid::new().to_string();

        let tx = Transaction {
            tx_id: tx_id.clone(),
            state: TransactionState::Preparing,
            participants: participants.clone(),
            slot_id,
            object_meta,
            votes: HashMap::new(),
            created_at: chrono::Utc::now(),
        };

        let mut txs = self.transactions.write().await;
        txs.insert(tx_id.clone(), tx);

        tracing::info!(
            "Started 2PC transaction {} for slot {} with {} participants",
            tx_id,
            slot_id,
            participants.len()
        );

        Ok(tx_id)
    }

    /// Record a vote from a participant
    pub async fn record_vote(&self, tx_id: &str, node_id: &str, vote: Vote) -> Result<()> {
        let mut txs = self.transactions.write().await;
        let tx = txs
            .get_mut(tx_id)
            .ok_or_else(|| AmberError::TwoPhaseCommit(format!("Transaction {} not found", tx_id)))?;

        if tx.state != TransactionState::Preparing {
            return Err(AmberError::TwoPhaseCommit(
                "Transaction is not in preparing state".to_string(),
            ));
        }

        tx.votes.insert(node_id.to_string(), vote);

        tracing::debug!(
            "Recorded vote from {} for transaction {}",
            node_id,
            tx_id
        );

        Ok(())
    }

    /// Check if all participants have voted
    pub async fn all_voted(&self, tx_id: &str) -> Result<bool> {
        let txs = self.transactions.read().await;
        let tx = txs
            .get(tx_id)
            .ok_or_else(|| AmberError::TwoPhaseCommit(format!("Transaction {} not found", tx_id)))?;

        Ok(tx.votes.len() == tx.participants.len())
    }

    /// Determine if transaction can commit (all voted Yes)
    pub async fn can_commit(&self, tx_id: &str) -> Result<bool> {
        let txs = self.transactions.read().await;
        let tx = txs
            .get(tx_id)
            .ok_or_else(|| AmberError::TwoPhaseCommit(format!("Transaction {} not found", tx_id)))?;

        if tx.votes.len() != tx.participants.len() {
            return Ok(false);
        }

        let all_yes = tx.votes.values().all(|v| matches!(v, Vote::Yes));
        Ok(all_yes)
    }

    /// Commit the transaction
    pub async fn commit(&self, tx_id: &str) -> Result<()> {
        let mut txs = self.transactions.write().await;
        let tx = txs
            .get_mut(tx_id)
            .ok_or_else(|| AmberError::TwoPhaseCommit(format!("Transaction {} not found", tx_id)))?;

        if tx.state != TransactionState::Preparing {
            return Err(AmberError::TwoPhaseCommit(
                "Transaction is not in preparing state".to_string(),
            ));
        }

        tx.state = TransactionState::Committed;

        tracing::info!("Committed transaction {}", tx_id);

        Ok(())
    }

    /// Abort the transaction
    pub async fn abort(&self, tx_id: &str) -> Result<()> {
        let mut txs = self.transactions.write().await;
        let tx = txs
            .get_mut(tx_id)
            .ok_or_else(|| AmberError::TwoPhaseCommit(format!("Transaction {} not found", tx_id)))?;

        tx.state = TransactionState::Aborted;

        tracing::info!("Aborted transaction {}", tx_id);

        Ok(())
    }

    /// Get transaction state
    pub async fn get_transaction(&self, tx_id: &str) -> Result<Option<Transaction>> {
        let txs = self.transactions.read().await;
        Ok(txs.get(tx_id).cloned())
    }

    /// Cleanup old transactions
    pub async fn cleanup_old_transactions(&self, max_age: chrono::Duration) -> Result<usize> {
        let mut txs = self.transactions.write().await;
        let now = chrono::Utc::now();
        let to_remove: Vec<String> = txs
            .iter()
            .filter(|(_, tx)| now - tx.created_at > max_age)
            .map(|(id, _)| id.clone())
            .collect();

        let count = to_remove.len();
        for id in to_remove {
            txs.remove(&id);
        }

        Ok(count)
    }
}

/// Participant side of 2PC
pub struct TwoPhaseParticipant {
    node_id: String,
}

impl TwoPhaseParticipant {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    /// Prepare phase - validate and vote
    pub async fn prepare(&self, _tx: &Transaction) -> Result<Vote> {
        // In a real implementation, this would:
        // 1. Validate the object metadata
        // 2. Check if all chunks are available
        // 3. Write prepare record to local WAL
        // 4. Return Yes if all checks pass

        Ok(Vote::Yes)
    }

    /// Commit phase - apply changes
    pub async fn commit(&self, tx: &Transaction) -> Result<()> {
        // In a real implementation, this would:
        // 1. Write the object metadata to local SQLite
        // 2. Update the slot seq
        // 3. Clean up prepare record

        tracing::info!(
            "Participant {} committing transaction {} for object {}",
            self.node_id,
            tx.tx_id,
            tx.object_meta.path
        );

        Ok(())
    }

    /// Abort phase - rollback changes
    pub async fn abort(&self, tx: &Transaction) -> Result<()> {
        // In a real implementation, this would:
        // 1. Mark chunks as orphan (for later cleanup)
        // 2. Clean up prepare record

        tracing::info!(
            "Participant {} aborting transaction {} for object {}",
            self.node_id,
            tx.tx_id,
            tx.object_meta.path
        );

        Ok(())
    }
}
