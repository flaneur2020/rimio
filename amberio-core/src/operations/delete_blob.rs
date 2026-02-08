use crate::{
    AmberError, Coordinator, MetadataStore, Result, SlotManager, TombstoneMeta, compute_hash,
};
use chrono::Utc;
use std::sync::Arc;

#[derive(Clone)]
pub struct DeleteBlobOperation {
    slot_manager: Arc<SlotManager>,
    coordinator: Arc<Coordinator>,
}

#[derive(Debug, Clone)]
pub struct DeleteBlobOperationRequest {
    pub path: String,
    pub slot_id: u16,
    pub write_id: String,
    pub replicas: Vec<crate::NodeInfo>,
    pub local_node_id: String,
}

#[derive(Debug, Clone)]
pub struct DeleteBlobOperationResult {
    pub generation: i64,
    pub committed_replicas: usize,
}

#[derive(Debug, Clone)]
pub enum DeleteBlobOperationOutcome {
    Committed(DeleteBlobOperationResult),
    Conflict,
}

impl DeleteBlobOperation {
    pub fn new(slot_manager: Arc<SlotManager>, coordinator: Arc<Coordinator>) -> Self {
        Self {
            slot_manager,
            coordinator,
        }
    }

    pub async fn run(
        &self,
        request: DeleteBlobOperationRequest,
    ) -> Result<DeleteBlobOperationOutcome> {
        let DeleteBlobOperationRequest {
            path,
            slot_id,
            write_id,
            replicas,
            local_node_id,
        } = request;

        let store = self.ensure_store(slot_id).await?;
        let generation = store.next_generation(&path)?;

        let tombstone = TombstoneMeta {
            path: path.clone(),
            slot_id,
            generation,
            deleted_at: Utc::now(),
            reason: "api-delete".to_string(),
        };

        let tombstone_bytes = serde_json::to_vec(&tombstone)?;
        let tombstone_sha = compute_hash(&tombstone_bytes);

        let applied =
            store.insert_tombstone_with_payload(&tombstone, &tombstone_bytes, &tombstone_sha)?;
        if !applied {
            return Ok(DeleteBlobOperationOutcome::Conflict);
        }

        let quorum = self.coordinator.write_quorum(replicas.len());
        let mut committed_replicas = 1usize;

        for replica in replicas
            .iter()
            .filter(|node| node.node_id != local_node_id.as_str())
        {
            let response = self
                .coordinator
                .replicate_tombstone_write(
                    replica,
                    slot_id,
                    &path,
                    &write_id,
                    generation,
                    &tombstone,
                    &tombstone_sha,
                )
                .await;

            if response.is_ok() {
                committed_replicas += 1;
            } else if let Err(error) = response {
                tracing::warn!(
                    "Replica tombstone write failed: node={} slot={} path={} error={}",
                    replica.node_id,
                    slot_id,
                    path,
                    error
                );
            }
        }

        if committed_replicas < quorum {
            return Err(AmberError::InsufficientReplicas {
                required: quorum,
                found: committed_replicas,
            });
        }

        Ok(DeleteBlobOperationOutcome::Committed(
            DeleteBlobOperationResult {
                generation,
                committed_replicas,
            },
        ))
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}
