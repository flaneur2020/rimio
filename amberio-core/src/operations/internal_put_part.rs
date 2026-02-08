use crate::{AmberError, MetadataStore, PartStore, Result, SlotManager, compute_hash};
use bytes::Bytes;
use std::sync::Arc;

#[derive(Clone)]
pub struct InternalPutPartOperation {
    slot_manager: Arc<SlotManager>,
    part_store: Arc<PartStore>,
}

#[derive(Debug, Clone)]
pub struct InternalPutPartOperationRequest {
    pub slot_id: u16,
    pub path: String,
    pub generation: i64,
    pub part_no: u32,
    pub sha256: String,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct InternalPutPartOperationResult {
    pub reused: bool,
    pub sha256: String,
}

impl InternalPutPartOperation {
    pub fn new(slot_manager: Arc<SlotManager>, part_store: Arc<PartStore>) -> Self {
        Self {
            slot_manager,
            part_store,
        }
    }

    pub async fn run(
        &self,
        request: InternalPutPartOperationRequest,
    ) -> Result<InternalPutPartOperationResult> {
        let InternalPutPartOperationRequest {
            slot_id,
            path,
            generation,
            part_no,
            sha256,
            body,
        } = request;

        if compute_hash(&body) != sha256 {
            return Err(AmberError::InvalidRequest(
                "part sha256 mismatch".to_string(),
            ));
        }

        let store = self.ensure_store(slot_id).await?;

        let put_result = self
            .part_store
            .put_part(slot_id, &path, generation, part_no, &sha256, body)
            .await?;

        let length = std::fs::metadata(&put_result.part_path)
            .map(|meta| meta.len())
            .unwrap_or(0);

        store.upsert_part_entry(
            &path,
            generation,
            part_no,
            &sha256,
            length,
            Some(put_result.part_path.to_string_lossy().as_ref()),
            None,
        )?;

        Ok(InternalPutPartOperationResult {
            reused: put_result.reused,
            sha256,
        })
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}
