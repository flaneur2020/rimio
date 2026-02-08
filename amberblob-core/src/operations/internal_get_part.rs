use crate::{MetadataStore, PartStore, Result, SlotManager};
use bytes::Bytes;
use std::sync::Arc;

#[derive(Clone)]
pub struct InternalGetPartOperation {
    slot_manager: Arc<SlotManager>,
    part_store: Arc<PartStore>,
}

#[derive(Debug, Clone)]
pub struct InternalGetPartOperationRequest {
    pub slot_id: u16,
    pub sha256: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone)]
pub enum InternalGetPartOperationOutcome {
    Found(Bytes),
    NotFound,
}

impl InternalGetPartOperation {
    pub fn new(slot_manager: Arc<SlotManager>, part_store: Arc<PartStore>) -> Self {
        Self {
            slot_manager,
            part_store,
        }
    }

    pub async fn run(
        &self,
        request: InternalGetPartOperationRequest,
    ) -> Result<InternalGetPartOperationOutcome> {
        let InternalGetPartOperationRequest {
            slot_id,
            sha256,
            path,
        } = request;

        if let Some(path) = path {
            match self.part_store.get_part(slot_id, &path, &sha256).await {
                Ok(bytes) => return Ok(InternalGetPartOperationOutcome::Found(bytes)),
                Err(_) => return Ok(InternalGetPartOperationOutcome::NotFound),
            }
        }

        let store = self.ensure_store(slot_id).await?;
        let external_path = store.find_part_external_path(&sha256, None)?;

        let Some(external_path) = external_path else {
            return Ok(InternalGetPartOperationOutcome::NotFound);
        };

        let bytes = tokio::fs::read(external_path).await?;
        Ok(InternalGetPartOperationOutcome::Found(Bytes::from(bytes)))
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}
