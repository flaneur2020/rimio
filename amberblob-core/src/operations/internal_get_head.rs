use crate::{BlobMeta, HeadKind, MetadataStore, Result, SlotManager, TombstoneMeta};
use std::sync::Arc;

#[derive(Clone)]
pub struct InternalGetHeadOperation {
    slot_manager: Arc<SlotManager>,
}

#[derive(Debug, Clone)]
pub struct InternalGetHeadOperationRequest {
    pub slot_id: u16,
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct InternalHeadRecord {
    pub head_kind: HeadKind,
    pub generation: i64,
    pub head_sha256: String,
    pub meta: Option<BlobMeta>,
    pub tombstone: Option<TombstoneMeta>,
}

#[derive(Debug, Clone)]
pub enum InternalGetHeadOperationOutcome {
    Found(InternalHeadRecord),
    NotFound,
}

impl InternalGetHeadOperation {
    pub fn new(slot_manager: Arc<SlotManager>) -> Self {
        Self { slot_manager }
    }

    pub async fn run(
        &self,
        request: InternalGetHeadOperationRequest,
    ) -> Result<InternalGetHeadOperationOutcome> {
        let InternalGetHeadOperationRequest { slot_id, path } = request;

        let store = self.ensure_store(slot_id).await?;
        let head = store.get_current_head(&path)?;

        let Some(head) = head else {
            return Ok(InternalGetHeadOperationOutcome::NotFound);
        };

        Ok(InternalGetHeadOperationOutcome::Found(InternalHeadRecord {
            head_kind: head.head_kind,
            generation: head.generation,
            head_sha256: head.head_sha256,
            meta: head.meta,
            tombstone: head.tombstone,
        }))
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}
