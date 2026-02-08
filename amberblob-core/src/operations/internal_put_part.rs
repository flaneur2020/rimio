use crate::{AmberError, MetadataStore, PartRef, PartStore, Result, SlotManager, compute_hash};
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
    pub sha256: String,
    pub body: Bytes,
    pub offset: u64,
    pub length: Option<u64>,
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
            sha256,
            body,
            offset,
            length,
        } = request;

        if compute_hash(&body) != sha256 {
            return Err(AmberError::InvalidRequest(
                "part sha256 mismatch".to_string(),
            ));
        }

        let store = self.ensure_store(slot_id).await?;

        let put_result = self
            .part_store
            .put_part(slot_id, &path, &sha256, body)
            .await?;

        let length = length.unwrap_or_else(|| {
            std::fs::metadata(&put_result.part_path)
                .map(|meta| meta.len())
                .unwrap_or(0)
        });

        let part = PartRef {
            name: format!("part.{}", sha256),
            sha256: sha256.clone(),
            offset,
            length,
            external_path: Some(put_result.part_path.to_string_lossy().to_string()),
            archive_url: None,
        };

        store.upsert_part_entry(&path, &part)?;

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
