use crate::{
    AmberError, BlobMeta, MetadataStore, Result, SlotManager, TombstoneMeta, compute_hash,
};
use chrono::Utc;
use std::sync::Arc;

#[derive(Clone)]
pub struct InternalPutHeadOperation {
    slot_manager: Arc<SlotManager>,
}

#[derive(Debug, Clone)]
pub struct InternalPutHeadOperationRequest {
    pub slot_id: u16,
    pub query_path: Option<String>,
    pub head_kind: String,
    pub generation: i64,
    pub head_sha256: String,
    pub meta: Option<BlobMeta>,
    pub tombstone: Option<TombstoneMeta>,
}

#[derive(Debug, Clone)]
pub struct InternalPutHeadOperationResult {
    pub head_kind: String,
    pub generation: i64,
}

impl InternalPutHeadOperation {
    pub fn new(slot_manager: Arc<SlotManager>) -> Self {
        Self { slot_manager }
    }

    pub async fn run(
        &self,
        request: InternalPutHeadOperationRequest,
    ) -> Result<InternalPutHeadOperationResult> {
        let InternalPutHeadOperationRequest {
            slot_id,
            query_path,
            head_kind,
            generation,
            head_sha256,
            meta,
            tombstone,
        } = request;

        let store = self.ensure_store(slot_id).await?;

        match head_kind.as_str() {
            "meta" => {
                let mut meta = meta.ok_or_else(|| {
                    AmberError::InvalidRequest("meta payload is required".to_string())
                })?;

                if let Some(path) = query_path {
                    meta.path = path;
                }

                meta.slot_id = slot_id;
                meta.generation = generation;
                if meta.version == 0 {
                    meta.version = meta.generation;
                }
                meta.updated_at = Utc::now();

                let inline_data = serde_json::to_vec(&meta)?;
                let head_sha = if head_sha256.is_empty() {
                    compute_hash(&inline_data)
                } else {
                    head_sha256
                };

                store.upsert_meta_with_payload(&meta, &inline_data, &head_sha)?;

                Ok(InternalPutHeadOperationResult {
                    head_kind: "meta".to_string(),
                    generation: meta.generation,
                })
            }
            "tombstone" => {
                let mut tombstone = tombstone.ok_or_else(|| {
                    AmberError::InvalidRequest("tombstone payload is required".to_string())
                })?;

                if let Some(path) = query_path {
                    tombstone.path = path;
                }

                tombstone.slot_id = slot_id;
                tombstone.generation = generation;
                tombstone.deleted_at = Utc::now();

                let inline_data = serde_json::to_vec(&tombstone)?;
                let head_sha = if head_sha256.is_empty() {
                    compute_hash(&inline_data)
                } else {
                    head_sha256
                };

                store.insert_tombstone_with_payload(&tombstone, &inline_data, &head_sha)?;

                Ok(InternalPutHeadOperationResult {
                    head_kind: "tombstone".to_string(),
                    generation: tombstone.generation,
                })
            }
            _ => Err(AmberError::InvalidRequest(
                "head_kind must be meta or tombstone".to_string(),
            )),
        }
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}
