use crate::{MetadataStore, PartStore, Result, SlotManager, compute_hash};
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct InternalGetPartOperation {
    slot_manager: Arc<SlotManager>,
    part_store: Arc<PartStore>,
}

#[derive(Debug, Clone)]
pub struct InternalGetPartOperationRequest {
    pub slot_id: u16,
    pub sha256: Option<String>,
    pub path: Option<String>,
    pub generation: Option<i64>,
    pub part_no: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct InternalPartPayload {
    pub bytes: Bytes,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub enum InternalGetPartOperationOutcome {
    Found(InternalPartPayload),
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
            generation,
            part_no,
        } = request;

        let store = self.ensure_store(slot_id).await?;

        if let (Some(path), Some(generation), Some(part_no)) =
            (path.as_deref(), generation, part_no)
        {
            if let Some(entry) = store.get_part_entry(path, generation, part_no)? {
                if let Ok(bytes) = self
                    .part_store
                    .get_part(slot_id, path, generation, part_no, &entry.sha256)
                    .await
                {
                    return Ok(InternalGetPartOperationOutcome::Found(
                        InternalPartPayload {
                            bytes,
                            sha256: entry.sha256,
                        },
                    ));
                }

                if let Some(external_path) = entry.external_path {
                    if Path::new(&external_path).exists() {
                        let bytes = tokio::fs::read(external_path).await?;
                        let payload = Bytes::from(bytes);
                        let sha = compute_hash(&payload);
                        return Ok(InternalGetPartOperationOutcome::Found(
                            InternalPartPayload {
                                bytes: payload,
                                sha256: sha,
                            },
                        ));
                    }
                }
            }

            if let Some(sha256) = normalized_sha256(sha256.as_deref()) {
                if let Ok(bytes) = self
                    .part_store
                    .get_part(slot_id, path, generation, part_no, sha256)
                    .await
                {
                    return Ok(InternalGetPartOperationOutcome::Found(
                        InternalPartPayload {
                            bytes,
                            sha256: sha256.to_string(),
                        },
                    ));
                }
            }

            return Ok(InternalGetPartOperationOutcome::NotFound);
        }

        let lookup_sha = normalized_sha256(sha256.as_deref());
        let Some(lookup_sha) = lookup_sha else {
            return Ok(InternalGetPartOperationOutcome::NotFound);
        };

        let external_path = store.find_part_external_path(lookup_sha, path.as_deref())?;

        let Some(external_path) = external_path else {
            return Ok(InternalGetPartOperationOutcome::NotFound);
        };

        let bytes = tokio::fs::read(external_path).await?;
        Ok(InternalGetPartOperationOutcome::Found(
            InternalPartPayload {
                bytes: Bytes::from(bytes),
                sha256: lookup_sha.to_string(),
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

fn normalized_sha256(value: Option<&str>) -> Option<&str> {
    let value = value?;
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "_" {
        return None;
    }
    Some(trimmed)
}
