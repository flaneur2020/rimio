use crate::{HeadKind, MetadataStore, Result, SlotManager};
use chrono::Utc;
use std::sync::Arc;

#[derive(Clone)]
pub struct ListBlobsOperation {
    slot_manager: Arc<SlotManager>,
}

#[derive(Debug, Clone)]
pub struct ListBlobsOperationRequest {
    pub prefix: String,
    pub limit: usize,
    pub cursor: Option<String>,
    pub include_deleted: bool,
}

#[derive(Debug, Clone)]
pub struct ListBlobItem {
    pub path: String,
    pub generation: i64,
    pub etag: String,
    pub size_bytes: u64,
    pub deleted: bool,
    pub updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ListBlobsOperationResult {
    pub items: Vec<ListBlobItem>,
    pub next_cursor: Option<String>,
}

impl ListBlobsOperation {
    pub fn new(slot_manager: Arc<SlotManager>) -> Self {
        Self { slot_manager }
    }

    pub async fn run(
        &self,
        request: ListBlobsOperationRequest,
    ) -> Result<ListBlobsOperationResult> {
        let ListBlobsOperationRequest {
            prefix,
            limit,
            cursor,
            include_deleted,
        } = request;

        let slots = self.slot_manager.get_assigned_slots().await;
        let mut heads = Vec::new();

        for slot_id in slots {
            let slot = match self.slot_manager.get_slot(slot_id).await {
                Ok(slot) => slot,
                Err(_) => continue,
            };

            let store = match MetadataStore::new(slot) {
                Ok(store) => store,
                Err(error) => {
                    tracing::warn!("Failed to open metadata for slot {}: {}", slot_id, error);
                    continue;
                }
            };

            let list = match store.list_heads(
                &prefix,
                limit.saturating_mul(2),
                include_deleted,
                cursor.as_deref(),
            ) {
                Ok(list) => list,
                Err(error) => {
                    tracing::warn!("Failed to list slot {}: {}", slot_id, error);
                    continue;
                }
            };

            heads.extend(list);
        }

        heads.sort_by(|a, b| a.path.cmp(&b.path));
        heads.dedup_by(|a, b| a.path == b.path);

        let mut items = Vec::new();
        for head in heads.into_iter().take(limit) {
            let (etag, size_bytes, deleted) = match head.head_kind {
                HeadKind::Meta => {
                    let meta = head.meta.clone();
                    (
                        meta.as_ref()
                            .map(|item| item.etag.clone())
                            .unwrap_or_default(),
                        meta.as_ref().map(|item| item.size_bytes).unwrap_or(0),
                        false,
                    )
                }
                HeadKind::Tombstone => (String::new(), 0, true),
            };

            items.push(ListBlobItem {
                path: head.path,
                generation: head.generation,
                etag,
                size_bytes,
                deleted,
                updated_at: head.updated_at,
            });
        }

        let next_cursor = items.last().map(|item| item.path.clone());

        Ok(ListBlobsOperationResult { items, next_cursor })
    }
}
