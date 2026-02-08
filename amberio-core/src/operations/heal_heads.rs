use crate::{HeadKind, MetadataStore, Result, SlotManager};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone)]
pub struct HealHeadsOperation {
    slot_manager: Arc<SlotManager>,
}

#[derive(Debug, Clone)]
pub struct HealHeadsOperationRequest {
    pub slot_id: u16,
    pub prefixes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct HealHeadItem {
    pub path: String,
    pub head_kind: String,
    pub generation: i64,
    pub head_sha256: String,
}

#[derive(Debug, Clone)]
pub struct HealHeadsOperationResult {
    pub slot_id: u16,
    pub heads: Vec<HealHeadItem>,
}

impl HealHeadsOperation {
    pub fn new(slot_manager: Arc<SlotManager>) -> Self {
        Self { slot_manager }
    }

    pub async fn run(
        &self,
        request: HealHeadsOperationRequest,
    ) -> Result<HealHeadsOperationResult> {
        let HealHeadsOperationRequest { slot_id, prefixes } = request;

        let store = self.ensure_store(slot_id).await?;
        let heads = store.list_heads("", usize::MAX, true, None)?;

        let filter_set: HashSet<String> = prefixes.into_iter().collect();

        let filtered = heads
            .into_iter()
            .filter(|head| {
                let prefix = short_hash_hex(&head.path)
                    .chars()
                    .take(2)
                    .collect::<String>();
                filter_set.contains(&prefix)
            })
            .map(|head| HealHeadItem {
                path: head.path,
                head_kind: match head.head_kind {
                    HeadKind::Meta => "meta".to_string(),
                    HeadKind::Tombstone => "tombstone".to_string(),
                },
                generation: head.generation,
                head_sha256: head.head_sha256,
            })
            .collect();

        Ok(HealHeadsOperationResult {
            slot_id,
            heads: filtered,
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

fn fold_hash_u64(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn short_hash_hex(value: &str) -> String {
    format!("{:016x}", fold_hash_u64(value))
}
