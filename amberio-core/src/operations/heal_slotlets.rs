use crate::{HeadKind, MetadataStore, Result, SlotManager};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone)]
pub struct HealSlotletsOperation {
    slot_manager: Arc<SlotManager>,
}

#[derive(Debug, Clone)]
pub struct HealSlotletsOperationRequest {
    pub slot_id: u16,
    pub prefix_len: usize,
}

#[derive(Debug, Clone)]
pub struct HealSlotletItem {
    pub prefix: String,
    pub digest: String,
    pub objects: usize,
}

#[derive(Debug, Clone)]
pub struct HealSlotletsOperationResult {
    pub slot_id: u16,
    pub prefix_len: usize,
    pub slotlets: Vec<HealSlotletItem>,
}

impl HealSlotletsOperation {
    pub fn new(slot_manager: Arc<SlotManager>) -> Self {
        Self { slot_manager }
    }

    pub async fn run(
        &self,
        request: HealSlotletsOperationRequest,
    ) -> Result<HealSlotletsOperationResult> {
        let HealSlotletsOperationRequest {
            slot_id,
            prefix_len,
        } = request;

        let store = self.ensure_store(slot_id).await?;
        let heads = store.list_heads("", usize::MAX, true, None)?;

        let prefix_len = prefix_len.clamp(1, 8);
        let mut slotlets: BTreeMap<String, (u64, usize)> = BTreeMap::new();

        for head in heads {
            let hash = short_hash_hex(&head.path);
            let prefix = hash.chars().take(prefix_len).collect::<String>();

            let kind = match head.head_kind {
                HeadKind::Meta => "meta",
                HeadKind::Tombstone => "tombstone",
            };

            let digest_source = format!(
                "{}|{}|{}|{}",
                head.path, head.generation, head.head_sha256, kind
            );

            let entry = slotlets.entry(prefix).or_insert((0, 0));
            entry.0 ^= fold_hash_u64(&digest_source);
            entry.1 += 1;
        }

        Ok(HealSlotletsOperationResult {
            slot_id,
            prefix_len,
            slotlets: slotlets
                .into_iter()
                .map(|(prefix, (digest, objects))| HealSlotletItem {
                    prefix,
                    digest: format!("{:016x}", digest),
                    objects,
                })
                .collect(),
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
