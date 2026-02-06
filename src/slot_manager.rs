use crate::error::{AmberError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use ulid::Ulid;

pub const TOTAL_SLOTS: u16 = 2048;
pub const CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64MB

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotInfo {
    pub slot_id: u16,
    pub replicas: Vec<String>, // node_ids
    pub primary: String,       // primary node_id
    pub latest_seq: String,    // ULID string
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotHealth {
    pub slot_id: u16,
    pub node_id: String,
    pub seq: String,
    pub status: ReplicaStatus,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReplicaStatus {
    Healthy,
    Syncing,
    Offline,
}

pub struct SlotManager {
    node_id: String,
    data_dir: PathBuf,
    slots: Arc<RwLock<HashMap<u16, Slot>>>,
}

pub struct Slot {
    pub slot_id: u16,
    pub seq: Arc<RwLock<Ulid>>,
    pub data_path: PathBuf,
}

impl SlotManager {
    pub fn new(node_id: String, data_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&data_dir)?;

        Ok(Self {
            node_id,
            data_dir,
            slots: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn init_slot(&self, slot_id: u16) -> Result<()> {
        let slot_path = self.data_dir.join(format!("slot_{:04}", slot_id));
        std::fs::create_dir_all(&slot_path)?;
        std::fs::create_dir_all(slot_path.join("chunks"))?;
        std::fs::create_dir_all(slot_path.join("meta"))?;

        let slot = Slot {
            slot_id,
            seq: Arc::new(RwLock::new(Ulid::new())),
            data_path: slot_path,
        };

        let mut slots = self.slots.write().await;
        slots.insert(slot_id, slot);

        tracing::info!("Initialized slot {} on node {}", slot_id, self.node_id);
        Ok(())
    }

    pub async fn get_slot(&self, slot_id: u16) -> Result<Arc<Slot>> {
        let slots = self.slots.read().await;
        slots
            .get(&slot_id)
            .map(|s| Arc::new(Slot {
                slot_id: s.slot_id,
                seq: Arc::clone(&s.seq),
                data_path: s.data_path.clone(),
            }))
            .ok_or_else(|| AmberError::SlotNotFound(slot_id))
    }

    pub async fn has_slot(&self, slot_id: u16) -> bool {
        let slots = self.slots.read().await;
        slots.contains_key(&slot_id)
    }

    pub async fn next_seq(&self, slot_id: u16) -> Result<Ulid> {
        let slots = self.slots.read().await;
        let slot = slots
            .get(&slot_id)
            .ok_or_else(|| AmberError::SlotNotFound(slot_id))?;

        let new_seq = Ulid::new();
        let mut seq = slot.seq.write().await;
        *seq = new_seq;
        Ok(new_seq)
    }

    pub async fn get_current_seq(&self, slot_id: u16) -> Result<Ulid> {
        let slots = self.slots.read().await;
        let slot = slots
            .get(&slot_id)
            .ok_or_else(|| AmberError::SlotNotFound(slot_id))?;

        let seq = slot.seq.read().await;
        Ok(*seq)
    }

    pub async fn get_assigned_slots(&self) -> Vec<u16> {
        let slots = self.slots.read().await;
        slots.keys().copied().collect()
    }
}

impl Slot {
    pub fn chunk_path(&self, chunk_hash: &str) -> PathBuf {
        self.data_path.join("chunks").join(chunk_hash)
    }

    pub fn meta_db_path(&self) -> PathBuf {
        self.data_path.join("meta").join("metadata.db")
    }
}

/// Calculate which slot an object belongs to based on its path
pub fn slot_for_key(key: &str, total_slots: u16) -> u16 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % total_slots as u64) as u16
}
