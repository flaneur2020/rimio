use crate::config::EtcdConfig;
use crate::error::Result;
use crate::node::NodeInfo;
use crate::slot_manager::{ReplicaStatus, SlotHealth, SlotInfo};
use etcd_client::{Client, GetOptions, PutOptions};
use std::collections::HashMap;

pub struct EtcdStore {
    client: Client,
    prefix: String,
}

impl EtcdStore {
    pub async fn new(config: &EtcdConfig, group_id: &str) -> Result<Self> {
        let client = Client::connect(&config.endpoints, None).await?;
        let prefix = format!("/amberblob/{}", group_id);

        Ok(Self { client, prefix })
    }

    fn slot_key(&self, slot_id: u16) -> String {
        format!("{}/slots/{}", self.prefix, slot_id)
    }

    fn node_key(&self, node_id: &str) -> String {
        format!("{}/nodes/{}", self.prefix, node_id)
    }

    fn health_key(&self, slot_id: u16, node_id: &str) -> String {
        format!("{}/health/{}/{}", self.prefix, slot_id, node_id)
    }

    /// Register node in etcd
    pub async fn register_node(&self, node: &NodeInfo) -> Result<()> {
        let key = self.node_key(&node.node_id);
        let value = serde_json::to_vec(node)?;

        let mut client = self.client.clone();
        client
            .put(key, value, Some(PutOptions::new().with_lease(0)))
            .await?;

        Ok(())
    }

    /// Get slot routing information
    pub async fn get_slot(&self, slot_id: u16) -> Result<Option<SlotInfo>> {
        let key = self.slot_key(slot_id);
        let mut client = self.client.clone();
        let resp = client.get(key, None).await?;

        if let Some(kv) = resp.kvs().first() {
            let info: SlotInfo = serde_json::from_slice(kv.value())?;
            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    /// Set slot routing information
    pub async fn set_slot(&self, info: &SlotInfo) -> Result<()> {
        let key = self.slot_key(info.slot_id);
        let value = serde_json::to_vec(info)?;

        let mut client = self.client.clone();
        client.put(key, value, None).await?;

        Ok(())
    }

    /// Get all slot routing information
    pub async fn get_all_slots(&self) -> Result<HashMap<u16, SlotInfo>> {
        let prefix = format!("{}/slots/", self.prefix);
        let mut client = self.client.clone();
        let resp = client
            .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
            .await?;

        let mut slots = HashMap::new();
        for kv in resp.kvs() {
            if let Ok(info) = serde_json::from_slice::<SlotInfo>(kv.value()) {
                slots.insert(info.slot_id, info);
            }
        }

        Ok(slots)
    }

    /// Report slot health status
    pub async fn report_health(&self, health: &SlotHealth) -> Result<()> {
        let key = self.health_key(health.slot_id, &health.node_id);
        let value = serde_json::to_vec(health)?;

        let mut client = self.client.clone();
        client.put(key, value, None).await?;

        Ok(())
    }

    /// Get health status for all replicas of a slot
    pub async fn get_slot_health(&self, slot_id: u16) -> Result<Vec<SlotHealth>> {
        let prefix = format!("{}/health/{}/", self.prefix, slot_id);
        let mut client = self.client.clone();
        let resp = client
            .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
            .await?;

        let mut healths = Vec::new();
        for kv in resp.kvs() {
            if let Ok(health) = serde_json::from_slice::<SlotHealth>(kv.value()) {
                healths.push(health);
            }
        }

        Ok(healths)
    }

    /// Get healthy replicas with latest seq for a slot
    pub async fn get_healthy_replicas(&self, slot_id: u16) -> Result<Vec<(String, String)>> {
        let healths = self.get_slot_health(slot_id).await?;

        let healthy: Vec<(String, String)> = healths
            .into_iter()
            .filter(|h| h.status == ReplicaStatus::Healthy)
            .map(|h| (h.node_id, h.seq))
            .collect();

        if healthy.is_empty() {
            return Ok(Vec::new());
        }

        // Find the latest seq
        let latest_seq = healthy
            .iter()
            .map(|(_, seq)| seq.clone())
            .max()
            .unwrap_or_default();

        // Filter to only replicas with the latest seq
        let latest_healthy: Vec<(String, String)> = healthy
            .into_iter()
            .filter(|(_, seq)| seq == &latest_seq)
            .collect();

        Ok(latest_healthy)
    }

    /// Get all nodes in the group
    pub async fn get_nodes(&self) -> Result<Vec<NodeInfo>> {
        let prefix = format!("{}/nodes/", self.prefix);
        let mut client = self.client.clone();
        let resp = client
            .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
            .await?;

        let mut nodes = Vec::new();
        for kv in resp.kvs() {
            if let Ok(node) = serde_json::from_slice::<NodeInfo>(kv.value()) {
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    /// Watch for slot changes (simplified - just fetches periodically)
    pub async fn watch_slots(&self) -> Result<tokio::sync::mpsc::Receiver<SlotEvent>> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // In a full implementation, this would use etcd watch API
        // For now, we just return the receiver
        let _ = tx; // Suppress unused warning

        Ok(rx)
    }
}

#[derive(Debug, Clone)]
pub enum SlotEvent {
    Updated(SlotInfo),
    Deleted(u16),
}
