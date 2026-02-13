use crate::error::{Result, RimError};
use crate::node::{NodeInfo, NodeStatus};
use crate::registry::Registry;
use crate::slot_manager::{ReplicaStatus, SlotHealth, SlotInfo};
use async_trait::async_trait;
use rimio_meta::{MetaError, MetaKv, MetaKvOptions, MetaMemberState};
use std::collections::HashMap;

fn map_meta_error(error: MetaError) -> RimError {
    match error {
        MetaError::Config(message) => RimError::Config(message),
        other => RimError::Internal(other.to_string()),
    }
}

fn slot_key(slot_id: u16) -> String {
    format!("slots/{}", slot_id)
}

fn node_key(node_id: &str) -> String {
    format!("nodes/{}", node_id)
}

fn health_key(slot_id: u16, node_id: &str) -> String {
    format!("health/{}/{}", slot_id, node_id)
}

fn health_prefix(slot_id: u16) -> String {
    format!("health/{}/", slot_id)
}

fn slots_prefix() -> &'static str {
    "slots/"
}

fn bootstrap_key() -> &'static str {
    "bootstrap/state"
}

fn map_member_status(state: MetaMemberState) -> NodeStatus {
    match state {
        MetaMemberState::Alive => NodeStatus::Healthy,
        MetaMemberState::Suspect => NodeStatus::Degraded,
        MetaMemberState::Other => NodeStatus::Unhealthy,
    }
}

fn is_health_expired(last_updated: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::Utc::now().signed_duration_since(last_updated) > chrono::Duration::seconds(60)
}

pub struct GossipMemberlistRegistry {
    namespace: String,
    kv: MetaKv,
}

impl GossipMemberlistRegistry {
    pub async fn new(
        namespace: &str,
        node_id: &str,
        bind_addr: &str,
        advertise_addr: Option<&str>,
        seeds: Vec<String>,
        transport: Option<&str>,
    ) -> Result<Self> {
        let namespace = namespace.trim().to_string();
        if namespace.is_empty() {
            return Err(RimError::Config(
                "registry namespace cannot be empty".to_string(),
            ));
        }

        let options = MetaKvOptions {
            namespace: namespace.clone(),
            node_id: node_id.to_string(),
            bind_addr: bind_addr.to_string(),
            advertise_addr: advertise_addr.map(str::to_string),
            seeds,
            transport: transport.map(str::to_string),
        };

        let kv = MetaKv::new(options).await.map_err(map_meta_error)?;

        Ok(Self { namespace, kv })
    }
}

#[async_trait]
impl Registry for GossipMemberlistRegistry {
    async fn register_node(&self, node: &NodeInfo) -> Result<()> {
        if node.group_id.trim() != self.namespace {
            return Err(RimError::Config(format!(
                "node group_id mismatch: expected='{}', got='{}'",
                self.namespace, node.group_id
            )));
        }

        self.kv
            .set_local_node(&node.node_id, &node.address)
            .await
            .map_err(map_meta_error)?;

        let key = node_key(&node.node_id);
        let value = serde_json::to_vec(node)?;
        self.kv.put(&key, &value).await.map_err(map_meta_error)?;
        self.kv.sync_once().await.map_err(map_meta_error)?;

        Ok(())
    }

    async fn get_slot(&self, slot_id: u16) -> Result<Option<SlotInfo>> {
        let key = slot_key(slot_id);
        let value = self.kv.get(&key).await.map_err(map_meta_error)?;

        match value {
            Some(data) => {
                let info: SlotInfo = serde_json::from_slice(&data)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    async fn set_slot(&self, info: &SlotInfo) -> Result<()> {
        let key = slot_key(info.slot_id);
        let value = serde_json::to_vec(info)?;
        self.kv.put(&key, &value).await.map_err(map_meta_error)
    }

    async fn get_all_slots(&self) -> Result<HashMap<u16, SlotInfo>> {
        let items = self
            .kv
            .list_prefix(slots_prefix())
            .await
            .map_err(map_meta_error)?;

        let mut slots = HashMap::new();
        for (_key, data) in items {
            if let Ok(info) = serde_json::from_slice::<SlotInfo>(&data) {
                slots.insert(info.slot_id, info);
            }
        }

        Ok(slots)
    }

    async fn report_health(&self, health: &SlotHealth) -> Result<()> {
        let key = health_key(health.slot_id, &health.node_id);
        let value = serde_json::to_vec(health)?;
        self.kv.put(&key, &value).await.map_err(map_meta_error)
    }

    async fn get_slot_health(&self, slot_id: u16) -> Result<Vec<SlotHealth>> {
        let prefix = health_prefix(slot_id);
        let items = self.kv.list_prefix(&prefix).await.map_err(map_meta_error)?;

        let mut healths = Vec::new();
        for (_key, data) in items {
            if let Ok(health) = serde_json::from_slice::<SlotHealth>(&data)
                && !is_health_expired(health.last_updated)
            {
                healths.push(health);
            }
        }

        Ok(healths)
    }

    async fn get_healthy_replicas(&self, slot_id: u16) -> Result<Vec<(String, String)>> {
        let healths = self.get_slot_health(slot_id).await?;

        let healthy: Vec<(String, String)> = healths
            .into_iter()
            .filter(|health| health.status == ReplicaStatus::Healthy)
            .map(|health| (health.node_id, health.seq))
            .collect();

        if healthy.is_empty() {
            return Ok(Vec::new());
        }

        let latest_seq = healthy
            .iter()
            .map(|(_, seq)| seq.clone())
            .max()
            .unwrap_or_default();

        Ok(healthy
            .into_iter()
            .filter(|(_, seq)| seq == &latest_seq)
            .collect())
    }

    async fn get_nodes(&self) -> Result<Vec<NodeInfo>> {
        let members = self.kv.members().await.map_err(map_meta_error)?;

        Ok(members
            .into_iter()
            .map(|member| NodeInfo {
                node_id: member.node_id,
                group_id: member.namespace,
                address: member.address,
                status: map_member_status(member.state),
                slots: Vec::new(),
            })
            .collect())
    }

    async fn get_bootstrap_state(&self) -> Result<Option<Vec<u8>>> {
        self.kv.sync_once().await.map_err(map_meta_error)?;
        self.kv.get(bootstrap_key()).await.map_err(map_meta_error)
    }

    async fn set_bootstrap_state_if_absent(&self, payload: &[u8]) -> Result<bool> {
        let created = self
            .kv
            .put_if_absent(bootstrap_key(), payload)
            .await
            .map_err(map_meta_error)?;
        self.kv.sync_once().await.map_err(map_meta_error)?;
        Ok(created)
    }
}
