use crate::error::{Result, RimError};
use crate::node::NodeInfo;
use crate::registry::Registry;
use crate::slot_manager::{ReplicaStatus, SlotHealth, SlotInfo};
use async_trait::async_trait;
use redis::{AsyncCommands, Client};
use std::collections::HashMap;
use tokio::sync::Mutex;

/// Redis-based registry implementation
pub struct RedisRegistry {
    conn: Mutex<redis::aio::MultiplexedConnection>,
    prefix: String,
}

impl RedisRegistry {
    /// Create a new Redis registry client
    pub async fn new(url: &str, group_id: &str) -> Result<Self> {
        let client = Client::open(url)
            .map_err(|e| RimError::Config(format!("Failed to connect to Redis: {}", e)))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| RimError::Config(format!("Failed to connect to Redis: {}", e)))?;

        // Test with a ping
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| RimError::Config(format!("Redis ping failed: {}", e)))?;

        let prefix = format!("rimio:{}", group_id);

        Ok(Self {
            conn: Mutex::new(conn),
            prefix,
        })
    }

    fn slot_key(&self, slot_id: u16) -> String {
        format!("{}:slots:{}", self.prefix, slot_id)
    }

    fn node_key(&self, node_id: &str) -> String {
        format!("{}:nodes:{}", self.prefix, node_id)
    }

    fn health_key(&self, slot_id: u16, node_id: &str) -> String {
        format!("{}:health:{}:{}", self.prefix, slot_id, node_id)
    }

    fn nodes_pattern(&self) -> String {
        format!("{}:nodes:*", self.prefix)
    }

    fn slots_pattern(&self) -> String {
        format!("{}:slots:*", self.prefix)
    }

    fn health_pattern(&self, slot_id: u16) -> String {
        format!("{}:health:{}:*", self.prefix, slot_id)
    }

    fn bootstrap_key(&self) -> String {
        format!("{}:bootstrap:state", self.prefix)
    }

    pub async fn get_bootstrap_bytes(&self) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn.lock().await;
        let key = self.bootstrap_key();

        conn.get(&key).await.map_err(|error| {
            RimError::Internal(format!(
                "Failed to get bootstrap state from Redis: {}",
                error
            ))
        })
    }

    pub async fn set_bootstrap_bytes_if_absent(&self, bytes: &[u8]) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let key = self.bootstrap_key();

        conn.set_nx(&key, bytes).await.map_err(|error| {
            RimError::Internal(format!("Failed to set bootstrap state in Redis: {}", error))
        })
    }
}

#[async_trait]
impl Registry for RedisRegistry {
    async fn register_node(&self, node: &NodeInfo) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let key = self.node_key(&node.node_id);
        let value = serde_json::to_vec(node)?;

        // Set with expiration (TTL of 60 seconds) - nodes should heartbeat
        let _: () = conn
            .set_ex(key, value, 60)
            .await
            .map_err(|e| RimError::Internal(format!("Failed to register node in Redis: {}", e)))?;

        Ok(())
    }

    async fn get_slot(&self, slot_id: u16) -> Result<Option<SlotInfo>> {
        let mut conn = self.conn.lock().await;
        let key = self.slot_key(slot_id);

        let value: Option<Vec<u8>> = conn
            .get(&key)
            .await
            .map_err(|e| RimError::Internal(format!("Failed to get slot from Redis: {}", e)))?;

        match value {
            Some(data) => {
                let info: SlotInfo = serde_json::from_slice(&data)?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    async fn set_slot(&self, info: &SlotInfo) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let key = self.slot_key(info.slot_id);
        let value = serde_json::to_vec(info)?;

        let _: () = conn
            .set(key, value)
            .await
            .map_err(|e| RimError::Internal(format!("Failed to set slot in Redis: {}", e)))?;

        Ok(())
    }

    async fn get_all_slots(&self) -> Result<HashMap<u16, SlotInfo>> {
        let mut conn = self.conn.lock().await;
        let pattern = self.slots_pattern();

        let keys: Vec<String> = conn.keys(&pattern).await.map_err(|e| {
            RimError::Internal(format!("Failed to get slot keys from Redis: {}", e))
        })?;

        let mut slots = HashMap::new();
        for key in keys {
            if let Ok(Some(data)) = conn.get::<_, Option<Vec<u8>>>(&key).await {
                if let Ok(info) = serde_json::from_slice::<SlotInfo>(&data) {
                    slots.insert(info.slot_id, info);
                }
            }
        }

        Ok(slots)
    }

    async fn report_health(&self, health: &SlotHealth) -> Result<()> {
        let mut conn = self.conn.lock().await;
        let key = self.health_key(health.slot_id, &health.node_id);
        let value = serde_json::to_vec(health)?;

        // Set with expiration (TTL of 60 seconds) - health should be reported periodically
        let _: () = conn
            .set_ex(key, value, 60)
            .await
            .map_err(|e| RimError::Internal(format!("Failed to report health to Redis: {}", e)))?;

        Ok(())
    }

    async fn get_slot_health(&self, slot_id: u16) -> Result<Vec<SlotHealth>> {
        let mut conn = self.conn.lock().await;
        let pattern = self.health_pattern(slot_id);

        let keys: Vec<String> = conn.keys(&pattern).await.map_err(|e| {
            RimError::Internal(format!("Failed to get health keys from Redis: {}", e))
        })?;

        let mut healths = Vec::new();
        for key in keys {
            if let Ok(Some(data)) = conn.get::<_, Option<Vec<u8>>>(&key).await {
                if let Ok(health) = serde_json::from_slice::<SlotHealth>(&data) {
                    healths.push(health);
                }
            }
        }

        Ok(healths)
    }

    async fn get_healthy_replicas(&self, slot_id: u16) -> Result<Vec<(String, String)>> {
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

    async fn get_nodes(&self) -> Result<Vec<NodeInfo>> {
        let mut conn = self.conn.lock().await;
        let pattern = self.nodes_pattern();

        let keys: Vec<String> = conn.keys(&pattern).await.map_err(|e| {
            RimError::Internal(format!("Failed to get node keys from Redis: {}", e))
        })?;

        let mut nodes = Vec::new();
        for key in keys {
            if let Ok(Some(data)) = conn.get::<_, Option<Vec<u8>>>(&key).await {
                if let Ok(node) = serde_json::from_slice::<NodeInfo>(&data) {
                    nodes.push(node);
                }
            }
        }

        Ok(nodes)
    }

    async fn get_bootstrap_state(&self) -> Result<Option<Vec<u8>>> {
        self.get_bootstrap_bytes().await
    }

    async fn set_bootstrap_state_if_absent(&self, payload: &[u8]) -> Result<bool> {
        self.set_bootstrap_bytes_if_absent(payload).await
    }
}
