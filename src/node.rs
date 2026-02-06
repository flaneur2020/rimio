use crate::config::NodeConfig;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub group_id: String,
    pub address: String,
    pub status: NodeStatus,
    pub slots: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

pub struct Node {
    config: NodeConfig,
    info: Arc<RwLock<NodeInfo>>,
}

impl Node {
    pub fn new(config: NodeConfig, bind_addr: String) -> Result<Self> {
        let info = NodeInfo {
            node_id: config.node_id.clone(),
            group_id: config.group_id.clone(),
            address: bind_addr,
            status: NodeStatus::Healthy,
            slots: Vec::new(),
        };

        Ok(Self {
            config,
            info: Arc::new(RwLock::new(info)),
        })
    }

    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    pub fn group_id(&self) -> &str {
        &self.config.group_id
    }

    pub fn disks(&self) -> &[crate::config::DiskConfig] {
        &self.config.disks
    }

    pub async fn info(&self) -> NodeInfo {
        self.info.read().await.clone()
    }

    pub async fn update_status(&self, status: NodeStatus) {
        let mut info = self.info.write().await;
        info.status = status;
    }

    pub async fn assign_slots(&self, slots: Vec<u16>) {
        let mut info = self.info.write().await;
        info.slots = slots;
    }
}
