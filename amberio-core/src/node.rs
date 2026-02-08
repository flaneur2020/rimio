use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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
    node_id: String,
    group_id: String,
    disks: Vec<PathBuf>,
    info: Arc<RwLock<NodeInfo>>,
}

impl Node {
    pub fn new(
        node_id: String,
        group_id: String,
        bind_addr: String,
        disks: Vec<PathBuf>,
    ) -> Result<Self> {
        let info = NodeInfo {
            node_id: node_id.clone(),
            group_id: group_id.clone(),
            address: bind_addr,
            status: NodeStatus::Healthy,
            slots: Vec::new(),
        };

        Ok(Self {
            node_id,
            group_id,
            disks,
            info: Arc::new(RwLock::new(info)),
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn disks(&self) -> &[PathBuf] {
        &self.disks
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
