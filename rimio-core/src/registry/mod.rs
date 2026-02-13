//! Registry module for node coordination and service discovery
//!
//! Provides a trait-based abstraction for different backend implementations
//! (etcd, Redis, etc.)

pub mod etcd;
pub mod factory;
pub mod gossip_memberlist;
pub mod redis;

use crate::error::Result;
use crate::node::NodeInfo;
use crate::slot_manager::{SlotHealth, SlotInfo};
use async_trait::async_trait;
use std::collections::HashMap;

pub use factory::RegistryBuilder;

/// Trait for registry implementations
#[async_trait]
pub trait Registry: Send + Sync {
    /// Register a node in the registry
    async fn register_node(&self, node: &NodeInfo) -> Result<()>;

    /// Get slot routing information
    async fn get_slot(&self, slot_id: u16) -> Result<Option<SlotInfo>>;

    /// Set slot routing information
    async fn set_slot(&self, info: &SlotInfo) -> Result<()>;

    /// Get all slot routing information
    async fn get_all_slots(&self) -> Result<HashMap<u16, SlotInfo>>;

    /// Report slot health status
    async fn report_health(&self, health: &SlotHealth) -> Result<()>;

    /// Get health status for all replicas of a slot
    async fn get_slot_health(&self, slot_id: u16) -> Result<Vec<SlotHealth>>;

    /// Get healthy replicas with latest seq for a slot
    async fn get_healthy_replicas(&self, slot_id: u16) -> Result<Vec<(String, String)>>;

    /// Get all nodes in the group
    async fn get_nodes(&self) -> Result<Vec<NodeInfo>>;

    /// Get persisted cluster bootstrap state bytes
    async fn get_bootstrap_state(&self) -> Result<Option<Vec<u8>>>;

    /// Persist bootstrap state only if absent (first-wins)
    async fn set_bootstrap_state_if_absent(&self, payload: &[u8]) -> Result<bool>;
}

/// Type alias for dynamic registry
pub type DynRegistry = dyn Registry;

/// Slot change events
#[derive(Debug, Clone)]
pub enum SlotEvent {
    Updated(SlotInfo),
    Deleted(u16),
}
