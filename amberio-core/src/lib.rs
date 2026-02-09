//! Amberio Core - Core library for lightweight object storage for edge cloud nodes

pub mod cluster;
pub mod error;
pub mod node;
pub mod operations;
pub mod registry;
pub mod slot_manager;
pub mod storage;

pub use cluster::*;
pub use error::{AmberError, Result};
pub use node::{Node, NodeInfo, NodeStatus};
pub use operations::*;
pub use registry::etcd::EtcdRegistry;
pub use registry::redis::RedisRegistry;
pub use registry::{DynRegistry, Registry, RegistryBuilder, SlotEvent};
pub use slot_manager::{
    PART_SIZE, ReplicaStatus, Slot, SlotHealth, SlotInfo, SlotManager, TOTAL_SLOTS, slot_for_key,
};
pub use storage::{
    ArchiveStore, BlobHead, BlobMeta, HeadKind, MetadataStore, PartEntry, PartIndexState,
    PartStore, PutPartResult, RedisArchiveStore, TombstoneMeta, compute_hash, verify_hash,
};
