//! AmberBlob Core - Core library for lightweight object storage for edge cloud nodes

pub mod coordinator;
pub mod error;
pub mod node;
pub mod registry;
pub mod slot_manager;
pub mod storage;

pub use coordinator::{Coordinator, ReplicatedPart};
pub use error::{AmberError, Result};
pub use node::{Node, NodeInfo, NodeStatus};
pub use registry::etcd::EtcdRegistry;
pub use registry::redis::RedisRegistry;
pub use registry::{DynRegistry, Registry, SlotEvent};
pub use slot_manager::{
    PART_SIZE, ReplicaStatus, Slot, SlotHealth, SlotInfo, SlotManager, TOTAL_SLOTS, slot_for_key,
};
pub use storage::{
    BlobHead, BlobMeta, HeadKind, MetadataStore, PartRef, PartStore, PutPartResult, TombstoneMeta,
    compute_hash, verify_hash,
};
