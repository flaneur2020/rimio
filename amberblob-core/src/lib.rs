//! AmberBlob Core - Core library for lightweight object storage for edge cloud nodes

pub mod error;
pub mod node;
pub mod registry;
pub mod slot_manager;
pub mod storage;
pub mod two_phase_commit;

pub use error::{AmberError, Result};
pub use node::{Node, NodeInfo, NodeStatus};
pub use registry::etcd::EtcdRegistry;
pub use registry::redis::RedisRegistry;
pub use registry::{DynRegistry, Registry, SlotEvent};
pub use slot_manager::{
    CHUNK_SIZE, ReplicaStatus, Slot, SlotHealth, SlotInfo, SlotManager, TOTAL_SLOTS, slot_for_key,
};
pub use storage::{
    BlobHead, BlobMeta, ChunkRef, ChunkStore, HeadKind, MetadataStore, PutPartResult,
    TombstoneMeta, compute_hash, verify_hash,
};

pub use storage::BlobMeta as ObjectMeta;
pub use storage::ChunkRef as ChunkInfo;
pub use two_phase_commit::{
    Transaction, TransactionState, TwoPhaseCommit, TwoPhaseParticipant, Vote,
};
