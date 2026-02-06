//! AmberBlob Core - Core library for lightweight object storage for edge cloud nodes
//!
//! A fixed-topology, leaderless object storage system using:
//! - 2048 fixed slots per group
//! - 2PC for consistency
//! - SHA256 content-addressed chunks
//! - SQLite for local metadata

pub mod error;
pub mod node;
pub mod registry;
pub mod slot_manager;
pub mod storage;
pub mod two_phase_commit;

pub use error::{AmberError, Result};
pub use node::{Node, NodeInfo, NodeStatus};
pub use registry::{Registry, DynRegistry, SlotEvent};
pub use registry::etcd::EtcdRegistry;
pub use registry::redis::RedisRegistry;
pub use slot_manager::{SlotManager, Slot, SlotInfo, SlotHealth, ReplicaStatus, slot_for_key, TOTAL_SLOTS, CHUNK_SIZE};
pub use storage::{ChunkStore, MetadataStore, ObjectMeta, ChunkInfo, compute_hash, verify_hash};
pub use two_phase_commit::{TwoPhaseCommit, TwoPhaseParticipant, Transaction, TransactionState, Vote};
