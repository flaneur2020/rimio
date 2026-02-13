//! Rimio Core - Core library for lightweight object storage for edge cloud nodes

pub mod cluster;
pub mod error;
pub mod node;
pub mod operations;
pub mod registry;
pub mod slot_manager;
pub mod storage;

pub use cluster::*;
pub use error::{Result, RimError};
pub use node::{Node, NodeInfo, NodeStatus};
pub use operations::*;
pub use registry::etcd::EtcdRegistry;
pub use registry::gossip_internal_transport::{
    clear_global_ingress as clear_global_gossip_ingress,
    ingest_global_packet as ingest_global_gossip_packet,
    ingest_global_stream as ingest_global_gossip_stream,
};
pub use registry::redis::RedisRegistry;
pub use registry::{DynRegistry, Registry, RegistryBuilder, SlotEvent};
pub use slot_manager::{
    PART_SIZE, ReplicaStatus, Slot, SlotHealth, SlotInfo, SlotManager, TOTAL_SLOTS, slot_for_key,
};
pub use storage::{
    ArchiveListPage, ArchiveStore, BlobHead, BlobMeta, HeadKind, MetadataStore, PartEntry,
    PartIndexState, PartStore, PutPartResult, RedisArchiveStore, S3ArchiveStore, TombstoneMeta,
    compute_hash, parse_redis_archive_url, parse_s3_archive_url, read_archive_range_bytes,
    set_default_s3_archive_store, verify_hash,
};
