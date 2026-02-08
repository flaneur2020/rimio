//! Storage modules for AmberBlob
//!
//! Provides filesystem part storage and metadata management.

pub mod chunk_store;
pub mod metadata_store;

pub use chunk_store::{ChunkStore, PutPartResult, compute_hash, verify_hash};
pub use metadata_store::{BlobHead, BlobMeta, ChunkRef, HeadKind, MetadataStore, TombstoneMeta};

pub use metadata_store::ChunkInfo as _ChunkInfoAlias;
pub use metadata_store::ObjectMeta as _ObjectMetaAlias;
