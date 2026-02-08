//! Storage modules for AmberBlob
//!
//! Provides filesystem part storage and metadata management.

pub mod metadata_store;
pub mod part_store;

pub use metadata_store::{BlobHead, BlobMeta, HeadKind, MetadataStore, PartRef, TombstoneMeta};
pub use part_store::{PartStore, PutPartResult, compute_hash, verify_hash};
