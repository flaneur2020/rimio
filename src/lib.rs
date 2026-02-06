//! AmberBlob - Lightweight object storage for edge cloud nodes
//!
//! A fixed-topology, leaderless object storage system using:
//! - 2048 fixed slots per group
//! - 2PC for consistency
//! - SHA256 content-addressed chunks
//! - SQLite for local metadata

pub mod chunk_store;
pub mod config;
pub mod error;
pub mod etcd_store;
pub mod metadata_store;
pub mod node;
pub mod server;
pub mod slot_manager;
pub mod two_phase_commit;

pub use config::Config;
pub use error::{AmberError, Result};
