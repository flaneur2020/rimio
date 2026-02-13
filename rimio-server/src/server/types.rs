use rimio_core::{BlobMeta, ClusterState, TombstoneMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub(crate) struct PutCacheEntry {
    pub(crate) generation: i64,
    pub(crate) etag: String,
    pub(crate) size_bytes: u64,
    pub(crate) committed_replicas: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct ErrorResponse {
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct HealthResponse {
    pub(crate) status: String,
    pub(crate) node_id: String,
    pub(crate) group_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct NodesResponse {
    pub(crate) nodes: Vec<NodeItem>,
}

#[derive(Debug, Serialize)]
pub(crate) struct NodeItem {
    pub(crate) node_id: String,
    pub(crate) address: String,
    pub(crate) status: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResolveSlotQuery {
    pub(crate) path: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ResolveSlotResponse {
    pub(crate) path: String,
    pub(crate) slot_id: u16,
    pub(crate) replicas: Vec<String>,
    pub(crate) write_quorum: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct PutBlobResponse {
    pub(crate) path: String,
    pub(crate) slot_id: u16,
    pub(crate) generation: i64,
    pub(crate) etag: String,
    pub(crate) size_bytes: u64,
    pub(crate) committed_replicas: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) idempotent_replay: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListQuery {
    #[serde(default)]
    pub(crate) prefix: String,
    #[serde(default = "default_limit")]
    pub(crate) limit: usize,
    #[serde(default)]
    pub(crate) cursor: Option<String>,
    #[serde(default)]
    pub(crate) include_deleted: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListResponse {
    pub(crate) items: Vec<ListItem>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListItem {
    pub(crate) path: String,
    pub(crate) generation: i64,
    pub(crate) etag: String,
    pub(crate) size_bytes: u64,
    pub(crate) deleted: bool,
    pub(crate) updated_at: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InternalPathQuery {
    pub(crate) path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InternalPartQuery {
    pub(crate) path: Option<String>,
    pub(crate) generation: Option<i64>,
    pub(crate) part_no: Option<u32>,
}

#[derive(Debug, Serialize)]
pub(crate) struct InternalPartPutResponse {
    pub(crate) accepted: bool,
    pub(crate) reused: bool,
    pub(crate) sha256: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct InternalHeadApplyRequest {
    pub(crate) head_kind: String,
    pub(crate) generation: i64,
    pub(crate) head_sha256: String,
    pub(crate) meta: Option<BlobMeta>,
    pub(crate) tombstone: Option<TombstoneMeta>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct InternalHeadResponse {
    pub(crate) found: bool,
    pub(crate) head_kind: Option<String>,
    pub(crate) generation: Option<i64>,
    pub(crate) head_sha256: Option<String>,
    pub(crate) meta: Option<BlobMeta>,
    pub(crate) tombstone: Option<TombstoneMeta>,
}

#[derive(Debug, Serialize)]
pub(crate) struct InternalHeadApplyResponse {
    pub(crate) applied: bool,
    pub(crate) head_kind: String,
    pub(crate) generation: i64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HealSlotletsQuery {
    #[serde(default = "default_slotlet_prefix_len")]
    pub(crate) prefix_len: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct HealSlotletsResponse {
    pub(crate) slot_id: u16,
    pub(crate) prefix_len: usize,
    pub(crate) slotlets: Vec<HealSlotlet>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HealSlotlet {
    pub(crate) prefix: String,
    pub(crate) digest: String,
    pub(crate) objects: usize,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HealHeadsRequest {
    pub(crate) prefixes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HealHeadsResponse {
    pub(crate) slot_id: u16,
    pub(crate) heads: Vec<HealHeadItem>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HealHeadItem {
    pub(crate) path: String,
    pub(crate) head_kind: String,
    pub(crate) generation: i64,
    pub(crate) head_sha256: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HealRepairRequest {
    pub(crate) source_node_id: String,
    pub(crate) blob_paths: Vec<String>,
    #[serde(default)]
    pub(crate) dry_run: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct HealRepairResponse {
    pub(crate) slot_id: u16,
    pub(crate) repaired_objects: usize,
    pub(crate) skipped_objects: usize,
    pub(crate) errors: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct InternalBootstrapResponse {
    pub(crate) found: bool,
    pub(crate) namespace: String,
    pub(crate) state: Option<ClusterState>,
}

#[derive(Debug, Serialize)]
pub(crate) struct InternalGossipSeedsResponse {
    pub(crate) seeds: Vec<String>,
}

fn default_limit() -> usize {
    100
}

fn default_slotlet_prefix_len() -> usize {
    2
}
