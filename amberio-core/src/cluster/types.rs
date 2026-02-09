use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone)]
pub struct Coordinator {
    min_write_replicas: usize,
}

#[derive(Clone)]
pub struct ReplicatedPart {
    pub part_no: u32,
    pub sha256: String,
    pub length: u64,
    pub data: Bytes,
}

impl Coordinator {
    pub fn new(min_write_replicas: usize) -> Self {
        Self { min_write_replicas }
    }

    pub fn write_quorum(&self, replica_count: usize) -> usize {
        self.min_write_replicas.min(replica_count).max(1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterDiskConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeConfig {
    pub node_id: String,
    pub bind_addr: String,
    #[serde(default)]
    pub advertise_addr: Option<String>,
    pub disks: Vec<ClusterDiskConfig>,
}

impl ClusterNodeConfig {
    pub fn effective_address(&self) -> String {
        self.advertise_addr
            .clone()
            .unwrap_or_else(|| self.bind_addr.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterReplicationConfig {
    pub min_write_replicas: usize,
    pub total_slots: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterArchiveConfig {
    pub archive_type: String,
    pub s3: Option<ClusterArchiveS3Config>,
    pub redis: Option<ClusterArchiveRedisConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterArchiveS3Config {
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
    pub credentials: ClusterArchiveS3Credentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterArchiveS3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterArchiveRedisConfig {
    pub url: String,
    #[serde(default = "default_archive_redis_key_prefix")]
    pub key_prefix: String,
}

fn default_archive_redis_key_prefix() -> String {
    "amberio:archive".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInitScanConfig {
    #[serde(default)]
    pub enabled: bool,
    pub redis: ClusterInitScanRedisConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInitScanRedisConfig {
    pub url: String,
    pub list_key: String,
    #[serde(default = "default_init_scan_page_size")]
    pub page_size: usize,
}

fn default_init_scan_page_size() -> usize {
    500
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInitScanEntry {
    pub path: String,
    pub size_bytes: u64,
    pub etag: String,
    pub archive_url: String,
    #[serde(default = "default_part_size")]
    pub part_size: u64,
    #[serde(default)]
    pub updated_at: Option<String>,
}

fn default_part_size() -> u64 {
    64 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub initialized_at: String,
    pub current_node: String,
    pub nodes: Vec<ClusterNodeConfig>,
    pub replication: ClusterReplicationConfig,
    pub archive: Option<ClusterArchiveConfig>,
    pub initialized_by: String,
}

#[derive(Debug, Clone)]
pub struct ClusterInitRequest {
    pub current_node: String,
    pub nodes: Vec<ClusterNodeConfig>,
    pub replication: ClusterReplicationConfig,
    pub archive: Option<ClusterArchiveConfig>,
    pub init_scan: Option<ClusterInitScanConfig>,
}

#[derive(Debug, Clone)]
pub struct ClusterInitResult {
    pub bootstrap_state: ClusterState,
    pub won_bootstrap_race: bool,
}
