use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node: NodeConfig,
    pub etcd: EtcdConfig,
    pub archive: Option<ArchiveConfig>,
    pub replication: ReplicationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub group_id: String,
    pub bind_addr: String,
    pub disks: Vec<DiskConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    pub archive_type: String,
    pub s3: Option<S3Config>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub credentials: S3Credentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub min_write_replicas: usize,
    pub total_slots: u16,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            min_write_replicas: 3,
            total_slots: 2048,
        }
    }
}

impl Config {
    pub fn from_file(path: &str) -> crate::error::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path))
            .add_source(config::Environment::with_prefix("AMBERBLOB"))
            .build()
            .map_err(|e| crate::error::AmberError::Config(e.to_string()))?;

        let config: Config = settings
            .try_deserialize()
            .map_err(|e| crate::error::AmberError::Config(e.to_string()))?;

        Ok(config)
    }
}
