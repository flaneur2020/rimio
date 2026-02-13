use rimio_core::{
    ClusterArchiveConfig, ClusterArchiveRedisConfig, ClusterArchiveS3Config,
    ClusterArchiveS3Credentials, ClusterDiskConfig, ClusterInitRequest, ClusterInitScanConfig,
    ClusterInitScanRedisConfig, ClusterNodeConfig, ClusterReplicationConfig, ClusterState,
    RegistryBuilder, Result, RimError,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub registry: RegistryConfig,
    pub initial_cluster: InitialClusterConfig,
    pub archive: Option<ArchiveConfig>,
    #[serde(default)]
    pub init_scan: Option<InitScanConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialClusterConfig {
    pub nodes: Vec<InitialNodeConfig>,
    pub replication: ReplicationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialNodeConfig {
    pub node_id: String,
    pub bind_addr: String,
    #[serde(default)]
    pub advertise_addr: Option<String>,
    pub disks: Vec<DiskConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub node: RuntimeNodeConfig,
    pub replication: ReplicationConfig,
    pub registry: RegistryConfig,
    pub archive: Option<ArchiveConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeNodeConfig {
    pub node_id: String,
    pub bind_addr: String,
    pub advertise_addr: String,
    pub disks: Vec<DiskConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub backend: RegistryBackend,
    #[serde(default)]
    pub namespace: Option<String>,
    pub etcd: Option<EtcdConfig>,
    pub redis: Option<RedisConfig>,
    pub gossip: Option<GossipConfig>,
}

impl RegistryConfig {
    pub fn namespace_or_default(&self) -> &str {
        self.namespace
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("default")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RegistryBackend {
    Etcd,
    Redis,
    Gossip,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    #[serde(default = "default_redis_pool_size")]
    pub pool_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipConfig {
    #[serde(default = "default_gossip_transport")]
    pub transport: String,
    #[serde(default)]
    pub seeds: Vec<String>,
}

fn default_gossip_transport() -> String {
    "internal_http".to_string()
}

fn default_redis_pool_size() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    pub archive_type: String,
    pub s3: Option<S3Config>,
    pub redis: Option<ArchiveRedisConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveRedisConfig {
    pub url: String,
    #[serde(default = "default_archive_redis_key_prefix")]
    pub key_prefix: String,
}

fn default_archive_redis_key_prefix() -> String {
    "rimio:archive".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitScanConfig {
    #[serde(default)]
    pub enabled: bool,
    pub redis: InitScanRedisConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitScanRedisConfig {
    pub url: String,
    pub list_key: String,
    #[serde(default = "default_init_scan_page_size")]
    pub page_size: usize,
}

fn default_init_scan_page_size() -> usize {
    500
}

pub type BootstrapState = ClusterState;

impl Config {
    pub fn from_file(path: &str) -> Result<Self> {
        let settings = ::config::Config::builder()
            .add_source(::config::File::with_name(path))
            .add_source(::config::Environment::with_prefix("RIMIO"))
            .build()
            .map_err(|e| RimError::Config(e.to_string()))?;

        let config: Config = settings
            .try_deserialize()
            .map_err(|e| RimError::Config(e.to_string()))?;

        Ok(config)
    }

    #[allow(dead_code)]
    pub fn to_init_cluster_request(&self) -> ClusterInitRequest {
        self.to_init_cluster_request_for_node("")
    }

    pub fn to_init_cluster_request_for_node(&self, current_node: &str) -> ClusterInitRequest {
        ClusterInitRequest {
            current_node: current_node.to_string(),
            nodes: self
                .initial_cluster
                .nodes
                .iter()
                .map(|node| ClusterNodeConfig {
                    node_id: node.node_id.clone(),
                    bind_addr: node.bind_addr.clone(),
                    advertise_addr: node.advertise_addr.clone(),
                    disks: node
                        .disks
                        .iter()
                        .map(|disk| ClusterDiskConfig {
                            path: disk.path.clone(),
                        })
                        .collect(),
                })
                .collect(),
            replication: ClusterReplicationConfig {
                min_write_replicas: self.initial_cluster.replication.min_write_replicas,
                total_slots: self.initial_cluster.replication.total_slots,
            },
            archive: self.archive.as_ref().map(|archive| ClusterArchiveConfig {
                archive_type: archive.archive_type.clone(),
                s3: archive.s3.as_ref().map(|s3| ClusterArchiveS3Config {
                    bucket: s3.bucket.clone(),
                    region: s3.region.clone(),
                    endpoint: s3.endpoint.clone(),
                    allow_http: s3.allow_http,
                    credentials: ClusterArchiveS3Credentials {
                        access_key_id: s3.credentials.access_key_id.clone(),
                        secret_access_key: s3.credentials.secret_access_key.clone(),
                    },
                }),
                redis: archive
                    .redis
                    .as_ref()
                    .map(|redis| ClusterArchiveRedisConfig {
                        url: redis.url.clone(),
                        key_prefix: redis.key_prefix.clone(),
                    }),
            }),
            init_scan: self.init_scan.as_ref().map(|scan| ClusterInitScanConfig {
                enabled: scan.enabled,
                redis: ClusterInitScanRedisConfig {
                    url: scan.redis.url.clone(),
                    list_key: scan.redis.list_key.clone(),
                    page_size: scan.redis.page_size,
                },
            }),
        }
    }

    #[allow(dead_code)]
    pub fn registry_builder(&self) -> RegistryBuilder {
        self.registry_builder_for_node("")
    }

    pub fn registry_builder_for_node(&self, node_id: &str) -> RegistryBuilder {
        let builder = RegistryBuilder::new().namespace(self.registry.namespace_or_default());

        match self.registry.backend {
            RegistryBackend::Etcd => {
                let endpoints = self
                    .registry
                    .etcd
                    .as_ref()
                    .map(|cfg| cfg.endpoints.clone())
                    .unwrap_or_default();

                builder.backend("etcd").etcd_endpoints(endpoints)
            }
            RegistryBackend::Redis => {
                let url = self
                    .registry
                    .redis
                    .as_ref()
                    .map(|cfg| cfg.url.clone())
                    .unwrap_or_default();

                builder.backend("redis").redis_url(url)
            }
            RegistryBackend::Gossip => {
                let gossip = self.registry.gossip.clone().unwrap_or(GossipConfig {
                    transport: default_gossip_transport(),
                    seeds: Vec::new(),
                });

                let mut builder = builder
                    .backend("gossip")
                    .gossip_transport(gossip.transport)
                    .gossip_node_id(node_id.to_string())
                    .gossip_seeds(gossip.seeds);

                if let Some(node) = self
                    .initial_cluster
                    .nodes
                    .iter()
                    .find(|node| node.node_id == node_id)
                {
                    if !node.bind_addr.trim().is_empty() {
                        builder = builder.gossip_bind_addr(node.bind_addr.clone());
                    }

                    if let Some(advertise_addr) = node
                        .advertise_addr
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                    {
                        builder = builder.gossip_advertise_addr(advertise_addr.to_string());
                    }
                }

                builder
            }
        }
    }

    #[allow(dead_code)]
    pub fn runtime_from_bootstrap(&self, bootstrap: &BootstrapState) -> Result<RuntimeConfig> {
        let current_node = self
            .initial_cluster
            .nodes
            .first()
            .ok_or_else(|| RimError::Config("initial_cluster.nodes cannot be empty".to_string()))?
            .node_id
            .clone();

        Self::runtime_from_bootstrap_for_node(bootstrap, &current_node, self.registry.clone())
    }

    pub fn runtime_from_bootstrap_for_node(
        bootstrap: &BootstrapState,
        current_node: &str,
        registry: RegistryConfig,
    ) -> Result<RuntimeConfig> {
        let current_node = bootstrap
            .nodes
            .iter()
            .find(|node| node.node_id == current_node)
            .ok_or_else(|| {
                RimError::Config(format!(
                    "current_node '{}' not found in bootstrap nodes",
                    current_node
                ))
            })?;

        Ok(RuntimeConfig {
            node: RuntimeNodeConfig {
                node_id: current_node.node_id.clone(),
                bind_addr: current_node.bind_addr.clone(),
                advertise_addr: current_node.effective_address(),
                disks: current_node
                    .disks
                    .iter()
                    .map(|disk| DiskConfig {
                        path: disk.path.clone(),
                    })
                    .collect(),
            },
            replication: ReplicationConfig {
                min_write_replicas: bootstrap.replication.min_write_replicas,
                total_slots: bootstrap.replication.total_slots,
            },
            registry,
            archive: bootstrap.archive.as_ref().map(|archive| ArchiveConfig {
                archive_type: archive.archive_type.clone(),
                s3: archive.s3.as_ref().map(|s3| S3Config {
                    bucket: s3.bucket.clone(),
                    region: s3.region.clone(),
                    endpoint: s3.endpoint.clone(),
                    allow_http: s3.allow_http,
                    credentials: S3Credentials {
                        access_key_id: s3.credentials.access_key_id.clone(),
                        secret_access_key: s3.credentials.secret_access_key.clone(),
                    },
                }),
                redis: archive.redis.as_ref().map(|redis| ArchiveRedisConfig {
                    url: redis.url.clone(),
                    key_prefix: redis.key_prefix.clone(),
                }),
            }),
        })
    }
}
