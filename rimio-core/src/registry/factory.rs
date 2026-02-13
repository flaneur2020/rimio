use super::{
    Registry, etcd::EtcdRegistry, gossip_memberlist::GossipMemberlistRegistry, redis::RedisRegistry,
};
use crate::{Result, RimError};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct RegistryBuilder {
    backend: Option<String>,
    namespace: Option<String>,
    gossip_node_id: Option<String>,
    gossip_transport: Option<String>,
    etcd_endpoints: Option<Vec<String>>,
    redis_url: Option<String>,
    gossip_bind_addr: Option<String>,
    gossip_advertise_addr: Option<String>,
    gossip_seeds: Option<Vec<String>>,
}

impl RegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn backend(mut self, backend: impl Into<String>) -> Self {
        self.backend = Some(backend.into());
        self
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn gossip_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.gossip_node_id = Some(node_id.into());
        self
    }

    pub fn gossip_transport(mut self, transport: impl Into<String>) -> Self {
        self.gossip_transport = Some(transport.into());
        self
    }

    pub fn etcd_endpoints(mut self, endpoints: Vec<String>) -> Self {
        self.etcd_endpoints = Some(endpoints);
        self
    }

    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.redis_url = Some(url.into());
        self
    }

    pub fn gossip_bind_addr(mut self, addr: impl Into<String>) -> Self {
        self.gossip_bind_addr = Some(addr.into());
        self
    }

    pub fn gossip_advertise_addr(mut self, addr: impl Into<String>) -> Self {
        self.gossip_advertise_addr = Some(addr.into());
        self
    }

    pub fn gossip_seeds(mut self, seeds: Vec<String>) -> Self {
        self.gossip_seeds = Some(seeds);
        self
    }

    fn resolve_namespace(&self) -> Result<String> {
        let namespace = self
            .namespace
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_string();
        if namespace.is_empty() {
            return Err(RimError::Config(
                "registry namespace cannot be empty".to_string(),
            ));
        }

        Ok(namespace)
    }

    fn resolve_backend(&self) -> Result<String> {
        let backend = self
            .backend
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();

        if backend.is_empty() {
            return Err(RimError::Config(
                "registry backend cannot be empty".to_string(),
            ));
        }

        Ok(backend)
    }

    pub async fn build(&self) -> Result<Arc<dyn Registry>> {
        let namespace = self.resolve_namespace()?;
        let backend = self.resolve_backend()?;

        match backend.as_str() {
            "etcd" => {
                let endpoints = self.etcd_endpoints.clone().ok_or_else(|| {
                    RimError::Config("etcd endpoints are required for etcd backend".to_string())
                })?;

                if endpoints.is_empty() {
                    return Err(RimError::Config(
                        "etcd endpoints cannot be empty for etcd backend".to_string(),
                    ));
                }

                let registry = EtcdRegistry::new(&endpoints, &namespace).await?;
                Ok(Arc::new(registry))
            }
            "redis" => {
                let url = self.redis_url.as_deref().unwrap_or_default().trim();
                if url.is_empty() {
                    return Err(RimError::Config(
                        "redis url is required for redis backend".to_string(),
                    ));
                }

                let registry = RedisRegistry::new(url, &namespace).await?;
                Ok(Arc::new(registry))
            }
            "gossip" => {
                let transport = self
                    .gossip_transport
                    .as_deref()
                    .unwrap_or("internal_http")
                    .trim()
                    .to_ascii_lowercase();

                if transport != "internal_http"
                    && transport != "openraft_http"
                    && transport != "openraft"
                {
                    return Err(RimError::Config(format!(
                        "unsupported gossip transport '{}': expected internal_http",
                        transport
                    )));
                }

                let bind_addr = self.gossip_bind_addr.as_deref().unwrap_or_default().trim();
                if bind_addr.is_empty() {
                    return Err(RimError::Config(
                        "gossip bind_addr is required for gossip backend".to_string(),
                    ));
                }

                let node_id = self.gossip_node_id.as_deref().unwrap_or_default().trim();
                if node_id.is_empty() {
                    return Err(RimError::Config(
                        "gossip node_id is required for gossip backend".to_string(),
                    ));
                }

                let advertise_addr = self
                    .gossip_advertise_addr
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string);

                let seeds = self
                    .gossip_seeds
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|seed| seed.trim().to_string())
                    .filter(|seed| !seed.is_empty())
                    .collect();

                let registry = GossipMemberlistRegistry::new(
                    &namespace,
                    node_id,
                    bind_addr,
                    advertise_addr.as_deref(),
                    seeds,
                    Some(transport.as_str()),
                )
                .await?;
                Ok(Arc::new(registry))
            }
            other => Err(RimError::Config(format!(
                "unsupported registry backend: {}",
                other
            ))),
        }
    }
}
