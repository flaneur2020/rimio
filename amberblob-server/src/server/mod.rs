use crate::config::{Config, RegistryBackend};
use amberblob_core::{
    AmberError, Coordinator, EtcdRegistry, MetadataStore, Node, NodeInfo, PartStore, RedisRegistry,
    Registry, Result,
};
use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};

mod external;
mod internal;
mod types;

use external::{
    health, v1_delete_blob, v1_get_blob, v1_head_blob, v1_healthz, v1_list_blobs, v1_nodes,
    v1_put_blob, v1_resolve_slot,
};
use internal::{
    internal_get_head, internal_get_part, internal_put_head, internal_put_part,
    v1_internal_heal_buckets, v1_internal_heal_heads, v1_internal_heal_repair,
};
pub(crate) use types::*;

pub struct ServerState {
    pub(crate) node: Arc<Node>,
    pub(crate) slot_manager: Arc<amberblob_core::SlotManager>,
    pub(crate) part_store: Arc<PartStore>,
    pub(crate) registry: Arc<dyn Registry>,
    pub(crate) config: Config,
    pub(crate) coordinator: Arc<Coordinator>,
    pub(crate) idempotent_puts: Arc<RwLock<HashMap<String, PutCacheEntry>>>,
}

pub async fn run_server(config: Config) -> Result<()> {
    let node_cfg = config.node.clone();

    let disk_paths: Vec<std::path::PathBuf> = node_cfg
        .disks
        .iter()
        .map(|disk| disk.path.clone())
        .collect();

    let node = Arc::new(Node::new(
        node_cfg.node_id.clone(),
        node_cfg.group_id.clone(),
        node_cfg.bind_addr.clone(),
        disk_paths,
    )?);

    let data_dir = node_cfg
        .disks
        .first()
        .map(|disk| disk.path.clone())
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/amberblob"));

    let slot_manager = Arc::new(amberblob_core::SlotManager::new(
        node_cfg.node_id.clone(),
        data_dir.clone(),
    )?);

    let part_store = Arc::new(PartStore::new(data_dir)?);

    let registry: Arc<dyn Registry> = match config.registry.backend {
        RegistryBackend::Etcd => {
            let etcd_cfg = config.registry.etcd.as_ref().ok_or_else(|| {
                AmberError::Config("etcd configuration is required for etcd backend".to_string())
            })?;
            Arc::new(EtcdRegistry::new(&etcd_cfg.endpoints, &node_cfg.group_id).await?)
        }
        RegistryBackend::Redis => {
            let redis_cfg = config.registry.redis.as_ref().ok_or_else(|| {
                AmberError::Config("redis configuration is required for redis backend".to_string())
            })?;
            Arc::new(RedisRegistry::new(&redis_cfg.url, &node_cfg.group_id).await?)
        }
    };

    let coordinator = Arc::new(Coordinator::new(config.replication.min_write_replicas));

    let state = Arc::new(ServerState {
        node,
        slot_manager,
        part_store,
        registry,
        config,
        coordinator,
        idempotent_puts: Arc::new(RwLock::new(HashMap::new())),
    });

    register_local_node(&state).await?;

    {
        let heartbeat_state = state.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(20));
            loop {
                ticker.tick().await;
                if let Err(error) = register_local_node(&heartbeat_state).await {
                    tracing::warn!("Failed to refresh node registration: {}", error);
                }
            }
        });
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/healthz", get(v1_healthz))
        .route("/api/v1/nodes", get(v1_nodes))
        .route("/api/v1/slots/resolve", get(v1_resolve_slot))
        .route("/api/v1/blobs", get(v1_list_blobs))
        .route(
            "/api/v1/blobs/*path",
            get(v1_get_blob)
                .head(v1_head_blob)
                .put(v1_put_blob)
                .delete(v1_delete_blob),
        )
        .route(
            "/internal/v1/slots/:slot_id/parts/:sha256",
            put(internal_put_part).get(internal_get_part),
        )
        .route(
            "/internal/v1/slots/:slot_id/heads",
            put(internal_put_head).get(internal_get_head),
        )
        .route(
            "/internal/v1/slots/:slot_id/heal/buckets",
            get(v1_internal_heal_buckets),
        )
        .route(
            "/internal/v1/slots/:slot_id/heal/heads",
            post(v1_internal_heal_heads),
        )
        .route(
            "/internal/v1/slots/:slot_id/heal/repair",
            post(v1_internal_heal_repair),
        )
        .with_state(state);

    let listener = TcpListener::bind(&node_cfg.bind_addr).await?;
    tracing::info!("AmberBlob listening on {}", node_cfg.bind_addr);

    axum::serve(listener, app)
        .await
        .map_err(|error| AmberError::Http(error.to_string()))?;

    Ok(())
}

pub(crate) async fn register_local_node(state: &ServerState) -> Result<()> {
    let info = state.node.info().await;
    state.registry.register_node(&info).await
}

pub(crate) async fn ensure_store(state: &ServerState, slot_id: u16) -> Result<MetadataStore> {
    if !state.slot_manager.has_slot(slot_id).await {
        state.slot_manager.init_slot(slot_id).await?;
    }

    let slot = state.slot_manager.get_slot(slot_id).await?;
    MetadataStore::new(slot)
}

pub(crate) async fn current_nodes(state: &ServerState) -> Result<Vec<NodeInfo>> {
    let mut nodes = state.registry.get_nodes().await.unwrap_or_default();

    let local = state.node.info().await;
    if !nodes.iter().any(|node| node.node_id == local.node_id) {
        nodes.push(local);
    }

    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    nodes.dedup_by(|a, b| a.node_id == b.node_id);

    Ok(nodes)
}

pub(crate) async fn resolve_replica_nodes(
    state: &ServerState,
    slot_id: u16,
) -> Result<Vec<NodeInfo>> {
    let nodes = current_nodes(state).await?;
    if nodes.is_empty() {
        return Err(AmberError::Internal("no nodes found".to_string()));
    }

    let start = (slot_id as usize) % nodes.len();
    let mut rotated = Vec::with_capacity(nodes.len());
    for index in 0..nodes.len() {
        rotated.push(nodes[(start + index) % nodes.len()].clone());
    }

    let replica_count = rotated.len().min(3).max(1);
    Ok(rotated.into_iter().take(replica_count).collect())
}

pub(crate) fn normalize_blob_path(path: &str) -> Result<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AmberError::InvalidRequest(
            "blob path cannot be empty".to_string(),
        ));
    }

    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(AmberError::InvalidRequest(format!(
                "invalid blob path component: {}",
                component
            )));
        }
        components.push(component);
    }

    Ok(components.join("/"))
}

pub(crate) fn response_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
        .into_response()
}

pub(crate) fn status_string(status: &amberblob_core::NodeStatus) -> &'static str {
    match status {
        amberblob_core::NodeStatus::Healthy => "healthy",
        amberblob_core::NodeStatus::Degraded => "degraded",
        amberblob_core::NodeStatus::Unhealthy => "unhealthy",
    }
}
