use crate::config::{ArchiveConfig, RuntimeConfig};
use amberio_core::{
    AmberError, ArchiveStore, ClusterClient, Coordinator, DeleteBlobOperation, HealHeadsOperation,
    HealRepairOperation, HealSlotletsOperation, InternalGetHeadOperation, InternalGetPartOperation,
    InternalPutHeadOperation, InternalPutPartOperation, ListBlobsOperation, Node, NodeInfo,
    PartStore, PutBlobArchiveWriter, PutBlobOperation, ReadBlobOperation, RedisArchiveStore,
    Registry, Result, S3ArchiveStore, set_default_s3_archive_store,
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
    v1_internal_heal_heads, v1_internal_heal_repair, v1_internal_heal_slotlets,
};
pub(crate) use types::*;

pub struct ServerState {
    pub(crate) node: Arc<Node>,
    pub(crate) registry: Arc<dyn Registry>,
    pub(crate) config: RuntimeConfig,
    pub(crate) coordinator: Arc<Coordinator>,
    pub(crate) put_blob_operation: Arc<PutBlobOperation>,
    pub(crate) read_blob_operation: Arc<ReadBlobOperation>,
    pub(crate) delete_blob_operation: Arc<DeleteBlobOperation>,
    pub(crate) list_blobs_operation: Arc<ListBlobsOperation>,
    pub(crate) internal_put_part_operation: Arc<InternalPutPartOperation>,
    pub(crate) internal_get_part_operation: Arc<InternalGetPartOperation>,
    pub(crate) internal_put_head_operation: Arc<InternalPutHeadOperation>,
    pub(crate) internal_get_head_operation: Arc<InternalGetHeadOperation>,
    pub(crate) heal_slotlets_operation: Arc<HealSlotletsOperation>,
    pub(crate) heal_heads_operation: Arc<HealHeadsOperation>,
    pub(crate) heal_repair_operation: Arc<HealRepairOperation>,
    pub(crate) idempotent_puts: Arc<RwLock<HashMap<String, PutCacheEntry>>>,
}

pub async fn run_server(config: RuntimeConfig, registry: Arc<dyn Registry>) -> Result<()> {
    let node_cfg = config.node.clone();

    let disk_paths: Vec<std::path::PathBuf> = node_cfg
        .disks
        .iter()
        .map(|disk| disk.path.clone())
        .collect();

    let node = Arc::new(Node::new(
        node_cfg.node_id.clone(),
        config.registry.namespace_or_default().to_string(),
        node_cfg.advertise_addr.clone(),
        disk_paths,
    )?);

    let data_dir = node_cfg
        .disks
        .first()
        .map(|disk| disk.path.clone())
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/amberio"));

    let slot_manager = Arc::new(amberio_core::SlotManager::new(
        node_cfg.node_id.clone(),
        data_dir.clone(),
    )?);

    let part_store = Arc::new(PartStore::new(data_dir)?);

    let coordinator = Arc::new(Coordinator::new(config.replication.min_write_replicas));
    let cluster_client = Arc::new(ClusterClient::new(registry.clone()));

    let (runtime_archive_store, archive_key_prefix) =
        build_runtime_archive(config.archive.as_ref())?;
    let archive_writer = runtime_archive_store.as_ref().and_then(|store| {
        archive_key_prefix
            .as_ref()
            .map(|prefix| PutBlobArchiveWriter::new(store.clone(), prefix.clone()))
    });

    let put_blob_operation = Arc::new(PutBlobOperation::new(
        slot_manager.clone(),
        part_store.clone(),
        coordinator.clone(),
        cluster_client.clone(),
        archive_writer,
    ));
    let read_blob_operation = Arc::new(ReadBlobOperation::new(
        slot_manager.clone(),
        part_store.clone(),
        cluster_client.clone(),
    ));
    let delete_blob_operation = Arc::new(DeleteBlobOperation::new(
        slot_manager.clone(),
        coordinator.clone(),
        cluster_client.clone(),
    ));
    let list_blobs_operation = Arc::new(ListBlobsOperation::new(slot_manager.clone()));

    let internal_put_part_operation = Arc::new(InternalPutPartOperation::new(
        slot_manager.clone(),
        part_store.clone(),
    ));
    let internal_get_part_operation = Arc::new(InternalGetPartOperation::new(
        slot_manager.clone(),
        part_store.clone(),
    ));
    let internal_put_head_operation = Arc::new(InternalPutHeadOperation::new(slot_manager.clone()));
    let internal_get_head_operation = Arc::new(InternalGetHeadOperation::new(slot_manager.clone()));

    let heal_slotlets_operation = Arc::new(HealSlotletsOperation::new(slot_manager.clone()));
    let heal_heads_operation = Arc::new(HealHeadsOperation::new(slot_manager.clone()));
    let heal_repair_operation = Arc::new(HealRepairOperation::new(read_blob_operation.clone()));

    let state = Arc::new(ServerState {
        node,
        registry,
        config,
        coordinator,
        put_blob_operation,
        read_blob_operation,
        delete_blob_operation,
        list_blobs_operation,
        internal_put_part_operation,
        internal_get_part_operation,
        internal_put_head_operation,
        internal_get_head_operation,
        heal_slotlets_operation,
        heal_heads_operation,
        heal_repair_operation,
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
            "/internal/v1/slots/:slot_id/heal/slotlets",
            get(v1_internal_heal_slotlets),
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
    tracing::info!("Amberio listening on {}", node_cfg.bind_addr);

    axum::serve(listener, app)
        .await
        .map_err(|error| AmberError::Http(error.to_string()))?;

    Ok(())
}

fn build_runtime_archive(
    config: Option<&ArchiveConfig>,
) -> Result<(Option<Arc<dyn ArchiveStore>>, Option<String>)> {
    let Some(config) = config else {
        return Ok((None, None));
    };

    if config.archive_type.eq_ignore_ascii_case("redis") {
        let redis = config.redis.as_ref().ok_or_else(|| {
            AmberError::Config("archive.redis is required when archive_type=redis".to_string())
        })?;

        let store: Arc<dyn ArchiveStore> = Arc::new(RedisArchiveStore::new(redis.url.as_str())?);
        return Ok((Some(store), Some(redis.key_prefix.clone())));
    }

    if config.archive_type.eq_ignore_ascii_case("s3") {
        let s3 = config.s3.as_ref().ok_or_else(|| {
            AmberError::Config("archive.s3 is required when archive_type=s3".to_string())
        })?;

        let s3_store = Arc::new(S3ArchiveStore::new(
            s3.bucket.as_str(),
            s3.region.as_str(),
            s3.endpoint.as_deref(),
            s3.allow_http,
            s3.credentials.access_key_id.as_str(),
            s3.credentials.secret_access_key.as_str(),
        )?);
        set_default_s3_archive_store(s3_store.clone());

        let store: Arc<dyn ArchiveStore> = s3_store;
        return Ok((Some(store), Some("amberio/archive".to_string())));
    }

    Err(AmberError::Config(format!(
        "unsupported archive_type for runtime: {}",
        config.archive_type
    )))
}

pub(crate) async fn register_local_node(state: &ServerState) -> Result<()> {
    let info = state.node.info().await;
    state.registry.register_node(&info).await
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

pub(crate) fn status_string(status: &amberio_core::NodeStatus) -> &'static str {
    match status {
        amberio_core::NodeStatus::Healthy => "healthy",
        amberio_core::NodeStatus::Degraded => "degraded",
        amberio_core::NodeStatus::Unhealthy => "unhealthy",
    }
}
