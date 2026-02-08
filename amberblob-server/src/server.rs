use crate::config::{Config, RegistryBackend};
use amberblob_core::{
    AmberError, BlobHead, BlobMeta, CHUNK_SIZE, ChunkRef, ChunkStore, EtcdRegistry, HeadKind,
    MetadataStore, Node, NodeInfo, RedisRegistry, Registry, Result, TombstoneMeta, compute_hash,
    slot_for_key,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};

pub struct ServerState {
    node: Arc<Node>,
    slot_manager: Arc<amberblob_core::SlotManager>,
    chunk_store: Arc<ChunkStore>,
    registry: Arc<dyn Registry>,
    config: Config,
    client: reqwest::Client,
    idempotent_puts: Arc<RwLock<HashMap<String, PutCacheEntry>>>,
}

#[derive(Debug, Clone)]
struct PutCacheEntry {
    generation: i64,
    etag: String,
    size_bytes: u64,
    committed_replicas: usize,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    node_id: String,
    group_id: String,
}

#[derive(Debug, Serialize)]
struct NodesResponse {
    nodes: Vec<NodeItem>,
}

#[derive(Debug, Serialize)]
struct NodeItem {
    node_id: String,
    address: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ResolveSlotQuery {
    path: String,
}

#[derive(Debug, Serialize)]
struct ResolveSlotResponse {
    path: String,
    slot_id: u16,
    replicas: Vec<String>,
    write_quorum: usize,
}

#[derive(Debug, Serialize)]
struct PutBlobResponse {
    path: String,
    slot_id: u16,
    generation: i64,
    etag: String,
    size_bytes: u64,
    committed_replicas: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotent_replay: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default)]
    prefix: String,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    include_deleted: bool,
}

#[derive(Debug, Serialize)]
struct ListResponse {
    items: Vec<ListItem>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct ListItem {
    path: String,
    generation: i64,
    etag: String,
    size_bytes: u64,
    deleted: bool,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
struct InternalPathQuery {
    path: Option<String>,
}

#[derive(Debug, Serialize)]
struct InternalPartPutResponse {
    accepted: bool,
    reused: bool,
    sha256: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct InternalHeadApplyRequest {
    head_kind: String,
    generation: i64,
    head_sha256: String,
    meta: Option<BlobMeta>,
    tombstone: Option<TombstoneMeta>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct InternalHeadResponse {
    found: bool,
    head_kind: Option<String>,
    generation: Option<i64>,
    head_sha256: Option<String>,
    meta: Option<BlobMeta>,
    tombstone: Option<TombstoneMeta>,
}

#[derive(Debug, Serialize)]
struct InternalHeadApplyResponse {
    applied: bool,
    head_kind: String,
    generation: i64,
}

#[derive(Debug, Deserialize)]
struct HealBucketsQuery {
    #[serde(default = "default_bucket_prefix_len")]
    prefix_len: usize,
}

#[derive(Debug, Serialize)]
struct HealBucketsResponse {
    slot_id: u16,
    prefix_len: usize,
    buckets: Vec<HealBucket>,
}

#[derive(Debug, Serialize)]
struct HealBucket {
    prefix: String,
    digest: String,
    objects: usize,
}

#[derive(Debug, Deserialize)]
struct HealHeadsRequest {
    prefixes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct HealHeadsResponse {
    slot_id: u16,
    heads: Vec<HealHeadItem>,
}

#[derive(Debug, Serialize)]
struct HealHeadItem {
    path: String,
    head_kind: String,
    generation: i64,
    head_sha256: String,
}

#[derive(Debug, Deserialize)]
struct HealRepairRequest {
    source_node_id: String,
    blob_paths: Vec<String>,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Serialize)]
struct HealRepairResponse {
    slot_id: u16,
    repaired_objects: usize,
    skipped_objects: usize,
    errors: Vec<String>,
}

#[derive(Clone)]
struct PartPayload {
    part: ChunkRef,
    data: Bytes,
}

fn default_limit() -> usize {
    100
}

fn default_bucket_prefix_len() -> usize {
    2
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

    let chunk_store = Arc::new(ChunkStore::new(data_dir)?);

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

    let state = Arc::new(ServerState {
        node,
        slot_manager,
        chunk_store,
        registry,
        config,
        client: reqwest::Client::new(),
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

async fn register_local_node(state: &ServerState) -> Result<()> {
    let info = state.node.info().await;
    state.registry.register_node(&info).await
}

async fn ensure_store(state: &ServerState, slot_id: u16) -> Result<MetadataStore> {
    if !state.slot_manager.has_slot(slot_id).await {
        state.slot_manager.init_slot(slot_id).await?;
    }

    let slot = state.slot_manager.get_slot(slot_id).await?;
    MetadataStore::new(slot)
}

async fn current_nodes(state: &ServerState) -> Result<Vec<NodeInfo>> {
    let mut nodes = state.registry.get_nodes().await.unwrap_or_default();

    let local = state.node.info().await;
    if !nodes.iter().any(|node| node.node_id == local.node_id) {
        nodes.push(local);
    }

    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    nodes.dedup_by(|a, b| a.node_id == b.node_id);

    Ok(nodes)
}

async fn resolve_replica_nodes(state: &ServerState, slot_id: u16) -> Result<Vec<NodeInfo>> {
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

fn write_quorum(state: &ServerState, replica_count: usize) -> usize {
    state
        .config
        .replication
        .min_write_replicas
        .min(replica_count)
        .max(1)
}

fn normalize_blob_path(path: &str) -> Result<String> {
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

fn response_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
        .into_response()
}

fn status_string(status: &amberblob_core::NodeStatus) -> &'static str {
    match status {
        amberblob_core::NodeStatus::Healthy => "healthy",
        amberblob_core::NodeStatus::Degraded => "degraded",
        amberblob_core::NodeStatus::Unhealthy => "unhealthy",
    }
}

async fn health(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        node_id: state.node.node_id().to_string(),
        group_id: state.node.group_id().to_string(),
    })
}

async fn v1_healthz(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        node_id: state.node.node_id().to_string(),
        group_id: state.node.group_id().to_string(),
    })
}

async fn v1_nodes(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let nodes = match current_nodes(&state).await {
        Ok(nodes) => nodes,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let payload = NodesResponse {
        nodes: nodes
            .into_iter()
            .map(|node| NodeItem {
                node_id: node.node_id,
                address: node.address,
                status: status_string(&node.status).to_string(),
            })
            .collect(),
    };

    (StatusCode::OK, Json(payload)).into_response()
}

async fn v1_resolve_slot(
    State(state): State<Arc<ServerState>>,
    Query(query): Query<ResolveSlotQuery>,
) -> impl IntoResponse {
    let path = match normalize_blob_path(&query.path) {
        Ok(path) => path,
        Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
    };

    let slot_id = slot_for_key(&path, state.config.replication.total_slots);
    let replicas = match resolve_replica_nodes(&state, slot_id).await {
        Ok(replicas) => replicas,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let write_quorum_value = write_quorum(&state, replicas.len());

    let payload = ResolveSlotResponse {
        path,
        slot_id,
        replicas: replicas.into_iter().map(|node| node.node_id).collect(),
        write_quorum: write_quorum_value,
    };

    (StatusCode::OK, Json(payload)).into_response()
}

async fn v1_put_blob(
    State(state): State<Arc<ServerState>>,
    Path(raw_path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let path = match normalize_blob_path(&raw_path) {
        Ok(path) => path,
        Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
    };

    let slot_id = slot_for_key(&path, state.config.replication.total_slots);
    let write_id = headers
        .get("x-amberblob-write-id")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("auto-{}", ulid::Ulid::new()));

    let cache_key = format!("{}:{}:{}", slot_id, path, write_id);
    if let Some(cached) = state.idempotent_puts.read().await.get(&cache_key).cloned() {
        let response = PutBlobResponse {
            path,
            slot_id,
            generation: cached.generation,
            etag: cached.etag,
            size_bytes: cached.size_bytes,
            committed_replicas: cached.committed_replicas,
            idempotent_replay: Some(true),
        };

        return (StatusCode::OK, Json(response)).into_response();
    }

    let store = match ensure_store(&state, slot_id).await {
        Ok(store) => store,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let generation = match store.next_generation(&path) {
        Ok(generation) => generation,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let etag = compute_hash(&body);
    let mut part_payloads = Vec::new();

    let mut offset = 0usize;
    while offset < body.len() {
        let end = (offset + CHUNK_SIZE).min(body.len());
        let part_body = body.slice(offset..end);
        let part_sha = compute_hash(&part_body);

        let put_result = match state
            .chunk_store
            .put_part(slot_id, &path, &part_sha, part_body.clone())
            .await
        {
            Ok(result) => result,
            Err(error) => {
                return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
        };

        let part = ChunkRef {
            name: format!("part.{}", part_sha),
            sha256: part_sha,
            offset: offset as u64,
            length: (end - offset) as u64,
            external_path: Some(put_result.part_path.to_string_lossy().to_string()),
            archive_url: None,
        };

        if let Err(error) = store.upsert_part_entry(&path, &part) {
            return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }

        part_payloads.push(PartPayload {
            part,
            data: part_body,
        });
        offset = end;
    }

    let meta = BlobMeta {
        path: path.clone(),
        slot_id,
        generation,
        version: generation,
        size_bytes: body.len() as u64,
        etag: etag.clone(),
        parts: part_payloads
            .iter()
            .map(|payload| payload.part.clone())
            .collect(),
        updated_at: Utc::now(),
    };

    let meta_bytes = match serde_json::to_vec(&meta) {
        Ok(bytes) => bytes,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };
    let meta_sha = compute_hash(&meta_bytes);

    let applied = match store.upsert_meta_with_payload(&meta, &meta_bytes, &meta_sha) {
        Ok(applied) => applied,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    if !applied {
        return response_error(
            StatusCode::CONFLICT,
            "meta commit rejected by generation check",
        );
    }

    let replicas = match resolve_replica_nodes(&state, slot_id).await {
        Ok(replicas) => replicas,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let quorum = write_quorum(&state, replicas.len());
    let local_node_id = state.node.node_id().to_string();
    let mut committed_replicas = 1usize;

    for replica in replicas.iter().filter(|node| node.node_id != local_node_id) {
        let write_result = replicate_meta_write(
            &state,
            replica,
            slot_id,
            &path,
            &write_id,
            generation,
            &part_payloads,
            &meta,
            &meta_sha,
        )
        .await;

        if write_result.is_ok() {
            committed_replicas += 1;
        } else if let Err(error) = write_result {
            tracing::warn!(
                "Replica write failed: node={} slot={} path={} error={}",
                replica.node_id,
                slot_id,
                path,
                error
            );
        }
    }

    if committed_replicas < quorum {
        return response_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "quorum not reached: required={}, committed={}",
                quorum, committed_replicas
            ),
        );
    }

    state.idempotent_puts.write().await.insert(
        cache_key,
        PutCacheEntry {
            generation,
            etag: etag.clone(),
            size_bytes: body.len() as u64,
            committed_replicas,
        },
    );

    let response = PutBlobResponse {
        path,
        slot_id,
        generation,
        etag,
        size_bytes: body.len() as u64,
        committed_replicas,
        idempotent_replay: None,
    };

    (StatusCode::CREATED, Json(response)).into_response()
}

async fn replicate_meta_write(
    state: &Arc<ServerState>,
    replica: &NodeInfo,
    slot_id: u16,
    path: &str,
    write_id: &str,
    generation: i64,
    parts: &[PartPayload],
    meta: &BlobMeta,
    head_sha256: &str,
) -> Result<()> {
    for part in parts {
        let part_url = internal_part_url(&replica.address, slot_id, &part.part.sha256, path)?;

        let response = state
            .client
            .put(part_url)
            .header("x-amberblob-write-id", write_id)
            .header("x-amberblob-generation", generation.to_string())
            .header("x-amberblob-part-offset", part.part.offset.to_string())
            .header("x-amberblob-part-length", part.part.length.to_string())
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(part.data.clone())
            .send()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        if !response.status().is_success() {
            return Err(AmberError::Http(format!(
                "replica part write failed: node={} status={} sha={} path={}",
                replica.node_id,
                response.status(),
                part.part.sha256,
                path
            )));
        }
    }

    let head_url = internal_head_url(&replica.address, slot_id, path)?;
    let payload = InternalHeadApplyRequest {
        head_kind: "meta".to_string(),
        generation,
        head_sha256: head_sha256.to_string(),
        meta: Some(meta.clone()),
        tombstone: None,
    };

    let response = state
        .client
        .put(head_url)
        .header("x-amberblob-write-id", write_id)
        .header(header::CONTENT_TYPE, "application/json")
        .json(&payload)
        .send()
        .await
        .map_err(|error| AmberError::Http(error.to_string()))?;

    if !response.status().is_success() {
        return Err(AmberError::Http(format!(
            "replica head write failed: node={} status={} path={}",
            replica.node_id,
            response.status(),
            path
        )));
    }

    Ok(())
}

async fn v1_get_blob(
    State(state): State<Arc<ServerState>>,
    Path(raw_path): Path<String>,
) -> impl IntoResponse {
    let path = match normalize_blob_path(&raw_path) {
        Ok(path) => path,
        Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
    };

    let slot_id = slot_for_key(&path, state.config.replication.total_slots);
    let head = match ensure_head_available(&state, slot_id, &path).await {
        Ok(Some(head)) => head,
        Ok(None) => return response_error(StatusCode::NOT_FOUND, "object not found"),
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    if head.head_kind == HeadKind::Tombstone {
        return response_error(StatusCode::GONE, "object deleted");
    }

    let meta = match head.meta {
        Some(meta) => meta,
        None => return response_error(StatusCode::INTERNAL_SERVER_ERROR, "meta payload missing"),
    };

    let replicas = match resolve_replica_nodes(&state, slot_id).await {
        Ok(replicas) => replicas,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let local_node_id = state.node.node_id().to_string();
    let peer_nodes: Vec<NodeInfo> = replicas
        .into_iter()
        .filter(|node| node.node_id != local_node_id)
        .collect();

    let mut body = Vec::with_capacity(meta.size_bytes as usize);
    for part in &meta.parts {
        let bytes = if state.chunk_store.part_exists(slot_id, &path, &part.sha256) {
            match state
                .chunk_store
                .get_part(slot_id, &path, &part.sha256)
                .await
            {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::warn!(
                        "Failed to read local part. slot={} path={} sha={} error={}",
                        slot_id,
                        path,
                        part.sha256,
                        error
                    );
                    match fetch_part_from_peers_and_store(&state, &peer_nodes, slot_id, &path, part)
                        .await
                    {
                        Ok(bytes) => bytes,
                        Err(fetch_error) => {
                            return response_error(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                fetch_error.to_string(),
                            );
                        }
                    }
                }
            }
        } else {
            match fetch_part_from_peers_and_store(&state, &peer_nodes, slot_id, &path, part).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
                }
            }
        };

        body.extend_from_slice(&bytes);
    }

    let mut response = Response::new(Bytes::from(body).into());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Ok(value) = HeaderValue::from_str(&meta.etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&meta.generation.to_string()) {
        response
            .headers_mut()
            .insert("x-amberblob-generation", value);
    }

    response
}

async fn v1_head_blob(
    State(state): State<Arc<ServerState>>,
    Path(raw_path): Path<String>,
) -> impl IntoResponse {
    let path = match normalize_blob_path(&raw_path) {
        Ok(path) => path,
        Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
    };

    let slot_id = slot_for_key(&path, state.config.replication.total_slots);
    let head = match ensure_head_available(&state, slot_id, &path).await {
        Ok(Some(head)) => head,
        Ok(None) => return response_error(StatusCode::NOT_FOUND, "object not found"),
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    if head.head_kind == HeadKind::Tombstone {
        return response_error(StatusCode::GONE, "object deleted");
    }

    let meta = match head.meta {
        Some(meta) => meta,
        None => return response_error(StatusCode::INTERNAL_SERVER_ERROR, "meta payload missing"),
    };

    let mut response = Response::new(axum::body::Body::empty());
    *response.status_mut() = StatusCode::OK;
    if let Ok(value) = HeaderValue::from_str(&meta.etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&meta.generation.to_string()) {
        response
            .headers_mut()
            .insert("x-amberblob-generation", value);
    }
    if let Ok(value) = HeaderValue::from_str(&meta.size_bytes.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }

    response
}

async fn ensure_head_available(
    state: &Arc<ServerState>,
    slot_id: u16,
    path: &str,
) -> Result<Option<BlobHead>> {
    let store = ensure_store(state, slot_id).await?;
    if let Some(head) = store.get_current_head(path)? {
        return Ok(Some(head));
    }

    let replicas = resolve_replica_nodes(state, slot_id).await?;
    let local_node_id = state.node.node_id().to_string();

    for node in replicas
        .into_iter()
        .filter(|node| node.node_id != local_node_id)
    {
        if let Some(remote_head) = fetch_remote_head(state, &node.address, slot_id, path).await? {
            apply_remote_head_locally(state, slot_id, path, &remote_head).await?;
            return Ok(Some(remote_head));
        }
    }

    Ok(None)
}

async fn fetch_part_from_peers_and_store(
    state: &Arc<ServerState>,
    peers: &[NodeInfo],
    slot_id: u16,
    path: &str,
    part: &ChunkRef,
) -> Result<Bytes> {
    for peer in peers {
        let part_url = internal_part_url(&peer.address, slot_id, &part.sha256, path)?;
        let response = match state.client.get(part_url).send().await {
            Ok(response) => response,
            Err(_) => continue,
        };

        if !response.status().is_success() {
            continue;
        }

        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };

        let put_result = state
            .chunk_store
            .put_part(slot_id, path, &part.sha256, bytes.clone())
            .await?;

        let store = ensure_store(state, slot_id).await?;
        let mut local_part = part.clone();
        local_part.external_path = Some(put_result.part_path.to_string_lossy().to_string());
        store.upsert_part_entry(path, &local_part)?;

        return Ok(bytes);
    }

    Err(AmberError::ChunkNotFound(part.sha256.clone()))
}

async fn v1_delete_blob(
    State(state): State<Arc<ServerState>>,
    Path(raw_path): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let path = match normalize_blob_path(&raw_path) {
        Ok(path) => path,
        Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
    };

    let slot_id = slot_for_key(&path, state.config.replication.total_slots);
    let write_id = headers
        .get("x-amberblob-write-id")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("delete-{}", ulid::Ulid::new()));

    let store = match ensure_store(&state, slot_id).await {
        Ok(store) => store,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let generation = match store.next_generation(&path) {
        Ok(generation) => generation,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let tombstone = TombstoneMeta {
        path: path.clone(),
        slot_id,
        generation,
        deleted_at: Utc::now(),
        reason: "api-delete".to_string(),
    };

    let tombstone_bytes = match serde_json::to_vec(&tombstone) {
        Ok(bytes) => bytes,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };
    let tombstone_sha = compute_hash(&tombstone_bytes);

    let applied =
        match store.insert_tombstone_with_payload(&tombstone, &tombstone_bytes, &tombstone_sha) {
            Ok(applied) => applied,
            Err(error) => {
                return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
        };

    if !applied {
        return response_error(
            StatusCode::CONFLICT,
            "tombstone commit rejected by generation check",
        );
    }

    let replicas = match resolve_replica_nodes(&state, slot_id).await {
        Ok(replicas) => replicas,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let quorum = write_quorum(&state, replicas.len());
    let local_node_id = state.node.node_id().to_string();
    let mut committed_replicas = 1usize;

    for replica in replicas.iter().filter(|node| node.node_id != local_node_id) {
        let response = replicate_tombstone_write(
            &state,
            replica,
            slot_id,
            &path,
            &write_id,
            generation,
            &tombstone,
            &tombstone_sha,
        )
        .await;

        if response.is_ok() {
            committed_replicas += 1;
        } else if let Err(error) = response {
            tracing::warn!(
                "Replica tombstone write failed: node={} slot={} path={} error={}",
                replica.node_id,
                slot_id,
                path,
                error
            );
        }
    }

    if committed_replicas < quorum {
        return response_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "quorum not reached: required={}, committed={}",
                quorum, committed_replicas
            ),
        );
    }

    StatusCode::NO_CONTENT.into_response()
}

async fn replicate_tombstone_write(
    state: &Arc<ServerState>,
    replica: &NodeInfo,
    slot_id: u16,
    path: &str,
    write_id: &str,
    generation: i64,
    tombstone: &TombstoneMeta,
    head_sha256: &str,
) -> Result<()> {
    let head_url = internal_head_url(&replica.address, slot_id, path)?;
    let payload = InternalHeadApplyRequest {
        head_kind: "tombstone".to_string(),
        generation,
        head_sha256: head_sha256.to_string(),
        meta: None,
        tombstone: Some(tombstone.clone()),
    };

    let response = state
        .client
        .put(head_url)
        .header("x-amberblob-write-id", write_id)
        .header(header::CONTENT_TYPE, "application/json")
        .json(&payload)
        .send()
        .await
        .map_err(|error| AmberError::Http(error.to_string()))?;

    if !response.status().is_success() {
        return Err(AmberError::Http(format!(
            "replica tombstone write failed: node={} status={} path={}",
            replica.node_id,
            response.status(),
            path
        )));
    }

    Ok(())
}

async fn v1_list_blobs(
    State(state): State<Arc<ServerState>>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let slots = state.slot_manager.get_assigned_slots().await;
    let mut heads = Vec::new();

    for slot_id in slots {
        let slot = match state.slot_manager.get_slot(slot_id).await {
            Ok(slot) => slot,
            Err(_) => continue,
        };

        let store = match MetadataStore::new(slot) {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!("Failed to open metadata for slot {}: {}", slot_id, error);
                continue;
            }
        };

        let list = match store.list_heads(
            &query.prefix,
            query.limit.saturating_mul(2),
            query.include_deleted,
            query.cursor.as_deref(),
        ) {
            Ok(list) => list,
            Err(error) => {
                tracing::warn!("Failed to list slot {}: {}", slot_id, error);
                continue;
            }
        };

        heads.extend(list);
    }

    heads.sort_by(|a, b| a.path.cmp(&b.path));
    heads.dedup_by(|a, b| a.path == b.path);

    let mut items = Vec::new();
    for head in heads.into_iter().take(query.limit) {
        let (etag, size_bytes, deleted) = match head.head_kind {
            HeadKind::Meta => {
                let meta = head.meta.clone();
                (
                    meta.as_ref()
                        .map(|item| item.etag.clone())
                        .unwrap_or_default(),
                    meta.as_ref().map(|item| item.size_bytes).unwrap_or(0),
                    false,
                )
            }
            HeadKind::Tombstone => (String::new(), 0, true),
        };

        items.push(ListItem {
            path: head.path,
            generation: head.generation,
            etag,
            size_bytes,
            deleted,
            updated_at: head.updated_at.to_rfc3339(),
        });
    }

    let next_cursor = items.last().map(|item| item.path.clone());

    (StatusCode::OK, Json(ListResponse { items, next_cursor })).into_response()
}

async fn internal_put_part(
    State(state): State<Arc<ServerState>>,
    Path((slot_id, sha256)): Path<(u16, String)>,
    Query(query): Query<InternalPathQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let path = match query.path {
        Some(path) => match normalize_blob_path(&path) {
            Ok(path) => path,
            Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
        },
        None => return response_error(StatusCode::BAD_REQUEST, "path query is required"),
    };

    let store = match ensure_store(&state, slot_id).await {
        Ok(store) => store,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    if compute_hash(&body) != sha256 {
        return response_error(StatusCode::BAD_REQUEST, "part sha256 mismatch");
    }

    let put_result = match state
        .chunk_store
        .put_part(slot_id, &path, &sha256, body)
        .await
    {
        Ok(result) => result,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let offset = headers
        .get("x-amberblob-part-offset")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);

    let length = headers
        .get("x-amberblob-part-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| {
            std::fs::metadata(&put_result.part_path)
                .map(|meta| meta.len())
                .unwrap_or(0)
        });

    let part = ChunkRef {
        name: format!("part.{}", sha256),
        sha256: sha256.clone(),
        offset,
        length,
        external_path: Some(put_result.part_path.to_string_lossy().to_string()),
        archive_url: None,
    };

    if let Err(error) = store.upsert_part_entry(&path, &part) {
        return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
    }

    (
        StatusCode::OK,
        Json(InternalPartPutResponse {
            accepted: true,
            reused: put_result.reused,
            sha256,
        }),
    )
        .into_response()
}

async fn internal_get_part(
    State(state): State<Arc<ServerState>>,
    Path((slot_id, sha256)): Path<(u16, String)>,
    Query(query): Query<InternalPathQuery>,
) -> impl IntoResponse {
    let store = match ensure_store(&state, slot_id).await {
        Ok(store) => store,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let part_bytes = if let Some(path) = query.path {
        let normalized_path = match normalize_blob_path(&path) {
            Ok(path) => path,
            Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
        };

        match state
            .chunk_store
            .get_part(slot_id, &normalized_path, &sha256)
            .await
        {
            Ok(bytes) => bytes,
            Err(_) => return response_error(StatusCode::NOT_FOUND, "part not found for path"),
        }
    } else {
        let external_path = match store.find_part_external_path(&sha256, None) {
            Ok(path) => path,
            Err(error) => {
                return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
        };

        let Some(external_path) = external_path else {
            return response_error(StatusCode::NOT_FOUND, "part not found");
        };

        match tokio::fs::read(external_path).await {
            Ok(bytes) => Bytes::from(bytes),
            Err(error) => {
                return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }
        }
    };

    let mut response = Response::new(part_bytes.into());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
}

async fn internal_put_head(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Query(query): Query<InternalPathQuery>,
    Json(request): Json<InternalHeadApplyRequest>,
) -> impl IntoResponse {
    let store = match ensure_store(&state, slot_id).await {
        Ok(store) => store,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let query_path = query.path.and_then(|path| normalize_blob_path(&path).ok());

    match request.head_kind.as_str() {
        "meta" => {
            let mut meta = match request.meta {
                Some(meta) => meta,
                None => return response_error(StatusCode::BAD_REQUEST, "meta payload is required"),
            };

            if let Some(path) = query_path {
                meta.path = path;
            }

            meta.slot_id = slot_id;
            meta.generation = request.generation;
            if meta.version == 0 {
                meta.version = meta.generation;
            }
            meta.updated_at = Utc::now();

            for part in &mut meta.parts {
                if part.external_path.is_none() {
                    if let Ok(part_path) =
                        state
                            .chunk_store
                            .part_path(slot_id, &meta.path, &part.sha256)
                    {
                        part.external_path = Some(part_path.to_string_lossy().to_string());
                    }
                }
                if let Err(error) = store.upsert_part_entry(&meta.path, part) {
                    return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
                }
            }

            let inline_data = match serde_json::to_vec(&meta) {
                Ok(bytes) => bytes,
                Err(error) => {
                    return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
                }
            };
            let head_sha = if request.head_sha256.is_empty() {
                compute_hash(&inline_data)
            } else {
                request.head_sha256
            };

            if let Err(error) = store.upsert_meta_with_payload(&meta, &inline_data, &head_sha) {
                return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }

            (
                StatusCode::OK,
                Json(InternalHeadApplyResponse {
                    applied: true,
                    head_kind: "meta".to_string(),
                    generation: meta.generation,
                }),
            )
                .into_response()
        }
        "tombstone" => {
            let mut tombstone = match request.tombstone {
                Some(tombstone) => tombstone,
                None => {
                    return response_error(StatusCode::BAD_REQUEST, "tombstone payload is required");
                }
            };

            if let Some(path) = query_path {
                tombstone.path = path;
            }

            tombstone.slot_id = slot_id;
            tombstone.generation = request.generation;
            tombstone.deleted_at = Utc::now();

            let inline_data = match serde_json::to_vec(&tombstone) {
                Ok(bytes) => bytes,
                Err(error) => {
                    return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
                }
            };
            let head_sha = if request.head_sha256.is_empty() {
                compute_hash(&inline_data)
            } else {
                request.head_sha256
            };

            if let Err(error) =
                store.insert_tombstone_with_payload(&tombstone, &inline_data, &head_sha)
            {
                return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
            }

            (
                StatusCode::OK,
                Json(InternalHeadApplyResponse {
                    applied: true,
                    head_kind: "tombstone".to_string(),
                    generation: tombstone.generation,
                }),
            )
                .into_response()
        }
        _ => response_error(
            StatusCode::BAD_REQUEST,
            "head_kind must be meta or tombstone",
        ),
    }
}

async fn internal_get_head(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Query(query): Query<InternalPathQuery>,
) -> impl IntoResponse {
    let Some(path) = query.path else {
        return response_error(StatusCode::BAD_REQUEST, "path query is required");
    };

    let path = match normalize_blob_path(&path) {
        Ok(path) => path,
        Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
    };

    let store = match ensure_store(&state, slot_id).await {
        Ok(store) => store,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let head = match store.get_current_head(&path) {
        Ok(head) => head,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let Some(head) = head else {
        return (
            StatusCode::NOT_FOUND,
            Json(InternalHeadResponse {
                found: false,
                head_kind: None,
                generation: None,
                head_sha256: None,
                meta: None,
                tombstone: None,
            }),
        )
            .into_response();
    };

    (
        StatusCode::OK,
        Json(InternalHeadResponse {
            found: true,
            head_kind: Some(match head.head_kind {
                HeadKind::Meta => "meta".to_string(),
                HeadKind::Tombstone => "tombstone".to_string(),
            }),
            generation: Some(head.generation),
            head_sha256: Some(head.head_sha256),
            meta: head.meta,
            tombstone: head.tombstone,
        }),
    )
        .into_response()
}

async fn fetch_remote_head(
    state: &Arc<ServerState>,
    address: &str,
    slot_id: u16,
    path: &str,
) -> Result<Option<BlobHead>> {
    let head_url = internal_head_url(address, slot_id, path)?;
    let response = state
        .client
        .get(head_url)
        .send()
        .await
        .map_err(|error| AmberError::Http(error.to_string()))?;

    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    if !response.status().is_success() {
        return Err(AmberError::Http(format!(
            "internal head fetch failed: status={} path={}",
            response.status(),
            path
        )));
    }

    let payload: InternalHeadResponse = response
        .json()
        .await
        .map_err(|error| AmberError::Http(error.to_string()))?;

    if !payload.found {
        return Ok(None);
    }

    let head_kind = match payload.head_kind.as_deref() {
        Some("meta") => HeadKind::Meta,
        Some("tombstone") => HeadKind::Tombstone,
        _ => return Err(AmberError::Internal("invalid remote head kind".to_string())),
    };

    let generation = payload
        .generation
        .ok_or_else(|| AmberError::Internal("missing remote generation".to_string()))?;

    let head_sha256 = payload
        .head_sha256
        .ok_or_else(|| AmberError::Internal("missing remote head_sha256".to_string()))?;

    Ok(Some(BlobHead {
        path: path.to_string(),
        generation,
        head_kind,
        head_sha256,
        updated_at: Utc::now(),
        meta: payload.meta,
        tombstone: payload.tombstone,
    }))
}

async fn apply_remote_head_locally(
    state: &Arc<ServerState>,
    slot_id: u16,
    path: &str,
    head: &BlobHead,
) -> Result<()> {
    let store = ensure_store(state, slot_id).await?;

    match head.head_kind {
        HeadKind::Meta => {
            let mut meta = head
                .meta
                .clone()
                .ok_or_else(|| AmberError::Internal("missing meta payload".to_string()))?;
            meta.path = path.to_string();
            meta.slot_id = slot_id;
            meta.generation = head.generation;
            if meta.version == 0 {
                meta.version = meta.generation;
            }

            for part in &mut meta.parts {
                if part.external_path.is_none() {
                    if let Ok(part_path) = state.chunk_store.part_path(slot_id, path, &part.sha256)
                    {
                        part.external_path = Some(part_path.to_string_lossy().to_string());
                    }
                }
                store.upsert_part_entry(path, part)?;
            }

            let inline_data = serde_json::to_vec(&meta)?;
            store.upsert_meta_with_payload(&meta, &inline_data, &head.head_sha256)?;
        }
        HeadKind::Tombstone => {
            let mut tombstone = head
                .tombstone
                .clone()
                .ok_or_else(|| AmberError::Internal("missing tombstone payload".to_string()))?;
            tombstone.path = path.to_string();
            tombstone.slot_id = slot_id;
            tombstone.generation = head.generation;

            let inline_data = serde_json::to_vec(&tombstone)?;
            store.insert_tombstone_with_payload(&tombstone, &inline_data, &head.head_sha256)?;
        }
    }

    Ok(())
}

fn internal_head_url(address: &str, slot_id: u16, path: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(&format!(
        "http://{}/internal/v1/slots/{}/heads",
        address, slot_id
    ))
    .map_err(|error| AmberError::Http(error.to_string()))?;

    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("path", path);
    }

    Ok(url)
}

fn internal_part_url(
    address: &str,
    slot_id: u16,
    sha256: &str,
    path: &str,
) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(&format!(
        "http://{}/internal/v1/slots/{}/parts/{}",
        address, slot_id, sha256
    ))
    .map_err(|error| AmberError::Http(error.to_string()))?;

    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("path", path);
    }

    Ok(url)
}

async fn v1_internal_heal_buckets(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Query(query): Query<HealBucketsQuery>,
) -> impl IntoResponse {
    let store = match ensure_store(&state, slot_id).await {
        Ok(store) => store,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let heads = match store.list_heads("", usize::MAX, true, None) {
        Ok(heads) => heads,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let prefix_len = query.prefix_len.clamp(1, 8);
    let mut buckets: BTreeMap<String, (u64, usize)> = BTreeMap::new();

    for head in heads {
        let hash = short_hash_hex(&head.path);
        let prefix = hash.chars().take(prefix_len).collect::<String>();

        let kind = match head.head_kind {
            HeadKind::Meta => "meta",
            HeadKind::Tombstone => "tombstone",
        };

        let digest_source = format!(
            "{}|{}|{}|{}",
            head.path, head.generation, head.head_sha256, kind
        );

        let entry = buckets.entry(prefix).or_insert((0, 0));
        entry.0 ^= fold_hash_u64(&digest_source);
        entry.1 += 1;
    }

    let response = HealBucketsResponse {
        slot_id,
        prefix_len,
        buckets: buckets
            .into_iter()
            .map(|(prefix, (digest, objects))| HealBucket {
                prefix,
                digest: format!("{:016x}", digest),
                objects,
            })
            .collect(),
    };

    (StatusCode::OK, Json(response)).into_response()
}

async fn v1_internal_heal_heads(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Json(request): Json<HealHeadsRequest>,
) -> impl IntoResponse {
    let store = match ensure_store(&state, slot_id).await {
        Ok(store) => store,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let heads = match store.list_heads("", usize::MAX, true, None) {
        Ok(heads) => heads,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let filter_set: std::collections::HashSet<String> = request.prefixes.into_iter().collect();

    let filtered = heads
        .into_iter()
        .filter(|head| {
            let prefix = short_hash_hex(&head.path)
                .chars()
                .take(2)
                .collect::<String>();
            filter_set.contains(&prefix)
        })
        .map(|head| HealHeadItem {
            path: head.path,
            head_kind: match head.head_kind {
                HeadKind::Meta => "meta".to_string(),
                HeadKind::Tombstone => "tombstone".to_string(),
            },
            generation: head.generation,
            head_sha256: head.head_sha256,
        })
        .collect();

    (
        StatusCode::OK,
        Json(HealHeadsResponse {
            slot_id,
            heads: filtered,
        }),
    )
        .into_response()
}

async fn v1_internal_heal_repair(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Json(request): Json<HealRepairRequest>,
) -> impl IntoResponse {
    let source_node_id = request.source_node_id.trim().to_string();
    if source_node_id.is_empty() {
        return response_error(StatusCode::BAD_REQUEST, "source_node_id is required");
    }

    let nodes = match current_nodes(&state).await {
        Ok(nodes) => nodes,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let source = match nodes.iter().find(|node| node.node_id == source_node_id) {
        Some(source) => source.clone(),
        None => return response_error(StatusCode::NOT_FOUND, "source node not found"),
    };

    let mut repaired_objects = 0usize;
    let mut skipped_objects = 0usize;
    let mut errors = Vec::new();

    for raw_path in request.blob_paths {
        let path = match normalize_blob_path(&raw_path) {
            Ok(path) => path,
            Err(error) => {
                skipped_objects += 1;
                errors.push(format!("{}: {}", raw_path, error));
                continue;
            }
        };

        if request.dry_run {
            skipped_objects += 1;
            continue;
        }

        let remote_head = match fetch_remote_head(&state, &source.address, slot_id, &path).await {
            Ok(Some(head)) => head,
            Ok(None) => {
                skipped_objects += 1;
                errors.push(format!("{}: source has no head", path));
                continue;
            }
            Err(error) => {
                skipped_objects += 1;
                errors.push(format!("{}: {}", path, error));
                continue;
            }
        };

        match repair_path_from_head(&state, &source, slot_id, &path, &remote_head).await {
            Ok(_) => repaired_objects += 1,
            Err(error) => {
                skipped_objects += 1;
                errors.push(format!("{}: {}", path, error));
            }
        }
    }

    (
        StatusCode::OK,
        Json(HealRepairResponse {
            slot_id,
            repaired_objects,
            skipped_objects,
            errors,
        }),
    )
        .into_response()
}

async fn repair_path_from_head(
    state: &Arc<ServerState>,
    source: &NodeInfo,
    slot_id: u16,
    path: &str,
    remote_head: &BlobHead,
) -> Result<()> {
    if let HeadKind::Meta = remote_head.head_kind {
        let meta = remote_head
            .meta
            .clone()
            .ok_or_else(|| AmberError::Internal("missing meta payload".to_string()))?;

        for part in &meta.parts {
            if state.chunk_store.part_exists(slot_id, path, &part.sha256) {
                continue;
            }

            let part_url = internal_part_url(&source.address, slot_id, &part.sha256, path)?;
            let response = state
                .client
                .get(part_url)
                .send()
                .await
                .map_err(|error| AmberError::Http(error.to_string()))?;

            if !response.status().is_success() {
                return Err(AmberError::Http(format!(
                    "failed to fetch part {} from source {}: {}",
                    part.sha256,
                    source.node_id,
                    response.status()
                )));
            }

            let bytes = response
                .bytes()
                .await
                .map_err(|error| AmberError::Http(error.to_string()))?;

            let put_result = state
                .chunk_store
                .put_part(slot_id, path, &part.sha256, bytes)
                .await?;

            let store = ensure_store(state, slot_id).await?;
            let mut local_part = part.clone();
            local_part.external_path = Some(put_result.part_path.to_string_lossy().to_string());
            store.upsert_part_entry(path, &local_part)?;
        }
    }

    apply_remote_head_locally(state, slot_id, path, remote_head).await
}

fn fold_hash_u64(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn short_hash_hex(value: &str) -> String {
    format!("{:016x}", fold_hash_u64(value))
}
