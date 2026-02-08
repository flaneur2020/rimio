use super::internal::{apply_remote_head_locally, fetch_remote_head};
use super::{
    ListItem, ListQuery, ListResponse, NodeItem, NodesResponse, PutBlobResponse, PutCacheEntry,
    ResolveSlotQuery, ResolveSlotResponse, ServerState, current_nodes, ensure_store,
    normalize_blob_path, resolve_replica_nodes, response_error, status_string,
};
use amberblob_core::{
    AmberError, BlobHead, HeadKind, PartRef, PutBlobOperationOutcome, PutBlobOperationRequest,
    Result, TombstoneMeta, compute_hash, slot_for_key,
};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use std::sync::Arc;

pub(crate) async fn health(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    Json(super::HealthResponse {
        status: "ok".to_string(),
        node_id: state.node.node_id().to_string(),
        group_id: state.node.group_id().to_string(),
    })
}

pub(crate) async fn v1_healthz(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    Json(super::HealthResponse {
        status: "ok".to_string(),
        node_id: state.node.node_id().to_string(),
        group_id: state.node.group_id().to_string(),
    })
}

pub(crate) async fn v1_nodes(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
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

pub(crate) async fn v1_resolve_slot(
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

    let write_quorum = state.coordinator.write_quorum(replicas.len());
    let payload = ResolveSlotResponse {
        path,
        slot_id,
        replicas: replicas.into_iter().map(|node| node.node_id).collect(),
        write_quorum,
    };

    (StatusCode::OK, Json(payload)).into_response()
}

pub(crate) async fn v1_put_blob(
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

    let replicas = match resolve_replica_nodes(&state, slot_id).await {
        Ok(replicas) => replicas,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let operation_result = state
        .put_blob_operation
        .run(PutBlobOperationRequest {
            path: path.clone(),
            slot_id,
            write_id: write_id.clone(),
            body,
            replicas,
            local_node_id: state.node.node_id().to_string(),
        })
        .await;

    let (status, generation, etag, size_bytes, committed_replicas) = match operation_result {
        Ok(PutBlobOperationOutcome::Committed(result)) => (
            StatusCode::CREATED,
            result.generation,
            result.etag,
            result.size_bytes,
            result.committed_replicas,
        ),
        Ok(PutBlobOperationOutcome::Conflict) => {
            return response_error(
                StatusCode::CONFLICT,
                "meta commit rejected by generation check",
            );
        }
        Err(AmberError::InsufficientReplicas { required, found }) => {
            return response_error(
                StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "quorum not reached: required={}, committed={}",
                    required, found
                ),
            );
        }
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    state.idempotent_puts.write().await.insert(
        cache_key,
        PutCacheEntry {
            generation,
            etag: etag.clone(),
            size_bytes,
            committed_replicas,
        },
    );

    let response = PutBlobResponse {
        path,
        slot_id,
        generation,
        etag,
        size_bytes,
        committed_replicas,
        idempotent_replay: None,
    };

    (status, Json(response)).into_response()
}
pub(crate) async fn v1_get_blob(
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
    let peer_nodes: Vec<amberblob_core::NodeInfo> = replicas
        .into_iter()
        .filter(|node| node.node_id != local_node_id)
        .collect();

    let mut body = Vec::with_capacity(meta.size_bytes as usize);
    for part in &meta.parts {
        let bytes = if state.part_store.part_exists(slot_id, &path, &part.sha256) {
            match state
                .part_store
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

pub(crate) async fn v1_head_blob(
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

pub(crate) async fn v1_delete_blob(
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

    let quorum = state.coordinator.write_quorum(replicas.len());
    let local_node_id = state.node.node_id().to_string();
    let mut committed_replicas = 1usize;

    for replica in replicas.iter().filter(|node| node.node_id != local_node_id) {
        let response = state
            .coordinator
            .replicate_tombstone_write(
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

pub(crate) async fn v1_list_blobs(
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

        let store = match amberblob_core::MetadataStore::new(slot) {
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
    peers: &[amberblob_core::NodeInfo],
    slot_id: u16,
    path: &str,
    part: &PartRef,
) -> Result<Bytes> {
    for peer in peers {
        let part_url =
            state
                .coordinator
                .internal_part_url(&peer.address, slot_id, &part.sha256, path)?;

        let response = match state.coordinator.client().get(part_url).send().await {
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
            .part_store
            .put_part(slot_id, path, &part.sha256, bytes.clone())
            .await?;

        let store = ensure_store(state, slot_id).await?;
        let mut local_part = part.clone();
        local_part.external_path = Some(put_result.part_path.to_string_lossy().to_string());
        store.upsert_part_entry(path, &local_part)?;

        return Ok(bytes);
    }

    Err(AmberError::PartNotFound(part.sha256.clone()))
}
