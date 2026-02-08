use super::{
    ListItem, ListQuery, ListResponse, NodeItem, NodesResponse, PutBlobResponse, PutCacheEntry,
    ResolveSlotQuery, ResolveSlotResponse, ServerState, current_nodes, normalize_blob_path,
    resolve_replica_nodes, response_error, status_string,
};
use amberio_core::{
    AmberError, DeleteBlobOperationOutcome, DeleteBlobOperationRequest, ListBlobsOperationRequest,
    PutBlobOperationOutcome, PutBlobOperationRequest, ReadBlobOperationOutcome,
    ReadBlobOperationRequest, ReadByteRange, slot_for_key,
};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
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
        .get("x-amberio-write-id")
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
    headers: HeaderMap,
) -> impl IntoResponse {
    let path = match normalize_blob_path(&raw_path) {
        Ok(path) => path,
        Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
    };

    let requested_range = match parse_range_header(&headers) {
        Ok(range) => range,
        Err(message) => return response_error(StatusCode::RANGE_NOT_SATISFIABLE, message),
    };

    let slot_id = slot_for_key(&path, state.config.replication.total_slots);
    let replicas = match resolve_replica_nodes(&state, slot_id).await {
        Ok(replicas) => replicas,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let outcome = state
        .read_blob_operation
        .run(ReadBlobOperationRequest {
            slot_id,
            path: path.clone(),
            replicas,
            local_node_id: state.node.node_id().to_string(),
            include_body: true,
            range: requested_range,
        })
        .await;

    let result = match outcome {
        Ok(ReadBlobOperationOutcome::Found(result)) => result,
        Ok(ReadBlobOperationOutcome::NotFound) => {
            return response_error(StatusCode::NOT_FOUND, "object not found");
        }
        Ok(ReadBlobOperationOutcome::Deleted) => {
            return response_error(StatusCode::GONE, "object deleted");
        }
        Err(AmberError::InvalidRequest(message)) => {
            return response_error(StatusCode::RANGE_NOT_SATISFIABLE, message);
        }
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let body = result.body.unwrap_or_default();
    let mut response = Response::new(body.clone().into());
    *response.status_mut() = if requested_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    if let Ok(value) = HeaderValue::from_str(&body.len().to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }

    if let Ok(value) = HeaderValue::from_str(&result.meta.etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&result.meta.generation.to_string()) {
        response.headers_mut().insert("x-amberio-generation", value);
    }

    if requested_range.is_some() {
        if let Some(range) = result.body_range {
            let content_range = format!(
                "bytes {}-{}/{}",
                range.start, range.end, result.meta.size_bytes
            );
            if let Ok(value) = HeaderValue::from_str(&content_range) {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
        }
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
    let replicas = match resolve_replica_nodes(&state, slot_id).await {
        Ok(replicas) => replicas,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let outcome = state
        .read_blob_operation
        .run(ReadBlobOperationRequest {
            slot_id,
            path,
            replicas,
            local_node_id: state.node.node_id().to_string(),
            include_body: false,
            range: None,
        })
        .await;

    let result = match outcome {
        Ok(ReadBlobOperationOutcome::Found(result)) => result,
        Ok(ReadBlobOperationOutcome::NotFound) => {
            return response_error(StatusCode::NOT_FOUND, "object not found");
        }
        Ok(ReadBlobOperationOutcome::Deleted) => {
            return response_error(StatusCode::GONE, "object deleted");
        }
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let mut response = Response::new(axum::body::Body::empty());
    *response.status_mut() = StatusCode::OK;
    if let Ok(value) = HeaderValue::from_str(&result.meta.etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&result.meta.generation.to_string()) {
        response.headers_mut().insert("x-amberio-generation", value);
    }
    if let Ok(value) = HeaderValue::from_str(&result.meta.size_bytes.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

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
        .get("x-amberio-write-id")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("delete-{}", ulid::Ulid::new()));

    let replicas = match resolve_replica_nodes(&state, slot_id).await {
        Ok(replicas) => replicas,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let operation_result = state
        .delete_blob_operation
        .run(DeleteBlobOperationRequest {
            path,
            slot_id,
            write_id,
            replicas,
            local_node_id: state.node.node_id().to_string(),
        })
        .await;

    match operation_result {
        Ok(DeleteBlobOperationOutcome::Committed(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(DeleteBlobOperationOutcome::Conflict) => response_error(
            StatusCode::CONFLICT,
            "tombstone commit rejected by generation check",
        ),
        Err(AmberError::InsufficientReplicas { required, found }) => response_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "quorum not reached: required={}, committed={}",
                required, found
            ),
        ),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn v1_list_blobs(
    State(state): State<Arc<ServerState>>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let result = state
        .list_blobs_operation
        .run(ListBlobsOperationRequest {
            prefix: query.prefix,
            limit: query.limit,
            cursor: query.cursor,
            include_deleted: query.include_deleted,
        })
        .await;

    let result = match result {
        Ok(result) => result,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let items = result
        .items
        .into_iter()
        .map(|item| ListItem {
            path: item.path,
            generation: item.generation,
            etag: item.etag,
            size_bytes: item.size_bytes,
            deleted: item.deleted,
            updated_at: item.updated_at.to_rfc3339(),
        })
        .collect();

    (
        StatusCode::OK,
        Json(ListResponse {
            items,
            next_cursor: result.next_cursor,
        }),
    )
        .into_response()
}

fn parse_range_header(headers: &HeaderMap) -> std::result::Result<Option<ReadByteRange>, String> {
    let Some(value) = headers.get(header::RANGE) else {
        return Ok(None);
    };

    let range_value = value
        .to_str()
        .map_err(|_| "invalid Range header".to_string())?
        .trim();

    if range_value.is_empty() {
        return Ok(None);
    }

    let Some(raw) = range_value.strip_prefix("bytes=") else {
        return Err("only bytes= range is supported".to_string());
    };

    let mut parts = raw.splitn(2, '-');
    let start_str = parts.next().unwrap_or_default().trim();
    let end_str = parts.next().unwrap_or_default().trim();

    if start_str.is_empty() || end_str.is_empty() {
        return Err("only explicit bytes=start-end is supported".to_string());
    }

    let start = start_str
        .parse::<u64>()
        .map_err(|_| "invalid range start".to_string())?;
    let end = end_str
        .parse::<u64>()
        .map_err(|_| "invalid range end".to_string())?;

    if start > end {
        return Err("range start must be <= range end".to_string());
    }

    Ok(Some(ReadByteRange { start, end }))
}
