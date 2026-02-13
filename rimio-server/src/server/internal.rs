use super::{
    HealHeadItem, HealHeadsRequest, HealHeadsResponse, HealRepairRequest, HealRepairResponse,
    HealSlotlet, HealSlotletsQuery, HealSlotletsResponse, InternalBootstrapResponse,
    InternalGossipFromQuery, InternalGossipSeedsResponse, InternalHeadApplyRequest,
    InternalHeadApplyResponse, InternalHeadResponse, InternalPartPutResponse, InternalPartQuery,
    InternalPathQuery, ServerState, normalize_blob_path, response_error,
};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use rimio_core::{
    HeadKind, HealHeadsOperationRequest, HealRepairOperationRequest, HealSlotletsOperationRequest,
    InternalGetHeadOperationOutcome, InternalGetHeadOperationRequest,
    InternalGetPartOperationOutcome, InternalGetPartOperationRequest,
    InternalPutHeadOperationRequest, InternalPutPartOperationRequest, RimError,
    ingest_global_gossip_packet, ingest_global_gossip_stream,
};
use std::sync::Arc;

pub(crate) async fn internal_put_part(
    State(state): State<Arc<ServerState>>,
    Path((slot_id, sha256)): Path<(u16, String)>,
    Query(query): Query<InternalPartQuery>,
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

    let generation = query
        .generation
        .or_else(|| {
            headers
                .get("x-rimio-generation")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<i64>().ok())
        })
        .unwrap_or(0);

    let Some(part_no) = query.part_no.or_else(|| {
        headers
            .get("x-rimio-part-no")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u32>().ok())
    }) else {
        return response_error(StatusCode::BAD_REQUEST, "part_no is required");
    };

    let result = state
        .internal_put_part_operation
        .run(InternalPutPartOperationRequest {
            slot_id,
            path,
            generation,
            part_no,
            sha256,
            body,
        })
        .await;

    match result {
        Ok(result) => (
            StatusCode::OK,
            Json(InternalPartPutResponse {
                accepted: true,
                reused: result.reused,
                sha256: result.sha256,
            }),
        )
            .into_response(),
        Err(RimError::InvalidRequest(message)) => response_error(StatusCode::BAD_REQUEST, message),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn internal_get_part(
    State(state): State<Arc<ServerState>>,
    Path((slot_id, sha256)): Path<(u16, String)>,
    Query(query): Query<InternalPartQuery>,
) -> impl IntoResponse {
    let path = match query.path {
        Some(path) => match normalize_blob_path(&path) {
            Ok(path) => Some(path),
            Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
        },
        None => None,
    };

    let result = state
        .internal_get_part_operation
        .run(InternalGetPartOperationRequest {
            slot_id,
            sha256: Some(sha256),
            path,
            generation: query.generation,
            part_no: query.part_no,
        })
        .await;

    match result {
        Ok(InternalGetPartOperationOutcome::Found(part)) => {
            let mut response = Response::new(part.bytes.into());
            *response.status_mut() = StatusCode::OK;
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            if let Ok(value) = HeaderValue::from_str(&part.sha256) {
                response.headers_mut().insert("x-rimio-sha256", value);
            }
            response
        }
        Ok(InternalGetPartOperationOutcome::NotFound) => {
            response_error(StatusCode::NOT_FOUND, "part not found")
        }
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn internal_put_head(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Query(query): Query<InternalPathQuery>,
    Json(request): Json<InternalHeadApplyRequest>,
) -> impl IntoResponse {
    let query_path = match query.path {
        Some(path) => match normalize_blob_path(&path) {
            Ok(path) => Some(path),
            Err(error) => return response_error(StatusCode::BAD_REQUEST, error.to_string()),
        },
        None => None,
    };

    let result = state
        .internal_put_head_operation
        .run(InternalPutHeadOperationRequest {
            slot_id,
            query_path,
            head_kind: request.head_kind,
            generation: request.generation,
            head_sha256: request.head_sha256,
            meta: request.meta,
            tombstone: request.tombstone,
        })
        .await;

    match result {
        Ok(result) => (
            StatusCode::OK,
            Json(InternalHeadApplyResponse {
                applied: true,
                head_kind: result.head_kind,
                generation: result.generation,
            }),
        )
            .into_response(),
        Err(RimError::InvalidRequest(message)) => response_error(StatusCode::BAD_REQUEST, message),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn internal_get_head(
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

    let result = state
        .internal_get_head_operation
        .run(InternalGetHeadOperationRequest { slot_id, path })
        .await;

    match result {
        Ok(InternalGetHeadOperationOutcome::NotFound) => (
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
            .into_response(),
        Ok(InternalGetHeadOperationOutcome::Found(head)) => (
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
            .into_response(),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn v1_internal_heal_slotlets(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Query(query): Query<HealSlotletsQuery>,
) -> impl IntoResponse {
    let result = state
        .heal_slotlets_operation
        .run(HealSlotletsOperationRequest {
            slot_id,
            prefix_len: query.prefix_len,
        })
        .await;

    match result {
        Ok(result) => (
            StatusCode::OK,
            Json(HealSlotletsResponse {
                slot_id: result.slot_id,
                prefix_len: result.prefix_len,
                slotlets: result
                    .slotlets
                    .into_iter()
                    .map(|slotlet| HealSlotlet {
                        prefix: slotlet.prefix,
                        digest: slotlet.digest,
                        objects: slotlet.objects,
                    })
                    .collect(),
            }),
        )
            .into_response(),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn v1_internal_heal_heads(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Json(request): Json<HealHeadsRequest>,
) -> impl IntoResponse {
    let result = state
        .heal_heads_operation
        .run(HealHeadsOperationRequest {
            slot_id,
            prefixes: request.prefixes,
        })
        .await;

    match result {
        Ok(result) => (
            StatusCode::OK,
            Json(HealHeadsResponse {
                slot_id: result.slot_id,
                heads: result
                    .heads
                    .into_iter()
                    .map(|head| HealHeadItem {
                        path: head.path,
                        head_kind: head.head_kind,
                        generation: head.generation,
                        head_sha256: head.head_sha256,
                    })
                    .collect(),
            }),
        )
            .into_response(),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn v1_internal_heal_repair(
    State(state): State<Arc<ServerState>>,
    Path(slot_id): Path<u16>,
    Json(request): Json<HealRepairRequest>,
) -> impl IntoResponse {
    let source_node_id = request.source_node_id.trim().to_string();
    if source_node_id.is_empty() {
        return response_error(StatusCode::BAD_REQUEST, "source_node_id is required");
    }

    let nodes = match super::current_nodes(&state).await {
        Ok(nodes) => nodes,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    if !nodes.iter().any(|node| node.node_id == source_node_id) {
        return response_error(StatusCode::NOT_FOUND, "source node not found");
    }

    let result = state
        .heal_repair_operation
        .run(HealRepairOperationRequest {
            slot_id,
            source_node_id,
            blob_paths: request.blob_paths,
            dry_run: request.dry_run,
        })
        .await;

    match result {
        Ok(result) => (
            StatusCode::OK,
            Json(HealRepairResponse {
                slot_id,
                repaired_objects: result.repaired_objects,
                skipped_objects: result.skipped_objects,
                errors: result.errors,
            }),
        )
            .into_response(),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn v1_internal_cluster_bootstrap(
    State(state): State<Arc<ServerState>>,
) -> impl IntoResponse {
    let bootstrap_bytes = match state.registry.get_bootstrap_state().await {
        Ok(payload) => payload,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let Some(payload) = bootstrap_bytes else {
        return (
            StatusCode::OK,
            Json(InternalBootstrapResponse {
                found: false,
                namespace: state.config.registry.namespace_or_default().to_string(),
                state: None,
            }),
        )
            .into_response();
    };

    let bootstrap_state: rimio_core::ClusterState = match serde_json::from_slice(&payload) {
        Ok(state) => state,
        Err(error) => {
            return response_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid bootstrap state payload in registry: {}", error),
            );
        }
    };

    (
        StatusCode::OK,
        Json(InternalBootstrapResponse {
            found: true,
            namespace: state.config.registry.namespace_or_default().to_string(),
            state: Some(bootstrap_state),
        }),
    )
        .into_response()
}

pub(crate) async fn v1_internal_cluster_gossip_seeds(
    State(state): State<Arc<ServerState>>,
) -> impl IntoResponse {
    let nodes = match super::current_nodes(&state).await {
        Ok(nodes) => nodes,
        Err(error) => return response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    let mut seeds = nodes
        .into_iter()
        .map(|node| node.address)
        .filter(|address| !address.trim().is_empty())
        .collect::<Vec<_>>();
    seeds.sort();
    seeds.dedup();

    (StatusCode::OK, Json(InternalGossipSeedsResponse { seeds })).into_response()
}

fn parse_from_socket_addr(raw: &str) -> std::result::Result<std::net::SocketAddr, String> {
    raw.trim().parse::<std::net::SocketAddr>().map_err(|error| {
        format!(
            "invalid gossip sender '{}': expected host:port ({})",
            raw, error
        )
    })
}

pub(crate) async fn v1_internal_gossip_packet(
    Query(query): Query<InternalGossipFromQuery>,
    body: Bytes,
) -> impl IntoResponse {
    let Some(from_raw) = query.from.as_deref() else {
        return response_error(StatusCode::BAD_REQUEST, "from query is required");
    };

    let from = match parse_from_socket_addr(from_raw) {
        Ok(addr) => addr,
        Err(message) => return response_error(StatusCode::BAD_REQUEST, message),
    };

    match ingest_global_gossip_packet(from, &body).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => response_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "gossip transport not initialized",
        ),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

pub(crate) async fn v1_internal_gossip_stream(
    Query(query): Query<InternalGossipFromQuery>,
    body: Bytes,
) -> impl IntoResponse {
    let Some(from_raw) = query.from.as_deref() else {
        return response_error(StatusCode::BAD_REQUEST, "from query is required");
    };

    let from = match parse_from_socket_addr(from_raw) {
        Ok(addr) => addr,
        Err(message) => return response_error(StatusCode::BAD_REQUEST, message),
    };

    match ingest_global_gossip_stream(from, &body).await {
        Ok(Some(response)) => (StatusCode::OK, response).into_response(),
        Ok(None) => response_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "gossip transport not initialized",
        ),
        Err(error) => response_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}
