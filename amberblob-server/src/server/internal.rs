use super::{
    HealBucket, HealBucketsQuery, HealBucketsResponse, HealHeadItem, HealHeadsRequest,
    HealHeadsResponse, HealRepairRequest, HealRepairResponse, InternalHeadApplyRequest,
    InternalHeadApplyResponse, InternalHeadResponse, InternalPartPutResponse, InternalPathQuery,
    ServerState, ensure_store, normalize_blob_path, response_error,
};
use amberblob_core::{AmberError, BlobHead, HeadKind, PartRef, Result, compute_hash};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub(crate) async fn internal_put_part(
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
        .part_store
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

    let part = PartRef {
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

pub(crate) async fn internal_get_part(
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
            .part_store
            .get_part(slot_id, &normalized_path, &sha256)
            .await
        {
            Ok(bytes) => bytes,
            Err(_) => {
                return response_error(StatusCode::NOT_FOUND, "part not found for path");
            }
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

pub(crate) async fn internal_put_head(
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
                            .part_store
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
                    return response_error(
                        StatusCode::BAD_REQUEST,
                        "tombstone payload is required",
                    );
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

pub(crate) async fn v1_internal_heal_buckets(
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

pub(crate) async fn v1_internal_heal_heads(
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

    let filter_set: HashSet<String> = request.prefixes.into_iter().collect();

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

pub(crate) async fn fetch_remote_head(
    state: &Arc<ServerState>,
    address: &str,
    slot_id: u16,
    path: &str,
) -> Result<Option<BlobHead>> {
    let head_url = state
        .coordinator
        .internal_head_url(address, slot_id, path)?;
    let response = state
        .coordinator
        .client()
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

pub(crate) async fn apply_remote_head_locally(
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
                    if let Ok(part_path) = state.part_store.part_path(slot_id, path, &part.sha256) {
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

async fn repair_path_from_head(
    state: &Arc<ServerState>,
    source: &amberblob_core::NodeInfo,
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
            if state.part_store.part_exists(slot_id, path, &part.sha256) {
                continue;
            }

            let part_url = state.coordinator.internal_part_url(
                &source.address,
                slot_id,
                &part.sha256,
                path,
            )?;
            let response = state
                .coordinator
                .client()
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
                .part_store
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
