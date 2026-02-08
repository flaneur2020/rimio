use crate::{
    AmberError, BlobHead, BlobMeta, Coordinator, HeadKind, MetadataStore, NodeInfo, PART_SIZE,
    PartStore, Result, SlotManager, compute_hash,
};
use bytes::Bytes;
use chrono::Utc;
use reqwest::{Url, header::HeaderMap};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct ReadBlobOperation {
    slot_manager: Arc<SlotManager>,
    part_store: Arc<PartStore>,
    coordinator: Arc<Coordinator>,
}

#[derive(Debug, Clone, Copy)]
pub struct ReadByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone)]
pub struct ReadBlobOperationRequest {
    pub slot_id: u16,
    pub path: String,
    pub replicas: Vec<NodeInfo>,
    pub local_node_id: String,
    pub include_body: bool,
    pub range: Option<ReadByteRange>,
}

#[derive(Debug, Clone)]
pub struct ReadBlobOperationResult {
    pub meta: BlobMeta,
    pub body: Option<Bytes>,
    pub body_range: Option<ReadByteRange>,
}

#[derive(Debug, Clone)]
pub enum ReadBlobOperationOutcome {
    Found(ReadBlobOperationResult),
    NotFound,
    Deleted,
}

impl ReadBlobOperation {
    pub fn new(
        slot_manager: Arc<SlotManager>,
        part_store: Arc<PartStore>,
        coordinator: Arc<Coordinator>,
    ) -> Self {
        Self {
            slot_manager,
            part_store,
            coordinator,
        }
    }

    pub async fn run(&self, request: ReadBlobOperationRequest) -> Result<ReadBlobOperationOutcome> {
        let ReadBlobOperationRequest {
            slot_id,
            path,
            replicas,
            local_node_id,
            include_body,
            range,
        } = request;

        let head = self
            .ensure_head_available(slot_id, &path, &replicas, &local_node_id)
            .await?;
        let Some(head) = head else {
            return Ok(ReadBlobOperationOutcome::NotFound);
        };

        if head.head_kind == HeadKind::Tombstone {
            return Ok(ReadBlobOperationOutcome::Deleted);
        }

        let meta = head
            .meta
            .ok_or_else(|| AmberError::Internal("meta payload missing".to_string()))?;

        if !include_body {
            return Ok(ReadBlobOperationOutcome::Found(ReadBlobOperationResult {
                meta,
                body: None,
                body_range: None,
            }));
        }

        if meta.size_bytes == 0 {
            if range.is_some() {
                return Err(AmberError::InvalidRequest(
                    "range not satisfiable for empty blob".to_string(),
                ));
            }

            return Ok(ReadBlobOperationOutcome::Found(ReadBlobOperationResult {
                meta,
                body: Some(Bytes::new()),
                body_range: None,
            }));
        }

        let body_range = resolve_effective_range(meta.size_bytes, range)?;
        let part_size = meta.part_size.max(1);

        let first_part = body_range.start / part_size;
        let last_part = body_range.end / part_size;

        let peer_nodes: Vec<NodeInfo> = replicas
            .into_iter()
            .filter(|node| node.node_id != local_node_id)
            .collect();

        let mut body = Vec::with_capacity((body_range.end - body_range.start + 1) as usize);
        for part_no_u64 in first_part..=last_part {
            let part_no = u32::try_from(part_no_u64).map_err(|_| {
                AmberError::Internal(format!("part index overflow: {}", part_no_u64))
            })?;

            let bytes = self
                .read_part_bytes(&peer_nodes, slot_id, &path, &meta, part_no)
                .await?;

            let part_start = part_no_u64 * part_size;
            let slice_start = if part_no_u64 == first_part {
                (body_range.start - part_start) as usize
            } else {
                0
            };
            let slice_end_exclusive = if part_no_u64 == last_part {
                ((body_range.end - part_start) + 1) as usize
            } else {
                bytes.len()
            };

            if slice_start > slice_end_exclusive || slice_end_exclusive > bytes.len() {
                return Err(AmberError::Internal(format!(
                    "invalid part slice: path={} generation={} part_no={} start={} end={} len={}",
                    path,
                    meta.generation,
                    part_no,
                    slice_start,
                    slice_end_exclusive,
                    bytes.len()
                )));
            }

            body.extend_from_slice(&bytes[slice_start..slice_end_exclusive]);
        }

        Ok(ReadBlobOperationOutcome::Found(ReadBlobOperationResult {
            meta,
            body: Some(Bytes::from(body)),
            body_range: Some(body_range),
        }))
    }

    pub async fn fetch_remote_head(
        &self,
        address: &str,
        slot_id: u16,
        path: &str,
    ) -> Result<Option<BlobHead>> {
        let head_url = self.coordinator.internal_head_url(address, slot_id, path)?;
        let response = self
            .coordinator
            .client()
            .get(head_url)
            .send()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            return Err(AmberError::Http(format!(
                "internal head fetch failed: status={} path={}",
                response.status(),
                path
            )));
        }

        #[derive(serde::Deserialize)]
        struct InternalHeadResponse {
            found: bool,
            head_kind: Option<String>,
            generation: Option<i64>,
            head_sha256: Option<String>,
            meta: Option<BlobMeta>,
            tombstone: Option<crate::TombstoneMeta>,
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

    pub async fn apply_remote_head_locally(
        &self,
        slot_id: u16,
        path: &str,
        head: &BlobHead,
    ) -> Result<()> {
        let store = self.ensure_store(slot_id).await?;

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
                if meta.part_size == 0 {
                    meta.part_size = PART_SIZE as u64;
                }
                if meta.part_count == 0 && meta.size_bytes > 0 {
                    meta.part_count = meta.size_bytes.div_ceil(meta.part_size.max(1)) as u32;
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

    pub async fn repair_path_from_head(
        &self,
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

            let store = self.ensure_store(slot_id).await?;

            for part_no in 0..meta.part_count {
                let already_local = match store.get_part_entry(path, meta.generation, part_no)? {
                    Some(entry) => {
                        if let Some(external_path) = entry.external_path {
                            Path::new(&external_path).exists()
                        } else {
                            self.part_store.part_exists(
                                slot_id,
                                path,
                                meta.generation,
                                part_no,
                                &entry.sha256,
                            )
                        }
                    }
                    None => false,
                };

                if already_local {
                    continue;
                }

                let part_url = self.coordinator.internal_part_url_by_index(
                    &source.address,
                    slot_id,
                    path,
                    meta.generation,
                    part_no,
                )?;

                let response = self
                    .coordinator
                    .client()
                    .get(part_url)
                    .send()
                    .await
                    .map_err(|error| AmberError::Http(error.to_string()))?;

                if !response.status().is_success() {
                    return Err(AmberError::Http(format!(
                        "failed to fetch part_no {} from source {}: {}",
                        part_no,
                        source.node_id,
                        response.status()
                    )));
                }

                let headers = response.headers().clone();
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|error| AmberError::Http(error.to_string()))?;

                let sha256 = resolve_part_sha256(Some(&headers), &bytes, None);

                let put_result = self
                    .part_store
                    .put_part(
                        slot_id,
                        path,
                        meta.generation,
                        part_no,
                        &sha256,
                        bytes.clone(),
                    )
                    .await?;

                store.upsert_part_entry(
                    path,
                    meta.generation,
                    part_no,
                    &sha256,
                    bytes.len() as u64,
                    Some(put_result.part_path.to_string_lossy().as_ref()),
                    None,
                )?;
            }
        }

        self.apply_remote_head_locally(slot_id, path, remote_head)
            .await
    }

    async fn ensure_head_available(
        &self,
        slot_id: u16,
        path: &str,
        replicas: &[NodeInfo],
        local_node_id: &str,
    ) -> Result<Option<BlobHead>> {
        let store = self.ensure_store(slot_id).await?;
        if let Some(head) = store.get_current_head(path)? {
            return Ok(Some(head));
        }

        for node in replicas.iter().filter(|node| node.node_id != local_node_id) {
            if let Some(remote_head) = self.fetch_remote_head(&node.address, slot_id, path).await? {
                self.apply_remote_head_locally(slot_id, path, &remote_head)
                    .await?;
                return Ok(Some(remote_head));
            }
        }

        Ok(None)
    }

    async fn read_part_bytes(
        &self,
        peers: &[NodeInfo],
        slot_id: u16,
        path: &str,
        meta: &BlobMeta,
        part_no: u32,
    ) -> Result<Bytes> {
        let store = self.ensure_store(slot_id).await?;

        if let Some(entry) = store.get_part_entry(path, meta.generation, part_no)? {
            if let Ok(local) = self
                .read_local_part(
                    slot_id,
                    path,
                    meta.generation,
                    part_no,
                    &entry.sha256,
                    entry.external_path.as_deref(),
                )
                .await
            {
                return Ok(local);
            }

            if let Some(archive_url) = entry.archive_url.as_deref().or(meta.archive_url.as_deref())
            {
                match self
                    .fetch_part_from_archive_and_store(
                        slot_id,
                        path,
                        meta,
                        part_no,
                        Some(entry.sha256.as_str()),
                        archive_url,
                    )
                    .await
                {
                    Ok(bytes) => return Ok(bytes),
                    Err(error) => {
                        tracing::warn!(
                            "archive fallback failed. slot={} path={} generation={} part_no={} archive_url={} error={}",
                            slot_id,
                            path,
                            meta.generation,
                            part_no,
                            archive_url,
                            error
                        );
                    }
                }
            }

            return self
                .fetch_part_from_peers_and_store(
                    peers,
                    slot_id,
                    path,
                    meta.generation,
                    part_no,
                    Some(entry.sha256.as_str()),
                )
                .await;
        }

        if let Some(archive_url) = meta.archive_url.as_deref() {
            match self
                .fetch_part_from_archive_and_store(slot_id, path, meta, part_no, None, archive_url)
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(error) => {
                    tracing::warn!(
                        "archive fallback failed without local index. slot={} path={} generation={} part_no={} archive_url={} error={}",
                        slot_id,
                        path,
                        meta.generation,
                        part_no,
                        archive_url,
                        error
                    );
                }
            }
        }

        self.fetch_part_from_peers_and_store(peers, slot_id, path, meta.generation, part_no, None)
            .await
    }

    async fn read_local_part(
        &self,
        slot_id: u16,
        path: &str,
        generation: i64,
        part_no: u32,
        sha256: &str,
        external_path: Option<&str>,
    ) -> Result<Bytes> {
        if self
            .part_store
            .part_exists(slot_id, path, generation, part_no, sha256)
        {
            return self
                .part_store
                .get_part(slot_id, path, generation, part_no, sha256)
                .await;
        }

        if let Some(external_path) = external_path {
            if Path::new(external_path).exists() {
                let bytes = tokio::fs::read(external_path).await?;
                return Ok(Bytes::from(bytes));
            }
        }

        Err(AmberError::PartNotFound(format!(
            "path={} generation={} part_no={}",
            path, generation, part_no
        )))
    }

    async fn fetch_part_from_archive_and_store(
        &self,
        slot_id: u16,
        path: &str,
        meta: &BlobMeta,
        part_no: u32,
        expected_sha256: Option<&str>,
        archive_url: &str,
    ) -> Result<Bytes> {
        let (range_start, range_end) = part_byte_range(meta, part_no)?;
        let bytes = fetch_archive_range_bytes(archive_url, range_start, range_end).await?;

        let expected_length = (range_end - range_start + 1) as usize;
        if bytes.len() != expected_length {
            return Err(AmberError::Internal(format!(
                "archive range length mismatch: archive_url={} path={} part_no={} expected={} actual={}",
                archive_url,
                path,
                part_no,
                expected_length,
                bytes.len()
            )));
        }

        let sha256 = resolve_part_sha256(None, &bytes, expected_sha256);
        if let Some(expected) = expected_sha256 {
            if sha256 != expected {
                return Err(AmberError::HashMismatch {
                    expected: expected.to_string(),
                    actual: sha256,
                });
            }
        }

        let put_result = self
            .part_store
            .put_part(
                slot_id,
                path,
                meta.generation,
                part_no,
                &sha256,
                bytes.clone(),
            )
            .await?;

        let store = self.ensure_store(slot_id).await?;
        store.upsert_part_entry(
            path,
            meta.generation,
            part_no,
            &sha256,
            bytes.len() as u64,
            Some(put_result.part_path.to_string_lossy().as_ref()),
            Some(archive_url),
        )?;

        Ok(bytes)
    }

    async fn fetch_part_from_peers_and_store(
        &self,
        peers: &[NodeInfo],
        slot_id: u16,
        path: &str,
        generation: i64,
        part_no: u32,
        expected_sha256: Option<&str>,
    ) -> Result<Bytes> {
        for peer in peers {
            let part_url = if let Some(sha256) = expected_sha256 {
                self.coordinator.internal_part_url_by_sha(
                    &peer.address,
                    slot_id,
                    sha256,
                    path,
                    generation,
                    part_no,
                )
            } else {
                self.coordinator.internal_part_url_by_index(
                    &peer.address,
                    slot_id,
                    path,
                    generation,
                    part_no,
                )
            };

            let Ok(part_url) = part_url else {
                continue;
            };

            let response = match self.coordinator.client().get(part_url).send().await {
                Ok(response) => response,
                Err(_) => continue,
            };

            if !response.status().is_success() {
                continue;
            }

            let headers = response.headers().clone();
            let bytes = match response.bytes().await {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };

            let sha256 = resolve_part_sha256(Some(&headers), &bytes, expected_sha256);
            if let Some(expected_sha256) = expected_sha256 {
                if sha256 != expected_sha256 {
                    continue;
                }
            }

            let put_result = match self
                .part_store
                .put_part(slot_id, path, generation, part_no, &sha256, bytes.clone())
                .await
            {
                Ok(result) => result,
                Err(_) => continue,
            };

            let store = self.ensure_store(slot_id).await?;
            store.upsert_part_entry(
                path,
                generation,
                part_no,
                &sha256,
                bytes.len() as u64,
                Some(put_result.part_path.to_string_lossy().as_ref()),
                None,
            )?;

            return Ok(bytes);
        }

        Err(AmberError::PartNotFound(format!(
            "path={} generation={} part_no={}",
            path, generation, part_no
        )))
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}

fn resolve_effective_range(
    size_bytes: u64,
    requested: Option<ReadByteRange>,
) -> Result<ReadByteRange> {
    match requested {
        Some(range) => {
            if range.start > range.end || range.end >= size_bytes {
                return Err(AmberError::InvalidRequest(format!(
                    "range not satisfiable: start={} end={} size={}",
                    range.start, range.end, size_bytes
                )));
            }
            Ok(range)
        }
        None => Ok(ReadByteRange {
            start: 0,
            end: size_bytes - 1,
        }),
    }
}

fn resolve_part_sha256(
    headers: Option<&HeaderMap>,
    body: &[u8],
    expected_sha256: Option<&str>,
) -> String {
    if let Some(headers) = headers {
        if let Some(value) = headers.get("x-amberio-sha256") {
            if let Ok(value) = value.to_str() {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    let actual = compute_hash(body);
                    if actual == trimmed {
                        return trimmed.to_string();
                    }
                    return actual;
                }
            }
        }
    }

    if let Some(expected) = expected_sha256 {
        let actual = compute_hash(body);
        if actual == expected {
            return expected.to_string();
        }
        return actual;
    }

    compute_hash(body)
}

fn part_byte_range(meta: &BlobMeta, part_no: u32) -> Result<(u64, u64)> {
    let part_size = meta.part_size.max(1);
    let start = part_no as u64 * part_size;
    if start >= meta.size_bytes {
        return Err(AmberError::InvalidRequest(format!(
            "part_no out of range: part_no={} size_bytes={} part_size={}",
            part_no, meta.size_bytes, part_size
        )));
    }

    let end = (start + part_size - 1).min(meta.size_bytes - 1);
    Ok((start, end))
}

async fn fetch_archive_range_bytes(archive_url: &str, start: u64, end: u64) -> Result<Bytes> {
    let parsed = Url::parse(archive_url)
        .map_err(|error| AmberError::InvalidRequest(format!("invalid archive_url: {}", error)))?;

    match parsed.scheme() {
        "redis" => fetch_redis_archive_range_bytes(&parsed, start, end).await,
        "s3" => Err(AmberError::Internal(
            "archive_url with s3:// is not implemented yet".to_string(),
        )),
        scheme => Err(AmberError::InvalidRequest(format!(
            "unsupported archive_url scheme: {}",
            scheme
        ))),
    }
}

async fn fetch_redis_archive_range_bytes(parsed: &Url, start: u64, end: u64) -> Result<Bytes> {
    let (redis_url, key) = parse_redis_archive_url(parsed)?;

    let client = redis::Client::open(redis_url.as_str())
        .map_err(|error| AmberError::Internal(format!("redis archive init failed: {}", error)))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| {
            AmberError::Internal(format!("redis archive connect failed: {}", error))
        })?;

    let start_i64 = i64::try_from(start)
        .map_err(|_| AmberError::Internal(format!("invalid redis range start: {}", start)))?;
    let end_i64 = i64::try_from(end)
        .map_err(|_| AmberError::Internal(format!("invalid redis range end: {}", end)))?;

    let payload: Vec<u8> = redis::cmd("GETRANGE")
        .arg(&key)
        .arg(start_i64)
        .arg(end_i64)
        .query_async(&mut conn)
        .await
        .map_err(|error| {
            AmberError::Internal(format!("redis archive GETRANGE failed: {}", error))
        })?;

    Ok(Bytes::from(payload))
}

fn parse_redis_archive_url(parsed: &Url) -> Result<(String, String)> {
    if parsed.scheme() != "redis" {
        return Err(AmberError::InvalidRequest(format!(
            "not a redis archive url: {}",
            parsed
        )));
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| AmberError::InvalidRequest("redis archive_url missing host".to_string()))?;
    let port = parsed.port().unwrap_or(6379);

    let raw_segments: Vec<&str> = parsed
        .path()
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    if raw_segments.is_empty() {
        return Err(AmberError::InvalidRequest(
            "redis archive_url missing object key".to_string(),
        ));
    }

    let (db_segment, key_segments): (Option<&str>, Vec<&str>) =
        if raw_segments.len() >= 2 && raw_segments[0].chars().all(|char| char.is_ascii_digit()) {
            (Some(raw_segments[0]), raw_segments[1..].to_vec())
        } else {
            (None, raw_segments)
        };

    if key_segments.is_empty() {
        return Err(AmberError::InvalidRequest(
            "redis archive_url missing object key".to_string(),
        ));
    }

    let key = key_segments.join("/");

    let mut redis_url = String::from("redis://");
    if !parsed.username().is_empty() {
        redis_url.push_str(parsed.username());
        if let Some(password) = parsed.password() {
            redis_url.push(':');
            redis_url.push_str(password);
        }
        redis_url.push('@');
    }

    redis_url.push_str(host);
    redis_url.push(':');
    redis_url.push_str(&port.to_string());

    if let Some(db_segment) = db_segment {
        redis_url.push('/');
        redis_url.push_str(db_segment);
    }

    if let Some(query) = parsed.query() {
        redis_url.push('?');
        redis_url.push_str(query);
    }

    Ok((redis_url, key))
}
