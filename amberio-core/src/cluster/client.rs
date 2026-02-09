use super::types::ReplicatedPart;
use crate::{AmberError, BlobHead, BlobMeta, HeadKind, NodeInfo, Registry, Result, TombstoneMeta};
use chrono::Utc;
use reqwest::{
    Client, Url,
    header::{self, HeaderMap},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const PART_INDEX_SENTINEL_SHA256: &str = "_";

#[derive(Debug, Clone, Serialize)]
struct InternalHeadApplyRequest {
    head_kind: String,
    generation: i64,
    head_sha256: String,
    meta: Option<BlobMeta>,
    tombstone: Option<TombstoneMeta>,
}

#[derive(Debug, Deserialize)]
struct InternalHeadResponsePayload {
    found: bool,
    head_kind: Option<String>,
    generation: Option<i64>,
    head_sha256: Option<String>,
    meta: Option<BlobMeta>,
    tombstone: Option<TombstoneMeta>,
}

#[derive(Debug)]
pub struct ClusterPartPayload {
    pub headers: HeaderMap,
    pub bytes: bytes::Bytes,
}

#[derive(Clone)]
pub struct ClusterClient {
    client: Client,
    registry: Arc<dyn Registry>,
}

impl ClusterClient {
    pub fn new(registry: Arc<dyn Registry>) -> Self {
        Self {
            client: Client::new(),
            registry,
        }
    }

    pub async fn replicate_meta_write(
        &self,
        target_node_id: &str,
        slot_id: u16,
        path: &str,
        write_id: &str,
        generation: i64,
        parts: &[ReplicatedPart],
        meta: &BlobMeta,
        head_sha256: &str,
    ) -> Result<()> {
        let target = self.resolve_node(target_node_id).await?;

        for part in parts {
            let part_url = self
                .internal_part_url_by_sha(
                    &target.node_id,
                    slot_id,
                    &part.sha256,
                    path,
                    generation,
                    part.part_no,
                )
                .await?;

            let response = self
                .client
                .put(part_url)
                .header("x-amberio-write-id", write_id)
                .header("x-amberio-generation", generation.to_string())
                .header("x-amberio-part-no", part.part_no.to_string())
                .header("x-amberio-part-length", part.length.to_string())
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(part.data.clone())
                .send()
                .await
                .map_err(|error| AmberError::Http(error.to_string()))?;

            if !response.status().is_success() {
                return Err(AmberError::Http(format!(
                    "replica part write failed: node={} status={} part_no={} path={}",
                    target.node_id,
                    response.status(),
                    part.part_no,
                    path
                )));
            }
        }

        let head_url = self
            .internal_head_url(&target.node_id, slot_id, path)
            .await?;
        let payload = InternalHeadApplyRequest {
            head_kind: "meta".to_string(),
            generation,
            head_sha256: head_sha256.to_string(),
            meta: Some(meta.clone()),
            tombstone: None,
        };

        let response = self
            .client
            .put(head_url)
            .header("x-amberio-write-id", write_id)
            .header(header::CONTENT_TYPE, "application/json")
            .json(&payload)
            .send()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        if !response.status().is_success() {
            return Err(AmberError::Http(format!(
                "replica head write failed: node={} status={} path={}",
                target.node_id,
                response.status(),
                path
            )));
        }

        Ok(())
    }

    pub async fn replicate_tombstone_write(
        &self,
        target_node_id: &str,
        slot_id: u16,
        path: &str,
        write_id: &str,
        generation: i64,
        tombstone: &TombstoneMeta,
        head_sha256: &str,
    ) -> Result<()> {
        let target = self.resolve_node(target_node_id).await?;

        let head_url = self
            .internal_head_url(&target.node_id, slot_id, path)
            .await?;
        let payload = InternalHeadApplyRequest {
            head_kind: "tombstone".to_string(),
            generation,
            head_sha256: head_sha256.to_string(),
            meta: None,
            tombstone: Some(tombstone.clone()),
        };

        let response = self
            .client
            .put(head_url)
            .header("x-amberio-write-id", write_id)
            .header(header::CONTENT_TYPE, "application/json")
            .json(&payload)
            .send()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        if !response.status().is_success() {
            return Err(AmberError::Http(format!(
                "replica tombstone write failed: node={} status={} path={}",
                target.node_id,
                response.status(),
                path
            )));
        }

        Ok(())
    }

    pub async fn fetch_remote_head(
        &self,
        source_node_id: &str,
        slot_id: u16,
        path: &str,
    ) -> Result<Option<BlobHead>> {
        let head_url = self
            .internal_head_url(source_node_id, slot_id, path)
            .await?;
        let response = self
            .client
            .get(head_url)
            .send()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            return Err(AmberError::Http(format!(
                "internal head fetch failed: node={} status={} path={}",
                source_node_id,
                response.status(),
                path
            )));
        }

        let payload: InternalHeadResponsePayload = response
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

    pub async fn fetch_part_by_sha(
        &self,
        source_node_id: &str,
        slot_id: u16,
        sha256: &str,
        path: &str,
        generation: i64,
        part_no: u32,
    ) -> Result<ClusterPartPayload> {
        let part_url = self
            .internal_part_url_by_sha(source_node_id, slot_id, sha256, path, generation, part_no)
            .await?;

        self.fetch_part_payload(source_node_id, part_url, path, part_no)
            .await
    }

    pub async fn fetch_part_by_index(
        &self,
        source_node_id: &str,
        slot_id: u16,
        path: &str,
        generation: i64,
        part_no: u32,
    ) -> Result<ClusterPartPayload> {
        let part_url = self
            .internal_part_url_by_index(source_node_id, slot_id, path, generation, part_no)
            .await?;

        self.fetch_part_payload(source_node_id, part_url, path, part_no)
            .await
    }

    async fn fetch_part_payload(
        &self,
        source_node_id: &str,
        part_url: Url,
        path: &str,
        part_no: u32,
    ) -> Result<ClusterPartPayload> {
        let response = self
            .client
            .get(part_url)
            .send()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        if !response.status().is_success() {
            return Err(AmberError::Http(format!(
                "failed to fetch part_no {} from source {}: status={} path={}",
                part_no,
                source_node_id,
                response.status(),
                path
            )));
        }

        let headers = response.headers().clone();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| AmberError::Http(error.to_string()))?;

        Ok(ClusterPartPayload { headers, bytes })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    async fn internal_head_url(&self, node_id: &str, slot_id: u16, path: &str) -> Result<Url> {
        let node = self.resolve_node(node_id).await?;
        let mut url = Url::parse(&format!(
            "http://{}/internal/v1/slots/{}/heads",
            node.address, slot_id
        ))
        .map_err(|error| AmberError::Http(error.to_string()))?;

        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("path", path);
        }

        Ok(url)
    }

    async fn internal_part_url_by_sha(
        &self,
        node_id: &str,
        slot_id: u16,
        sha256: &str,
        path: &str,
        generation: i64,
        part_no: u32,
    ) -> Result<Url> {
        self.build_internal_part_url(node_id, slot_id, sha256, path, generation, part_no)
            .await
    }

    async fn internal_part_url_by_index(
        &self,
        node_id: &str,
        slot_id: u16,
        path: &str,
        generation: i64,
        part_no: u32,
    ) -> Result<Url> {
        self.build_internal_part_url(
            node_id,
            slot_id,
            PART_INDEX_SENTINEL_SHA256,
            path,
            generation,
            part_no,
        )
        .await
    }

    async fn build_internal_part_url(
        &self,
        node_id: &str,
        slot_id: u16,
        sha256: &str,
        path: &str,
        generation: i64,
        part_no: u32,
    ) -> Result<Url> {
        let node = self.resolve_node(node_id).await?;
        let mut url = Url::parse(&format!(
            "http://{}/internal/v1/slots/{}/parts/{}",
            node.address, slot_id, sha256
        ))
        .map_err(|error| AmberError::Http(error.to_string()))?;

        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("path", path);
            pairs.append_pair("generation", &generation.to_string());
            pairs.append_pair("part_no", &part_no.to_string());
        }

        Ok(url)
    }

    async fn resolve_node(&self, node_id: &str) -> Result<NodeInfo> {
        let nodes = self.registry.get_nodes().await?;
        nodes
            .into_iter()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| AmberError::Internal(format!("node not found in registry: {}", node_id)))
    }
}
