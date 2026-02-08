use crate::{AmberError, BlobMeta, NodeInfo, PartRef, Result, TombstoneMeta};
use bytes::Bytes;
use reqwest::{Client, Url, header};
use serde::Serialize;

#[derive(Clone)]
pub struct Coordinator {
    client: Client,
    min_write_replicas: usize,
}

#[derive(Clone)]
pub struct ReplicatedPart {
    pub part: PartRef,
    pub data: Bytes,
}

#[derive(Debug, Serialize)]
struct InternalHeadApplyRequest {
    head_kind: String,
    generation: i64,
    head_sha256: String,
    meta: Option<BlobMeta>,
    tombstone: Option<TombstoneMeta>,
}

impl Coordinator {
    pub fn new(min_write_replicas: usize) -> Self {
        Self {
            client: Client::new(),
            min_write_replicas,
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn write_quorum(&self, replica_count: usize) -> usize {
        self.min_write_replicas.min(replica_count).max(1)
    }

    pub fn internal_head_url(&self, address: &str, slot_id: u16, path: &str) -> Result<Url> {
        let mut url = Url::parse(&format!(
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

    pub fn internal_part_url(
        &self,
        address: &str,
        slot_id: u16,
        sha256: &str,
        path: &str,
    ) -> Result<Url> {
        let mut url = Url::parse(&format!(
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

    pub async fn replicate_meta_write(
        &self,
        replica: &NodeInfo,
        slot_id: u16,
        path: &str,
        write_id: &str,
        generation: i64,
        parts: &[ReplicatedPart],
        meta: &BlobMeta,
        head_sha256: &str,
    ) -> Result<()> {
        for part in parts {
            let part_url =
                self.internal_part_url(&replica.address, slot_id, &part.part.sha256, path)?;

            let response = self
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

        let head_url = self.internal_head_url(&replica.address, slot_id, path)?;
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

    pub async fn replicate_tombstone_write(
        &self,
        replica: &NodeInfo,
        slot_id: u16,
        path: &str,
        write_id: &str,
        generation: i64,
        tombstone: &TombstoneMeta,
        head_sha256: &str,
    ) -> Result<()> {
        let head_url = self.internal_head_url(&replica.address, slot_id, path)?;
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
}
