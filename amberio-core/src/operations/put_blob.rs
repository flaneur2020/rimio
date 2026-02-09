use crate::{
    AmberError, ArchiveStore, BlobMeta, ClusterClient, Coordinator, MetadataStore, PART_SIZE,
    PartIndexState, PartStore, ReplicatedPart, Result, SlotManager, compute_hash,
};
use bytes::Bytes;
use chrono::Utc;
use std::sync::Arc;

#[derive(Clone)]
pub struct PutBlobArchiveWriter {
    store: Arc<dyn ArchiveStore>,
    key_prefix: String,
}

impl PutBlobArchiveWriter {
    pub fn new(store: Arc<dyn ArchiveStore>, key_prefix: impl Into<String>) -> Self {
        Self {
            store,
            key_prefix: key_prefix.into(),
        }
    }

    fn object_key_for(&self, path: &str, generation: i64) -> String {
        let prefix = self.key_prefix.trim_matches('/');
        if prefix.is_empty() {
            format!("{}/g.{}", path, generation)
        } else {
            format!("{}/{}/g.{}", prefix, path, generation)
        }
    }

    pub async fn write_blob(&self, path: &str, generation: i64, body: &[u8]) -> Result<String> {
        let object_key = self.object_key_for(path, generation);
        self.store.write_blob(&object_key, body).await?;
        Ok(self.store.archive_url_for_key(&object_key))
    }
}

#[derive(Clone)]
pub struct PutBlobOperation {
    slot_manager: Arc<SlotManager>,
    part_store: Arc<PartStore>,
    coordinator: Arc<Coordinator>,
    cluster_client: Arc<ClusterClient>,
    archive_writer: Option<PutBlobArchiveWriter>,
}

#[derive(Debug, Clone)]
pub struct PutBlobOperationRequest {
    pub path: String,
    pub slot_id: u16,
    pub write_id: String,
    pub body: Bytes,
    pub replicas: Vec<crate::NodeInfo>,
    pub local_node_id: String,
}

#[derive(Debug, Clone)]
pub struct PutBlobOperationResult {
    pub generation: i64,
    pub etag: String,
    pub size_bytes: u64,
    pub committed_replicas: usize,
}

#[derive(Debug, Clone)]
pub enum PutBlobOperationOutcome {
    Committed(PutBlobOperationResult),
    Conflict,
}

impl PutBlobOperation {
    pub fn new(
        slot_manager: Arc<SlotManager>,
        part_store: Arc<PartStore>,
        coordinator: Arc<Coordinator>,
        cluster_client: Arc<ClusterClient>,
        archive_writer: Option<PutBlobArchiveWriter>,
    ) -> Self {
        Self {
            slot_manager,
            part_store,
            coordinator,
            cluster_client,
            archive_writer,
        }
    }

    pub async fn run(&self, request: PutBlobOperationRequest) -> Result<PutBlobOperationOutcome> {
        let PutBlobOperationRequest {
            path,
            slot_id,
            write_id,
            body,
            replicas,
            local_node_id,
        } = request;

        let store = self.ensure_store(slot_id).await?;
        let generation = store.next_generation(&path)?;
        let etag = compute_hash(&body);

        let mut replicated_parts: Vec<ReplicatedPart> = Vec::new();

        let mut offset = 0usize;
        let mut part_no = 0u32;
        while offset < body.len() {
            let end = (offset + PART_SIZE).min(body.len());
            let part_body = body.slice(offset..end);
            let part_sha = compute_hash(&part_body);

            let put_result = self
                .part_store
                .put_part(
                    slot_id,
                    &path,
                    generation,
                    part_no,
                    &part_sha,
                    part_body.clone(),
                )
                .await?;

            let external_path = put_result.part_path.to_string_lossy().to_string();
            let part_len = (end - offset) as u64;
            store.upsert_part_entry(
                &path,
                generation,
                part_no,
                &part_sha,
                part_len,
                Some(external_path.as_str()),
                None,
            )?;

            replicated_parts.push(ReplicatedPart {
                part_no,
                sha256: part_sha,
                length: part_len,
                data: part_body,
            });

            offset = end;
            part_no += 1;
        }

        let part_count = if body.is_empty() {
            0
        } else {
            body.len().div_ceil(PART_SIZE) as u32
        };

        let archive_url = match &self.archive_writer {
            Some(writer) => Some(writer.write_blob(&path, generation, body.as_ref()).await?),
            None => None,
        };

        let meta = BlobMeta {
            path: path.clone(),
            slot_id,
            generation,
            version: generation,
            size_bytes: body.len() as u64,
            etag: etag.clone(),
            part_size: PART_SIZE as u64,
            part_count,
            part_index_state: PartIndexState::Complete,
            archive_url,
            updated_at: Utc::now(),
        };

        let meta_bytes = serde_json::to_vec(&meta)?;
        let meta_sha = compute_hash(&meta_bytes);

        let applied = store.upsert_meta_with_payload(&meta, &meta_bytes, &meta_sha)?;
        if !applied {
            return Ok(PutBlobOperationOutcome::Conflict);
        }

        let quorum = self.coordinator.write_quorum(replicas.len());
        let mut committed_replicas = 1usize;

        for replica in replicas
            .iter()
            .filter(|node| node.node_id != local_node_id.as_str())
        {
            let write_result = self
                .cluster_client
                .replicate_meta_write(
                    &replica.node_id,
                    slot_id,
                    &path,
                    &write_id,
                    generation,
                    &replicated_parts,
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
            return Err(AmberError::InsufficientReplicas {
                required: quorum,
                found: committed_replicas,
            });
        }

        Ok(PutBlobOperationOutcome::Committed(PutBlobOperationResult {
            generation,
            etag,
            size_bytes: body.len() as u64,
            committed_replicas,
        }))
    }

    async fn ensure_store(&self, slot_id: u16) -> Result<MetadataStore> {
        if !self.slot_manager.has_slot(slot_id).await {
            self.slot_manager.init_slot(slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_id).await?;
        MetadataStore::new(slot)
    }
}
