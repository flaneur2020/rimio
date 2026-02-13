use crate::{
    ArchiveStore, BlobMeta, ClusterClient, MetadataStore, PartStore, PutBlobArchiveWriter,
    Registry, Result, RimError, SlotInfo, SlotManager,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

#[derive(Debug, Clone)]
pub struct ArchiveLifecycleConfig {
    pub sync_interval: Duration,
    pub batch_size: usize,
}

impl Default for ArchiveLifecycleConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(10),
            batch_size: 64,
        }
    }
}

#[derive(Debug, Clone)]
struct ArchiveSyncCursor {
    updated_at: String,
    blob_path: String,
    generation: i64,
}

impl ArchiveSyncCursor {
    fn from_meta(meta: &BlobMeta) -> Self {
        Self {
            updated_at: meta.updated_at.to_rfc3339(),
            blob_path: meta.path.clone(),
            generation: meta.generation,
        }
    }
}

struct ArchiveCursorStore {
    db_path: PathBuf,
}

impl ArchiveCursorStore {
    fn new(db_path: PathBuf) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let store = Self { db_path };
        store.ensure_schema()?;
        Ok(store)
    }

    fn ensure_schema(&self) -> Result<()> {
        let conn = self.connection()?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS archive_sync_cursor (
                slot_id INTEGER PRIMARY KEY,
                updated_at TEXT NOT NULL,
                blob_path TEXT NOT NULL,
                generation INTEGER NOT NULL,
                updated_local_at TEXT NOT NULL
            )",
            [],
        )?;

        Ok(())
    }

    fn connection(&self) -> Result<Connection> {
        let conn = Connection::open(&self.db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    fn load(&self, slot_id: u16) -> Result<Option<ArchiveSyncCursor>> {
        let conn = self.connection()?;
        let cursor = conn
            .query_row(
                "SELECT updated_at, blob_path, generation
                 FROM archive_sync_cursor
                 WHERE slot_id = ?1",
                params![slot_id as i64],
                |row| {
                    Ok(ArchiveSyncCursor {
                        updated_at: row.get(0)?,
                        blob_path: row.get(1)?,
                        generation: row.get(2)?,
                    })
                },
            )
            .optional()?;

        Ok(cursor)
    }

    fn save(&self, slot_id: u16, cursor: &ArchiveSyncCursor) -> Result<()> {
        let conn = self.connection()?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO archive_sync_cursor (
                slot_id,
                updated_at,
                blob_path,
                generation,
                updated_local_at
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(slot_id) DO UPDATE SET
                updated_at = excluded.updated_at,
                blob_path = excluded.blob_path,
                generation = excluded.generation,
                updated_local_at = excluded.updated_local_at",
            params![
                slot_id as i64,
                cursor.updated_at,
                cursor.blob_path,
                cursor.generation,
                now,
            ],
        )?;

        Ok(())
    }
}

pub struct ArchiveLifecycleManager {
    local_node_id: String,
    registry: Arc<dyn Registry>,
    slot_manager: Arc<SlotManager>,
    part_store: Arc<PartStore>,
    cluster_client: Arc<ClusterClient>,
    archive_writer: PutBlobArchiveWriter,
    cursor_store: ArchiveCursorStore,
    config: ArchiveLifecycleConfig,
}

impl ArchiveLifecycleManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node_id: String,
        registry: Arc<dyn Registry>,
        slot_manager: Arc<SlotManager>,
        part_store: Arc<PartStore>,
        cluster_client: Arc<ClusterClient>,
        archive_store: Arc<dyn ArchiveStore>,
        archive_key_prefix: String,
        local_data_dir: PathBuf,
        config: ArchiveLifecycleConfig,
    ) -> Result<Self> {
        let cursor_store =
            ArchiveCursorStore::new(local_data_dir.join("archive").join("sync_cursor.sqlite3"))?;

        Ok(Self {
            local_node_id,
            registry,
            slot_manager,
            part_store,
            cluster_client,
            archive_writer: PutBlobArchiveWriter::new(archive_store, archive_key_prefix),
            cursor_store,
            config,
        })
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = interval(self.config.sync_interval);
            loop {
                ticker.tick().await;
                if let Err(error) = self.sync_once().await {
                    tracing::warn!("archive lifecycle sync loop failed: {}", error);
                }
            }
        });
    }

    pub async fn sync_once(&self) -> Result<()> {
        let slots = self.registry.get_all_slots().await?;
        let mut primary_slots: Vec<SlotInfo> = slots
            .into_values()
            .filter(|slot| slot.primary == self.local_node_id)
            .collect();

        primary_slots.sort_by_key(|slot| slot.slot_id);

        for slot in primary_slots {
            if let Err(error) = self.sync_primary_slot(&slot).await {
                tracing::warn!(
                    "archive sync failed for slot={} primary={} error={}",
                    slot.slot_id,
                    slot.primary,
                    error
                );
            }
        }

        Ok(())
    }

    async fn sync_primary_slot(&self, slot_info: &SlotInfo) -> Result<()> {
        if !self.slot_manager.has_slot(slot_info.slot_id).await {
            self.slot_manager.init_slot(slot_info.slot_id).await?;
        }

        let slot = self.slot_manager.get_slot(slot_info.slot_id).await?;
        let metadata_store = MetadataStore::new(slot)?;
        let cursor = self.cursor_store.load(slot_info.slot_id)?;

        let updates = metadata_store.list_meta_updates_after(
            cursor.as_ref().map(|value| value.updated_at.as_str()),
            cursor.as_ref().map(|value| value.blob_path.as_str()),
            cursor.as_ref().map(|value| value.generation),
            self.config.batch_size,
        )?;

        for meta in updates {
            let cursor_value = ArchiveSyncCursor::from_meta(&meta);

            if meta.archive_url.is_none() {
                self.archive_and_notify(slot_info, &metadata_store, &meta)
                    .await?;
            }

            self.cursor_store.save(slot_info.slot_id, &cursor_value)?;
        }

        Ok(())
    }

    async fn archive_and_notify(
        &self,
        slot_info: &SlotInfo,
        metadata_store: &MetadataStore,
        meta: &BlobMeta,
    ) -> Result<()> {
        let blob_bytes = self.load_blob_bytes(metadata_store, meta).await?;
        let archive_url = self
            .archive_writer
            .write_blob(&meta.path, meta.generation, &blob_bytes)
            .await?;

        let mut archived_meta = meta.clone();
        archived_meta.archive_url = Some(archive_url.clone());
        archived_meta.updated_at = chrono::Utc::now();

        self.notify_replicas(slot_info, &archived_meta).await?;

        let payload = serde_json::to_vec(&archived_meta)?;
        let payload_sha = crate::compute_hash(&payload);
        metadata_store.upsert_meta_with_payload(&archived_meta, &payload, &payload_sha)?;

        tracing::info!(
            "archived blob and propagated archive_url slot={} path={} generation={} archive_url={}",
            slot_info.slot_id,
            archived_meta.path,
            archived_meta.generation,
            archive_url
        );

        Ok(())
    }

    async fn notify_replicas(&self, slot_info: &SlotInfo, meta: &BlobMeta) -> Result<()> {
        for replica in slot_info
            .replicas
            .iter()
            .filter(|replica| replica.as_str() != self.local_node_id.as_str())
        {
            self.cluster_client
                .replicate_archive_meta_update(replica, slot_info.slot_id, &meta.path, meta)
                .await?;
        }

        Ok(())
    }

    async fn load_blob_bytes(
        &self,
        metadata_store: &MetadataStore,
        meta: &BlobMeta,
    ) -> Result<Vec<u8>> {
        if meta.size_bytes == 0 {
            return Ok(Vec::new());
        }

        let part_size = meta.part_size.max(1);
        let part_count = if meta.part_count == 0 {
            meta.size_bytes.div_ceil(part_size) as u32
        } else {
            meta.part_count
        };

        let expected_size = usize::try_from(meta.size_bytes).unwrap_or(0);
        let mut all = Vec::with_capacity(expected_size);

        for part_no in 0..part_count {
            let part_entry = metadata_store
                .get_part_entry(&meta.path, meta.generation, part_no)?
                .ok_or_else(|| {
                    RimError::PartNotFound(format!(
                        "archive sync missing part entry: slot={} path={} generation={} part_no={}",
                        meta.slot_id, meta.path, meta.generation, part_no
                    ))
                })?;

            let bytes = self
                .part_store
                .get_part(
                    meta.slot_id,
                    &meta.path,
                    meta.generation,
                    part_no,
                    &part_entry.sha256,
                )
                .await?;

            all.extend_from_slice(&bytes);
        }

        if all.len() as u64 != meta.size_bytes {
            return Err(RimError::Internal(format!(
                "archive sync blob size mismatch: slot={} path={} generation={} expected={} actual={}",
                meta.slot_id,
                meta.path,
                meta.generation,
                meta.size_bytes,
                all.len()
            )));
        }

        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_cursor_store_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ArchiveCursorStore::new(dir.path().join("cursor.sqlite3")).expect("store");

        assert!(store.load(7).expect("load").is_none());

        let cursor = ArchiveSyncCursor {
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            blob_path: "a/b/c".to_string(),
            generation: 12,
        };

        store.save(7, &cursor).expect("save");

        let loaded = store.load(7).expect("reload").expect("cursor");
        assert_eq!(loaded.updated_at, cursor.updated_at);
        assert_eq!(loaded.blob_path, cursor.blob_path);
        assert_eq!(loaded.generation, cursor.generation);
    }
}
