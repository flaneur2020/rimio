use crate::error::{AmberError, Result};
use crate::slot_manager::Slot;
use crate::storage::compute_hash;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub name: String,
    pub sha256: String,
    pub offset: u64,
    pub length: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMeta {
    pub path: String,
    pub slot_id: u16,
    pub generation: i64,
    pub version: i64,
    pub size_bytes: u64,
    pub etag: String,
    pub parts: Vec<ChunkRef>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneMeta {
    pub path: String,
    pub slot_id: u16,
    pub generation: i64,
    pub deleted_at: DateTime<Utc>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HeadKind {
    Meta,
    Tombstone,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobHead {
    pub path: String,
    pub generation: i64,
    pub head_kind: HeadKind,
    pub head_sha256: String,
    pub updated_at: DateTime<Utc>,
    pub meta: Option<BlobMeta>,
    pub tombstone: Option<TombstoneMeta>,
}

pub struct MetadataStore {
    slot: Arc<Slot>,
}

struct HeadRow {
    blob_path: String,
    file_kind: String,
    generation: i64,
    sha256: String,
    updated_at: String,
    inline_data: Vec<u8>,
}

impl MetadataStore {
    pub fn new(slot: Arc<Slot>) -> Result<Self> {
        let store = Self { slot };
        store.init_schema()?;
        Ok(store)
    }

    pub fn slot_id(&self) -> u16 {
        self.slot.slot_id
    }

    fn get_conn(&self) -> Result<Connection> {
        let db_path = self.slot.meta_db_path();
        let conn = Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.get_conn()?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS file_entries (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                slot_id INTEGER NOT NULL,
                blob_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_kind TEXT NOT NULL CHECK(file_kind IN ('meta', 'part', 'tombstone')),
                storage_kind TEXT NOT NULL CHECK(storage_kind IN ('inline', 'external')),
                inline_data BLOB,
                external_path TEXT,
                archive_url TEXT,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                sha256 TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 0,
                etag TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(slot_id, blob_path, file_name)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_entries_head
             ON file_entries(slot_id, blob_path, file_kind, generation DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_entries_part_sha
             ON file_entries(slot_id, file_kind, sha256)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_entries_blob_path
             ON file_entries(slot_id, blob_path)",
            [],
        )?;

        Ok(())
    }

    pub fn next_generation(&self, blob_path: &str) -> Result<i64> {
        let conn = self.get_conn()?;
        let max_generation: Option<i64> = conn
            .query_row(
                "SELECT MAX(generation)
                 FROM file_entries
                 WHERE slot_id = ?1
                   AND blob_path = ?2
                   AND file_kind IN ('meta', 'tombstone')",
                params![self.slot.slot_id as i64, blob_path],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        Ok(max_generation.unwrap_or(0) + 1)
    }

    pub fn upsert_part_entry(&self, blob_path: &str, part: &ChunkRef) -> Result<()> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO file_entries (
                slot_id,
                blob_path,
                file_name,
                file_kind,
                storage_kind,
                inline_data,
                external_path,
                archive_url,
                size_bytes,
                sha256,
                generation,
                etag,
                created_at,
                updated_at
            ) VALUES (?1, ?2, ?3, 'part', 'external', NULL, ?4, ?5, ?6, ?7, 0, NULL, ?8, ?8)
            ON CONFLICT(slot_id, blob_path, file_name) DO UPDATE SET
                external_path = excluded.external_path,
                archive_url = excluded.archive_url,
                size_bytes = excluded.size_bytes,
                sha256 = excluded.sha256,
                updated_at = excluded.updated_at",
            params![
                self.slot.slot_id as i64,
                blob_path,
                part.name,
                part.external_path,
                part.archive_url,
                part.length as i64,
                part.sha256,
                now,
            ],
        )?;

        Ok(())
    }

    pub fn upsert_meta(&self, meta: &BlobMeta) -> Result<bool> {
        let inline_data = serde_json::to_vec(meta)?;
        let head_sha256 = compute_hash(&inline_data);
        self.upsert_meta_with_payload(meta, &inline_data, &head_sha256)
    }

    pub fn upsert_meta_with_payload(
        &self,
        meta: &BlobMeta,
        inline_data: &[u8],
        head_sha256: &str,
    ) -> Result<bool> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();

        let affected = conn.execute(
            "INSERT INTO file_entries (
                slot_id,
                blob_path,
                file_name,
                file_kind,
                storage_kind,
                inline_data,
                external_path,
                archive_url,
                size_bytes,
                sha256,
                generation,
                etag,
                created_at,
                updated_at
            ) VALUES (?1, ?2, 'meta.json', 'meta', 'inline', ?3, NULL, NULL, ?4, ?5, ?6, ?7, ?8, ?8)
            ON CONFLICT(slot_id, blob_path, file_name) DO UPDATE SET
                inline_data = excluded.inline_data,
                size_bytes = excluded.size_bytes,
                sha256 = excluded.sha256,
                generation = excluded.generation,
                etag = excluded.etag,
                updated_at = excluded.updated_at
            WHERE excluded.generation >= file_entries.generation",
            params![
                self.slot.slot_id as i64,
                meta.path,
                inline_data,
                meta.size_bytes as i64,
                head_sha256,
                meta.generation,
                meta.etag,
                now,
            ],
        )?;

        Ok(affected > 0)
    }

    pub fn insert_tombstone(&self, tombstone: &TombstoneMeta) -> Result<String> {
        let inline_data = serde_json::to_vec(tombstone)?;
        let head_sha256 = compute_hash(&inline_data);
        self.insert_tombstone_with_payload(tombstone, &inline_data, &head_sha256)?;
        Ok(head_sha256)
    }

    pub fn insert_tombstone_with_payload(
        &self,
        tombstone: &TombstoneMeta,
        inline_data: &[u8],
        head_sha256: &str,
    ) -> Result<bool> {
        let conn = self.get_conn()?;
        let now = Utc::now().to_rfc3339();
        let file_name = format!("tombstone.{}", head_sha256);

        let affected = conn.execute(
            "INSERT INTO file_entries (
                slot_id,
                blob_path,
                file_name,
                file_kind,
                storage_kind,
                inline_data,
                external_path,
                archive_url,
                size_bytes,
                sha256,
                generation,
                etag,
                created_at,
                updated_at
            ) VALUES (?1, ?2, ?3, 'tombstone', 'inline', ?4, NULL, NULL, ?5, ?6, ?7, NULL, ?8, ?8)
            ON CONFLICT(slot_id, blob_path, file_name) DO UPDATE SET
                inline_data = excluded.inline_data,
                size_bytes = excluded.size_bytes,
                sha256 = excluded.sha256,
                generation = excluded.generation,
                updated_at = excluded.updated_at",
            params![
                self.slot.slot_id as i64,
                tombstone.path,
                file_name,
                inline_data,
                inline_data.len() as i64,
                head_sha256,
                tombstone.generation,
                now,
            ],
        )?;

        Ok(affected > 0)
    }

    pub fn get_current_head(&self, blob_path: &str) -> Result<Option<BlobHead>> {
        let conn = self.get_conn()?;

        let row: Option<HeadRow> = conn
            .query_row(
                "SELECT blob_path, file_kind, generation, sha256, updated_at, inline_data
                 FROM file_entries
                 WHERE slot_id = ?1
                   AND blob_path = ?2
                   AND file_kind IN ('meta', 'tombstone')
                 ORDER BY generation DESC,
                          CASE file_kind WHEN 'tombstone' THEN 1 ELSE 0 END DESC,
                          pk DESC
                 LIMIT 1",
                params![self.slot.slot_id as i64, blob_path],
                |row| {
                    Ok(HeadRow {
                        blob_path: row.get(0)?,
                        file_kind: row.get(1)?,
                        generation: row.get(2)?,
                        sha256: row.get(3)?,
                        updated_at: row.get(4)?,
                        inline_data: row.get(5)?,
                    })
                },
            )
            .optional()?;

        match row {
            Some(row) => self.decode_head_row(row),
            None => Ok(None),
        }
    }

    pub fn list_heads(
        &self,
        prefix: &str,
        limit: usize,
        include_deleted: bool,
        cursor: Option<&str>,
    ) -> Result<Vec<BlobHead>> {
        let conn = self.get_conn()?;
        let pattern = format!("{}%", prefix);

        let sql = if cursor.is_some() {
            "SELECT blob_path, file_kind, generation, sha256, updated_at, inline_data
             FROM file_entries
             WHERE slot_id = ?1
               AND blob_path LIKE ?2
               AND blob_path > ?3
               AND file_kind IN ('meta', 'tombstone')
             ORDER BY blob_path ASC,
                      generation DESC,
                      CASE file_kind WHEN 'tombstone' THEN 1 ELSE 0 END DESC,
                      pk DESC"
        } else {
            "SELECT blob_path, file_kind, generation, sha256, updated_at, inline_data
             FROM file_entries
             WHERE slot_id = ?1
               AND blob_path LIKE ?2
               AND file_kind IN ('meta', 'tombstone')
             ORDER BY blob_path ASC,
                      generation DESC,
                      CASE file_kind WHEN 'tombstone' THEN 1 ELSE 0 END DESC,
                      pk DESC"
        };

        let mut stmt = conn.prepare(sql)?;

        let mut rows = if let Some(c) = cursor {
            stmt.query(params![self.slot.slot_id as i64, pattern, c])?
        } else {
            stmt.query(params![self.slot.slot_id as i64, pattern])?
        };

        let mut selected = Vec::new();
        let mut seen_path: Option<String> = None;

        while let Some(row) = rows.next()? {
            let blob_path: String = row.get(0)?;
            if seen_path.as_deref() == Some(blob_path.as_str()) {
                continue;
            }

            let head_row = HeadRow {
                blob_path: blob_path.clone(),
                file_kind: row.get(1)?,
                generation: row.get(2)?,
                sha256: row.get(3)?,
                updated_at: row.get(4)?,
                inline_data: row.get(5)?,
            };

            if let Some(head) = self.decode_head_row(head_row)? {
                if !include_deleted && head.head_kind == HeadKind::Tombstone {
                    seen_path = Some(blob_path);
                    continue;
                }

                selected.push(head);
                seen_path = Some(blob_path);

                if selected.len() >= limit {
                    break;
                }
            }
        }

        Ok(selected)
    }

    pub fn find_part_external_path(
        &self,
        sha256: &str,
        blob_path: Option<&str>,
    ) -> Result<Option<String>> {
        let conn = self.get_conn()?;

        let sql = if blob_path.is_some() {
            "SELECT external_path
             FROM file_entries
             WHERE slot_id = ?1
               AND file_kind = 'part'
               AND sha256 = ?2
               AND blob_path = ?3
             ORDER BY updated_at DESC
             LIMIT 1"
        } else {
            "SELECT external_path
             FROM file_entries
             WHERE slot_id = ?1
               AND file_kind = 'part'
               AND sha256 = ?2
             ORDER BY updated_at DESC
             LIMIT 1"
        };

        let path: Option<String> = if let Some(blob_path) = blob_path {
            conn.query_row(
                sql,
                params![self.slot.slot_id as i64, sha256, blob_path],
                |row| row.get(0),
            )
            .optional()?
        } else {
            conn.query_row(sql, params![self.slot.slot_id as i64, sha256], |row| {
                row.get(0)
            })
            .optional()?
        };

        Ok(path)
    }

    fn decode_head_row(&self, row: HeadRow) -> Result<Option<BlobHead>> {
        let updated_at = parse_rfc3339(&row.updated_at)?;

        match row.file_kind.as_str() {
            "meta" => {
                let mut meta: BlobMeta = serde_json::from_slice(&row.inline_data)?;
                meta.path = row.blob_path.clone();
                meta.slot_id = self.slot.slot_id;
                if meta.version == 0 {
                    meta.version = meta.generation;
                }

                Ok(Some(BlobHead {
                    path: row.blob_path,
                    generation: row.generation,
                    head_kind: HeadKind::Meta,
                    head_sha256: row.sha256,
                    updated_at,
                    meta: Some(meta),
                    tombstone: None,
                }))
            }
            "tombstone" => {
                let mut tombstone: TombstoneMeta = serde_json::from_slice(&row.inline_data)?;
                tombstone.path = row.blob_path.clone();
                tombstone.slot_id = self.slot.slot_id;

                Ok(Some(BlobHead {
                    path: row.blob_path,
                    generation: row.generation,
                    head_kind: HeadKind::Tombstone,
                    head_sha256: row.sha256,
                    updated_at,
                    meta: None,
                    tombstone: Some(tombstone),
                }))
            }
            other => Err(AmberError::Internal(format!(
                "unexpected head kind in file_entries: {}",
                other
            ))),
        }
    }
}

fn parse_rfc3339(value: &str) -> Result<DateTime<Utc>> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|error| AmberError::Internal(format!("invalid RFC3339 timestamp: {}", error)))?;
    Ok(parsed.with_timezone(&Utc))
}

pub type ObjectMeta = BlobMeta;
pub type ChunkInfo = ChunkRef;
