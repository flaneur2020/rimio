use crate::error::Result;
use crate::slot_manager::Slot;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub path: String,
    pub size: u64,
    pub chunks: Vec<ChunkInfo>,
    pub seq: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub modified_at: chrono::DateTime<chrono::Utc>,
    pub archived: bool,
    pub archive_location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub hash: String,
    pub size: u64,
    pub offset: u64,
}

pub struct MetadataStore {
    slot: Arc<Slot>,
}

impl MetadataStore {
    pub fn new(slot: Arc<Slot>) -> Result<Self> {
        let store = Self { slot };
        store.init_schema()?;
        Ok(store)
    }

    fn get_conn(&self) -> Result<Connection> {
        let db_path = self.slot.meta_db_path();
        let conn = Connection::open(&db_path)?;
        Ok(conn)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.get_conn()?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS objects (
                path TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                chunks TEXT NOT NULL,
                seq TEXT NOT NULL,
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0,
                archive_location TEXT
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_objects_seq ON objects(seq)",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS slot_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;

        Ok(())
    }

    pub fn put_object(&self, meta: &ObjectMeta) -> Result<()> {
        let conn = self.get_conn()?;
        let chunks_json = serde_json::to_string(&meta.chunks)?;

        conn.execute(
            "INSERT OR REPLACE INTO objects (
                path, size, chunks, seq, created_at, modified_at, archived, archive_location
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                meta.path,
                meta.size as i64,
                chunks_json,
                meta.seq,
                meta.created_at.to_rfc3339(),
                meta.modified_at.to_rfc3339(),
                if meta.archived { 1 } else { 0 },
                meta.archive_location,
            ],
        )?;

        Ok(())
    }

    pub fn get_object(&self, path: &str) -> Result<Option<ObjectMeta>> {
        let conn = self.get_conn()?;

        let row: Option<(i64, String, String, String, String, i64, Option<String>)> = conn
            .query_row(
                "SELECT size, chunks, seq, created_at, modified_at, archived, archive_location
                 FROM objects WHERE path = ?1",
                [path],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;

        match row {
            Some((size, chunks_json, seq, created_at, modified_at, archived, archive_location)) => {
                let chunks: Vec<ChunkInfo> = serde_json::from_str(&chunks_json)?;
                Ok(Some(ObjectMeta {
                    path: path.to_string(),
                    size: size as u64,
                    chunks,
                    seq,
                    created_at: chrono::DateTime::parse_from_rfc3339(&created_at)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                        .with_timezone(&chrono::Utc),
                    modified_at: chrono::DateTime::parse_from_rfc3339(&modified_at)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                        .with_timezone(&chrono::Utc),
                    archived: archived != 0,
                    archive_location,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn delete_object(&self, path: &str) -> Result<bool> {
        let conn = self.get_conn()?;
        let affected = conn.execute("DELETE FROM objects WHERE path = ?1", [path])?;
        Ok(affected > 0)
    }

    pub fn list_objects(&self, prefix: &str, limit: usize) -> Result<Vec<ObjectMeta>> {
        let conn = self.get_conn()?;
        let pattern = format!("{}%", prefix);

        let mut stmt = conn.prepare(
            "SELECT path, size, chunks, seq, created_at, modified_at, archived, archive_location
             FROM objects WHERE path LIKE ?1 LIMIT ?2",
        )?;

        let rows = stmt.query_map([pattern, limit.to_string()], |row| {
            let path: String = row.get(0)?;
            let size: i64 = row.get(1)?;
            let chunks_json: String = row.get(2)?;
            let seq: String = row.get(3)?;
            let created_at: String = row.get(4)?;
            let modified_at: String = row.get(5)?;
            let archived: i64 = row.get(6)?;
            let archive_location: Option<String> = row.get(7)?;

            let chunks: Vec<ChunkInfo> = serde_json::from_str(&chunks_json)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

            Ok(ObjectMeta {
                path,
                size: size as u64,
                chunks,
                seq,
                created_at: chrono::DateTime::parse_from_rfc3339(&created_at)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .with_timezone(&chrono::Utc),
                modified_at: chrono::DateTime::parse_from_rfc3339(&modified_at)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                    .with_timezone(&chrono::Utc),
                archived: archived != 0,
                archive_location,
            })
        })?;

        let mut objects = Vec::new();
        for row in rows {
            objects.push(row?);
        }

        Ok(objects)
    }

    pub fn get_latest_seq(&self) -> Result<Ulid> {
        let conn = self.get_conn()?;

        let seq_str: Option<String> = conn
            .query_row(
                "SELECT seq FROM objects ORDER BY seq DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;

        match seq_str {
            Some(s) => Ulid::from_string(&s)
                .map_err(|e| crate::error::AmberError::Internal(e.to_string())),
            None => Ok(Ulid::nil()),
        }
    }
}
