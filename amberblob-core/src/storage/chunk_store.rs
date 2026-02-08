use crate::error::{AmberError, Result};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub struct PutPartResult {
    pub part_path: PathBuf,
    pub reused: bool,
}

/// ChunkStore stores external blob data as `part.{sha256}` files:
/// `slots/{slot_id}/blobs/{blob_path}/part.{sha256}`.
pub struct ChunkStore {
    base_path: PathBuf,
}

impl ChunkStore {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    pub async fn put_part(
        &self,
        slot_id: u16,
        blob_path: &str,
        sha256: &str,
        data: Bytes,
    ) -> Result<PutPartResult> {
        verify_hash(&data, sha256)?;

        let part_path = self.part_path(slot_id, blob_path, sha256)?;
        if let Some(parent) = part_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if part_path.exists() {
            return Ok(PutPartResult {
                part_path,
                reused: true,
            });
        }

        let tmp_path = part_path.with_extension(format!("{}.tmp", ulid::Ulid::new()));
        let mut file = fs::File::create(&tmp_path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&tmp_path, &part_path).await?;

        Ok(PutPartResult {
            part_path,
            reused: false,
        })
    }

    pub async fn get_part(&self, slot_id: u16, blob_path: &str, sha256: &str) -> Result<Bytes> {
        let part_path = self.part_path(slot_id, blob_path, sha256)?;
        if !part_path.exists() {
            return Err(AmberError::ChunkNotFound(sha256.to_string()));
        }

        let bytes = fs::read(part_path).await?;
        Ok(Bytes::from(bytes))
    }

    pub fn part_exists(&self, slot_id: u16, blob_path: &str, sha256: &str) -> bool {
        self.part_path(slot_id, blob_path, sha256)
            .map(|path| path.exists())
            .unwrap_or(false)
    }

    pub async fn delete_blob_parts(&self, slot_id: u16, blob_path: &str) -> Result<()> {
        let blob_dir = self.blob_dir(slot_id, blob_path)?;
        if blob_dir.exists() {
            fs::remove_dir_all(blob_dir).await?;
        }
        Ok(())
    }

    pub fn part_path(&self, slot_id: u16, blob_path: &str, sha256: &str) -> Result<PathBuf> {
        let file_name = format!("part.{}", sha256);
        Ok(self.blob_dir(slot_id, blob_path)?.join(file_name))
    }

    pub fn blob_dir(&self, slot_id: u16, blob_path: &str) -> Result<PathBuf> {
        let mut path = self
            .base_path
            .join("slots")
            .join(slot_id.to_string())
            .join("blobs");
        for component in normalize_blob_path(blob_path)?.split('/') {
            path.push(component);
        }
        Ok(path)
    }
}

fn normalize_blob_path(input: &str) -> Result<String> {
    let trimmed = input.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AmberError::InvalidRequest(
            "blob path cannot be empty".to_string(),
        ));
    }

    let mut parts = Vec::new();
    for part in trimmed.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(AmberError::InvalidRequest(format!(
                "invalid blob path component: {}",
                part
            )));
        }
        parts.push(part);
    }

    Ok(parts.join("/"))
}

/// Compute SHA256 hash of data.
pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Verify data hash.
pub fn verify_hash(data: &[u8], expected_hash: &str) -> Result<()> {
    let actual = compute_hash(data);
    if actual != expected_hash {
        return Err(AmberError::HashMismatch {
            expected: expected_hash.to_string(),
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_part_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf()).unwrap();

        let slot_id = 7;
        let blob_path = "a/b/c.txt";
        let body = Bytes::from("hello-world");
        let sha = compute_hash(&body);

        let put = store
            .put_part(slot_id, blob_path, &sha, body.clone())
            .await
            .unwrap();
        assert!(!put.reused);
        assert!(put.part_path.exists());

        let read = store.get_part(slot_id, blob_path, &sha).await.unwrap();
        assert_eq!(read, body);

        let reused = store
            .put_part(slot_id, blob_path, &sha, body.clone())
            .await
            .unwrap();
        assert!(reused.reused);

        assert!(store.part_exists(slot_id, blob_path, &sha));
        store.delete_blob_parts(slot_id, blob_path).await.unwrap();
        assert!(!store.part_exists(slot_id, blob_path, &sha));
    }
}
