use crate::error::{AmberError, Result};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;

pub struct ChunkStore {
    base_path: PathBuf,
}

impl ChunkStore {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    /// Store a chunk and return its SHA256 hash
    pub async fn put(&self, data: Bytes) -> Result<String> {
        let hash = compute_hash(&data);
        let chunk_path = self.chunk_path(&hash);

        // Check if chunk already exists (content-addressed)
        if chunk_path.exists() {
            return Ok(hash);
        }

        // Write to temporary file first, then rename for atomicity
        let temp_path = chunk_path.with_extension("tmp");
        let mut file = fs::File::create(&temp_path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_path, &chunk_path).await?;

        tracing::debug!("Stored chunk with hash {}", hash);
        Ok(hash)
    }

    /// Retrieve a chunk by its hash
    pub async fn get(&self, hash: &str) -> Result<Bytes> {
        let chunk_path = self.chunk_path(hash);

        if !chunk_path.exists() {
            return Err(AmberError::ChunkNotFound(hash.to_string()));
        }

        let data = fs::read(&chunk_path).await?;
        Ok(Bytes::from(data))
    }

    /// Check if a chunk exists
    pub fn exists(&self, hash: &str) -> bool {
        self.chunk_path(hash).exists()
    }

    /// Delete a chunk
    pub async fn delete(&self, hash: &str) -> Result<()> {
        let chunk_path = self.chunk_path(hash);
        if chunk_path.exists() {
            fs::remove_file(&chunk_path).await?;
        }
        Ok(())
    }

    fn chunk_path(&self, hash: &str) -> PathBuf {
        // Use first 2 chars as subdirectory to avoid too many files in one dir
        let prefix = &hash[..2.min(hash.len())];
        self.base_path.join(prefix).join(hash)
    }
}

/// Compute SHA256 hash of data
pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Verify that data matches the expected hash
pub fn verify_hash(data: &[u8], expected_hash: &str) -> Result<()> {
    let actual_hash = compute_hash(data);
    if actual_hash != expected_hash {
        return Err(AmberError::HashMismatch {
            expected: expected_hash.to_string(),
            actual: actual_hash,
        });
    }
    Ok(())
}

