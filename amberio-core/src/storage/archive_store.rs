use crate::{AmberError, Result};
use async_trait::async_trait;
use redis::AsyncCommands;

#[async_trait]
pub trait ArchiveStore: Send + Sync {
    async fn list_blobs(&self, list_key: &str) -> Result<Vec<String>>;
}

pub struct RedisArchiveStore {
    client: redis::Client,
}

impl RedisArchiveStore {
    pub fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|error| {
            AmberError::Config(format!("archive redis connection config error: {}", error))
        })?;

        Ok(Self { client })
    }
}

#[async_trait]
impl ArchiveStore for RedisArchiveStore {
    async fn list_blobs(&self, list_key: &str) -> Result<Vec<String>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| {
                AmberError::Internal(format!("archive redis connection failed: {}", error))
            })?;

        conn.lrange(list_key, 0, -1).await.map_err(|error| {
            AmberError::Internal(format!("archive redis LRANGE failed: {}", error))
        })
    }
}
