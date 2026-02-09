use crate::{AmberError, Result};
use async_trait::async_trait;
use bytes::Bytes;
use redis::AsyncCommands;
use reqwest::Url;

#[derive(Debug, Clone)]
pub struct ArchiveListPage {
    pub entries: Vec<String>,
    pub next_cursor: Option<String>,
}

#[async_trait]
pub trait ArchiveStore: Send + Sync {
    async fn list_blobs(&self, list_key: &str) -> Result<Vec<String>> {
        let mut cursor: Option<String> = None;
        let mut all = Vec::new();

        loop {
            let page = self
                .list_blobs_page(list_key, cursor.as_deref(), 500)
                .await?;
            if page.entries.is_empty() {
                break;
            }

            all.extend(page.entries);

            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        Ok(all)
    }

    async fn list_blobs_page(
        &self,
        list_key: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ArchiveListPage>;

    async fn read_range(&self, object_key: &str, start: u64, end: u64) -> Result<Bytes>;

    async fn write_blob(&self, object_key: &str, body: &[u8]) -> Result<()>;

    fn archive_url_for_key(&self, object_key: &str) -> String;
}

pub struct RedisArchiveStore {
    client: redis::Client,
    base_url: String,
}

impl RedisArchiveStore {
    pub fn new(url: &str) -> Result<Self> {
        let normalized = normalize_base_redis_url(url)?;
        let client = redis::Client::open(normalized.as_str()).map_err(|error| {
            AmberError::Config(format!("archive redis connection config error: {}", error))
        })?;

        Ok(Self {
            client,
            base_url: normalized,
        })
    }
}

#[async_trait]
impl ArchiveStore for RedisArchiveStore {
    async fn list_blobs_page(
        &self,
        list_key: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ArchiveListPage> {
        if limit == 0 {
            return Ok(ArchiveListPage {
                entries: Vec::new(),
                next_cursor: None,
            });
        }

        let start = cursor
            .map(|value| {
                value.parse::<usize>().map_err(|_| {
                    AmberError::InvalidRequest(format!(
                        "invalid archive list cursor '{}': expected numeric offset",
                        value
                    ))
                })
            })
            .transpose()?
            .unwrap_or(0);

        let end = start + (limit - 1);

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| {
                AmberError::Internal(format!("archive redis connection failed: {}", error))
            })?;

        let entries: Vec<String> = conn
            .lrange(list_key, start as isize, end as isize)
            .await
            .map_err(|error| {
                AmberError::Internal(format!("archive redis LRANGE failed: {}", error))
            })?;

        let next_cursor = if entries.len() >= limit {
            Some((start + entries.len()).to_string())
        } else {
            None
        };

        Ok(ArchiveListPage {
            entries,
            next_cursor,
        })
    }

    async fn read_range(&self, object_key: &str, start: u64, end: u64) -> Result<Bytes> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| {
                AmberError::Internal(format!("archive redis connection failed: {}", error))
            })?;

        let start_i64 = i64::try_from(start)
            .map_err(|_| AmberError::Internal(format!("invalid redis range start: {}", start)))?;
        let end_i64 = i64::try_from(end)
            .map_err(|_| AmberError::Internal(format!("invalid redis range end: {}", end)))?;

        let payload: Vec<u8> = redis::cmd("GETRANGE")
            .arg(object_key)
            .arg(start_i64)
            .arg(end_i64)
            .query_async(&mut conn)
            .await
            .map_err(|error| {
                AmberError::Internal(format!("archive redis GETRANGE failed: {}", error))
            })?;

        Ok(Bytes::from(payload))
    }

    async fn write_blob(&self, object_key: &str, body: &[u8]) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| {
                AmberError::Internal(format!("archive redis connection failed: {}", error))
            })?;

        let _: () = conn.set(object_key, body).await.map_err(|error| {
            AmberError::Internal(format!("archive redis SET failed: {}", error))
        })?;

        Ok(())
    }

    fn archive_url_for_key(&self, object_key: &str) -> String {
        let key = object_key.trim_start_matches('/');
        format!("{}/{}", self.base_url.trim_end_matches('/'), key)
    }
}

pub async fn read_archive_range_bytes(archive_url: &str, start: u64, end: u64) -> Result<Bytes> {
    let parsed = Url::parse(archive_url)
        .map_err(|error| AmberError::InvalidRequest(format!("invalid archive_url: {}", error)))?;

    match parsed.scheme() {
        "redis" => {
            let (redis_url, key) = parse_redis_archive_url(&parsed)?;
            let store = RedisArchiveStore::new(redis_url.as_str())?;
            store.read_range(&key, start, end).await
        }
        "s3" => Err(AmberError::Internal(
            "archive_url with s3:// is not implemented yet".to_string(),
        )),
        scheme => Err(AmberError::InvalidRequest(format!(
            "unsupported archive_url scheme: {}",
            scheme
        ))),
    }
}

pub fn parse_redis_archive_url(parsed: &Url) -> Result<(String, String)> {
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

fn normalize_base_redis_url(url: &str) -> Result<String> {
    let trimmed = url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(AmberError::Config(
            "archive redis url cannot be empty".to_string(),
        ));
    }

    let parsed = Url::parse(trimmed)
        .map_err(|error| AmberError::Config(format!("invalid archive redis url: {}", error)))?;

    if parsed.scheme() != "redis" {
        return Err(AmberError::Config(format!(
            "archive redis url must start with redis://, got {}",
            parsed.scheme()
        )));
    }

    Ok(trimmed.to_string())
}
