use crate::S3GatewayResult;
use async_trait::async_trait;
use axum::body::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone)]
pub struct PutObjectRequest {
    pub bucket: String,
    pub key: String,
    pub body: Bytes,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_length: Option<u64>,
    pub content_md5: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub metadata: HashMap<String, String>,
    pub acl: Option<String>,
    pub checksum_algorithm: Option<String>,
    pub expected_bucket_owner: Option<String>,
    pub request_payer: Option<String>,
    pub server_side_encryption: Option<String>,
    pub ssekms_key_id: Option<String>,
    pub ssekms_encryption_context: Option<String>,
    pub sse_customer_algorithm: Option<String>,
    pub sse_customer_key: Option<String>,
    pub sse_customer_key_md5: Option<String>,
    pub storage_class: Option<String>,
    pub tagging: Option<String>,
    pub website_redirect_location: Option<String>,
    pub object_lock_mode: Option<String>,
    pub object_lock_retain_until_date: Option<String>,
    pub object_lock_legal_hold_status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PutObjectResponse {
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct GetObjectRequest {
    pub bucket: String,
    pub checksum_mode: Option<String>,
    pub expected_bucket_owner: Option<String>,
    pub if_match: Option<String>,
    pub if_modified_since: Option<String>,
    pub if_none_match: Option<String>,
    pub if_unmodified_since: Option<String>,
    pub key: String,
    pub part_number: Option<u32>,
    pub range: Option<ByteRange>,
    pub request_payer: Option<String>,
    pub response_cache_control: Option<String>,
    pub response_content_disposition: Option<String>,
    pub response_content_encoding: Option<String>,
    pub response_content_language: Option<String>,
    pub response_content_type: Option<String>,
    pub response_expires: Option<String>,
    pub sse_customer_algorithm: Option<String>,
    pub sse_customer_key: Option<String>,
    pub sse_customer_key_md5: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetObjectResponse {
    pub body: Bytes,
    pub etag: String,
    pub last_modified: String,
    pub size_bytes: u64,
    pub body_range: Option<ByteRange>,
}

#[derive(Debug, Clone)]
pub struct HeadObjectRequest {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct HeadObjectResponse {
    pub etag: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct DeleteObjectRequest {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct ListObjectsV2Request {
    pub bucket: String,
    pub prefix: String,
    pub continuation_cursor: Option<String>,
    pub max_keys: usize,
    pub delimiter: Option<String>,
    pub encoding_type: Option<String>,
    pub fetch_owner: bool,
    pub start_after: Option<String>,
    pub expected_bucket_owner: Option<String>,
    pub request_payer: Option<String>,
    pub optional_object_attributes: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListObjectItem {
    pub key: String,
    pub etag: String,
    pub size_bytes: u64,
    pub last_modified: String,
}

#[derive(Debug, Clone)]
pub struct ListObjectsV2Response {
    pub items: Vec<ListObjectItem>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_cursor: Option<String>,
}

#[async_trait]
pub trait S3GatewayBackend: Send + Sync + 'static {
    async fn put_object(&self, request: PutObjectRequest) -> S3GatewayResult<PutObjectResponse>;

    async fn get_object(&self, request: GetObjectRequest) -> S3GatewayResult<GetObjectResponse>;

    async fn head_object(&self, request: HeadObjectRequest) -> S3GatewayResult<HeadObjectResponse>;

    async fn delete_object(&self, request: DeleteObjectRequest) -> S3GatewayResult<()>;

    async fn list_objects_v2(
        &self,
        request: ListObjectsV2Request,
    ) -> S3GatewayResult<ListObjectsV2Response>;
}
