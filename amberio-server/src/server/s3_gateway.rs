use super::{ServerState, normalize_blob_path, resolve_replica_nodes};
use amberio_core::{
    AmberError, DeleteBlobOperationOutcome, DeleteBlobOperationRequest, ListBlobsOperationRequest,
    PutBlobOperationOutcome, PutBlobOperationRequest, ReadBlobOperationOutcome,
    ReadBlobOperationRequest, slot_for_key,
};
use amberio_s3_gateway::{
    DeleteObjectRequest, GetObjectRequest, GetObjectResponse, HeadObjectRequest,
    HeadObjectResponse, ListObjectItem, ListObjectsV2Request, ListObjectsV2Response,
    PutObjectRequest, PutObjectResponse, S3Error, S3GatewayBackend, S3GatewayResult,
};
use async_trait::async_trait;
use axum::http::StatusCode;
use chrono::SecondsFormat;

fn validate_bucket(bucket: &str) -> S3GatewayResult<String> {
    let trimmed = bucket.trim().trim_matches('/');
    if trimmed.is_empty() {
        return Err(S3Error::invalid_argument("bucket cannot be empty"));
    }

    if trimmed.contains('/') {
        return Err(S3Error::invalid_argument("bucket cannot contain '/'"));
    }

    Ok(trimmed.to_string())
}

fn s3_object_path(bucket: &str, key: &str) -> S3GatewayResult<String> {
    let bucket = validate_bucket(bucket)?;
    let key = key.trim_matches('/');

    if key.is_empty() {
        return Err(S3Error::invalid_argument("object key cannot be empty"));
    }

    let joined = format!("{}/{}", bucket, key);
    normalize_blob_path(joined.as_str())
        .map_err(|error| S3Error::invalid_argument(error.to_string()))
}

fn map_write_error(error: AmberError) -> S3Error {
    match error {
        AmberError::InsufficientReplicas { required, found } => S3Error::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            format!(
                "quorum not reached: required replicas={}, committed replicas={}",
                required, found
            ),
        ),
        AmberError::InvalidRequest(message) => S3Error::invalid_argument(message),
        other => S3Error::internal(other.to_string()),
    }
}

fn map_read_error(error: AmberError) -> S3Error {
    match error {
        AmberError::InvalidRequest(message) => {
            S3Error::new(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange", message)
        }
        other => S3Error::internal(other.to_string()),
    }
}

#[async_trait]
impl S3GatewayBackend for ServerState {
    async fn put_object(&self, request: PutObjectRequest) -> S3GatewayResult<PutObjectResponse> {
        let PutObjectRequest { bucket, key, body } = request;
        let path = s3_object_path(bucket.as_str(), key.as_str())?;
        let slot_id = slot_for_key(&path, self.config.replication.total_slots);
        let replicas = resolve_replica_nodes(self, slot_id)
            .await
            .map_err(|error| S3Error::internal(error.to_string()))?;

        let outcome = self
            .put_blob_operation
            .run(PutBlobOperationRequest {
                path,
                slot_id,
                write_id: format!("s3-put-{}", ulid::Ulid::new()),
                body,
                replicas,
                local_node_id: self.node.node_id().to_string(),
            })
            .await;

        match outcome {
            Ok(PutBlobOperationOutcome::Committed(result)) => {
                Ok(PutObjectResponse { etag: result.etag })
            }
            Ok(PutBlobOperationOutcome::Conflict) => Err(S3Error::new(
                StatusCode::CONFLICT,
                "OperationAborted",
                "meta commit rejected by generation check",
            )),
            Err(error) => Err(map_write_error(error)),
        }
    }

    async fn get_object(&self, request: GetObjectRequest) -> S3GatewayResult<GetObjectResponse> {
        let GetObjectRequest { bucket, key, range } = request;
        let path = s3_object_path(bucket.as_str(), key.as_str())?;
        let slot_id = slot_for_key(&path, self.config.replication.total_slots);

        let replicas = resolve_replica_nodes(self, slot_id)
            .await
            .map_err(|error| S3Error::internal(error.to_string()))?;

        let outcome = self
            .read_blob_operation
            .run(ReadBlobOperationRequest {
                slot_id,
                path: path.clone(),
                replicas,
                local_node_id: self.node.node_id().to_string(),
                include_body: true,
                range: range.map(|range| amberio_core::ReadByteRange {
                    start: range.start,
                    end: range.end,
                }),
            })
            .await;

        match outcome {
            Ok(ReadBlobOperationOutcome::Found(result)) => Ok(GetObjectResponse {
                body: result.body.unwrap_or_default(),
                etag: result.meta.etag,
                size_bytes: result.meta.size_bytes,
                body_range: result
                    .body_range
                    .map(|range| amberio_s3_gateway::ByteRange {
                        start: range.start,
                        end: range.end,
                    }),
            }),
            Ok(ReadBlobOperationOutcome::NotFound) | Ok(ReadBlobOperationOutcome::Deleted) => {
                Err(S3Error::no_such_key(bucket.as_str(), key.as_str()))
            }
            Err(error) => Err(map_read_error(error)),
        }
    }

    async fn head_object(&self, request: HeadObjectRequest) -> S3GatewayResult<HeadObjectResponse> {
        let HeadObjectRequest { bucket, key } = request;
        let path = s3_object_path(bucket.as_str(), key.as_str())?;
        let slot_id = slot_for_key(&path, self.config.replication.total_slots);
        let replicas = resolve_replica_nodes(self, slot_id)
            .await
            .map_err(|error| S3Error::internal(error.to_string()))?;

        let outcome = self
            .read_blob_operation
            .run(ReadBlobOperationRequest {
                slot_id,
                path,
                replicas,
                local_node_id: self.node.node_id().to_string(),
                include_body: false,
                range: None,
            })
            .await;

        match outcome {
            Ok(ReadBlobOperationOutcome::Found(result)) => Ok(HeadObjectResponse {
                etag: result.meta.etag,
                size_bytes: result.meta.size_bytes,
            }),
            Ok(ReadBlobOperationOutcome::NotFound) | Ok(ReadBlobOperationOutcome::Deleted) => {
                Err(S3Error::no_such_key(bucket.as_str(), key.as_str()))
            }
            Err(error) => Err(map_read_error(error)),
        }
    }

    async fn delete_object(&self, request: DeleteObjectRequest) -> S3GatewayResult<()> {
        let DeleteObjectRequest { bucket, key } = request;
        let path = s3_object_path(bucket.as_str(), key.as_str())?;
        let slot_id = slot_for_key(&path, self.config.replication.total_slots);
        let replicas = resolve_replica_nodes(self, slot_id)
            .await
            .map_err(|error| S3Error::internal(error.to_string()))?;

        let outcome = self
            .delete_blob_operation
            .run(DeleteBlobOperationRequest {
                path,
                slot_id,
                write_id: format!("s3-delete-{}", ulid::Ulid::new()),
                replicas,
                local_node_id: self.node.node_id().to_string(),
            })
            .await;

        match outcome {
            Ok(DeleteBlobOperationOutcome::Committed(_))
            | Ok(DeleteBlobOperationOutcome::Conflict) => Ok(()),
            Err(error) => Err(map_write_error(error)),
        }
    }

    async fn list_objects_v2(
        &self,
        request: ListObjectsV2Request,
    ) -> S3GatewayResult<ListObjectsV2Response> {
        let bucket = validate_bucket(request.bucket.as_str())?;
        let bucket_prefix = format!("{}/", bucket);

        if let Some(cursor) = request.continuation_cursor.as_deref()
            && !cursor.starts_with(bucket_prefix.as_str())
        {
            return Err(S3Error::invalid_argument(
                "continuation-token does not belong to requested bucket",
            ));
        }

        let user_prefix = request.prefix.trim_start_matches('/');
        let internal_prefix = if user_prefix.is_empty() {
            bucket_prefix.clone()
        } else {
            format!("{}{}", bucket_prefix, user_prefix)
        };

        let fetch_limit = request.max_keys.saturating_add(1);
        let result = self
            .list_blobs_operation
            .run(ListBlobsOperationRequest {
                prefix: internal_prefix,
                limit: fetch_limit.max(1),
                cursor: request.continuation_cursor,
                include_deleted: false,
            })
            .await
            .map_err(|error| S3Error::internal(error.to_string()))?;

        let mut objects_with_cursor: Vec<(String, ListObjectItem)> = result
            .items
            .into_iter()
            .filter_map(|item| {
                let key = item.path.strip_prefix(bucket_prefix.as_str())?.to_string();
                if key.is_empty() {
                    return None;
                }

                Some((
                    item.path,
                    ListObjectItem {
                        key,
                        etag: item.etag,
                        size_bytes: item.size_bytes,
                        last_modified: item.updated_at.to_rfc3339_opts(SecondsFormat::Millis, true),
                    },
                ))
            })
            .collect();

        let is_truncated = objects_with_cursor.len() > request.max_keys;
        if is_truncated {
            objects_with_cursor.truncate(request.max_keys);
        }

        let next_cursor = if is_truncated {
            objects_with_cursor
                .last()
                .map(|(cursor, _)| cursor.to_string())
        } else {
            None
        };

        let items = objects_with_cursor
            .into_iter()
            .map(|(_, item)| item)
            .collect();

        Ok(ListObjectsV2Response {
            items,
            is_truncated,
            next_cursor,
        })
    }
}
