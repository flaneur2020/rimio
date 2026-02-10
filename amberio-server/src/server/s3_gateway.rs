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
use std::collections::HashSet;

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

fn normalize_etag(raw: &str) -> &str {
    raw.trim().trim_matches('"')
}

fn etag_matches(lhs: &str, rhs: &str) -> bool {
    normalize_etag(lhs) == normalize_etag(rhs)
}

fn etag_match_any(condition: &str, etag: &str) -> bool {
    condition
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || etag_matches(candidate, etag))
}

fn common_prefix_cursor(bucket_prefix: &str, common_prefix: &str) -> String {
    format!("{}{}\u{10FFFF}", bucket_prefix, common_prefix)
}

enum ListEntryWithCursor {
    Object {
        cursor: String,
        item: ListObjectItem,
    },
    CommonPrefix {
        cursor: String,
        prefix: String,
    },
}

impl ListEntryWithCursor {
    fn cursor(&self) -> &str {
        match self {
            Self::Object { cursor, .. } => cursor.as_str(),
            Self::CommonPrefix { cursor, .. } => cursor.as_str(),
        }
    }
}

#[async_trait]
impl S3GatewayBackend for ServerState {
    async fn put_object(&self, request: PutObjectRequest) -> S3GatewayResult<PutObjectResponse> {
        let PutObjectRequest {
            bucket,
            key,
            body,
            if_match,
            if_none_match,
            ..
        } = request;

        let path = s3_object_path(bucket.as_str(), key.as_str())?;
        let slot_id = slot_for_key(&path, self.config.replication.total_slots);

        let replicas = resolve_replica_nodes(self, slot_id)
            .await
            .map_err(|error| S3Error::internal(error.to_string()))?;

        if if_match.is_some() || if_none_match.is_some() {
            let head_outcome = self
                .read_blob_operation
                .run(ReadBlobOperationRequest {
                    slot_id,
                    path: path.clone(),
                    replicas: replicas.clone(),
                    local_node_id: self.node.node_id().to_string(),
                    include_body: false,
                    range: None,
                })
                .await;

            let existing_etag = match head_outcome {
                Ok(ReadBlobOperationOutcome::Found(head)) => Some(head.meta.etag),
                Ok(ReadBlobOperationOutcome::NotFound) | Ok(ReadBlobOperationOutcome::Deleted) => {
                    None
                }
                Err(error) => return Err(map_read_error(error)),
            };

            if let Some(if_match) = if_match.as_deref() {
                let Some(existing_etag) = existing_etag.as_deref() else {
                    return Err(S3Error::new(
                        StatusCode::PRECONDITION_FAILED,
                        "PreconditionFailed",
                        "if-match precondition failed",
                    ));
                };

                if !etag_match_any(if_match, existing_etag) {
                    return Err(S3Error::new(
                        StatusCode::PRECONDITION_FAILED,
                        "PreconditionFailed",
                        "if-match precondition failed",
                    ));
                }
            }

            if let Some(if_none_match) = if_none_match.as_deref()
                && existing_etag
                    .as_deref()
                    .is_some_and(|etag| etag_match_any(if_none_match, etag))
            {
                return Err(S3Error::new(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "if-none-match precondition failed",
                ));
            }
        }

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
        let GetObjectRequest {
            bucket,
            checksum_mode: _,
            expected_bucket_owner: _,
            if_match: _,
            if_modified_since: _,
            if_none_match: _,
            if_unmodified_since: _,
            key,
            part_number,
            range,
            request_payer: _,
            response_cache_control: _,
            response_content_disposition: _,
            response_content_encoding: _,
            response_content_language: _,
            response_content_type: _,
            response_expires: _,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            version_id,
        } = request;

        if version_id.is_some() {
            return Err(S3Error::not_implemented(
                "GetObject versionId is not implemented yet",
            ));
        }

        if sse_customer_algorithm.is_some()
            || sse_customer_key.is_some()
            || sse_customer_key_md5.is_some()
        {
            return Err(S3Error::not_implemented(
                "GetObject SSE-C is not implemented yet",
            ));
        }

        if part_number.is_some() && range.is_some() {
            return Err(S3Error::invalid_argument(
                "partNumber cannot be used with Range header",
            ));
        }

        let path = s3_object_path(bucket.as_str(), key.as_str())?;
        let slot_id = slot_for_key(&path, self.config.replication.total_slots);

        let replicas = resolve_replica_nodes(self, slot_id)
            .await
            .map_err(|error| S3Error::internal(error.to_string()))?;

        let effective_range = if let Some(part_number) = part_number {
            if !(1..=10_000).contains(&part_number) {
                return Err(S3Error::invalid_argument(
                    "partNumber must be between 1 and 10000",
                ));
            }

            let head_outcome = self
                .read_blob_operation
                .run(ReadBlobOperationRequest {
                    slot_id,
                    path: path.clone(),
                    replicas: replicas.clone(),
                    local_node_id: self.node.node_id().to_string(),
                    include_body: false,
                    range: None,
                })
                .await;

            let head = match head_outcome {
                Ok(ReadBlobOperationOutcome::Found(head)) => head,
                Ok(ReadBlobOperationOutcome::NotFound) | Ok(ReadBlobOperationOutcome::Deleted) => {
                    return Err(S3Error::no_such_key(bucket.as_str(), key.as_str()));
                }
                Err(error) => return Err(map_read_error(error)),
            };

            let part_size = head.meta.part_size.max(1);
            let start = (part_number as u64 - 1) * part_size;
            if start >= head.meta.size_bytes {
                return Err(S3Error::new(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "InvalidRange",
                    "partNumber out of range",
                ));
            }

            let end = (start + part_size - 1).min(head.meta.size_bytes.saturating_sub(1));

            Some(amberio_core::ReadByteRange { start, end })
        } else {
            range.map(|range| amberio_core::ReadByteRange {
                start: range.start,
                end: range.end,
            })
        };

        let outcome = self
            .read_blob_operation
            .run(ReadBlobOperationRequest {
                slot_id,
                path: path.clone(),
                replicas,
                local_node_id: self.node.node_id().to_string(),
                include_body: true,
                range: effective_range,
            })
            .await;

        match outcome {
            Ok(ReadBlobOperationOutcome::Found(result)) => Ok(GetObjectResponse {
                body: result.body.unwrap_or_default(),
                etag: result.meta.etag,
                last_modified: result.meta.updated_at.to_rfc2822(),
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
        let ListObjectsV2Request {
            bucket,
            prefix,
            continuation_cursor,
            max_keys,
            delimiter,
            encoding_type: _,
            fetch_owner: _,
            start_after,
            expected_bucket_owner: _,
            request_payer: _,
            optional_object_attributes: _,
        } = request;

        let bucket = validate_bucket(bucket.as_str())?;
        let bucket_prefix = format!("{}/", bucket);

        let effective_cursor = match continuation_cursor {
            Some(cursor) => Some(cursor),
            None => start_after.as_deref().map(|start_after| {
                format!("{}{}", bucket_prefix, start_after.trim_start_matches('/'))
            }),
        };

        if let Some(cursor) = effective_cursor.as_deref()
            && !cursor.starts_with(bucket_prefix.as_str())
        {
            return Err(S3Error::invalid_argument(
                "continuation-token does not belong to requested bucket",
            ));
        }

        let user_prefix = prefix.trim_start_matches('/');
        let internal_prefix = if user_prefix.is_empty() {
            bucket_prefix.clone()
        } else {
            format!("{}{}", bucket_prefix, user_prefix)
        };

        if delimiter.is_none() {
            let fetch_limit = max_keys.saturating_add(1);
            let result = self
                .list_blobs_operation
                .run(ListBlobsOperationRequest {
                    prefix: internal_prefix,
                    limit: fetch_limit.max(1),
                    cursor: effective_cursor,
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
                            last_modified: item
                                .updated_at
                                .to_rfc3339_opts(SecondsFormat::Millis, true),
                        },
                    ))
                })
                .collect();

            let is_truncated = objects_with_cursor.len() > max_keys;
            if is_truncated {
                objects_with_cursor.truncate(max_keys);
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

            return Ok(ListObjectsV2Response {
                items,
                common_prefixes: Vec::new(),
                is_truncated,
                next_cursor,
            });
        }

        let delimiter = delimiter.unwrap_or_default();
        let mut emitted: Vec<ListEntryWithCursor> = Vec::new();
        let mut seen_common_prefixes: HashSet<String> = HashSet::new();
        let mut scan_cursor = effective_cursor;

        let target_count = max_keys.saturating_add(1);
        let batch_limit = max_keys.saturating_mul(4).clamp(32, 1000).max(1);

        while emitted.len() < target_count {
            let batch = self
                .list_blobs_operation
                .run(ListBlobsOperationRequest {
                    prefix: internal_prefix.clone(),
                    limit: batch_limit,
                    cursor: scan_cursor.clone(),
                    include_deleted: false,
                })
                .await
                .map_err(|error| S3Error::internal(error.to_string()))?;

            let batch_len = batch.items.len();
            if batch_len == 0 {
                break;
            }

            for item in batch.items {
                let path = item.path.clone();
                scan_cursor = Some(path.clone());

                let Some(key) = item
                    .path
                    .strip_prefix(bucket_prefix.as_str())
                    .map(str::to_string)
                else {
                    continue;
                };

                if key.is_empty() {
                    continue;
                }

                let suffix = key.strip_prefix(user_prefix).unwrap_or(key.as_str());
                if let Some(position) = suffix.find(delimiter.as_str()) {
                    let common_prefix =
                        format!("{}{}", user_prefix, &suffix[..position + delimiter.len()]);
                    if seen_common_prefixes.insert(common_prefix.clone()) {
                        emitted.push(ListEntryWithCursor::CommonPrefix {
                            cursor: common_prefix_cursor(
                                bucket_prefix.as_str(),
                                common_prefix.as_str(),
                            ),
                            prefix: common_prefix,
                        });
                    }
                } else {
                    emitted.push(ListEntryWithCursor::Object {
                        cursor: path,
                        item: ListObjectItem {
                            key,
                            etag: item.etag,
                            size_bytes: item.size_bytes,
                            last_modified: item
                                .updated_at
                                .to_rfc3339_opts(SecondsFormat::Millis, true),
                        },
                    });
                }

                if emitted.len() >= target_count {
                    break;
                }
            }

            if emitted.len() >= target_count {
                break;
            }

            if batch_len < batch_limit {
                break;
            }
        }

        let is_truncated = emitted.len() > max_keys;
        if is_truncated {
            emitted.truncate(max_keys);
        }

        let next_cursor = if is_truncated {
            emitted.last().map(|entry| entry.cursor().to_string())
        } else {
            None
        };

        let mut items = Vec::new();
        let mut common_prefixes = Vec::new();

        for entry in emitted {
            match entry {
                ListEntryWithCursor::Object { item, .. } => items.push(item),
                ListEntryWithCursor::CommonPrefix { prefix, .. } => common_prefixes.push(prefix),
            }
        }

        Ok(ListObjectsV2Response {
            items,
            common_prefixes,
            is_truncated,
            next_cursor,
        })
    }
}
