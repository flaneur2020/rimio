use crate::util::{
    decode_continuation_token, parse_range_header, quote_etag, render_list_objects_v2_xml,
};
use crate::{
    DeleteObjectRequest, GetObjectRequest, HeadObjectRequest, ListObjectsV2Request,
    PutObjectRequest, S3Error, S3GatewayBackend,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::sync::Arc;

pub fn router<B>() -> Router<Arc<B>>
where
    B: S3GatewayBackend,
{
    Router::new().route("/:bucket", get(get_bucket::<B>)).route(
        "/:bucket/*key",
        get(get_object::<B>)
            .head(head_object::<B>)
            .put(put_object::<B>)
            .delete(delete_object::<B>)
            .post(post_object::<B>),
    )
}

async fn get_bucket<B>(
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(backend): State<Arc<B>>,
    headers: HeaderMap,
) -> Response
where
    B: S3GatewayBackend,
{
    match params.get("list-type").map(|value| value.as_str()) {
        Some("2") => list_objects_v2::<B>(bucket, params, headers, backend).await,
        _ => S3Error::invalid_request("only list-type=2 is supported for bucket route")
            .into_response(),
    }
}

async fn list_objects_v2<B>(
    bucket: String,
    params: HashMap<String, String>,
    headers: HeaderMap,
    backend: Arc<B>,
) -> Response
where
    B: S3GatewayBackend,
{
    let request = match build_list_objects_v2_request(bucket.clone(), &params, &headers) {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };

    if request.fetch_owner {
        return S3Error::not_implemented("ListObjectsV2 fetch-owner is not implemented yet")
            .into_response();
    }

    if request.optional_object_attributes.is_some() {
        return S3Error::not_implemented(
            "ListObjectsV2 optional-object-attributes is not implemented yet",
        )
        .into_response();
    }

    let max_keys = request.max_keys;
    let prefix = request.prefix.clone();
    let start_after = request.start_after.clone();
    let delimiter = request.delimiter.clone();
    let encoding_type = request.encoding_type.clone();

    let result = match backend.list_objects_v2(request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };

    let continuation_token = params.get("continuation-token").cloned();

    let next_continuation_token = result
        .next_cursor
        .as_ref()
        .map(|cursor| URL_SAFE_NO_PAD.encode(cursor.as_bytes()));

    let body = render_list_objects_v2_xml(
        bucket.as_str(),
        prefix.as_str(),
        continuation_token.as_deref(),
        start_after.as_deref(),
        delimiter.as_deref(),
        max_keys,
        &result.items,
        &result.common_prefixes,
        result.is_truncated,
        next_continuation_token.as_deref(),
        encoding_type.as_deref(),
    );

    let mut response = Response::new(body.into());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
}

async fn put_object<B>(
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    State(backend): State<Arc<B>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    B: S3GatewayBackend,
{
    if params.contains_key("partNumber") || params.contains_key("uploadId") {
        return S3Error::not_implemented("multipart upload is not implemented yet").into_response();
    }

    let request = match build_put_object_request(bucket, key, &headers, body) {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };

    if request.acl.is_some() {
        return S3Error::not_implemented("PutObject ACL is not implemented yet").into_response();
    }

    if request.checksum_algorithm.is_some() {
        return S3Error::not_implemented(
            "PutObject checksum algorithm is not implemented yet (Content-MD5 only)",
        )
        .into_response();
    }

    if request.tagging.is_some()
        || request.website_redirect_location.is_some()
        || request.object_lock_mode.is_some()
        || request.object_lock_retain_until_date.is_some()
        || request.object_lock_legal_hold_status.is_some()
    {
        return S3Error::not_implemented(
            "PutObject tagging/redirect/object-lock features are not implemented yet",
        )
        .into_response();
    }

    if request.server_side_encryption.is_some()
        || request.ssekms_key_id.is_some()
        || request.ssekms_encryption_context.is_some()
        || request.sse_customer_algorithm.is_some()
        || request.sse_customer_key.is_some()
        || request.sse_customer_key_md5.is_some()
    {
        return S3Error::not_implemented("PutObject encryption options are not implemented yet")
            .into_response();
    }

    if let Some(content_length) = request.content_length
        && content_length != request.body.len() as u64
    {
        return S3Error::invalid_argument("content-length does not match request body size")
            .into_response();
    }

    if let Err(error) = validate_content_md5(request.content_md5.as_deref(), request.body.as_ref())
    {
        return error.into_response();
    }

    let result = match backend.put_object(request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };

    let mut response = Response::new(axum::body::Body::empty());
    *response.status_mut() = StatusCode::OK;

    if let Ok(value) = HeaderValue::from_str(quote_etag(result.etag.as_str()).as_str()) {
        response.headers_mut().insert(header::ETAG, value);
    }

    response
}

async fn get_object<B>(
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    State(backend): State<Arc<B>>,
    headers: HeaderMap,
) -> Response
where
    B: S3GatewayBackend,
{
    let request = match build_get_object_request(bucket, key, &params, &headers) {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };

    if request.version_id.is_some() {
        return S3Error::not_implemented("GetObject versionId is not implemented yet")
            .into_response();
    }

    if request.sse_customer_algorithm.is_some()
        || request.sse_customer_key.is_some()
        || request.sse_customer_key_md5.is_some()
    {
        return S3Error::not_implemented("GetObject SSE-C is not implemented yet").into_response();
    }

    let result = match backend.get_object(request.clone()).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };

    if let Some(if_match) = request.if_match.as_deref()
        && !etag_matches(if_match, result.etag.as_str())
    {
        return s3_status(
            StatusCode::PRECONDITION_FAILED,
            Some(("PreconditionFailed", "if-match precondition failed")),
        );
    }

    if let Some(if_unmodified_since) = request.if_unmodified_since.as_deref() {
        let if_unmodified_since = match parse_http_datetime(if_unmodified_since) {
            Ok(value) => value,
            Err(error) => return error.into_response(),
        };
        let last_modified = match parse_http_datetime(result.last_modified.as_str()) {
            Ok(value) => value,
            Err(error) => return error.into_response(),
        };

        if last_modified > if_unmodified_since {
            return s3_status(
                StatusCode::PRECONDITION_FAILED,
                Some((
                    "PreconditionFailed",
                    "if-unmodified-since precondition failed",
                )),
            );
        }
    }

    if let Some(if_none_match) = request.if_none_match.as_deref()
        && etag_matches(if_none_match, result.etag.as_str())
    {
        return s3_status(StatusCode::NOT_MODIFIED, None);
    }

    if let Some(if_modified_since) = request.if_modified_since.as_deref() {
        let if_modified_since = match parse_http_datetime(if_modified_since) {
            Ok(value) => value,
            Err(error) => return error.into_response(),
        };
        let last_modified = match parse_http_datetime(result.last_modified.as_str()) {
            Ok(value) => value,
            Err(error) => return error.into_response(),
        };

        if last_modified <= if_modified_since {
            return s3_status(StatusCode::NOT_MODIFIED, None);
        }
    }

    let body_len = result.body.len();
    let mut response = Response::new(result.body.into());
    *response.status_mut() = if result.body_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    if let Some(content_type) = request.response_content_type.as_deref() {
        if let Ok(value) = HeaderValue::from_str(content_type) {
            response.headers_mut().insert(header::CONTENT_TYPE, value);
        }
    } else {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
    }

    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    if let Ok(value) = HeaderValue::from_str(&body_len.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }

    if let Ok(value) = HeaderValue::from_str(quote_etag(result.etag.as_str()).as_str()) {
        response.headers_mut().insert(header::ETAG, value);
    }

    if let Ok(value) = HeaderValue::from_str(result.last_modified.as_str()) {
        response.headers_mut().insert(header::LAST_MODIFIED, value);
    }

    if let Some(body_range) = result.body_range {
        let content_range = format!(
            "bytes {}-{}/{}",
            body_range.start, body_range.end, result.size_bytes
        );
        if let Ok(value) = HeaderValue::from_str(content_range.as_str()) {
            response.headers_mut().insert(header::CONTENT_RANGE, value);
        }
    }

    apply_response_header_override(
        response.headers_mut(),
        header::CACHE_CONTROL,
        request.response_cache_control.as_deref(),
    );
    apply_response_header_override(
        response.headers_mut(),
        header::CONTENT_DISPOSITION,
        request.response_content_disposition.as_deref(),
    );
    apply_response_header_override(
        response.headers_mut(),
        header::CONTENT_ENCODING,
        request.response_content_encoding.as_deref(),
    );
    apply_response_header_override(
        response.headers_mut(),
        header::CONTENT_LANGUAGE,
        request.response_content_language.as_deref(),
    );
    apply_response_header_override(
        response.headers_mut(),
        header::EXPIRES,
        request.response_expires.as_deref(),
    );

    response
}

async fn head_object<B>(
    Path((bucket, key)): Path<(String, String)>,
    State(backend): State<Arc<B>>,
) -> Response
where
    B: S3GatewayBackend,
{
    let request = HeadObjectRequest { bucket, key };

    let result = match backend.head_object(request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };

    let mut response = Response::new(axum::body::Body::empty());
    *response.status_mut() = StatusCode::OK;

    if let Ok(value) = HeaderValue::from_str(quote_etag(result.etag.as_str()).as_str()) {
        response.headers_mut().insert(header::ETAG, value);
    }

    if let Ok(value) = HeaderValue::from_str(&result.size_bytes.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }

    response
}

async fn delete_object<B>(
    Path((bucket, key)): Path<(String, String)>,
    State(backend): State<Arc<B>>,
) -> Response
where
    B: S3GatewayBackend,
{
    let request = DeleteObjectRequest { bucket, key };

    match backend.delete_object(request).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

async fn post_object<B>(
    Path((_bucket, _key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Response
where
    B: S3GatewayBackend,
{
    if params.contains_key("uploads")
        || (params.contains_key("uploadId") && !params.contains_key("partNumber"))
    {
        return S3Error::not_implemented("multipart upload is not implemented yet").into_response();
    }

    S3Error::invalid_request("unsupported S3 POST query parameters").into_response()
}

fn build_put_object_request(
    bucket: String,
    key: String,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<PutObjectRequest, S3Error> {
    let content_length = header_string(headers, "content-length")
        .map(|raw| {
            raw.parse::<u64>()
                .map_err(|_| S3Error::invalid_argument("content-length must be an integer"))
        })
        .transpose()?;

    let metadata = headers
        .iter()
        .filter_map(|(name, value)| {
            let suffix = name.as_str().strip_prefix("x-amz-meta-")?;
            if suffix.is_empty() {
                return None;
            }

            let value = value.to_str().ok()?.trim();
            Some((suffix.to_string(), value.to_string()))
        })
        .collect();

    Ok(PutObjectRequest {
        bucket,
        key,
        body,
        cache_control: header_string(headers, "cache-control"),
        content_disposition: header_string(headers, "content-disposition"),
        content_encoding: header_string(headers, "content-encoding"),
        content_language: header_string(headers, "content-language"),
        content_length,
        content_md5: header_string(headers, "content-md5"),
        content_type: header_string(headers, "content-type"),
        expires: header_string(headers, "expires"),
        if_match: header_string(headers, "if-match"),
        if_none_match: header_string(headers, "if-none-match"),
        metadata,
        acl: header_string(headers, "x-amz-acl"),
        checksum_algorithm: header_string(headers, "x-amz-checksum-algorithm"),
        expected_bucket_owner: header_string(headers, "x-amz-expected-bucket-owner"),
        request_payer: header_string(headers, "x-amz-request-payer"),
        server_side_encryption: header_string(headers, "x-amz-server-side-encryption"),
        ssekms_key_id: header_string(headers, "x-amz-server-side-encryption-aws-kms-key-id"),
        ssekms_encryption_context: header_string(headers, "x-amz-server-side-encryption-context"),
        sse_customer_algorithm: header_string(
            headers,
            "x-amz-server-side-encryption-customer-algorithm",
        ),
        sse_customer_key: header_string(headers, "x-amz-server-side-encryption-customer-key"),
        sse_customer_key_md5: header_string(
            headers,
            "x-amz-server-side-encryption-customer-key-md5",
        ),
        storage_class: header_string(headers, "x-amz-storage-class"),
        tagging: header_string(headers, "x-amz-tagging"),
        website_redirect_location: header_string(headers, "x-amz-website-redirect-location"),
        object_lock_mode: header_string(headers, "x-amz-object-lock-mode"),
        object_lock_retain_until_date: header_string(
            headers,
            "x-amz-object-lock-retain-until-date",
        ),
        object_lock_legal_hold_status: header_string(headers, "x-amz-object-lock-legal-hold"),
    })
}

fn build_get_object_request(
    bucket: String,
    key: String,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<GetObjectRequest, S3Error> {
    let range = parse_range_header(headers)?;

    let part_number = params
        .get("partNumber")
        .map(|raw| {
            raw.parse::<u32>()
                .map_err(|_| S3Error::invalid_argument("partNumber must be an integer"))
        })
        .transpose()?;

    if let Some(part_number) = part_number
        && !(1..=10_000).contains(&part_number)
    {
        return Err(S3Error::invalid_argument(
            "partNumber must be between 1 and 10000",
        ));
    }

    if part_number.is_some() && range.is_some() {
        return Err(S3Error::invalid_argument(
            "partNumber cannot be used with Range header",
        ));
    }

    Ok(GetObjectRequest {
        bucket,
        checksum_mode: header_string(headers, "x-amz-checksum-mode"),
        expected_bucket_owner: header_string(headers, "x-amz-expected-bucket-owner"),
        if_match: header_string(headers, "if-match"),
        if_modified_since: header_string(headers, "if-modified-since"),
        if_none_match: header_string(headers, "if-none-match"),
        if_unmodified_since: header_string(headers, "if-unmodified-since"),
        key,
        part_number,
        range,
        request_payer: header_string(headers, "x-amz-request-payer"),
        response_cache_control: params.get("response-cache-control").cloned(),
        response_content_disposition: params.get("response-content-disposition").cloned(),
        response_content_encoding: params.get("response-content-encoding").cloned(),
        response_content_language: params.get("response-content-language").cloned(),
        response_content_type: params.get("response-content-type").cloned(),
        response_expires: params.get("response-expires").cloned(),
        sse_customer_algorithm: header_string(
            headers,
            "x-amz-server-side-encryption-customer-algorithm",
        ),
        sse_customer_key: header_string(headers, "x-amz-server-side-encryption-customer-key"),
        sse_customer_key_md5: header_string(
            headers,
            "x-amz-server-side-encryption-customer-key-md5",
        ),
        version_id: params.get("versionId").cloned(),
    })
}

fn build_list_objects_v2_request(
    bucket: String,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<ListObjectsV2Request, S3Error> {
    let prefix = params.get("prefix").cloned().unwrap_or_default();

    let max_keys = match params.get("max-keys") {
        Some(raw) => {
            let parsed = raw
                .parse::<usize>()
                .map_err(|_| S3Error::invalid_argument("max-keys must be a positive integer"))?;
            parsed.clamp(1, 1000)
        }
        None => 1000,
    };

    let continuation_cursor = match params.get("continuation-token") {
        Some(token) => Some(decode_continuation_token(token)?),
        None => None,
    };

    let delimiter = params
        .get("delimiter")
        .cloned()
        .filter(|value| !value.is_empty());
    if let Some(delimiter) = delimiter.as_deref()
        && delimiter != "/"
    {
        return Err(S3Error::invalid_argument(
            "only delimiter='/' is currently supported",
        ));
    }

    let encoding_type = params.get("encoding-type").cloned();
    if let Some(encoding_type) = encoding_type.as_deref()
        && encoding_type != "url"
    {
        return Err(S3Error::invalid_argument(
            "encoding-type must be 'url' when specified",
        ));
    }

    let fetch_owner = match params.get("fetch-owner") {
        Some(raw) => parse_bool_query(raw, "fetch-owner")?,
        None => false,
    };

    Ok(ListObjectsV2Request {
        bucket,
        prefix,
        continuation_cursor,
        max_keys,
        delimiter,
        encoding_type,
        fetch_owner,
        start_after: params
            .get("start-after")
            .map(|value| value.trim_start_matches('/').to_string())
            .filter(|value| !value.is_empty()),
        expected_bucket_owner: header_string(headers, "x-amz-expected-bucket-owner"),
        request_payer: header_string(headers, "x-amz-request-payer"),
        optional_object_attributes: params.get("optional-object-attributes").cloned(),
    })
}

fn header_string(headers: &HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(std::string::ToString::to_string)
}

fn parse_bool_query(raw: &str, name: &str) -> Result<bool, S3Error> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(S3Error::invalid_argument(format!(
            "{} must be true or false",
            name
        ))),
    }
}

fn validate_content_md5(content_md5: Option<&str>, body: &[u8]) -> Result<(), S3Error> {
    let Some(raw) = content_md5 else {
        return Ok(());
    };

    let decoded = STANDARD.decode(raw.as_bytes()).map_err(|_| {
        S3Error::new(
            StatusCode::BAD_REQUEST,
            "InvalidDigest",
            "invalid Content-MD5",
        )
    })?;

    if decoded.len() != 16 {
        return Err(S3Error::new(
            StatusCode::BAD_REQUEST,
            "InvalidDigest",
            "invalid Content-MD5 length",
        ));
    }

    let actual = Md5::digest(body);
    if actual.as_slice() != decoded.as_slice() {
        return Err(S3Error::new(
            StatusCode::BAD_REQUEST,
            "BadDigest",
            "Content-MD5 mismatch",
        ));
    }

    Ok(())
}

fn apply_response_header_override(
    headers: &mut HeaderMap,
    header_name: header::HeaderName,
    value: Option<&str>,
) {
    if let Some(value) = value
        && let Ok(value) = HeaderValue::from_str(value)
    {
        headers.insert(header_name, value);
    }
}

fn s3_status(status: StatusCode, error: Option<(&str, &str)>) -> Response {
    match error {
        Some((code, message)) => S3Error::new(status, code, message).into_response(),
        None => {
            let mut response = Response::new(axum::body::Body::empty());
            *response.status_mut() = status;
            response
        }
    }
}

fn normalize_etag(raw: &str) -> &str {
    raw.trim().trim_matches('"')
}

fn etag_matches(lhs: &str, rhs: &str) -> bool {
    normalize_etag(lhs) == normalize_etag(rhs)
}

fn parse_http_datetime(value: &str) -> Result<DateTime<Utc>, S3Error> {
    let value = value.trim();

    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Ok(parsed.with_timezone(&Utc));
    }

    if let Ok(parsed) = DateTime::parse_from_rfc2822(value) {
        return Ok(parsed.with_timezone(&Utc));
    }

    Err(S3Error::invalid_argument(format!(
        "invalid datetime value: {}",
        value
    )))
}

#[allow(clippy::missing_const_for_fn)]
pub fn multipart_not_implemented_error() -> S3Error {
    S3Error::not_implemented("multipart upload is not implemented yet")
}
