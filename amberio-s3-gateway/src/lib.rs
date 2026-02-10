use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use std::collections::HashMap;
use std::sync::Arc;

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
}

#[derive(Debug, Clone)]
pub struct PutObjectResponse {
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct GetObjectRequest {
    pub bucket: String,
    pub key: String,
    pub range: Option<ByteRange>,
}

#[derive(Debug, Clone)]
pub struct GetObjectResponse {
    pub body: Bytes,
    pub etag: String,
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

#[derive(Debug, Clone)]
pub struct S3Error {
    status: StatusCode,
    code: String,
    message: String,
}

impl S3Error {
    pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidArgument", message)
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
    }

    pub fn no_such_key(bucket: &str, key: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            format!("object not found: {}/{}", bucket, key),
        )
    }

    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_IMPLEMENTED, "NotImplemented", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", message)
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    fn to_xml(&self) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message></Error>",
            xml_escape(self.code.as_str()),
            xml_escape(self.message.as_str())
        )
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let mut response = Response::new(self.to_xml().into());
        *response.status_mut() = self.status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
        response
    }
}

pub type S3GatewayResult<T> = std::result::Result<T, S3Error>;

async fn get_bucket<B>(
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(backend): State<Arc<B>>,
) -> Response
where
    B: S3GatewayBackend,
{
    match params.get("list-type").map(|value| value.as_str()) {
        Some("2") => list_objects_v2::<B>(bucket, params, backend).await,
        _ => S3Error::invalid_request("only list-type=2 is supported for bucket route")
            .into_response(),
    }
}

async fn list_objects_v2<B>(
    bucket: String,
    params: HashMap<String, String>,
    backend: Arc<B>,
) -> Response
where
    B: S3GatewayBackend,
{
    let prefix = params.get("prefix").cloned().unwrap_or_default();

    let max_keys = match params.get("max-keys") {
        Some(raw) => {
            let parsed = match raw.parse::<usize>() {
                Ok(value) => value,
                Err(_) => {
                    return S3Error::invalid_argument("max-keys must be a positive integer")
                        .into_response();
                }
            };
            parsed.clamp(1, 1000)
        }
        None => 1000,
    };

    let continuation_cursor = match params.get("continuation-token") {
        Some(token) => match decode_continuation_token(token) {
            Ok(cursor) => Some(cursor),
            Err(error) => return error.into_response(),
        },
        None => None,
    };

    let request = ListObjectsV2Request {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        continuation_cursor,
        max_keys,
    };

    let result = match backend.list_objects_v2(request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };

    let continuation_token = params
        .get("continuation-token")
        .map(std::string::String::as_str)
        .unwrap_or_default();

    let next_continuation_token = result
        .next_cursor
        .as_ref()
        .map(|cursor| URL_SAFE_NO_PAD.encode(cursor.as_bytes()));

    let body = render_list_objects_v2_xml(
        bucket.as_str(),
        prefix.as_str(),
        continuation_token,
        max_keys,
        &result.items,
        result.is_truncated,
        next_continuation_token.as_deref(),
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
    body: Bytes,
) -> Response
where
    B: S3GatewayBackend,
{
    if params.contains_key("partNumber") || params.contains_key("uploadId") {
        return S3Error::not_implemented("multipart upload is not implemented yet").into_response();
    }

    let request = PutObjectRequest { bucket, key, body };
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
    State(backend): State<Arc<B>>,
    headers: HeaderMap,
) -> Response
where
    B: S3GatewayBackend,
{
    let range = match parse_range_header(&headers) {
        Ok(range) => range,
        Err(error) => return error.into_response(),
    };

    let request = GetObjectRequest { bucket, key, range };

    let result = match backend.get_object(request).await {
        Ok(result) => result,
        Err(error) => return error.into_response(),
    };

    let body_len = result.body.len();
    let mut response = Response::new(result.body.into());
    *response.status_mut() = if result.body_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );

    if let Ok(value) = HeaderValue::from_str(&body_len.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }

    if let Ok(value) = HeaderValue::from_str(quote_etag(result.etag.as_str()).as_str()) {
        response.headers_mut().insert(header::ETAG, value);
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

fn parse_range_header(headers: &HeaderMap) -> S3GatewayResult<Option<ByteRange>> {
    let Some(value) = headers.get(header::RANGE) else {
        return Ok(None);
    };

    let value = value
        .to_str()
        .map_err(|_| S3Error::invalid_argument("invalid Range header"))?
        .trim();

    if value.is_empty() {
        return Ok(None);
    }

    let Some(raw) = value.strip_prefix("bytes=") else {
        return Err(S3Error::invalid_argument(
            "only bytes=start-end range format is supported",
        ));
    };

    let mut split = raw.splitn(2, '-');
    let start_raw = split.next().unwrap_or_default().trim();
    let end_raw = split.next().unwrap_or_default().trim();

    if start_raw.is_empty() || end_raw.is_empty() {
        return Err(S3Error::invalid_argument(
            "only explicit bytes=start-end is supported",
        ));
    }

    let start = start_raw
        .parse::<u64>()
        .map_err(|_| S3Error::invalid_argument("invalid range start"))?;
    let end = end_raw
        .parse::<u64>()
        .map_err(|_| S3Error::invalid_argument("invalid range end"))?;

    if start > end {
        return Err(S3Error::invalid_argument(
            "range start must be <= range end",
        ));
    }

    Ok(Some(ByteRange { start, end }))
}

fn decode_continuation_token(token: &str) -> S3GatewayResult<String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token.as_bytes())
        .map_err(|_| S3Error::invalid_argument("invalid continuation-token"))?;

    String::from_utf8(bytes)
        .map_err(|_| S3Error::invalid_argument("invalid continuation-token encoding"))
}

fn quote_etag(raw: &str) -> String {
    if raw.starts_with('"') && raw.ends_with('"') {
        raw.to_string()
    } else {
        format!("\"{}\"", raw)
    }
}

fn xml_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '\'' => escaped.push_str("&apos;"),
            '"' => escaped.push_str("&quot;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn render_list_objects_v2_xml(
    bucket: &str,
    prefix: &str,
    continuation_token: &str,
    max_keys: usize,
    items: &[ListObjectItem],
    is_truncated: bool,
    next_continuation_token: Option<&str>,
) -> String {
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml.push_str("<Name>");
    xml.push_str(xml_escape(bucket).as_str());
    xml.push_str("</Name>");
    xml.push_str("<Prefix>");
    xml.push_str(xml_escape(prefix).as_str());
    xml.push_str("</Prefix>");

    if !continuation_token.is_empty() {
        xml.push_str("<ContinuationToken>");
        xml.push_str(xml_escape(continuation_token).as_str());
        xml.push_str("</ContinuationToken>");
    }

    xml.push_str("<MaxKeys>");
    xml.push_str(max_keys.to_string().as_str());
    xml.push_str("</MaxKeys>");
    xml.push_str("<KeyCount>");
    xml.push_str(items.len().to_string().as_str());
    xml.push_str("</KeyCount>");
    xml.push_str("<IsTruncated>");
    xml.push_str(if is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    if let Some(token) = next_continuation_token {
        xml.push_str("<NextContinuationToken>");
        xml.push_str(xml_escape(token).as_str());
        xml.push_str("</NextContinuationToken>");
    }

    for item in items {
        xml.push_str("<Contents>");

        xml.push_str("<Key>");
        xml.push_str(xml_escape(item.key.as_str()).as_str());
        xml.push_str("</Key>");

        xml.push_str("<LastModified>");
        xml.push_str(xml_escape(item.last_modified.as_str()).as_str());
        xml.push_str("</LastModified>");

        xml.push_str("<ETag>");
        xml.push_str(xml_escape(quote_etag(item.etag.as_str()).as_str()).as_str());
        xml.push_str("</ETag>");

        xml.push_str("<Size>");
        xml.push_str(item.size_bytes.to_string().as_str());
        xml.push_str("</Size>");

        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

#[allow(clippy::missing_const_for_fn)]
pub fn multipart_not_implemented_error() -> S3Error {
    S3Error::not_implemented("multipart upload is not implemented yet")
}
