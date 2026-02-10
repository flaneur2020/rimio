use axum::{
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};

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
