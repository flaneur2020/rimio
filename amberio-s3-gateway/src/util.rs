use crate::{ByteRange, ListObjectItem, S3Error, S3GatewayResult};
use axum::http::{HeaderMap, header};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

pub(crate) fn parse_range_header(headers: &HeaderMap) -> S3GatewayResult<Option<ByteRange>> {
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

pub(crate) fn decode_continuation_token(token: &str) -> S3GatewayResult<String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token.as_bytes())
        .map_err(|_| S3Error::invalid_argument("invalid continuation-token"))?;

    String::from_utf8(bytes)
        .map_err(|_| S3Error::invalid_argument("invalid continuation-token encoding"))
}

pub(crate) fn quote_etag(raw: &str) -> String {
    if raw.starts_with('"') && raw.ends_with('"') {
        raw.to_string()
    } else {
        format!("\"{}\"", raw)
    }
}

pub(crate) fn xml_escape(input: &str) -> String {
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

pub(crate) fn render_list_objects_v2_xml(
    bucket: &str,
    prefix: &str,
    continuation_token: Option<&str>,
    start_after: Option<&str>,
    delimiter: Option<&str>,
    max_keys: usize,
    items: &[ListObjectItem],
    common_prefixes: &[String],
    is_truncated: bool,
    next_continuation_token: Option<&str>,
    encoding_type: Option<&str>,
) -> String {
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");

    push_tag(&mut xml, "Name", &encode_list_value(bucket, encoding_type));
    push_tag(
        &mut xml,
        "Prefix",
        &encode_list_value(prefix, encoding_type),
    );

    if let Some(delimiter) = delimiter {
        push_tag(
            &mut xml,
            "Delimiter",
            &encode_list_value(delimiter, encoding_type),
        );
    }

    if let Some(start_after) = start_after
        && !start_after.is_empty()
    {
        push_tag(
            &mut xml,
            "StartAfter",
            &encode_list_value(start_after, encoding_type),
        );
    }

    if let Some(continuation_token) = continuation_token
        && !continuation_token.is_empty()
    {
        push_tag(
            &mut xml,
            "ContinuationToken",
            &encode_list_value(continuation_token, encoding_type),
        );
    }

    if let Some(token) = next_continuation_token {
        push_tag(
            &mut xml,
            "NextContinuationToken",
            &encode_list_value(token, encoding_type),
        );
    }

    if let Some(encoding_type) = encoding_type {
        push_tag(
            &mut xml,
            "EncodingType",
            &encode_list_value(encoding_type, None),
        );
    }

    push_tag(&mut xml, "MaxKeys", max_keys.to_string().as_str());

    let key_count = items.len().saturating_add(common_prefixes.len());
    push_tag(&mut xml, "KeyCount", key_count.to_string().as_str());
    push_tag(
        &mut xml,
        "IsTruncated",
        if is_truncated { "true" } else { "false" },
    );

    for item in items {
        xml.push_str("<Contents>");

        push_tag(
            &mut xml,
            "Key",
            &encode_list_value(item.key.as_str(), encoding_type),
        );
        push_tag(&mut xml, "LastModified", item.last_modified.as_str());
        push_tag(&mut xml, "ETag", quote_etag(item.etag.as_str()).as_str());
        push_tag(&mut xml, "Size", item.size_bytes.to_string().as_str());
        push_tag(&mut xml, "StorageClass", "STANDARD");

        xml.push_str("</Contents>");
    }

    for prefix in common_prefixes {
        xml.push_str("<CommonPrefixes>");
        push_tag(
            &mut xml,
            "Prefix",
            &encode_list_value(prefix.as_str(), encoding_type),
        );
        xml.push_str("</CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

fn push_tag(xml: &mut String, name: &str, value: &str) {
    xml.push('<');
    xml.push_str(name);
    xml.push('>');
    xml.push_str(xml_escape(value).as_str());
    xml.push_str("</");
    xml.push_str(name);
    xml.push('>');
}

fn encode_list_value(value: &str, encoding_type: Option<&str>) -> String {
    match encoding_type {
        Some("url") => url_encode(value),
        _ => value.to_string(),
    }
}

fn url_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            encoded.push('%');
            encoded.push(hex_upper((*byte >> 4) & 0x0F));
            encoded.push(hex_upper(*byte & 0x0F));
        }
    }
    encoded
}

fn hex_upper(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'A' + (value - 10)),
        _ => '0',
    }
}
