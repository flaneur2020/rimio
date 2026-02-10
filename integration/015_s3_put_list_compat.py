#!/usr/bin/env python3
"""[015] S3 PutObject/ListObjectsV2 compatibility contract (field coverage + staged unsupported behavior)."""

from __future__ import annotations

import base64
import hashlib
import uuid
from urllib.parse import quote

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from _harness import build_case_parser, cluster_from_args, http_request


def _build_s3_client(endpoint_url: str, region: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id="amberio-it-access",
        aws_secret_access_key="amberio-it-secret",
        config=BotoConfig(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        ),
    )


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _expect_client_error_status(callable_fn, expected_statuses: set[int], context: str) -> ClientError:
    try:
        callable_fn()
    except ClientError as error:
        status = int(error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        if status not in expected_statuses:
            raise AssertionError(
                f"{context}: expected status in {sorted(expected_statuses)}, got {status}, "
                f"error={error.response}"
            )
        return error

    raise AssertionError(f"{context}: expected ClientError with status {sorted(expected_statuses)}")


def _assert_error_code(error: ClientError, expected_codes: set[str], context: str) -> None:
    error_code = str(error.response.get("Error", {}).get("Code", ""))
    if error_code not in expected_codes:
        raise AssertionError(
            f"{context}: expected error code in {sorted(expected_codes)}, got={error_code}, "
            f"full={error.response}"
        )


def _collect_listed_keys(client, *, bucket: str, prefix: str, max_keys: int = 2) -> list[str]:
    listed: list[str] = []
    token: str | None = None

    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": max_keys,
        }
        if token is not None:
            kwargs["ContinuationToken"] = token

        page = client.list_objects_v2(**kwargs)
        for item in page.get("Contents", []):
            key = item.get("Key")
            if isinstance(key, str):
                listed.append(key)

        if not page.get("IsTruncated"):
            return listed

        token = page.get("NextContinuationToken")
        _assert(
            isinstance(token, str) and token,
            f"truncated page missing NextContinuationToken: {page}",
        )


def main() -> None:
    parser = build_case_parser("015", "S3 PutObject/ListObjectsV2 compatibility contract")
    parser.add_argument(
        "--s3-region",
        default="us-east-1",
        help="region name used by SDK client",
    )
    parser.add_argument(
        "--s3-bucket-prefix",
        default="amberio-it-015",
        help="bucket prefix for this case",
    )
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        endpoint = cluster.node_url(0)
        client = _build_s3_client(endpoint, args.s3_region)

        bucket = f"{args.s3_bucket_prefix}-{uuid.uuid4().hex[:8]}"
        shared_prefix = f"cases/015/{uuid.uuid4().hex}/"

        md5_key = f"{shared_prefix}put-md5-ok.txt"
        md5_body = b"amberio-s3-put-md5-015"
        md5_base64 = base64.b64encode(hashlib.md5(md5_body).digest()).decode("ascii")

        put_ok = client.put_object(
            Bucket=bucket,
            Key=md5_key,
            Body=md5_body,
            ContentMD5=md5_base64,
            ContentType="text/plain",
            CacheControl="max-age=60",
            ContentLanguage="en-US",
            ContentDisposition="inline",
            Metadata={"suite": "015", "feature": "put-list-compat"},
        )
        _assert(
            int(put_ok["ResponseMetadata"]["HTTPStatusCode"]) in {200, 201},
            f"put_object with valid ContentMD5 failed: {put_ok}",
        )
        _assert(str(put_ok.get("ETag", "")).strip() != "", f"put_object missing ETag: {put_ok}")

        bad_digest_key = f"{shared_prefix}put-md5-bad.txt"
        bad_digest_response = http_request(
            "PUT",
            f"{endpoint}/{bucket}/{quote(bad_digest_key, safe='/')}",
            body=b"amberio-s3-put-md5-bad-015",
            headers={"Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="},
        )
        _assert(
            bad_digest_response.status == 400,
            (
                "put_object with mismatched Content-MD5 should return 400, "
                f"status={bad_digest_response.status}, body={bad_digest_response.body!r}"
            ),
        )
        _assert(
            (b"<Code>BadDigest</Code>" in bad_digest_response.body)
            or (b"<Code>InvalidDigest</Code>" in bad_digest_response.body),
            f"mismatched Content-MD5 should return BadDigest/InvalidDigest code: {bad_digest_response.body!r}",
        )

        conditional_key = f"{shared_prefix}put-conditional.txt"
        initial = client.put_object(Bucket=bucket, Key=conditional_key, Body=b"v1")
        _assert(
            int(initial["ResponseMetadata"]["HTTPStatusCode"]) in {200, 201},
            f"put_object initial conditional seed failed: {initial}",
        )

        if_none_match_error = _expect_client_error_status(
            lambda: client.put_object(
                Bucket=bucket,
                Key=conditional_key,
                Body=b"v2",
                IfNoneMatch="*",
            ),
            {412},
            "put_object IfNoneMatch=* should fail for existing object",
        )
        _assert_error_code(
            if_none_match_error,
            {"PreconditionFailed", "412"},
            "put_object IfNoneMatch existing object",
        )

        conditional_head = client.head_object(Bucket=bucket, Key=conditional_key)
        etag = str(conditional_head.get("ETag", "")).strip()
        _assert(etag != "", f"head_object missing etag for conditional key: {conditional_head}")

        if_match_ok = client.put_object(
            Bucket=bucket,
            Key=conditional_key,
            Body=b"v3",
            IfMatch=etag,
        )
        _assert(
            int(if_match_ok["ResponseMetadata"]["HTTPStatusCode"]) in {200, 201},
            f"put_object IfMatch should succeed for matching etag: {if_match_ok}",
        )

        _expect_client_error_status(
            lambda: client.put_object(
                Bucket=bucket,
                Key=conditional_key,
                Body=b"v4",
                IfMatch='"definitely-not-match"',
            ),
            {412},
            "put_object IfMatch mismatch should fail",
        )

        if_none_match_create = client.put_object(
            Bucket=bucket,
            Key=f"{shared_prefix}put-if-none-match-create.txt",
            Body=b"created",
            IfNoneMatch="*",
        )
        _assert(
            int(if_none_match_create["ResponseMetadata"]["HTTPStatusCode"]) in {200, 201},
            f"put_object IfNoneMatch=* should succeed for new object: {if_none_match_create}",
        )

        sse_key = f"{shared_prefix}sse-c-not-implemented.txt"
        sse_response = http_request(
            "PUT",
            f"{endpoint}/{bucket}/{quote(sse_key, safe='/')}",
            body=b"amberio-s3-sse-c-015",
            headers={
                "x-amz-server-side-encryption-customer-algorithm": "AES256",
                "x-amz-server-side-encryption-customer-key": "ZmFrZS1rZXk=",
                "x-amz-server-side-encryption-customer-key-md5": "ZmFrZS1tZDU=",
            },
        )
        _assert(
            sse_response.status == 501,
            f"put_object SSE-C should return 501, got status={sse_response.status}, body={sse_response.body!r}",
        )
        _assert(
            b"<Code>NotImplemented</Code>" in sse_response.body,
            f"put_object SSE-C should return NotImplemented code: body={sse_response.body!r}",
        )

        acl_key = f"{shared_prefix}acl-not-implemented.txt"
        acl_response = http_request(
            "PUT",
            f"{endpoint}/{bucket}/{quote(acl_key, safe='/')}",
            body=b"amberio-s3-acl-015",
            headers={"x-amz-acl": "public-read"},
        )
        _assert(
            acl_response.status == 501,
            f"put_object ACL should return 501, got status={acl_response.status}, body={acl_response.body!r}",
        )

        list_prefix = f"{shared_prefix}list/"
        list_objects = {
            f"{list_prefix}a.txt": b"a",
            f"{list_prefix}b.txt": b"b",
            f"{list_prefix}nested/c.txt": b"c",
            f"{list_prefix}nested/deeper/d.txt": b"d",
            f"{list_prefix}nested/e.txt": b"e",
        }
        for key, body in list_objects.items():
            put = client.put_object(Bucket=bucket, Key=key, Body=body)
            _assert(
                int(put["ResponseMetadata"]["HTTPStatusCode"]) in {200, 201},
                f"seed list object put failed for key={key}: {put}",
            )

        listed_keys = _collect_listed_keys(client, bucket=bucket, prefix=list_prefix, max_keys=2)
        _assert(
            set(listed_keys) == set(list_objects.keys()),
            f"list_objects_v2 pagination mismatch: expected={sorted(list_objects.keys())}, got={sorted(listed_keys)}",
        )

        start_after = sorted(list_objects.keys())[0]
        start_after_page = client.list_objects_v2(
            Bucket=bucket,
            Prefix=list_prefix,
            StartAfter=start_after,
            MaxKeys=1000,
        )
        start_after_keys = [
            item.get("Key")
            for item in start_after_page.get("Contents", [])
            if isinstance(item, dict) and isinstance(item.get("Key"), str)
        ]
        _assert(
            start_after not in start_after_keys,
            f"StartAfter result should not include marker key={start_after}: page={start_after_page}",
        )

        delimiter_page = client.list_objects_v2(
            Bucket=bucket,
            Prefix=list_prefix,
            Delimiter="/",
            MaxKeys=1000,
        )
        delimiter_contents = {
            item.get("Key")
            for item in delimiter_page.get("Contents", [])
            if isinstance(item, dict) and isinstance(item.get("Key"), str)
        }
        delimiter_prefixes = {
            item.get("Prefix")
            for item in delimiter_page.get("CommonPrefixes", [])
            if isinstance(item, dict) and isinstance(item.get("Prefix"), str)
        }
        _assert(
            delimiter_contents == {f"{list_prefix}a.txt", f"{list_prefix}b.txt"},
            f"Delimiter list contents mismatch: {delimiter_page}",
        )
        _assert(
            delimiter_prefixes == {f"{list_prefix}nested/"},
            f"Delimiter list common prefixes mismatch: {delimiter_page}",
        )

        fetch_owner_error = _expect_client_error_status(
            lambda: client.list_objects_v2(Bucket=bucket, Prefix=list_prefix, FetchOwner=True),
            {501},
            "ListObjectsV2 FetchOwner should be not implemented",
        )
        _assert_error_code(
            fetch_owner_error,
            {"NotImplemented", "501"},
            "ListObjectsV2 FetchOwner not implemented error code",
        )

        invalid_token_response = http_request(
            "GET",
            f"{endpoint}/{bucket}?list-type=2&prefix={quote(list_prefix, safe='')}&continuation-token=***",
        )
        _assert(
            invalid_token_response.status == 400,
            f"invalid continuation-token should return 400: status={invalid_token_response.status}, body={invalid_token_response.body!r}",
        )

        optional_attr_response = http_request(
            "GET",
            (
                f"{endpoint}/{bucket}?list-type=2&prefix={quote(list_prefix, safe='')}&"
                "optional-object-attributes=RestoreStatus"
            ),
        )
        _assert(
            optional_attr_response.status == 501,
            f"optional-object-attributes should return 501: status={optional_attr_response.status}, body={optional_attr_response.body!r}",
        )

    print("[015] PASS")


if __name__ == "__main__":
    main()
