#!/usr/bin/env python3
"""[014] S3 GetObject compatibility contract (field coverage + staged unsupported behavior)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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


def main() -> None:
    parser = build_case_parser("014", "S3 GetObject compatibility contract")
    parser.add_argument(
        "--s3-region",
        default="us-east-1",
        help="region name used by SDK client",
    )
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        endpoint = cluster.node_url(0)
        client = _build_s3_client(endpoint, args.s3_region)

        bucket = "amberio-it-014"
        key = "cases/014/get-object-compat.bin"
        body = b"amberio-s3-get-object-compatibility-014"

        put_result = client.put_object(Bucket=bucket, Key=key, Body=body)
        _assert(
            int(put_result["ResponseMetadata"]["HTTPStatusCode"]) in {200, 201},
            f"put_object failed: {put_result}",
        )

        head = client.head_object(Bucket=bucket, Key=key)
        etag = str(head.get("ETag", "")).strip()
        _assert(etag != "", f"head_object missing ETag: {head}")

        baseline = client.get_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
        _assert(
            baseline["Body"].read() == body,
            "baseline get_object with checksum_mode should return full body",
        )

        ranged = client.get_object(Bucket=bucket, Key=key, Range="bytes=5-10")
        _assert(ranged["Body"].read() == body[5:11], "range body mismatch")

        overridden = client.get_object(
            Bucket=bucket,
            Key=key,
            ResponseCacheControl="no-cache",
            ResponseContentDisposition="attachment; filename=get-object-014.bin",
            ResponseContentEncoding="identity",
            ResponseContentLanguage="en-US",
            ResponseContentType="text/plain",
            ResponseExpires=(datetime.now(timezone.utc) + timedelta(days=1)),
        )
        override_headers = {
            key.lower(): value
            for key, value in overridden.get("ResponseMetadata", {}).get("HTTPHeaders", {}).items()
        }
        _assert(
            override_headers.get("cache-control") == "no-cache",
            f"response cache-control override mismatch: {override_headers}",
        )
        _assert(
            override_headers.get("content-disposition") == "attachment; filename=get-object-014.bin",
            f"response content-disposition override mismatch: {override_headers}",
        )
        _assert(
            override_headers.get("content-language") == "en-US",
            f"response content-language override mismatch: {override_headers}",
        )
        _assert(
            override_headers.get("content-type") == "text/plain",
            f"response content-type override mismatch: {override_headers}",
        )

        match_ok = client.get_object(Bucket=bucket, Key=key, IfMatch=etag)
        _assert(match_ok["Body"].read() == body, "IfMatch success path should return full body")

        _expect_client_error_status(
            lambda: client.get_object(Bucket=bucket, Key=key, IfMatch='"definitely-not-match"'),
            {412},
            "IfMatch mismatch",
        )

        _expect_client_error_status(
            lambda: client.get_object(Bucket=bucket, Key=key, IfNoneMatch=etag),
            {304},
            "IfNoneMatch match should be not-modified",
        )

        _expect_client_error_status(
            lambda: client.get_object(
                Bucket=bucket,
                Key=key,
                IfModifiedSince=(datetime.now(timezone.utc) + timedelta(days=1)),
            ),
            {304},
            "IfModifiedSince future should be not-modified",
        )

        _expect_client_error_status(
            lambda: client.get_object(
                Bucket=bucket,
                Key=key,
                IfUnmodifiedSince=(datetime.now(timezone.utc) - timedelta(days=1)),
            ),
            {412},
            "IfUnmodifiedSince past should fail precondition",
        )

        part1 = client.get_object(Bucket=bucket, Key=key, PartNumber=1)
        _assert(part1["Body"].read() == body, "PartNumber=1 should return first part data")

        _expect_client_error_status(
            lambda: client.get_object(Bucket=bucket, Key=key, PartNumber=2),
            {416},
            "PartNumber out-of-range should fail",
        )

        _expect_client_error_status(
            lambda: client.get_object(Bucket=bucket, Key=key, VersionId="v-test"),
            {501},
            "version_id should be not implemented",
        )

        encoded_key = quote(key, safe="/")
        sse_response = http_request(
            "GET",
            f"{endpoint}/{bucket}/{encoded_key}",
            headers={
                "x-amz-server-side-encryption-customer-algorithm": "AES256",
                "x-amz-server-side-encryption-customer-key": "ZmFrZS1rZXk=",
                "x-amz-server-side-encryption-customer-key-md5": "ZmFrZS1tZDU=",
            },
        )
        _assert(
            sse_response.status == 501,
            f"SSE-C get should return 501, got status={sse_response.status}, body={sse_response.body!r}",
        )
        _assert(
            b"<Code>NotImplemented</Code>" in sse_response.body,
            f"SSE-C get should return NotImplemented code, body={sse_response.body!r}",
        )

    print("[014] PASS")


if __name__ == "__main__":
    main()
