#!/usr/bin/env python3
"""[013] S3 gateway basic protocol contract (put/get/head/list/delete + multipart placeholder)."""

from __future__ import annotations

import uuid

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from _harness import build_case_parser, cluster_from_args


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


def main() -> None:
    parser = build_case_parser("013", "S3 gateway basic contract")
    parser.add_argument(
        "--s3-region",
        default="us-east-1",
        help="region name used by SDK client",
    )
    parser.add_argument(
        "--s3-bucket-prefix",
        default="amberio-it-013",
        help="bucket prefix for this case",
    )
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        endpoint = cluster.node_url(0)
        client = _build_s3_client(endpoint, args.s3_region)

        bucket = f"{args.s3_bucket_prefix}-{uuid.uuid4().hex[:8]}"
        prefix = f"cases/013/{uuid.uuid4().hex}/"

        expected_objects: dict[str, bytes] = {}
        for index in range(5):
            key = f"{prefix}obj-{index}.bin"
            value = f"amberio-s3-case-013-{index}".encode("utf-8")
            expected_objects[key] = value

            put_result = client.put_object(Bucket=bucket, Key=key, Body=value)
            _assert(
                put_result["ResponseMetadata"]["HTTPStatusCode"] in {200, 201},
                f"put_object failed for key={key}: {put_result}",
            )

        sample_key = sorted(expected_objects.keys())[2]
        sample_body = expected_objects[sample_key]

        get_result = client.get_object(Bucket=bucket, Key=sample_key)
        actual_body = get_result["Body"].read()
        _assert(
            actual_body == sample_body,
            f"get_object body mismatch: expected={sample_body!r}, got={actual_body!r}",
        )

        head_result = client.head_object(Bucket=bucket, Key=sample_key)
        _assert(
            head_result.get("ContentLength") == len(sample_body),
            f"head_object content-length mismatch: {head_result}",
        )

        first_page = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=2)
        _assert(
            first_page.get("KeyCount") == 2,
            f"list_objects_v2 first page unexpected KeyCount: {first_page}",
        )
        _assert(
            first_page.get("IsTruncated") is True,
            f"list_objects_v2 first page should be truncated: {first_page}",
        )

        first_keys = [item.get("Key") for item in first_page.get("Contents", []) if isinstance(item, dict)]
        continuation = first_page.get("NextContinuationToken")
        _assert(
            isinstance(continuation, str) and continuation,
            f"list_objects_v2 missing continuation token: {first_page}",
        )

        second_page = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=2,
            ContinuationToken=continuation,
        )
        second_keys = [item.get("Key") for item in second_page.get("Contents", []) if isinstance(item, dict)]

        third_page = None
        third_keys: list[str | None] = []
        if second_page.get("IsTruncated"):
            next_continuation = second_page.get("NextContinuationToken")
            _assert(
                isinstance(next_continuation, str) and next_continuation,
                f"second page truncated but token missing: {second_page}",
            )
            third_page = client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=2,
                ContinuationToken=next_continuation,
            )
            third_keys = [
                item.get("Key") for item in third_page.get("Contents", []) if isinstance(item, dict)
            ]

        listed_keys = [key for key in first_keys + second_keys + third_keys if isinstance(key, str)]
        _assert(
            set(listed_keys) == set(expected_objects.keys()),
            f"listed keys mismatch: expected={sorted(expected_objects.keys())}, got={sorted(listed_keys)}",
        )

        delete_key = sorted(expected_objects.keys())[0]
        delete_result = client.delete_object(Bucket=bucket, Key=delete_key)
        _assert(
            delete_result["ResponseMetadata"]["HTTPStatusCode"] in {200, 204},
            f"delete_object failed: {delete_result}",
        )

        try:
            client.get_object(Bucket=bucket, Key=delete_key)
        except ClientError as error:
            error_code = str(error.response.get("Error", {}).get("Code", ""))
            _assert(
                error_code in {"NoSuchKey", "NotFound", "404"},
                f"unexpected error code after delete: {error_code}, full={error.response}",
            )
        else:
            raise AssertionError("get_object should fail after delete")

        multipart_key = f"{prefix}multipart-placeholder.bin"
        try:
            client.create_multipart_upload(Bucket=bucket, Key=multipart_key)
        except ClientError as error:
            status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            error_code = str(error.response.get("Error", {}).get("Code", ""))
            _assert(
                status_code in {400, 501},
                f"multipart placeholder should fail with 400/501, got: {error.response}",
            )
            _assert(
                error_code in {"NotImplemented", "InvalidRequest", "InvalidArgument"},
                f"unexpected multipart error code: {error.response}",
            )
        else:
            raise AssertionError("multipart upload should be not implemented in current phase")

    print("[013] PASS")


if __name__ == "__main__":
    main()
