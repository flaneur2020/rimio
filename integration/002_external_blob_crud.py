#!/usr/bin/env python3
"""[002] External API contract: PUT/GET/HEAD/LIST/DELETE."""

from __future__ import annotations

import json
import uuid

from _harness import (
    build_case_parser,
    cluster_from_args,
    expect_status,
    parse_json_body,
    quote_blob_path,
)


def main() -> None:
    parser = build_case_parser("002", "External blob CRUD contract")
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        blob_path = f"cases/002/{uuid.uuid4().hex}.txt"
        encoded_path = quote_blob_path(blob_path)
        body = b"amberio-integration-case-002\n"

        # PUT (first write)
        write_id = f"w-{uuid.uuid4()}"
        put_response = cluster.external_request(
            0,
            "PUT",
            f"/blobs/{encoded_path}",
            body=body,
            headers={
                "content-type": "application/octet-stream",
                "x-amberio-write-id": write_id,
            },
        )
        expect_status(put_response.status, {201}, "PUT blob first write")
        put_payload = parse_json_body(put_response)
        generation = put_payload.get("generation")
        if not isinstance(generation, int):
            raise AssertionError(f"PUT payload missing integer generation: {put_payload}")

        # PUT retry with same write_id should be idempotent
        put_retry_response = cluster.external_request(
            0,
            "PUT",
            f"/blobs/{encoded_path}",
            body=body,
            headers={
                "content-type": "application/octet-stream",
                "x-amberio-write-id": write_id,
            },
        )
        expect_status(put_retry_response.status, {200, 201}, "PUT idempotent retry")
        put_retry_payload = parse_json_body(put_retry_response)
        retry_generation = put_retry_payload.get("generation")
        if retry_generation != generation:
            raise AssertionError(
                "Idempotent retry must return same generation; "
                f"first={generation}, retry={retry_generation}"
            )

        # GET blob bytes
        get_response = cluster.external_request(1 % cluster.node_count, "GET", f"/blobs/{encoded_path}")
        expect_status(get_response.status, {200}, "GET blob")
        if get_response.body != body:
            raise AssertionError(
                f"GET body mismatch: expected {body!r}, got {get_response.body!r}"
            )

        # HEAD blob metadata
        head_response = cluster.external_request(2 % cluster.node_count, "HEAD", f"/blobs/{encoded_path}")
        expect_status(head_response.status, {200}, "HEAD blob")
        head_generation = head_response.headers.get("x-amberio-generation")
        if head_generation != str(generation):
            raise AssertionError(
                f"HEAD generation mismatch: expected {generation}, got {head_generation}"
            )

        # LIST by prefix
        list_response = cluster.external_request(0, "GET", "/blobs?prefix=cases/002/&limit=100")
        expect_status(list_response.status, {200}, "LIST blobs")
        list_payload = parse_json_body(list_response)
        items = list_payload.get("items")
        if not isinstance(items, list):
            raise AssertionError(f"LIST payload must include items[]: {list_payload}")

        matched = False
        for item in items:
            if isinstance(item, dict) and item.get("path") == blob_path:
                matched = True
                break
        if not matched:
            raise AssertionError(
                f"LIST did not return expected path {blob_path}. payload={json.dumps(list_payload)}"
            )

        # DELETE
        delete_response = cluster.external_request(
            0,
            "DELETE",
            f"/blobs/{encoded_path}",
            headers={"x-amberio-write-id": f"d-{uuid.uuid4()}"},
        )
        expect_status(delete_response.status, {200, 204}, "DELETE blob")

        # GET after delete
        get_deleted_response = cluster.external_request(0, "GET", f"/blobs/{encoded_path}")
        expect_status(get_deleted_response.status, {404, 410}, "GET deleted blob")

    print("[002] PASS")


if __name__ == "__main__":
    main()
