#!/usr/bin/env python3
"""[003] Internal API contract: slotlet diff + targeted healing."""

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
    parser = build_case_parser("003", "Internal healing flow")
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        if cluster.node_count < 3:
            raise AssertionError("case 003 requires at least 3 nodes")

        lagging_index = 2
        source_index = 0

        blob_path = f"cases/003/{uuid.uuid4().hex}.bin"
        encoded_path = quote_blob_path(blob_path)
        payload = b"integration-case-003-healing\n"

        # 1) Stop one node to create divergence.
        cluster.stop_node(lagging_index)

        # 2) Write while lagging node is offline.
        put_response = cluster.external_request(
            source_index,
            "PUT",
            f"/blobs/{encoded_path}",
            body=payload,
            headers={
                "content-type": "application/octet-stream",
                "x-amberio-write-id": f"w-{uuid.uuid4()}",
            },
        )
        expect_status(put_response.status, {201}, "PUT with one node offline")

        # 3) Start lagging node back.
        cluster.start_node(lagging_index)

        # 4) Resolve slot for target blob.
        resolve_response = cluster.external_request(
            source_index,
            "GET",
            f"/slots/resolve?path={blob_path}",
        )
        expect_status(resolve_response.status, {200}, "resolve slot for healing")
        resolve_payload = parse_json_body(resolve_response)
        slot_id = resolve_payload.get("slot_id")
        if not isinstance(slot_id, int):
            raise AssertionError(f"resolve payload missing slot_id: {resolve_payload}")

        # 5) Compare heal slotlet summary between source and lagging nodes.
        source_slotlets = cluster.internal_request(
            source_index,
            "GET",
            f"/slots/{slot_id}/heal/slotlets?prefix_len=2",
        )
        expect_status(source_slotlets.status, {200}, "source heal slotlets")
        source_payload = parse_json_body(source_slotlets)

        lagging_slotlets = cluster.internal_request(
            lagging_index,
            "GET",
            f"/slots/{slot_id}/heal/slotlets?prefix_len=2",
        )
        expect_status(lagging_slotlets.status, {200}, "lagging heal slotlets")
        lagging_payload = parse_json_body(lagging_slotlets)

        if not isinstance(source_payload.get("slotlets"), list):
            raise AssertionError(f"source slotlets payload invalid: {source_payload}")
        if not isinstance(lagging_payload.get("slotlets"), list):
            raise AssertionError(f"lagging slotlets payload invalid: {lagging_payload}")

        # 6) Targeted repair from source to lagging node.
        repair_request_body = {
            "source_node_id": cluster.node_id(source_index),
            "blob_paths": [blob_path],
            "dry_run": False,
        }
        repair_response = cluster.internal_request(
            lagging_index,
            "POST",
            f"/slots/{slot_id}/heal/repair",
            body=json.dumps(repair_request_body).encode("utf-8"),
            headers={"content-type": "application/json"},
        )
        expect_status(repair_response.status, {200}, "targeted heal repair")
        repair_payload = parse_json_body(repair_response)
        repaired = repair_payload.get("repaired_objects")
        if not isinstance(repaired, int) or repaired < 1:
            raise AssertionError(f"repair payload invalid: {repair_payload}")

        # 7) Verify lagging node can serve repaired object.
        repaired_get = cluster.external_request(lagging_index, "GET", f"/blobs/{encoded_path}")
        expect_status(repaired_get.status, {200}, "GET repaired object")
        if repaired_get.body != payload:
            raise AssertionError(
                f"repaired object body mismatch: expected {payload!r}, got {repaired_get.body!r}"
            )

    print("[003] PASS")


if __name__ == "__main__":
    main()
