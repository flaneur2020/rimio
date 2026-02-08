#!/usr/bin/env python3
"""[005] PUT layout contract for RFC 0003.

Contract expectations:
- part filename format: part.{index:08}.{sha256}
- part files are written under generation directory g.{generation}
- meta.json no longer carries parts[] list
- meta.json carries part_count/part_size/part_index_state
"""

from __future__ import annotations

import json
import re
import sqlite3
import uuid

from _harness import build_case_parser, cluster_from_args, expect_status, parse_json_body, quote_blob_path


INDEXED_PART_PATTERN = re.compile(r"^part\.\d{8}\.[0-9a-f]{64}$")
LEGACY_PART_PATTERN = re.compile(r"^part\.[0-9a-f]{64}$")


def main() -> None:
    parser = build_case_parser("005", "RFC003 PUT layout contract")
    parser.set_defaults(nodes=1, min_write_replicas=1)
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        blob_path = f"cases/rfc003/002/{uuid.uuid4().hex}.bin"
        encoded_path = quote_blob_path(blob_path)
        body = b"rfc003-put-layout-contract\n"

        put_response = cluster.external_request(
            0,
            "PUT",
            f"/blobs/{encoded_path}",
            body=body,
            headers={
                "content-type": "application/octet-stream",
                "x-amberio-write-id": f"rfc003-put-{uuid.uuid4()}",
            },
        )
        expect_status(put_response.status, {201}, "PUT blob")
        put_payload = parse_json_body(put_response)

        slot_id = put_payload.get("slot_id")
        if not isinstance(slot_id, int):
            raise AssertionError(f"PUT payload missing slot_id: {put_payload}")

        generation = put_payload.get("generation")
        if not isinstance(generation, int):
            raise AssertionError(f"PUT payload missing generation: {put_payload}")

        blob_root = (
            cluster.nodes[0].data_dir
            / "slots"
            / str(slot_id)
            / "blobs"
            / blob_path
        )
        if not blob_root.exists():
            raise AssertionError(f"blob root directory not found: {blob_root}")

        generation_dir = blob_root / f"g.{generation}"
        if not generation_dir.exists():
            raise AssertionError(f"generation directory not found: {generation_dir}")

        part_files = [path.name for path in generation_dir.iterdir() if path.name.startswith("part.")]
        if not part_files:
            raise AssertionError(f"No part files found under {generation_dir}")

        non_indexed = [name for name in part_files if not INDEXED_PART_PATTERN.match(name)]
        if non_indexed:
            raise AssertionError(
                "RFC003 requires indexed part filenames; non-indexed files found: "
                + ", ".join(sorted(non_indexed))
            )

        legacy_names = [name for name in part_files if LEGACY_PART_PATTERN.match(name)]
        if legacy_names:
            raise AssertionError(
                "RFC003 forbids legacy part.{sha256} naming; found: "
                + ", ".join(sorted(legacy_names))
            )

        db_path = cluster.nodes[0].data_dir / "slots" / str(slot_id) / "meta.sqlite3"
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                """
                SELECT inline_data
                FROM file_entries
                WHERE slot_id = ? AND blob_path = ? AND file_name = 'meta.json'
                ORDER BY generation DESC
                LIMIT 1
                """,
                (slot_id, blob_path),
            ).fetchone()
        finally:
            conn.close()

        if row is None or row[0] is None:
            raise AssertionError("meta.json inline_data not found in file_entries")

        meta = json.loads(row[0])

        if "parts" in meta:
            raise AssertionError("RFC003 expects meta.json without parts[] field")

        for required_key in ("part_count", "part_size", "part_index_state"):
            if required_key not in meta:
                raise AssertionError(
                    f"RFC003 expects {required_key} in meta.json, got keys={sorted(meta.keys())}"
                )

    print("[005] PASS")


if __name__ == "__main__":
    main()
