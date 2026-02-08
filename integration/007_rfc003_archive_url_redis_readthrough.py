#!/usr/bin/env python3
"""[007] archive_url(redis://) fallback + read-through contract for RFC003."""

from __future__ import annotations

import datetime
import hashlib
import json
import math
import socket
import sqlite3
import uuid
from pathlib import Path

from _harness import build_case_parser, cluster_from_args, expect_status, parse_json_body, quote_blob_path


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def utc_now_rfc3339() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def send_redis_command(host: str, port: int, *parts: bytes) -> bytes:
    payload = [f"*{len(parts)}\r\n".encode("utf-8")]
    for part in parts:
        payload.append(f"${len(part)}\r\n".encode("utf-8"))
        payload.append(part)
        payload.append(b"\r\n")

    with socket.create_connection((host, port), timeout=2.0) as connection:
        connection.sendall(b"".join(payload))
        return read_redis_reply(connection)


def read_line(connection: socket.socket) -> bytes:
    chunks = []
    while True:
        char = connection.recv(1)
        if not char:
            raise RuntimeError("redis connection closed while reading response")
        chunks.append(char)
        if len(chunks) >= 2 and chunks[-2:] == [b"\r", b"\n"]:
            return b"".join(chunks[:-2])


def read_exact(connection: socket.socket, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining > 0:
        data = connection.recv(remaining)
        if not data:
            raise RuntimeError("redis connection closed while reading payload")
        chunks.append(data)
        remaining -= len(data)
    return b"".join(chunks)


def read_redis_reply(connection: socket.socket) -> bytes:
    prefix = read_exact(connection, 1)
    if prefix == b"+":
        return read_line(connection)
    if prefix == b"-":
        error = read_line(connection).decode("utf-8", errors="replace")
        raise RuntimeError(f"redis error reply: {error}")
    if prefix == b":":
        return read_line(connection)
    if prefix == b"$":
        length = int(read_line(connection))
        if length == -1:
            return b""
        data = read_exact(connection, length)
        trailer = read_exact(connection, 2)
        if trailer != b"\r\n":
            raise RuntimeError("invalid redis bulk string trailer")
        return data
    raise RuntimeError(f"unsupported redis reply prefix: {prefix!r}")


def redis_set_binary(url: str, key: str, value: bytes) -> None:
    if not url.startswith("redis://"):
        raise AssertionError(f"unexpected redis url: {url}")

    host_port = url.removeprefix("redis://").split("/", 1)[0]
    host, _, port_text = host_port.partition(":")
    port = int(port_text or "6379")

    _ = send_redis_command(host, port, b"SET", key.encode("utf-8"), value)


def redis_del_key(url: str, key: str) -> None:
    host_port = url.removeprefix("redis://").split("/", 1)[0]
    host, _, port_text = host_port.partition(":")
    port = int(port_text or "6379")
    _ = send_redis_command(host, port, b"DEL", key.encode("utf-8"))


def ensure_schema_has_part_no(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("PRAGMA table_info(file_entries)").fetchall()
        return {row[1] for row in rows}
    finally:
        conn.close()


def insert_file_entry(db_path: Path, values: dict[str, object]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(file_entries)").fetchall()}
        present_keys = [key for key in values.keys() if key in columns]
        sql = (
            "INSERT OR REPLACE INTO file_entries ("
            + ",".join(present_keys)
            + ") VALUES ("
            + ",".join("?" for _ in present_keys)
            + ")"
        )
        conn.execute(sql, [values[key] for key in present_keys])
        conn.commit()
    finally:
        conn.close()


def list_part_rows(db_path: Path, slot_id: int, blob_path: str, generation: int) -> list[tuple[int, str, str]]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT part_no, sha256, external_path
            FROM file_entries
            WHERE slot_id = ?
              AND blob_path = ?
              AND file_kind = 'part'
              AND generation = ?
            ORDER BY part_no ASC
            """,
            (slot_id, blob_path, generation),
        ).fetchall()
        return [(int(row[0]), row[1], row[2]) for row in rows]
    finally:
        conn.close()


def main() -> None:
    parser = build_case_parser("007", "RFC003 archive_url redis read-through")
    parser.set_defaults(nodes=1, min_write_replicas=1)
    parser.add_argument("--archive-redis-url", default="redis://127.0.0.1:6379")
    args = parser.parse_args()

    with cluster_from_args(args) as cluster:
        blob_path = f"cases/rfc003/007/{uuid.uuid4().hex}.bin"
        encoded_path = quote_blob_path(blob_path)

        resolve_response = cluster.external_request(
            0,
            "GET",
            f"/slots/resolve?path={blob_path}",
        )
        expect_status(resolve_response.status, {200}, "resolve slot")
        resolve_payload = parse_json_body(resolve_response)
        slot_id = resolve_payload.get("slot_id")
        if not isinstance(slot_id, int):
            raise AssertionError(f"resolve payload missing slot_id: {resolve_payload}")

        _ = cluster.internal_request(
            0,
            "GET",
            f"/slots/{slot_id}/heads?path={encoded_path}",
        )

        slot_root = cluster.nodes[0].data_dir / "slots" / str(slot_id)
        db_path = slot_root / "meta.sqlite3"
        if not db_path.exists():
            raise AssertionError(f"meta.sqlite3 not found: {db_path}")

        columns = ensure_schema_has_part_no(db_path)
        if "part_no" not in columns:
            raise AssertionError("RFC003 contract requires file_entries.part_no column")

        generation = 1
        part_size = 256 * 1024
        body = (b"rfc003-archive-url-readthrough-" * 60000)[:900_000]
        part_count = math.ceil(len(body) / part_size)
        etag = sha256_hex(body)

        archive_key = f"amberio:archive:{uuid.uuid4().hex}"
        archive_url = f"{args.archive_redis_url}/{archive_key}"
        redis_set_binary(args.archive_redis_url, archive_key, body)

        now = utc_now_rfc3339()
        meta = {
            "path": blob_path,
            "slot_id": slot_id,
            "generation": generation,
            "version": generation,
            "size_bytes": len(body),
            "etag": etag,
            "part_size": part_size,
            "part_count": part_count,
            "part_index_state": "none",
            "archive_url": archive_url,
            "updated_at": now,
        }
        inline_data = json.dumps(meta, separators=(",", ":")).encode("utf-8")
        head_sha = sha256_hex(inline_data)

        insert_file_entry(
            db_path,
            {
                "slot_id": slot_id,
                "blob_path": blob_path,
                "file_name": "meta.json",
                "file_kind": "meta",
                "storage_kind": "inline",
                "inline_data": inline_data,
                "external_path": None,
                "archive_url": None,
                "size_bytes": len(body),
                "sha256": head_sha,
                "generation": generation,
                "etag": etag,
                "created_at": now,
                "updated_at": now,
            },
        )

        range_start = 111_111
        range_end = 711_111

        first_range = cluster.external_request(
            0,
            "GET",
            f"/blobs/{encoded_path}",
            headers={"range": f"bytes={range_start}-{range_end}"},
        )
        expect_status(first_range.status, {206}, "range GET via archive_url(redis://)")

        expected = body[range_start : range_end + 1]
        if first_range.body != expected:
            raise AssertionError(
                "range body mismatch from archive fallback: "
                f"expected {len(expected)} bytes, got {len(first_range.body)} bytes"
            )

        part_rows = list_part_rows(db_path, slot_id, blob_path, generation)
        if not part_rows:
            raise AssertionError("expected read-through to materialize part rows in file_entries")

        for part_no, sha256, external_path in part_rows:
            if not isinstance(external_path, str) or not external_path:
                raise AssertionError(
                    f"materialized part row missing external_path: part_no={part_no} row={part_rows}"
                )
            path = Path(external_path)
            if not path.exists():
                raise AssertionError(f"materialized part file not found: {path}")
            if sha256_hex(path.read_bytes()) != sha256:
                raise AssertionError(f"materialized part sha mismatch for {path}")

        redis_del_key(args.archive_redis_url, archive_key)

        second_range = cluster.external_request(
            0,
            "GET",
            f"/blobs/{encoded_path}",
            headers={"range": f"bytes={range_start}-{range_end}"},
        )
        expect_status(second_range.status, {206}, "range GET after redis archive key deletion")
        if second_range.body != expected:
            raise AssertionError(
                "range body mismatch after redis key deletion: "
                f"expected {len(expected)} bytes, got {len(second_range.body)} bytes"
            )

    print("[007] PASS")


if __name__ == "__main__":
    main()
