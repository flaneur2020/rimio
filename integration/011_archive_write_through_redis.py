#!/usr/bin/env python3
"""[011] archive(redis) write-through + fallback read-through."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from urllib import parse

from _harness import DEFAULT_BINARY, REPO_ROOT, expect_status, http_request, parse_json_body, quote_blob_path


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
    chunks: list[bytes] = []
    while True:
        char = connection.recv(1)
        if not char:
            raise RuntimeError("redis connection closed while reading response")
        chunks.append(char)
        if len(chunks) >= 2 and chunks[-2:] == [b"\r", b"\n"]:
            return b"".join(chunks[:-2])


def read_exact(connection: socket.socket, n: int) -> bytes:
    chunks: list[bytes] = []
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


def redis_url_host_port(url: str) -> tuple[str, int]:
    if not url.startswith("redis://"):
        raise AssertionError(f"unexpected redis url: {url}")

    host_port = url.removeprefix("redis://").split("/", 1)[0]
    host, _, port_text = host_port.partition(":")
    return host, int(port_text or "6379")


def redis_get(url: str, key: str) -> bytes:
    host, port = redis_url_host_port(url)
    return send_redis_command(host, port, b"GET", key.encode("utf-8"))


def redis_del(url: str, key: str) -> None:
    host, port = redis_url_host_port(url)
    _ = send_redis_command(host, port, b"DEL", key.encode("utf-8"))


def ensure_redis_reachable(redis_url: str) -> None:
    host, port = redis_url_host_port(redis_url)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
        connection.settimeout(1.5)
        try:
            connection.connect((host, port))
        except OSError as error_message:
            raise RuntimeError(
                f"Redis is not reachable at {host}:{port}. "
                f"Please start Redis before running integration tests. ({error_message})"
            ) from error_message


def ensure_binary(binary_path: Path, *, build_if_missing: bool) -> None:
    if binary_path.exists():
        return

    if not build_if_missing:
        raise RuntimeError(
            f"Rimio binary not found at {binary_path}. "
            "Build it first or use --build-if-missing."
        )

    command = ["cargo", "build", "--release", "-p", "rimio-server", "--bin", "rimio"]
    print(f"[011] building binary: {' '.join(command)}")
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def wait_for_health(port: int, timeout_seconds: float = 25.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = http_request("GET", f"http://127.0.0.1:{port}/health", timeout=1.5)
        if response.status == 200:
            return

        response = http_request("GET", f"http://127.0.0.1:{port}/_/api/v1/healthz", timeout=1.5)
        if response.status == 200:
            return

        time.sleep(0.25)

    raise AssertionError(f"server did not become healthy on port {port}")


def parse_redis_archive_url(archive_url: str) -> tuple[str, str]:
    parsed = parse.urlparse(archive_url)
    if parsed.scheme != "redis":
        raise AssertionError(f"expected redis archive_url, got: {archive_url}")

    host = parsed.hostname
    if not host:
        raise AssertionError(f"archive_url missing host: {archive_url}")

    port = parsed.port or 6379
    raw_segments = [segment for segment in parsed.path.split("/") if segment]

    if not raw_segments:
        raise AssertionError(f"archive_url missing key: {archive_url}")

    if len(raw_segments) >= 2 and raw_segments[0].isdigit():
        db_segment = raw_segments[0]
        key_segments = raw_segments[1:]
    else:
        db_segment = None
        key_segments = raw_segments

    if not key_segments:
        raise AssertionError(f"archive_url missing key: {archive_url}")

    key = "/".join(key_segments)

    redis_url = f"redis://{host}:{port}"
    if db_segment is not None:
        redis_url = f"{redis_url}/{db_segment}"

    return redis_url, key


def slot_blob_dir(disk_path: Path, slot_id: int, blob_path: str) -> Path:
    root = disk_path / "slots" / str(slot_id) / "blobs"
    for component in blob_path.split("/"):
        root = root / component
    return root


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="[011] archive(redis) write-through + fallback read-through")
    parser.add_argument("--binary", default=str(DEFAULT_BINARY))
    parser.add_argument(
        "--redis-url",
        default=os.getenv("RIMIO_REDIS_URL", "redis://127.0.0.1:6379"),
    )
    parser.add_argument("--base-port", type=int, default=20210)
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--build-if-missing", action="store_true")
    args = parser.parse_args()

    binary = Path(args.binary)
    ensure_redis_reachable(args.redis_url)
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    run_id = f"011-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    work_dir = Path(tempfile.mkdtemp(prefix=f"rimio-{run_id}-"))
    config_path = work_dir / "config.yaml"
    log_path = work_dir / "server.log"
    disk_path = work_dir / "disk0"
    disk_path.mkdir(parents=True, exist_ok=True)

    node_id = f"it-{run_id}-node-1"
    group_namespace = f"it-{run_id}"
    port = args.base_port

    blob_path = f"cases/011/{uuid.uuid4().hex}.bin"
    encoded_path = quote_blob_path(blob_path)
    archive_key_prefix = f"rimio:test011:{uuid.uuid4().hex}"
    body = (b"rimio-rfc0004-write-through-" * 12000)[:360_000]

    config_path.write_text(
        "\n".join(
            [
                "registry:",
                "  backend: redis",
                f'  namespace: "{group_namespace}"',
                "  redis:",
                f'    url: "{args.redis_url}"',
                "    pool_size: 8",
                "initial_cluster:",
                "  nodes:",
                f'    - node_id: "{node_id}"',
                f'      bind_addr: "127.0.0.1:{port}"',
                f'      advertise_addr: "127.0.0.1:{port}"',
                "      disks:",
                f'        - path: "{disk_path.as_posix()}"',
                "  replication:",
                "    min_write_replicas: 1",
                "    total_slots: 2048",
                "archive:",
                "  archive_type: redis",
                "  redis:",
                f'    url: "{args.redis_url}"',
                f'    key_prefix: "{archive_key_prefix}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.setdefault("RUST_LOG", "rimio=info")

    server_process = None
    log_handle = None
    archive_key = None

    try:
        with open(log_path, "ab") as handle:
            init_result = subprocess.run(
                [
                    str(binary),
                    "start",
                    "--conf",
                    str(config_path),
                    "--node",
                    node_id,
                    "--init",
                ],
                cwd=REPO_ROOT,
                stdout=handle,
                stderr=subprocess.STDOUT,
                env=env,
                timeout=25,
            )
        if init_result.returncode != 0:
            raise AssertionError(f"server --init failed with exit {init_result.returncode}")

        log_handle = open(log_path, "ab")
        server_process = subprocess.Popen(
            [
                str(binary),
                "start",
                "--conf",
                str(config_path),
                "--node",
                node_id,
            ],
            cwd=REPO_ROOT,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
        )

        wait_for_health(port)

        put_response = http_request(
            "PUT",
            f"http://127.0.0.1:{port}/_/api/v1/blobs/{encoded_path}",
            body=body,
            headers={"content-type": "application/octet-stream"},
            timeout=12.0,
        )
        expect_status(put_response.status, {201}, "PUT blob")
        put_payload = parse_json_body(put_response)
        generation = put_payload.get("generation")
        if not isinstance(generation, int):
            raise AssertionError(f"PUT response missing generation: {put_payload}")

        resolve_response = http_request(
            "GET",
            f"http://127.0.0.1:{port}/_/api/v1/slots/resolve?path={quote_blob_path(blob_path)}",
            timeout=10.0,
        )
        expect_status(resolve_response.status, {200}, "resolve slot")
        resolve_payload = parse_json_body(resolve_response)
        slot_id = resolve_payload.get("slot_id")
        if not isinstance(slot_id, int):
            raise AssertionError(f"resolve payload missing slot_id: {resolve_payload}")

        head_response = http_request(
            "GET",
            (
                f"http://127.0.0.1:{port}/internal/v1/slots/{slot_id}/heads"
                f"?path={quote_blob_path(blob_path)}"
            ),
            timeout=10.0,
        )
        expect_status(head_response.status, {200}, "internal head fetch")
        head_payload = parse_json_body(head_response)
        if not head_payload.get("found"):
            raise AssertionError(f"head not found for written blob: {head_payload}")

        meta = head_payload.get("meta")
        if not isinstance(meta, dict):
            raise AssertionError(f"head payload missing meta: {head_payload}")

        archive_url = meta.get("archive_url")
        if not isinstance(archive_url, str) or not archive_url:
            raise AssertionError(
                "expected archive_url in head meta for archive write-through, "
                f"got: {meta.get('archive_url')!r}"
            )

        archive_redis_url, archive_key = parse_redis_archive_url(archive_url)
        archived = redis_get(archive_redis_url, archive_key)
        if archived != body:
            raise AssertionError(
                f"archive object mismatch: expected={len(body)} got={len(archived)}"
            )

        blob_dir = slot_blob_dir(disk_path, slot_id, blob_path)
        if blob_dir.exists():
            shutil.rmtree(blob_dir)

        get_response = http_request(
            "GET",
            f"http://127.0.0.1:{port}/_/api/v1/blobs/{encoded_path}",
            timeout=12.0,
        )
        expect_status(get_response.status, {200}, "GET blob with local parts removed")
        if get_response.body != body:
            raise AssertionError(
                "body mismatch after archive fallback: "
                f"expected={len(body)} got={len(get_response.body)}"
            )

        generation_dir = slot_blob_dir(disk_path, slot_id, blob_path) / f"g.{generation}"
        part_files = list(generation_dir.glob("part.*")) if generation_dir.exists() else []
        if not part_files:
            raise AssertionError("expected read-through to rematerialize local part files")

    finally:
        if server_process is not None:
            server_process.terminate()
            try:
                server_process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                server_process.kill()
                server_process.wait(timeout=3)
        if log_handle is not None:
            log_handle.close()

        if archive_key is not None:
            redis_del(args.redis_url, archive_key)

        if args.keep_artifacts:
            print(f"[011] kept artifacts at: {work_dir}")
        else:
            shutil.rmtree(work_dir, ignore_errors=True)

    print("[011] PASS")


if __name__ == "__main__":
    main()
