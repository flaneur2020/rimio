#!/usr/bin/env python3
"""[010] --init with init_scan(redis mock) imports archive object heads."""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

from _harness import DEFAULT_BINARY, REPO_ROOT, expect_status, parse_json_body


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
    if prefix == b"*":
        count = int(read_line(connection))
        if count <= 0:
            return b""
        payload = []
        for _ in range(count):
            payload.append(read_redis_reply(connection))
        return b"\n".join(payload)
    raise RuntimeError(f"unsupported redis reply prefix: {prefix!r}")


def redis_url_host_port(url: str) -> tuple[str, int]:
    if not url.startswith("redis://"):
        raise AssertionError(f"unexpected redis url: {url}")

    host_port = url.removeprefix("redis://").split("/", 1)[0]
    host, _, port_text = host_port.partition(":")
    return host, int(port_text or "6379")


def redis_set_binary(url: str, key: str, value: bytes) -> None:
    host, port = redis_url_host_port(url)
    _ = send_redis_command(host, port, b"SET", key.encode("utf-8"), value)


def redis_rpush_json(url: str, list_key: str, payload: dict[str, object]) -> None:
    host, port = redis_url_host_port(url)
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    _ = send_redis_command(host, port, b"RPUSH", list_key.encode("utf-8"), encoded)


def redis_del(url: str, key: str) -> None:
    host, port = redis_url_host_port(url)
    _ = send_redis_command(host, port, b"DEL", key.encode("utf-8"))


def http_request(method: str, url: str, *, timeout: float = 6.0) -> tuple[int, dict[str, str], bytes]:
    from urllib import error, request

    req = request.Request(url=url, method=method.upper())
    try:
        with request.urlopen(req, timeout=timeout) as response:
            return (
                response.status,
                {key.lower(): value for key, value in response.headers.items()},
                response.read(),
            )
    except error.HTTPError as http_error:
        return (
            http_error.code,
            {key.lower(): value for key, value in http_error.headers.items()},
            http_error.read(),
        )
    except error.URLError as url_error:
        return (0, {}, str(url_error).encode("utf-8"))


def wait_for_health(port: int, timeout_seconds: float = 25.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status, _, _ = http_request("GET", f"http://127.0.0.1:{port}/health", timeout=1.5)
        if status == 200:
            return
        status, _, _ = http_request("GET", f"http://127.0.0.1:{port}/_/api/v1/healthz", timeout=1.5)
        if status == 200:
            return
        time.sleep(0.25)

    raise AssertionError(f"server did not become healthy on port {port}")


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
            f"Amberio binary not found at {binary_path}. "
            "Build it first or use --build-if-missing."
        )

    command = ["cargo", "build", "--release", "-p", "amberio-server", "--bin", "amberio"]
    print(f"[010] building binary: {' '.join(command)}")
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="[010] --init with init_scan(redis mock)")
    parser.add_argument("--binary", default=str(DEFAULT_BINARY))
    parser.add_argument(
        "--redis-url",
        default=os.getenv("AMBERIO_REDIS_URL", "redis://127.0.0.1:6379"),
    )
    parser.add_argument("--base-port", type=int, default=20180)
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--build-if-missing", action="store_true")
    args = parser.parse_args()

    binary = Path(args.binary)
    ensure_redis_reachable(args.redis_url)
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    run_id = f"010-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    work_dir = Path(tempfile.mkdtemp(prefix=f"amberio-{run_id}-"))
    config_path = work_dir / "config.yaml"
    log_path = work_dir / "server.log"
    disk_path = work_dir / "disk0"
    disk_path.mkdir(parents=True, exist_ok=True)

    node_id = f"it-{run_id}-node-1"
    group_namespace = f"it-{run_id}"
    port = args.base_port

    blob_path = f"cases/010/{uuid.uuid4().hex}.bin"
    archive_key = f"amberio:init-scan:archive:{uuid.uuid4().hex}"
    archive_url = f"{args.redis_url}/{archive_key}"
    body = (b"amberio-init-scan-redis-mock-" * 5000)[:180_000]
    etag = sha256_hex(body)
    scan_list_key = f"amberio:init-scan:list:{uuid.uuid4().hex}"

    redis_set_binary(args.redis_url, archive_key, body)
    redis_rpush_json(
        args.redis_url,
        scan_list_key,
        {
            "path": blob_path,
            "size_bytes": len(body),
            "etag": etag,
            "archive_url": archive_url,
            "part_size": 65536,
            "updated_at": datetime.datetime.now(datetime.timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        },
    )

    config_path.write_text(
        "\n".join(
            [
                f'current_node: "{node_id}"',
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
                "init_scan:",
                "  enabled: true",
                "  redis:",
                f'    url: "{args.redis_url}"',
                f'    list_key: "{scan_list_key}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.setdefault("RUST_LOG", "amberio=info")

    server_process = None
    log_handle = None

    try:
        with open(log_path, "ab") as handle:
            init_result = subprocess.run(
                [str(binary), "server", "--config", str(config_path), "--init"],
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
            [str(binary), "server", "--config", str(config_path)],
            cwd=REPO_ROOT,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
        )

        wait_for_health(port)

        from urllib import parse

        encoded = parse.quote(blob_path, safe="/")
        get_status, get_headers, get_body = http_request(
            "GET", f"http://127.0.0.1:{port}/_/api/v1/blobs/{encoded}", timeout=10.0
        )
        get_response = type("Resp", (), {"status": get_status, "headers": get_headers, "body": get_body})
        expect_status(get_response.status, {200}, "GET imported blob after init_scan")
        if get_response.body != body:
            raise AssertionError(
                f"imported blob body mismatch: expected={len(body)} got={len(get_response.body)}"
            )

        list_status, list_headers, list_body = http_request(
            "GET",
            f"http://127.0.0.1:{port}/_/api/v1/blobs?prefix=cases/010/&limit=50",
            timeout=10.0,
        )
        list_response = type(
            "Resp", (), {"status": list_status, "headers": list_headers, "body": list_body}
        )
        expect_status(list_response.status, {200}, "LIST imported blobs after init_scan")
        payload = parse_json_body(list_response)
        items = payload.get("items")
        if not isinstance(items, list):
            raise AssertionError(f"LIST payload missing items[]: {payload}")
        if not any(isinstance(item, dict) and item.get("path") == blob_path for item in items):
            raise AssertionError(f"imported blob not visible in LIST payload: {payload}")

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

        redis_del(args.redis_url, archive_key)
        redis_del(args.redis_url, scan_list_key)

        if args.keep_artifacts:
            print(f"[010] kept artifacts at: {work_dir}")
        else:
            shutil.rmtree(work_dir, ignore_errors=True)

    print("[010] PASS")


if __name__ == "__main__":
    main()

