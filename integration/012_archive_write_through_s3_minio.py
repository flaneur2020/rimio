#!/usr/bin/env python3
"""[012] archive(s3/minio) write-through + fallback read-through."""

from __future__ import annotations

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


def ensure_binary(binary_path: Path, *, build_if_missing: bool) -> None:
    if binary_path.exists():
        return

    if not build_if_missing:
        raise RuntimeError(
            f"Rimio binary not found at {binary_path}. "
            "Build it first or use --build-if-missing."
        )

    command = ["cargo", "build", "--release", "-p", "rimio-server", "--bin", "rimio"]
    print(f"[012] building binary: {' '.join(command)}")
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def ensure_socket_reachable(host: str, port: int, *, label: str) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
        connection.settimeout(1.5)
        try:
            connection.connect((host, port))
        except OSError as error_message:
            raise RuntimeError(
                f"{label} is not reachable at {host}:{port}. ({error_message})"
            ) from error_message


def parse_redis_host_port(url: str) -> tuple[str, int]:
    parsed = parse.urlparse(url)
    if parsed.scheme != "redis":
        raise RuntimeError(f"invalid redis URL: {url}")
    return parsed.hostname or "127.0.0.1", parsed.port or 6379


def parse_endpoint_host_port(endpoint: str) -> tuple[str, int, bool]:
    parsed = parse.urlparse(endpoint)
    if parsed.scheme not in {"http", "https"}:
        raise RuntimeError(f"minio endpoint must be http(s)://, got: {endpoint}")

    host = parsed.hostname or "127.0.0.1"
    if parsed.port is not None:
        port = parsed.port
    else:
        port = 443 if parsed.scheme == "https" else 80

    return host, port, parsed.scheme == "http"


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


def slot_blob_dir(disk_path: Path, slot_id: int, blob_path: str) -> Path:
    root = disk_path / "slots" / str(slot_id) / "blobs"
    for component in blob_path.split("/"):
        root = root / component
    return root


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="[012] archive(s3/minio) write-through + fallback read-through")
    parser.add_argument("--binary", default=str(DEFAULT_BINARY))
    parser.add_argument(
        "--redis-url",
        default=os.getenv("RIMIO_REDIS_URL", "redis://127.0.0.1:6379"),
    )
    parser.add_argument("--base-port", type=int, default=20240)
    parser.add_argument(
        "--s3-endpoint",
        default=os.getenv("RIMIO_S3_ENDPOINT", "http://127.0.0.1:9000"),
    )
    parser.add_argument(
        "--s3-bucket",
        default=os.getenv("RIMIO_S3_BUCKET", "rimio-it"),
    )
    parser.add_argument(
        "--s3-region",
        default=os.getenv("RIMIO_S3_REGION", "us-east-1"),
    )
    parser.add_argument(
        "--s3-access-key-id",
        default=os.getenv("RIMIO_S3_ACCESS_KEY_ID", "minioadmin"),
    )
    parser.add_argument(
        "--s3-secret-access-key",
        default=os.getenv("RIMIO_S3_SECRET_ACCESS_KEY", "minioadmin"),
    )
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--build-if-missing", action="store_true")
    args = parser.parse_args()

    binary = Path(args.binary)
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    redis_host, redis_port = parse_redis_host_port(args.redis_url)
    ensure_socket_reachable(redis_host, redis_port, label="Redis")

    endpoint_host, endpoint_port, endpoint_is_http = parse_endpoint_host_port(args.s3_endpoint)
    ensure_socket_reachable(endpoint_host, endpoint_port, label="S3 endpoint")

    run_id = f"012-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    work_dir = Path(tempfile.mkdtemp(prefix=f"rimio-{run_id}-"))
    config_path = work_dir / "config.yaml"
    log_path = work_dir / "server.log"
    disk_path = work_dir / "disk0"
    disk_path.mkdir(parents=True, exist_ok=True)

    node_id = f"it-{run_id}-node-1"
    group_namespace = f"it-{run_id}"
    port = args.base_port

    blob_path = f"cases/012/{uuid.uuid4().hex}.bin"
    encoded_path = quote_blob_path(blob_path)
    body = (b"rimio-rfc0004-s3-write-through-" * 12000)[:400_000]

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
                "  archive_type: s3",
                "  s3:",
                f'    bucket: "{args.s3_bucket}"',
                f'    region: "{args.s3_region}"',
                f'    endpoint: "{args.s3_endpoint}"',
                f"    allow_http: {'true' if endpoint_is_http else 'false'}",
                "    credentials:",
                f'      access_key_id: "{args.s3_access_key_id}"',
                f'      secret_access_key: "{args.s3_secret_access_key}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.setdefault("RUST_LOG", "rimio=info")

    server_process = None
    log_handle = None

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
                timeout=30,
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
        if not isinstance(archive_url, str) or not archive_url.startswith(f"s3://{args.s3_bucket}/"):
            raise AssertionError(
                f"expected s3 archive_url for bucket {args.s3_bucket}, got: {archive_url!r}"
            )

        blob_dir = slot_blob_dir(disk_path, slot_id, blob_path)
        if blob_dir.exists():
            shutil.rmtree(blob_dir)

        get_response = http_request(
            "GET",
            f"http://127.0.0.1:{port}/_/api/v1/blobs/{encoded_path}",
            timeout=14.0,
        )
        expect_status(get_response.status, {200}, "GET blob with local parts removed")
        if get_response.body != body:
            raise AssertionError(
                "body mismatch after s3 archive fallback: "
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

        if args.keep_artifacts:
            print(f"[012] kept artifacts at: {work_dir}")
        else:
            shutil.rmtree(work_dir, ignore_errors=True)

    print("[012] PASS")


if __name__ == "__main__":
    main()
