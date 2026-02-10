#!/usr/bin/env python3
"""[009] Init persistence + first-wins bootstrap contract."""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

from _harness import DEFAULT_BINARY, REPO_ROOT, http_request


def ensure_redis_reachable(redis_url: str) -> None:
    from urllib import parse

    parsed = parse.urlparse(redis_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 6379

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

    command = [
        "cargo",
        "build",
        "--release",
        "-p",
        "amberio-server",
        "--bin",
        "amberio",
    ]
    print(f"[009] building binary: {' '.join(command)}")
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def wait_for_health(port: int, timeout_seconds: float = 20.0) -> None:
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


def start_server(binary: Path, config_path: Path, log_path: Path, environment: dict[str, str]) -> subprocess.Popen:
    log_handle = open(log_path, "ab")
    process = subprocess.Popen(
        [str(binary), "server", "--config", str(config_path)],
        cwd=REPO_ROOT,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        env=environment,
    )
    setattr(process, "_log_handle", log_handle)
    return process


def stop_server(process: subprocess.Popen) -> None:
    process.terminate()
    try:
        process.wait(timeout=6)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)

    log_handle = getattr(process, "_log_handle", None)
    if log_handle is not None:
        log_handle.close()


def render_config(
    *,
    current_node: str,
    node1_id: str,
    node1_port: int,
    node1_disk: Path,
    node2_id: str,
    node2_port: int,
    node2_disk: Path,
    redis_url: str,
    namespace: str,
    total_slots: int,
) -> str:
    return (
        "\n".join(
            [
                f'current_node: "{current_node}"',
                "registry:",
                "  backend: redis",
                f'  namespace: "{namespace}"',
                "  redis:",
                f'    url: "{redis_url}"',
                "    pool_size: 8",
                "initial_cluster:",
                "  nodes:",
                f'    - node_id: "{node1_id}"',
                f'      bind_addr: "127.0.0.1:{node1_port}"',
                f'      advertise_addr: "127.0.0.1:{node1_port}"',
                "      disks:",
                f'        - path: "{node1_disk.as_posix()}"',
                f'    - node_id: "{node2_id}"',
                f'      bind_addr: "127.0.0.1:{node2_port}"',
                f'      advertise_addr: "127.0.0.1:{node2_port}"',
                "      disks:",
                f'        - path: "{node2_disk.as_posix()}"',
                "  replication:",
                "    min_write_replicas: 1",
                f"    total_slots: {total_slots}",
            ]
        )
        + "\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="[009] Init persistence + first-wins bootstrap")
    parser.add_argument(
        "--binary",
        default=str(DEFAULT_BINARY),
        help="Path to amberio server binary",
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("AMBERIO_REDIS_URL", "redis://127.0.0.1:6379"),
        help="Redis URL for cluster registry",
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=20080,
        help="HTTP port for this case",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Keep generated configs/data/logs under temp directory",
    )
    parser.add_argument(
        "--build-if-missing",
        action="store_true",
        help="Build amberio binary automatically when not found",
    )
    args = parser.parse_args()

    binary = Path(args.binary)
    ensure_redis_reachable(args.redis_url)
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    run_id = f"009-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    work_dir = Path(tempfile.mkdtemp(prefix=f"amberio-{run_id}-"))
    config_path = work_dir / "config.yaml"
    node1_port = args.base_port
    node2_port = args.base_port + 1

    node1_id = f"it-{run_id}-node-1"
    node2_id = f"it-{run_id}-node-2"

    node1_dir = work_dir / "node-1"
    node2_dir = work_dir / "node-2"
    node1_disk = node1_dir / "disk0"
    node2_disk = node2_dir / "disk0"
    node1_disk.mkdir(parents=True, exist_ok=True)
    node2_disk.mkdir(parents=True, exist_ok=True)

    node1_log = node1_dir / "server.log"
    node2_log = node2_dir / "server.log"

    node1_config = work_dir / "node-1-config.yaml"
    node2_config = work_dir / "node-2-config.yaml"

    # Node-1 uses 2048 slots and should win bootstrap.
    node1_config.write_text(
        render_config(
            current_node=node1_id,
            node1_id=node1_id,
            node1_port=node1_port,
            node1_disk=node1_disk,
            node2_id=node2_id,
            node2_port=node2_port,
            node2_disk=node2_disk,
            redis_url=args.redis_url,
            namespace=f"it-009-{run_id}",
            total_slots=2048,
        ),
        encoding="utf-8",
    )

    # Node-2 local file is intentionally conflicting (64 slots). First-wins should ignore this.
    node2_config.write_text(
        render_config(
            current_node=node2_id,
            node1_id=node1_id,
            node1_port=node1_port,
            node1_disk=node1_disk,
            node2_id=node2_id,
            node2_port=node2_port,
            node2_disk=node2_disk,
            redis_url=args.redis_url,
            namespace=f"it-009-{run_id}",
            total_slots=64,
        ),
        encoding="utf-8",
    )

    environment = os.environ.copy()
    environment.setdefault("RUST_LOG", "amberio=info")

    try:
        # 1) Auto-init on normal startup when node is not initialized yet.
        node1_server = start_server(binary, node1_config, node1_log, environment)
        try:
            wait_for_health(node1_port)
        finally:
            stop_server(node1_server)

        node1_amberio_dir = node1_disk / "amberio"
        if not node1_amberio_dir.exists():
            raise AssertionError(f"auto-init did not create expected directory: {node1_amberio_dir}")

        # 2) Explicit --init should still work and exit quickly.
        with open(node2_log, "ab") as log_handle:
            init_result = subprocess.run(
                [str(binary), "server", "--config", str(node2_config), "--init"],
                cwd=REPO_ROOT,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env=environment,
                timeout=20,
            )
        if init_result.returncode != 0:
            raise AssertionError(
                f"node-2 server --init exited with {init_result.returncode}. "
                f"See log: {node2_log}"
            )

        probe = http_request("GET", f"http://127.0.0.1:{node2_port}/health", timeout=1.5)
        if probe.status == 200:
            raise AssertionError("server --init must not keep HTTP service running")

        # 3) First-wins bootstrap from registry: node-2 must use winner's 2048 slot config,
        #    not its local conflicting 64-slot file.
        node2_server = start_server(binary, node2_config, node2_log, environment)
        try:
            wait_for_health(node2_port)

            saw_slot_above_63 = False
            for index in range(80):
                candidate = f"cases/009/first-wins-{index}/{uuid.uuid4().hex}.txt"
                response = http_request(
                    "GET",
                    f"http://127.0.0.1:{node2_port}/_/api/v1/slots/resolve?path={candidate}",
                    timeout=2.0,
                )
                if response.status != 200:
                    raise AssertionError(
                        f"resolve slot failed while checking first-wins: status={response.status} body={response.body[:200]!r}"
                    )
                try:
                    payload_text = response.body.decode("utf-8")
                    import json

                    payload = json.loads(payload_text)
                except Exception as decode_error:  # pragma: no cover - integration guard
                    raise AssertionError(
                        f"resolve slot returned invalid JSON: {response.body[:200]!r}"
                    ) from decode_error

                slot_id = payload.get("slot_id")
                if not isinstance(slot_id, int):
                    raise AssertionError(f"resolve payload missing slot_id: {payload}")

                if slot_id > 63:
                    saw_slot_above_63 = True
                    break

            if not saw_slot_above_63:
                raise AssertionError(
                    "expected at least one slot_id > 63. "
                    "bootstrap first-wins likely failed and node used local 64-slot config"
                )
        finally:
            stop_server(node2_server)

        # 4) Mutate node-2 config to invalid values; runtime should still boot from persisted init state.
        node2_config.write_text(
            "\n".join(
                [
                    f'current_node: "{node2_id}"',
                    "registry:",
                    "  backend: redis",
                    f'  namespace: "it-009-{run_id}"',
                    "  redis:",
                    f'    url: "{args.redis_url}"',
                    "    pool_size: 1",
                    "initial_cluster:",
                    "  nodes:",
                    '    - node_id: "non-existent-node"',
                    f'      bind_addr: "127.0.0.1:{node2_port + 1000}"',
                    f'      advertise_addr: "127.0.0.1:{node2_port + 1000}"',
                    "      disks:",
                    '        - path: "/tmp/invalid"',
                    "  replication:",
                    "    min_write_replicas: 1",
                    "    total_slots: 64",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        node2_server_after_mutation = start_server(binary, node2_config, node2_log, environment)
        try:
            wait_for_health(node2_port)
        finally:
            stop_server(node2_server_after_mutation)

    finally:
        if args.keep_artifacts:
            print(f"[009] kept artifacts at: {work_dir}")
        else:
            shutil.rmtree(work_dir, ignore_errors=True)

    print("[009] PASS")


if __name__ == "__main__":
    main()
