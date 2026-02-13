#!/usr/bin/env python3
"""[022] RFC0010 integration: single-port memberlist internal transport."""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from urllib import request

from _harness import DEFAULT_BINARY, build_case_parser, tail_file
from _rfc0008_probe import ensure_binary

ROOT_DIR = Path(__file__).resolve().parent.parent


def pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def read_json(url: str, timeout: float = 2.0) -> dict:
    with request.urlopen(url, timeout=timeout) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def wait_until(predicate, timeout_sec: float, interval_sec: float = 0.2) -> None:
    deadline = time.monotonic() + timeout_sec
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as error:  # noqa: BLE001
            last_error = error
        time.sleep(interval_sec)

    if last_error is not None:
        raise TimeoutError(f"condition not met before timeout: {last_error}")
    raise TimeoutError("condition not met before timeout")


def main() -> None:
    parser = build_case_parser("022", "RFC0010 single-port gossip start/join")
    args = parser.parse_args()

    binary = Path(args.binary) if args.binary else DEFAULT_BINARY
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    build_command = ["cargo", "build", "-p", "rimio-server", "--bin", "rimio"]
    if "release" in binary.parts:
        build_command.insert(2, "--release")
    subprocess.run(build_command, cwd=ROOT_DIR, check=True)

    run_root = Path(tempfile.mkdtemp(prefix="rimio-rfc0010-it-"))
    config_path = run_root / "config.yaml"
    node1_log = run_root / "node1.log"
    node2_log = run_root / "node2.log"
    keep_artifacts = bool(args.keep_artifacts)

    process_1: subprocess.Popen[str] | None = None
    process_2: subprocess.Popen[str] | None = None
    log_1 = None
    log_2 = None

    try:
        port_1 = pick_free_port()
        port_2 = pick_free_port()
        while port_2 == port_1:
            port_2 = pick_free_port()

        (run_root / "node1" / "disk").mkdir(parents=True, exist_ok=True)
        (run_root / "node2" / "disk").mkdir(parents=True, exist_ok=True)

        namespace = f"it-rfc0010-{uuid.uuid4().hex[:8]}"
        config_path.write_text(
            "\n".join(
                [
                    "registry:",
                    "  backend: gossip",
                    f"  namespace: {namespace}",
                    "  gossip:",
                    '    transport: "internal_http"',
                    "    seeds: []",
                    "",
                    "initial_cluster:",
                    "  nodes:",
                    '    - node_id: "node-1"',
                    f'      bind_addr: "127.0.0.1:{port_1}"',
                    f'      advertise_addr: "127.0.0.1:{port_1}"',
                    "      disks:",
                    f"        - path: {run_root / 'node1' / 'disk'}",
                    '    - node_id: "node-2"',
                    f'      bind_addr: "127.0.0.1:{port_2}"',
                    f'      advertise_addr: "127.0.0.1:{port_2}"',
                    "      disks:",
                    f"        - path: {run_root / 'node2' / 'disk'}",
                    "  replication:",
                    "    min_write_replicas: 1",
                    "    total_slots: 16",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        log_1 = node1_log.open("w", encoding="utf-8")
        launch_binary = binary.resolve()

        process_1 = subprocess.Popen(
            [
                str(launch_binary),
                "start",
                "--conf",
                str(config_path),
                "--node",
                "node-1",
            ],
            cwd=ROOT_DIR,
            stdout=log_1,
            stderr=subprocess.STDOUT,
            text=True,
        )

        wait_until(
            lambda: read_json(f"http://127.0.0.1:{port_1}/_/api/v1/healthz").get("status")
            == "ok",
            timeout_sec=20,
        )

        bootstrap = read_json(f"http://127.0.0.1:{port_1}/internal/v1/cluster/bootstrap")
        if not bootstrap.get("found"):
            raise AssertionError(
                "[022] bootstrap state not found from start node internal API"
            )

        log_2 = node2_log.open("w", encoding="utf-8")
        process_2 = subprocess.Popen(
            [
                str(launch_binary),
                "join",
                f"cluster://127.0.0.1:{port_1}",
                "--node",
                "node-2",
            ],
            cwd=ROOT_DIR,
            stdout=log_2,
            stderr=subprocess.STDOUT,
            text=True,
        )

        wait_until(
            lambda: read_json(f"http://127.0.0.1:{port_2}/_/api/v1/healthz").get("status")
            == "ok",
            timeout_sec=30,
        )

        wait_until(
            lambda: len(read_json(f"http://127.0.0.1:{port_1}/_/api/v1/nodes").get("nodes", []))
            >= 2,
            timeout_sec=30,
        )

        wait_until(
            lambda: len(read_json(f"http://127.0.0.1:{port_2}/_/api/v1/nodes").get("nodes", []))
            >= 2,
            timeout_sec=30,
        )

        nodes_1 = read_json(f"http://127.0.0.1:{port_1}/_/api/v1/nodes").get("nodes", [])
        nodes_2 = read_json(f"http://127.0.0.1:{port_2}/_/api/v1/nodes").get("nodes", [])

        ids_1 = {node.get("node_id") for node in nodes_1}
        ids_2 = {node.get("node_id") for node in nodes_2}

        if {"node-1", "node-2"} - ids_1:
            raise AssertionError(f"[022] node-1 view missing expected nodes: {nodes_1}")
        if {"node-1", "node-2"} - ids_2:
            raise AssertionError(f"[022] node-2 view missing expected nodes: {nodes_2}")

        if process_1.poll() is not None:
            raise AssertionError(
                f"[022] node-1 process exited early with code {process_1.returncode}"
            )
        if process_2.poll() is not None:
            raise AssertionError(
                f"[022] node-2 process exited early with code {process_2.returncode}"
            )

        print("[022] PASS")

    except Exception:  # noqa: BLE001
        print("[022] FAIL")
        print("--- node1 tail ---")
        print(tail_file(node1_log))
        print("--- node2 tail ---")
        print(tail_file(node2_log))
        raise
    finally:
        if process_2 is not None and process_2.poll() is None:
            process_2.terminate()
            try:
                process_2.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process_2.kill()
        if process_1 is not None and process_1.poll() is None:
            process_1.terminate()
            try:
                process_1.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process_1.kill()

        if log_2 is not None:
            log_2.close()
        if log_1 is not None:
            log_1.close()

        if keep_artifacts:
            print(f"[022] kept artifacts: {run_root}")
        else:
            shutil.rmtree(run_root, ignore_errors=True)


if __name__ == "__main__":
    main()
