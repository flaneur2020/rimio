#!/usr/bin/env python3
"""Integration test harness for Amberio.

This harness:
- Generates per-node config files dynamically.
- Starts/stops a local multi-node cluster.
- Uses a pre-existing Redis instance (default: redis://127.0.0.1:6379).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set
from urllib import error, parse, request


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "amberio"


@dataclass
class HttpResponse:
    status: int
    headers: Dict[str, str]
    body: bytes


@dataclass
class NodeRuntime:
    index: int
    node_id: str
    port: int
    data_dir: Path
    config_path: Path
    log_path: Path
    process: Optional[subprocess.Popen] = None
    log_handle: Optional[object] = None


class AmberCluster:
    def __init__(
        self,
        *,
        node_count: int,
        redis_url: str,
        binary_path: Path,
        min_write_replicas: int,
        total_slots: int,
        base_port: int,
        api_prefix: str,
        internal_prefix: str,
        keep_artifacts: bool,
        build_if_missing: bool,
    ) -> None:
        self.node_count = node_count
        self.redis_url = redis_url
        self.binary_path = Path(binary_path)
        self.min_write_replicas = min_write_replicas
        self.total_slots = total_slots
        self.base_port = base_port
        self.api_prefix = _normalize_prefix(api_prefix)
        self.internal_prefix = _normalize_prefix(internal_prefix)
        self.keep_artifacts = keep_artifacts
        self.build_if_missing = build_if_missing

        self.run_id = f"it-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        self.group_id = f"amberio-{self.run_id}"
        self.work_dir = Path(tempfile.mkdtemp(prefix=f"amberio-{self.run_id}-"))
        self.nodes: List[NodeRuntime] = []

    def __enter__(self) -> "AmberCluster":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def start(self) -> None:
        self._ensure_redis_reachable()
        self._ensure_binary()
        self._prepare_nodes()
        for node in self.nodes:
            self._start_node(node)
        for node in self.nodes:
            self._wait_node_ready(node)

    def stop(self) -> None:
        for node in reversed(self.nodes):
            self._stop_node_process(node)

        if self.keep_artifacts:
            print(f"[harness] kept artifacts at: {self.work_dir}")
        else:
            shutil.rmtree(self.work_dir, ignore_errors=True)

    def stop_node(self, index: int) -> None:
        node = self.nodes[index]
        self._stop_node_process(node)

    def start_node(self, index: int) -> None:
        node = self.nodes[index]
        if node.process is not None and node.process.poll() is None:
            return
        self._start_node(node)
        self._wait_node_ready(node)

    def node_url(self, index: int) -> str:
        return f"http://127.0.0.1:{self.nodes[index].port}"

    def node_id(self, index: int) -> str:
        return self.nodes[index].node_id

    def external_request(
        self,
        node_index: int,
        method: str,
        path_and_query: str,
        *,
        body: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 10.0,
    ) -> HttpResponse:
        path = _normalize_suffix(path_and_query)
        url = f"{self.node_url(node_index)}{self.api_prefix}{path}"
        return http_request(method, url, body=body, headers=headers, timeout=timeout)

    def internal_request(
        self,
        node_index: int,
        method: str,
        path_and_query: str,
        *,
        body: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 10.0,
    ) -> HttpResponse:
        path = _normalize_suffix(path_and_query)
        url = f"{self.node_url(node_index)}{self.internal_prefix}{path}"
        return http_request(method, url, body=body, headers=headers, timeout=timeout)

    def _ensure_redis_reachable(self) -> None:
        parsed = parse.urlparse(self.redis_url)
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
                )

    def _ensure_binary(self) -> None:
        if self.binary_path.exists():
            return
        if not self.build_if_missing:
            raise RuntimeError(
                f"Amberio binary not found at {self.binary_path}. "
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
        print(f"[harness] building binary: {' '.join(command)}")
        subprocess.run(command, cwd=REPO_ROOT, check=True)

    def _prepare_nodes(self) -> None:
        self.nodes.clear()
        for index in range(self.node_count):
            node_id = f"it-node-{index + 1}"
            port = self.base_port + index
            node_dir = self.work_dir / f"node-{index + 1}"
            data_dir = node_dir / "disk0"
            config_path = node_dir / "config.yaml"
            log_path = node_dir / "server.log"

            data_dir.mkdir(parents=True, exist_ok=True)
            node_dir.mkdir(parents=True, exist_ok=True)

            config_content = self._render_config(node_id=node_id, port=port, data_dir=data_dir)
            config_path.write_text(config_content, encoding="utf-8")

            self.nodes.append(
                NodeRuntime(
                    index=index,
                    node_id=node_id,
                    port=port,
                    data_dir=data_dir,
                    config_path=config_path,
                    log_path=log_path,
                )
            )

    def _render_config(self, *, node_id: str, port: int, data_dir: Path) -> str:
        data_dir_value = data_dir.as_posix()
        return (
            f"node:\n"
            f"  node_id: \"{node_id}\"\n"
            f"  group_id: \"{self.group_id}\"\n"
            f"  bind_addr: \"127.0.0.1:{port}\"\n"
            f"  disks:\n"
            f"    - path: \"{data_dir_value}\"\n"
            f"registry:\n"
            f"  backend: redis\n"
            f"  redis:\n"
            f"    url: \"{self.redis_url}\"\n"
            f"    pool_size: 8\n"
            f"replication:\n"
            f"  min_write_replicas: {self.min_write_replicas}\n"
            f"  total_slots: {self.total_slots}\n"
        )

    def _start_node(self, node: NodeRuntime) -> None:
        node.log_path.parent.mkdir(parents=True, exist_ok=True)
        node.log_handle = open(node.log_path, "ab")

        environment = os.environ.copy()
        environment.setdefault("RUST_LOG", "amberio=info")

        node.process = subprocess.Popen(
            [str(self.binary_path), "server", "--config", str(node.config_path)],
            cwd=REPO_ROOT,
            stdout=node.log_handle,
            stderr=subprocess.STDOUT,
            env=environment,
        )

    def _stop_node_process(self, node: NodeRuntime) -> None:
        process = node.process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

        if node.log_handle is not None:
            node.log_handle.close()
            node.log_handle = None

        node.process = None

    def _wait_node_ready(self, node: NodeRuntime) -> None:
        deadline = time.time() + 30.0
        probe_paths = ["/health", f"{self.api_prefix}/healthz"]

        while time.time() < deadline:
            if node.process is None:
                raise RuntimeError(f"Node {node.node_id} is not running")

            exit_code = node.process.poll()
            if exit_code is not None:
                raise RuntimeError(
                    f"Node {node.node_id} exited early with code {exit_code}. "
                    f"Log tail:\n{tail_file(node.log_path)}"
                )

            for path in probe_paths:
                response = http_request("GET", f"{self.node_url(node.index)}{path}", timeout=1.5)
                if response.status == 200:
                    return

            time.sleep(0.25)

        raise RuntimeError(
            f"Node {node.node_id} did not become ready in time. "
            f"Log tail:\n{tail_file(node.log_path)}"
        )


def _normalize_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    cleaned = prefix.strip()
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned.rstrip("/")


def _normalize_suffix(path_and_query: str) -> str:
    if not path_and_query.startswith("/"):
        return f"/{path_and_query}"
    return path_and_query


def http_request(
    method: str,
    url: str,
    *,
    body: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 10.0,
) -> HttpResponse:
    request_headers = headers.copy() if headers else {}
    req = request.Request(url=url, data=body, method=method.upper(), headers=request_headers)
    try:
        with request.urlopen(req, timeout=timeout) as response:
            return HttpResponse(
                status=response.status,
                headers={key.lower(): value for key, value in response.headers.items()},
                body=response.read(),
            )
    except error.HTTPError as http_error:
        return HttpResponse(
            status=http_error.code,
            headers={key.lower(): value for key, value in http_error.headers.items()},
            body=http_error.read(),
        )
    except error.URLError as url_error:
        return HttpResponse(
            status=0,
            headers={},
            body=str(url_error).encode("utf-8"),
        )


def parse_json_body(response: HttpResponse) -> Dict:
    if not response.body:
        return {}
    try:
        return json.loads(response.body.decode("utf-8"))
    except json.JSONDecodeError as decode_error:
        raise AssertionError(
            f"Expected JSON body but got invalid payload (status={response.status}): {decode_error}\n"
            f"Body: {response.body[:300]!r}"
        ) from decode_error


def expect_status(actual_status: int, allowed_statuses: Iterable[int], context: str) -> None:
    allowed_set: Set[int] = set(allowed_statuses)
    if actual_status not in allowed_set:
        raise AssertionError(
            f"{context}: expected status in {sorted(allowed_set)}, got {actual_status}"
        )


def quote_blob_path(blob_path: str) -> str:
    return parse.quote(blob_path, safe="/")


def tail_file(path: Path, max_lines: int = 80) -> str:
    if not path.exists():
        return "<no log file>"
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:])


def build_case_parser(case_id: str, description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"[{case_id}] {description}")
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
        "--nodes",
        type=int,
        default=3,
        help="Number of nodes to bootstrap",
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=19080,
        help="First node HTTP port",
    )
    parser.add_argument(
        "--min-write-replicas",
        type=int,
        default=2,
        help="replication.min_write_replicas in generated config",
    )
    parser.add_argument(
        "--total-slots",
        type=int,
        default=2048,
        help="replication.total_slots in generated config",
    )
    parser.add_argument(
        "--api-prefix",
        default=os.getenv("AMBERIO_API_PREFIX", "/api/v1"),
        help="External API prefix",
    )
    parser.add_argument(
        "--internal-prefix",
        default=os.getenv("AMBERIO_INTERNAL_PREFIX", "/internal/v1"),
        help="Internal API prefix",
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
    return parser


def cluster_from_args(args: argparse.Namespace) -> AmberCluster:
    return AmberCluster(
        node_count=args.nodes,
        redis_url=args.redis_url,
        binary_path=Path(args.binary),
        min_write_replicas=args.min_write_replicas,
        total_slots=args.total_slots,
        base_port=args.base_port,
        api_prefix=args.api_prefix,
        internal_prefix=args.internal_prefix,
        keep_artifacts=args.keep_artifacts,
        build_if_missing=args.build_if_missing,
    )
