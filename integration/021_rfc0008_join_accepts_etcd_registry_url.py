#!/usr/bin/env python3
"""[021] RFC0008 contract probe: join accepts etcd:// REGISTRY_URL.

This verifies join selects etcd backend path instead of rejecting URL format.
"""

from __future__ import annotations

from pathlib import Path

from _harness import DEFAULT_BINARY, build_case_parser
from _rfc0008_probe import ensure_binary, joined_output, run_cli


def main() -> None:
    parser = build_case_parser("021", "RFC0008 etcd:// URL contract")
    args = parser.parse_args()

    binary = Path(args.binary) if args.binary else DEFAULT_BINARY
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    result = run_cli(
        binary,
        [
            "join",
            "etcd://127.0.0.1:65534,127.0.0.1:65533",
            "--node",
            "node-rfc0008-etcd",
        ],
        timeout=15.0,
    )

    if result.returncode == 0:
        raise AssertionError("[021] expected etcd join to fail without live registry, but it succeeded")

    output = joined_output(result).lower()
    if "invalid registry url" in output:
        raise AssertionError(
            "[021] etcd:// URL should be accepted by parser and routed to etcd backend.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    if "failed to connect registry" not in output and "etcd" not in output:
        raise AssertionError(
            "[021] expected etcd backend connection failure signal.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    print("[021] PASS")


if __name__ == "__main__":
    main()

