#!/usr/bin/env python3
"""[020] RFC0008 contract probe: join accepts redis:// REGISTRY_URL.

This verifies join selects redis backend path instead of rejecting URL format.
"""

from __future__ import annotations

from pathlib import Path

from _harness import DEFAULT_BINARY, build_case_parser
from _rfc0008_probe import ensure_binary, joined_output, run_cli


def main() -> None:
    parser = build_case_parser("020", "RFC0008 redis:// URL contract")
    args = parser.parse_args()

    binary = Path(args.binary) if args.binary else DEFAULT_BINARY
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    result = run_cli(
        binary,
        ["join", "redis://127.0.0.1:65535", "--node", "node-rfc0008-redis"],
        timeout=15.0,
    )

    if result.returncode == 0:
        raise AssertionError("[020] expected redis join to fail without live registry, but it succeeded")

    output = joined_output(result).lower()
    if "invalid registry url" in output:
        raise AssertionError(
            "[020] redis:// URL should be accepted by parser and routed to redis backend.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    if "failed to connect registry" not in output and "redis" not in output:
        raise AssertionError(
            "[020] expected redis backend connection failure signal.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    print("[020] PASS")


if __name__ == "__main__":
    main()

