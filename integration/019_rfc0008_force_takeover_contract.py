#!/usr/bin/env python3
"""[019] RFC0008 contract probe: --force-takeover guardrails.

Verify unknown node + force-takeover still fails with explicit
argument/state validation error.
"""

from __future__ import annotations

from pathlib import Path

from _harness import DEFAULT_BINARY, build_case_parser
from _rfc0008_probe import ensure_binary, joined_output, run_cli


def main() -> None:
    parser = build_case_parser("019", "RFC0008 force-takeover contract placeholder")
    args = parser.parse_args()

    binary = Path(args.binary) if args.binary else DEFAULT_BINARY
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    result = run_cli(
        binary,
        [
            "join",
            "cluster://127.0.0.1:65535",
            "--node",
            "node-not-in-bootstrap",
            "--force-takeover",
        ],
        timeout=15.0,
    )

    if result.returncode == 0:
        raise AssertionError("[019] expected force-takeover guardrail failure, but command succeeded")

    output = joined_output(result).lower()
    if (
        "node" not in output
        and "bootstrap" not in output
        and "takeover" not in output
        and "failed to connect registry" not in output
        and "connection refused" not in output
    ):
        raise AssertionError(
            "[019] expected explicit node/takeover guardrail failure.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    print("[019] PASS")


if __name__ == "__main__":
    main()
