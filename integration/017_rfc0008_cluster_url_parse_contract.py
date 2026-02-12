#!/usr/bin/env python3
"""[017] RFC0008 contract probe: cluster:// registry URL parse semantics.

Pass malformed cluster URL and assert explicit parse/argument error.
"""

from __future__ import annotations

from pathlib import Path

from _harness import DEFAULT_BINARY, build_case_parser
from _rfc0008_probe import ensure_binary, joined_output, run_cli


def main() -> None:
    parser = build_case_parser("017", "RFC0008 cluster:// URL contract placeholder")
    args = parser.parse_args()

    binary = Path(args.binary) if args.binary else DEFAULT_BINARY
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    malformed_url = "cluster://seed1:8400,broken-host:::"
    result = run_cli(binary, ["join", malformed_url, "--node", "node-x"], timeout=15.0)

    if result.returncode == 0:
        raise AssertionError("[017] expected malformed cluster URL to fail, but it succeeded")

    output = joined_output(result).lower()
    if "cluster" not in output and "url" not in output and "invalid" not in output:
        raise AssertionError(
            "[017] expected explicit URL/argument parse error.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    print("[017] PASS")


if __name__ == "__main__":
    main()
