#!/usr/bin/env python3
"""[016] RFC0008 contract probe: join and local-config independence.

Coverage strategy:
- verify join without registry URL fails fast with argument/usage error.
"""

from __future__ import annotations

from pathlib import Path

from _harness import DEFAULT_BINARY, build_case_parser
from _rfc0008_probe import ensure_binary, joined_output, run_cli


def main() -> None:
    parser = build_case_parser("016", "RFC0008 join/local-config contract placeholder")
    args = parser.parse_args()

    binary = Path(args.binary) if args.binary else DEFAULT_BINARY
    ensure_binary(binary, build_if_missing=args.build_if_missing)

    result = run_cli(binary, ["join"])

    if result.returncode == 0:
        raise AssertionError("[016] expected join without REGISTRY_URL to fail, but it succeeded")

    output = joined_output(result).lower()
    if "usage" not in output and "required" not in output and "registry" not in output:
        raise AssertionError(
            "[016] expected argument/usage error for missing REGISTRY_URL.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    print("[016] PASS")


if __name__ == "__main__":
    main()
