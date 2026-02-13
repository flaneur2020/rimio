#!/usr/bin/env python3
"""Run all numbered integration cases under integration/ directory."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


INTEGRATION_DIR = Path(__file__).resolve().parent


RFC0008_PREFIXES = ("016_", "017_", "018_", "019_", "020_", "021_", "022_")


def discover_cases(*, include_s3: bool, include_rfc0008: bool) -> list[Path]:
    cases = sorted(
        path
        for path in INTEGRATION_DIR.glob("[0-9][0-9][0-9]_*.py")
        if path.name != "run_all.py"
    )

    filtered = cases if include_s3 else [case for case in cases if not case.name.startswith("012_")]

    if include_rfc0008:
        return filtered

    return [case for case in filtered if not case.name.startswith(RFC0008_PREFIXES)]


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Rimio integration cases")
    parser.add_argument(
        "--case-prefix",
        default="",
        help="Run only cases whose filename starts with this prefix (e.g. 002)",
    )
    parser.add_argument("--binary", default=None)
    parser.add_argument("--redis-url", default=None)
    parser.add_argument("--nodes", default=None)
    parser.add_argument("--base-port", default=None)
    parser.add_argument("--min-write-replicas", default=None)
    parser.add_argument("--total-slots", default=None)
    parser.add_argument("--api-prefix", default=None)
    parser.add_argument("--internal-prefix", default=None)
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--build-if-missing", action="store_true")
    parser.add_argument(
        "--include-s3",
        action="store_true",
        help="Include 012 S3/MinIO integration case",
    )
    parser.add_argument(
        "--include-rfc0008",
        action="store_true",
        help="Include 016-021 RFC0008 start/join/gossip integration cases",
    )
    args = parser.parse_args()

    include_s3 = args.include_s3 or _truthy(os.getenv("RIMIO_ENABLE_S3_IT"))
    include_rfc0008 = args.include_rfc0008 or _truthy(os.getenv("RIMIO_ENABLE_RFC0008_IT"))
    cases = discover_cases(include_s3=include_s3, include_rfc0008=include_rfc0008)
    if args.case_prefix:
        cases = [case for case in cases if case.name.startswith(args.case_prefix)]

    if not cases:
        print("No integration cases matched.")
        sys.exit(1)

    failed: list[Path] = []

    for case in cases:
        command = [sys.executable, str(case)]
        passthrough_pairs = {
            "--binary": args.binary,
            "--redis-url": args.redis_url,
            "--nodes": args.nodes,
            "--base-port": args.base_port,
            "--min-write-replicas": args.min_write_replicas,
            "--total-slots": args.total_slots,
            "--api-prefix": args.api_prefix,
            "--internal-prefix": args.internal_prefix,
        }
        for flag, value in passthrough_pairs.items():
            if value is not None:
                command.extend([flag, str(value)])

        if args.keep_artifacts:
            command.append("--keep-artifacts")
        if args.build_if_missing:
            command.append("--build-if-missing")

        print(f"\n=== Running {case.name} ===")
        result = subprocess.run(command, cwd=INTEGRATION_DIR.parent)
        if result.returncode != 0:
            failed.append(case)

    if failed:
        print("\nFAILED cases:")
        for case in failed:
            print(f"- {case.name}")
        sys.exit(1)

    print("\nAll integration cases passed.")


if __name__ == "__main__":
    main()
