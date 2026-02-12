#!/usr/bin/env python3
"""[018] RFC0008 contract placeholder: write-gate API allow/deny matrix.

Expected RFC behavior when write_gate=ClosedByInitCluster:
- allow: health/nodes/list/get/head
- deny: put/delete/internal write
- error: 503 + ClusterWriteDisabled

Current implementation does not expose write_gate controls yet.
"""

from __future__ import annotations

from _harness import build_case_parser


def main() -> None:
    parser = build_case_parser("018", "RFC0008 write gate API matrix placeholder")
    parser.parse_args()

    print("[018] SKIP (placeholder): write gate controls not implemented yet")


if __name__ == "__main__":
    main()

