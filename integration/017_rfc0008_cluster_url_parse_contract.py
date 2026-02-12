#!/usr/bin/env python3
"""[017] RFC0008 contract placeholder: cluster:// registry URL semantics.

Expected RFC behavior:
- accepts cluster://seed1:8400,seed2:8400
- succeeds if any seed is reachable
- invalid URL forms return explicit argument errors

Current implementation does not expose `rimio join` yet.
"""

from __future__ import annotations

from _harness import build_case_parser


def main() -> None:
    parser = build_case_parser("017", "RFC0008 cluster:// URL contract placeholder")
    parser.parse_args()

    print("[017] SKIP (placeholder): rimio join command not implemented yet")


if __name__ == "__main__":
    main()

