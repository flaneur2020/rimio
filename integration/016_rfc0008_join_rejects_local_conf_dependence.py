#!/usr/bin/env python3
"""[016] RFC0008 contract placeholder: join rejects local-conf-driven mismatch.

This case intentionally encodes the expected behavior from RFC0008:
- join does not treat local config as source of truth
- provided overrides must match registry bootstrap state

Current implementation does not expose `rimio join` yet, so this case is a
gated placeholder (runs only when explicitly included).
"""

from __future__ import annotations

from _harness import build_case_parser


def main() -> None:
    parser = build_case_parser("016", "RFC0008 join/local-config contract placeholder")
    parser.parse_args()

    print("[016] SKIP (placeholder): rimio join command not implemented yet")


if __name__ == "__main__":
    main()

