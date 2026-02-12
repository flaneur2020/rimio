#!/usr/bin/env python3
"""[019] RFC0008 contract placeholder: --force-takeover guardrails.

Expected RFC behavior:
- only effective for same-node Suspect state
- never overrides Alive state
- requires heartbeat-age threshold
- emits audit log on takeover

Current implementation does not expose `rimio join --force-takeover` yet.
"""

from __future__ import annotations

from _harness import build_case_parser


def main() -> None:
    parser = build_case_parser("019", "RFC0008 force-takeover contract placeholder")
    parser.parse_args()

    print("[019] SKIP (placeholder): force-takeover flow not implemented yet")


if __name__ == "__main__":
    main()

