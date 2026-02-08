# Amberio Integration Tests

These tests are contract tests for external/internal HTTP APIs, including RFC 0002 and RFC 0003 coverage.

## Prerequisites

- Redis is running at `redis://127.0.0.1:6379` (default).
- Amberio binary is available at `target/release/amberio`, or pass `--build-if-missing`.

## Run all cases

```bash
python3 integration/run_all.py --build-if-missing
```

## Run one case

```bash
python3 integration/002_external_blob_crud.py --build-if-missing
```

RFC003-related cases currently use normal numeric prefixes and run via `run_all.py`:

- `004_rfc003_file_entries_schema.py`
- `005_rfc003_put_layout_contract.py`
- `006_rfc003_range_read_without_parts.py`

## Notes

- Each case auto-generates cluster configs and data directories under a temporary folder.
- Redis is not started by scripts.
- Use `--keep-artifacts` to keep generated configs/logs for debugging.
- API prefixes default to:
  - External: `/api/v1`
  - Internal: `/internal/v1`
