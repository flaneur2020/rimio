# Rimio Integration Tests

These tests are contract tests for Rimio external/internal APIs and S3 gateway behavior.

## Prerequisites

- Redis is running at `redis://127.0.0.1:6379` (default).
- Rimio binary is available at `target/release/rimio`, or pass `--build-if-missing`.
- Install `uv` (https://docs.astral.sh/uv/).

## Python dependencies (uv)

From repo root:

```bash
uv run --project integration integration/run_all.py --help
```

`uv` will resolve dependencies from `integration/pyproject.toml` (including `boto3`).

## Run all cases

```bash
uv run --project integration integration/run_all.py --build-if-missing
```

## Run one case

```bash
uv run --project integration integration/002_external_blob_crud.py --build-if-missing
```

## TLA+ trace case

Case `008_tla_trace_check.py` generates a real write/delete trace from a live
cluster run and saves it as JSON. You can optionally validate this trace
against a TLA+ replay spec with TLC.

```bash
uv run --project integration integration/008_tla_trace_check.py \
  --build-if-missing \
  --tlc-jar /path/to/tla2tools.jar
```

## Init flow cases

- `009_server_init_only.py`: validates auto-init, explicit `--init`, and first-wins bootstrap persisted in registry.
- `010_init_scan_redis_mock.py`: validates optional `init_scan` import using Redis-mocked archive object listing.

## Archive cases

- `007_rfc003_archive_url_redis_readthrough.py`: validates `archive_url(redis://)` range fallback and read-through materialization.
- `011_archive_write_through_redis.py`: validates archive write-through on PUT plus fallback/read-through after local part deletion.
- `012_archive_write_through_s3_minio.py`: validates S3/MinIO write-through and fallback read-through.

## S3 gateway case

- `013_s3_gateway_basic.py`: validates S3-compatible `put/get/head/list/delete` via `boto3`, and checks multipart currently returns expected not-implemented style error.
- `014_s3_get_object_compat.py`: validates GetObject compatibility fields (range/partNumber/conditionals/response overrides) and explicit `NotImplemented` behavior for `versionId` and SSE-C.
- `015_s3_put_list_compat.py`: validates PutObject/ListObjectsV2 compatibility for conditional put, Content-MD5, delimiter/start-after listing, pagination, and staged `NotImplemented` behavior for unsupported options.

## RFC0008 gated cases (016-019)

These cases define contract placeholders for the planned RFC0008 start/join + gossip flow:

- `016_rfc0008_join_rejects_local_conf_dependence.py`
- `017_rfc0008_cluster_url_parse_contract.py`
- `018_rfc0008_write_gate_api_contract.py`
- `019_rfc0008_force_takeover_contract.py`

They are excluded from `run_all.py` by default until features land.

Enable with either:

- `RIMIO_ENABLE_RFC0008_IT=1 uv run --project integration integration/run_all.py ...`
- `uv run --project integration integration/run_all.py --include-rfc0008 ...`

## Optional MinIO case (012)

By default, `run_all.py` skips case `012`.

Enable it with either:

- `RIMIO_ENABLE_S3_IT=1 uv run --project integration integration/run_all.py ...`
- `uv run --project integration integration/run_all.py --include-s3 ...`

## Notes

- Each case auto-generates cluster configs and data directories under a temporary folder.
- Redis is not started by scripts.
- Case `012` expects a reachable S3-compatible endpoint (e.g. MinIO) with a pre-created bucket.
- Use `--keep-artifacts` to keep generated configs/logs for debugging.
- API prefixes default to:
  - External: `/_/api/v1`
  - Internal: `/internal/v1`
