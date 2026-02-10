# Amberio

Amberio is a lightweight write-through cache for cloud object storage services, designed as a practical storage layer for small on-premise clusters in edge environments where cluster size is limited, topology is relatively stable, and operational simplicity matters more than hyperscale features.

Amberio features:

- Stay focused on small-to-medium edge deployments instead of internet-scale multi-tenant object storage
- Provide predictable object routing through pre-sharded `slot`s (fixed 2048 slots)
- Minio-style Quorum based writes
- Use local, easy-to-operate metadata (`SQLite`) and filesystem-backed data
- Expose clear internal repair/sync APIs for resilience and recovery
- TLA+ proof of correctness
- S3 Compatibility (in development)

## TLA+

Amberio includes TLA+ specs and a trace-based checking workflow:

- Specs are under `tla/`
- Trace checker script is `scripts/tla/check_trace.py`
- Integration case `integration/008_tla_trace_check.py` can emit real runtime
  traces and optionally verify them with TLC

Quick example:

```bash
python3 integration/008_tla_trace_check.py \
  --build-if-missing \
  --tlc-jar /path/to/tla2tools.jar
```

## Quick Start

1. Start Redis (for example, `redis://127.0.0.1:6379`)
2. Build the binary:

```bash
cargo build --release -p amberio-server --bin amberio
```

3. Prepare config:

```bash
cp config.example.yaml config.yaml
```

4. (Optional) run init-only flow, then exit:

```bash
./target/release/amberio server --config config.yaml --init
```

5. Start the server:

```bash
./target/release/amberio server --config config.yaml
```

Notes:
- `initial_cluster` and replication bootstrap are persisted in registry on first init (first-wins).
- A normal `server` start auto-runs init when state is missing.
- `--init` runs initialization only and exits.

## External API (Amberio-native)

- `GET /_/api/v1/healthz`
- `GET /_/api/v1/nodes`
- `GET /_/api/v1/slots/resolve?path=<blob_path>`
- `PUT /_/api/v1/blobs/{path}`
- `GET /_/api/v1/blobs/{path}`
- `HEAD /_/api/v1/blobs/{path}`
- `DELETE /_/api/v1/blobs/{path}`
- `GET /_/api/v1/blobs?prefix=<p>&limit=<n>`

Common request/response headers:
- `x-amberio-write-id`
- `x-amberio-generation`

## S3-compatible API (V1)

- Endpoint style: path-style S3 routes at server root
- Covered ops: `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `ListObjectsV2`
- Multipart upload APIs currently return `NotImplemented`

## Integration Tests

```bash
uv run --project integration integration/run_all.py \
  --binary target/release/amberio \
  --redis-url redis://127.0.0.1:6379
```

## License

MIT
