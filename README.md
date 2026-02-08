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

4. Start the server:

```bash
./target/release/amberio server --config config.yaml
```

## External API (Simplified)

- `GET /api/v1/healthz`
- `GET /api/v1/nodes`
- `GET /api/v1/slots/resolve?path=<blob_path>`
- `PUT /api/v1/blobs/{path}`
- `GET /api/v1/blobs/{path}`
- `HEAD /api/v1/blobs/{path}`
- `DELETE /api/v1/blobs/{path}`
- `GET /api/v1/blobs?prefix=<p>&limit=<n>`

Common request/response headers:
- `x-amberio-write-id`
- `x-amberio-generation`

## Integration Tests

```bash
python3 integration/run_all.py \
  --binary target/release/amberio \
  --redis-url redis://127.0.0.1:6379
```

## License

MIT
