# Rimio

Rimio is a lightweight write-back cache that accelerates object-native systems (SlateDB, NeonDB, Greptime, Databend, WarpStream, Thanos, etc.) for edge and on-prem clusters with minimal operational overhead.

It bridges the gap between local SSD performance and cloud reliability.

```
+-------------------------------------------------------+       +------------------------+
|  ON-PREMISES / EDGE LOCATION                          |       |  PUBLIC CLOUD          |
|                                                       |       |                        |
|   +-------------+         +-----------------------+   |       |   +----------------+   |
|   |             |  S3 API |                       |   | HTTPS |   |                |   |
|   | Application | <-----> |     Rimio Cluster     | <-------->|   | Cloud Store    |   |
|   |    (App)    |         |   (Write-Back Cache)  |   |       |   | (e.g., AWS S3) |   |
|   |             |         |                       |   |       |   |                |   |
|   +-------------+         +-----------------------+   |       |   +----------------+   |
|                                                       |       |                        |
+-------------------------------------------------------+       +------------------------+
```

It features:

- Simple setup and configuration.
- Durability on your cloud, local SSD speed for read/write.
- Minimal architecture with low operational overhead.
- TLA+ backed design with extensive property-based testing.

Non-goals (important!):

- No full AWS S3 API compatibility (basic CRUD only).
- No full AWS IAM/ACL support.
- No multi-tenant support.
- Not built for dynamic cluster autoscaling or rebalancing (dynamic node addition and removal are not supported by design).

## Quick guide: bootstrap a local cluster

Prerequisites:

- Redis reachable at `redis://127.0.0.1:6379` (will remove redis dependency soon)
- Rust toolchain installed

1) Build:

```bash
cargo build --release -p rimio-server --bin rimio
```

2) Prepare config + local disks:

```bash
cp config.example.yaml config.yaml
mkdir -p demo/node1/disk demo/node2/disk demo/node3/disk
```

3) Initialize cluster state once (first wins):

```bash
./target/release/rimio server --config config.yaml --current-node node-1 --init
```

4) Start all nodes from the same config (using CLI override):

```bash
./target/release/rimio server --config config.yaml --current-node node-1
./target/release/rimio server --config config.yaml --current-node node-2
./target/release/rimio server --config config.yaml --current-node node-3
```

5) Verify:

```bash
curl http://127.0.0.1:19080/_/api/v1/healthz
curl http://127.0.0.1:19080/_/api/v1/nodes
```

## Integration check

```bash
uv run --project integration integration/run_all.py \
  --binary target/release/rimio \
  --redis-url redis://127.0.0.1:6379
```

## License

MIT
