# Rimio

Rimio is a lightweight write-back cache that accelerates object-native systems (SlateDB, ZeroFS, JuiceFS, NeonDB, Greptime, Databend, WarpStream, Thanos, etc.) for edge and on-prem clusters with minimal operational overhead.

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

## Usage

1) Build binary:

```bash
cargo build --release -p rimio-server --bin rimio
```

2) Prepare local disks:

```bash
mkdir -p demo/node1/disk demo/node2/disk demo/node3/disk
```

3) Create `config.yaml` (simple example):

```bash
cat > config.yaml <<'EOF'
registry:
  backend: gossip
  namespace: local-cluster-001
  gossip:
    transport: "internal_http"
    seeds:
      - "127.0.0.1:19080"

initial_cluster:
  nodes:
    - node_id: "node-1"
      bind_addr: "127.0.0.1:19080"
      advertise_addr: "127.0.0.1:19080"
      disks:
        - path: demo/node1/disk
    - node_id: "node-2"
      bind_addr: "127.0.0.1:19081"
      advertise_addr: "127.0.0.1:19081"
      disks:
        - path: demo/node2/disk
    - node_id: "node-3"
      bind_addr: "127.0.0.1:19082"
      advertise_addr: "127.0.0.1:19082"
      disks:
        - path: demo/node3/disk
  replication:
    min_write_replicas: 2
    total_slots: 2048

# Optional archive backend
# archive:
#   archive_type: s3
#   s3:
#     bucket: "rimio-archive"
#     region: "us-east-1"
#     # endpoint: "http://127.0.0.1:9000"
#     # allow_http: true
#     credentials:
#       access_key_id: "YOUR_ACCESS_KEY_ID"
#       secret_access_key: "YOUR_SECRET_ACCESS_KEY"
EOF
```

4) Start `node-1`:

```bash
./target/release/rimio start --conf config.yaml --node node-1
```

5) Join `node-2` and `node-3`:

```bash
./target/release/rimio join cluster://127.0.0.1:19080 --node node-2
./target/release/rimio join cluster://127.0.0.1:19080 --node node-3
```

6) Verify:

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
