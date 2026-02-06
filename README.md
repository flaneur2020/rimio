# AmberBlob

Lightweight object storage for edge cloud nodes.

## Overview

AmberBlob is a fixed-topology, leaderless object storage system designed for small-scale edge clusters (3-10 machines). It trades scalability for simplicity.

## Key Features

- **Fixed 2048 slots** per group - no dynamic scaling
- **Leaderless architecture** - any node can handle requests
- **2PC for consistency** - two-phase commit for metadata updates
- **Content-addressed storage** - SHA256 hash for chunk deduplication
- **Local SQLite metadata** - per-slot metadata management
- **etcd for coordination** - routing table and health status

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node 1    │◄───►│   Node 2    │◄───►│   Node 3    │
│  (Slots     │     │  (Slots     │     │  (Slots     │
│   0-682)    │     │   683-1365) │     │   1366-2047)│
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────┴──────┐
                    │    etcd     │
                    │ (routing &  │
                    │   health)   │
                    └─────────────┘
```

## Quick Start

### 1. Build

```bash
cargo build --release
```

### 2. Configure

Copy the example config and edit:

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your settings
```

### 3. Initialize

```bash
./target/release/amberblob init --config config.yaml
```

### 4. Run

```bash
./target/release/amberblob server --config config.yaml
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Node health check |
| PUT | `/objects/{path}` | Upload object |
| GET | `/objects/{path}` | Download object |
| DELETE | `/objects/{path}` | Delete object |
| GET | `/objects` | List objects |
| GET | `/slots/{id}` | Get slot info |
| GET | `/nodes` | List nodes |

## Example Usage

```bash
# Upload a file
curl -X PUT http://localhost:8080/objects/myfile.txt -d "Hello, World!"

# Download a file
curl http://localhost:8080/objects/myfile.txt

# List objects
curl http://localhost:8080/objects

# Check health
curl http://localhost:8080/health
```

## Configuration

```yaml
node:
  node_id: "edge-node-001"
  group_id: "edge-cluster-001"
  bind_addr: "0.0.0.0:8080"
  disks:
    - path: /data/disk1

etcd:
  endpoints:
    - "http://localhost:2379"

replication:
  min_write_replicas: 3
  total_slots: 2048
```

## Design Tradeoffs

| Aspect | Choice | Reason |
|--------|--------|--------|
| Scalability | Fixed 2048 slots | Edge scale fixed, simplify consistency |
| Architecture | No leader, 2PC | Eliminate single point, high availability |
| Consistency | 2PC metadata commit | Data and metadata strong consistency |
| Recovery | Anti-entropy | No central coordination needed |

## License

MIT
