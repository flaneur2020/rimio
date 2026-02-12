# Rimio Roadmap

## Positioning & Goals

Rimio is positioned as a practical object storage system for small edge
clusters with fixed or slowly changing topology.

Near-term goals:

- Deliver predictable routing and stable write semantics for edge deployments
- Provide reliable cluster coordination and anti-entropy recovery
- Keep operations simple with local metadata + filesystem-backed data

Long-term goals:

- Add policy-driven tiered storage and S3 archival
- Improve production operability (metrics, logging, migration tooling)
- Improve throughput and latency without sacrificing consistency guarantees

---

## Milestones

### ✅ Phase 1: Foundation (Completed)

Storage and API baseline for edge object storage.

- [x] SQLite metadata schema (`file_entries`)
- [x] Slot-based external part storage on filesystem
- [x] Generation-based object heads and tombstones
- [x] External API under `/api/v1/blobs/*`

### 🔄 Phase 2: Cluster Coordination (In Progress)

Distributed write coordination and node lifecycle management.

- [ ] Harden quorum-based distributed write behavior
- [ ] Slot assignment and rebalancing
- [ ] Node join/leave handling
- [ ] Health checking and failure detection

- [x] RFC0010 drafted for single-port memberlist transport
- [x] Added internal bootstrap and gossip seed discovery endpoints
- [ ] Implement memberlist custom internal transport (`Transport`)
- [ ] Wire gossip startup sequence to single-port flow
- [ ] Remove user-facing `registry.gossip.bind_addr/advertise_addr`

### 📋 Phase 3: Anti-Entropy

Background synchronization for replica convergence.

- [ ] Head/generation comparison for drift detection
- [ ] Background sync for missing blobs
- [ ] Tombstone propagation across replicas
- [ ] Orphan part cleanup

### 📋 Phase 4: Tiered Storage

Hot/warm/cold data lifecycle with S3 archival.

- [ ] Chunk-level archiving to S3
- [ ] On-demand restoration from archive
- [ ] LRU-based eviction policy
- [ ] Storage quota management

### 📋 Phase 5: Production Readiness

Operational capabilities for production deployment.

- [ ] Metrics and monitoring (Prometheus)
- [ ] Structured logging
- [ ] Configuration hot-reload
- [ ] Data migration tools
- [ ] Backup/restore functionality

### 📋 Phase 6: Performance Optimization

Performance and efficiency improvements.

- [ ] Chunk compression
- [ ] Read caching layer
- [ ] Parallel chunk operations
- [ ] Connection pooling optimization

---

## Quick Reference

### Current API

```
PUT    /api/v1/blobs/{path}                  # Create/update blob
GET    /api/v1/blobs/{path}                  # Read blob
HEAD   /api/v1/blobs/{path}                  # Read metadata
DELETE /api/v1/blobs/{path}                  # Delete blob
GET    /api/v1/blobs?prefix=<p>&limit=<n>    # List blobs
GET    /api/v1/healthz                       # Health check
GET    /api/v1/nodes                         # List cluster nodes
GET    /api/v1/slots/resolve?path=<blob_path> # Resolve slot
```

### Storage Layout

```
<disk>/
└── slots/
    └── <slot_id>/
        ├── meta.sqlite3
        └── blobs/
            └── <blob_path>/
                └── g.<generation>/
                    └── part.<index:08>.<sha256>
```

---

## Contributing

See individual issues for Phase 2-6 tasks. Suggested priority order:
1. Cluster coordination hardening
2. Anti-entropy
3. S3 archival and restore path
