# Amberio Roadmap

## Project Vision

A lightweight, fixed-topology blob storage system for edge cloud nodes, featuring strong consistency, multi-version support, and tiered storage with S3 archival.

---

## Milestones

### ✅ Phase 1: Core Storage (Completed)

RFC-compliant storage layer with versioning and soft delete.

- [x] Database schema (`blobs` table, `blob_chunk_archives` table)
- [x] Per-blob chunk storage (`blobs/{blob_id}/chunks/`)
- [x] Multi-version support with (path, version) → blob_id mapping
- [x] Tombstone-based soft delete
- [x] HTTP API with versioning support

### 🔄 Phase 2: Cluster Coordination (In Progress)

Distributed consensus and node management.

- [ ] Complete 2PC integration for distributed writes
- [ ] Slot assignment and rebalancing
- [ ] Node join/leave handling
- [ ] Health checking and failure detection

### 📋 Phase 3: Anti-Entropy

Automatic replica synchronization.

- [ ] Blob comparison by blob_id (ULID ordering)
- [ ] Background sync for missing blobs
- [ ] Tombstone propagation across replicas
- [ ] Orphan chunk cleanup

### 📋 Phase 4: Tiered Storage

Hot/warm/cold storage with S3 archival.

- [ ] Chunk-level archiving to S3
- [ ] On-demand restoration from archive
- [ ] LRU-based eviction policy
- [ ] Storage quota management

### 📋 Phase 5: Production Readiness

Operational features for production deployment.

- [ ] Metrics and monitoring (Prometheus)
- [ ] Structured logging
- [ ] Configuration hot-reload
- [ ] Data migration tools
- [ ] Backup/restore functionality

### 📋 Phase 6: Performance Optimization

Scalability and efficiency improvements.

- [ ] Chunk compression
- [ ] Read caching layer
- [ ] Parallel chunk operations
- [ ] Connection pooling optimization

---

## Quick Reference

### Current API

```
PUT   /objects/{path}?version=N        # Create/update blob
GET   /objects/{path}?version=N        # Read blob (latest if no version)
DELETE /objects/{path}?version=N       # Soft delete blob
GET   /objects?prefix=P&limit=N        # List blobs
GET   /health                          # Health check
GET   /nodes                           # List cluster nodes
GET   /slots/{id}                      # Get slot info
```

### Storage Layout

```
<disk>/
└── slots/
    └── <slot_id>/
        ├── meta.sqlite3
        └── blobs/
            └── <blob_id>/
                └── chunks/
                    └── <chunk_id>
```

---

## Contributing

See individual issues for Phase 2-6 tasks. Priority order:
1. 2PC completion
2. Anti-entropy
3. S3 archival
