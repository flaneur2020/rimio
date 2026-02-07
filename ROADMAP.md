# AmberBlob Roadmap

This document tracks the implementation status of the AmberBlob design as specified in [RFC 0001](./rfc/0001-initial-design.md).

## Implementation Status Overview

| Component | Status | Notes |
|-----------|--------|-------|
| Basic HTTP API | 🟢 Implemented | Versioning support added |
| Content-addressed Chunks | 🟢 Implemented | SHA256 content-addressed |
| SQLite Metadata | 🟢 Implemented | RFC-compliant schema |
| Directory Structure | 🟢 Implemented | Per-blob chunk storage |
| Two-Phase Commit | 🟡 Framework | Core logic exists but needs integration |
| Registry (etcd/Redis) | 🟢 Implemented | Pluggable registry backends working |
| Multi-Version Support | 🟢 Implemented | (path, version) → blob_id mapping |
| Chunk-Level Archiving | 🟡 Schema Ready | Table created, logic pending |
| Tombstone (Soft Delete) | 🟢 Implemented | Soft delete with tombstone pattern |
| Anti-Entropy | 🔴 Not Started | Framework only |

Legend:
- 🟢 Implemented: Matches RFC specification
- 🟡 Partial: Partially implemented or differs from RFC
- 🔴 Not Started: Not yet implemented

---

## Completed Work

### Phase 1-4: Storage Layer Refactoring ✅

The following changes have been implemented:

#### 1. Database Schema
- ✅ Renamed `objects` table to `blobs` with RFC-compliant columns:
  - `pk` (autoincrement primary key)
  - `path`, `version` (composite unique key)
  - `blob_id` (ULID, replaces seq)
  - `size`, `chunks` (JSON array)
  - `created_at`, `tombstoned_at`
- ✅ Created `blob_chunk_archives` table for chunk-level archiving
- ✅ Added proper indexes (`idx_blobs_blob_id`, `idx_blobs_path`)

#### 2. Directory Structure
- ✅ Changed to RFC structure: `slots/<slot_id>/`
- ✅ Renamed `meta/metadata.db` to `meta.sqlite3`
- ✅ Implemented per-blob chunk storage: `blobs/<blob_id>/chunks/`

#### 3. API Updates
- ✅ Added `version` query parameter to GET/PUT/DELETE
- ✅ Return `blob_id` and `version` in responses
- ✅ Implemented soft delete (tombstone pattern)
- ✅ Added `include_tombstoned` flag to list API

#### 4. Type System
- ✅ Renamed `ObjectMeta` → `BlobMeta`
- ✅ Renamed `ChunkInfo` → `ChunkRef` (with `id`, `len` fields)
- ✅ Added `BlobChunkArchive` struct
- ✅ Added `BlobNotFound` error variant
- ✅ Kept legacy type aliases for backward compatibility

---

## Remaining Work

### Phase 5: Chunk-Level Archiving (Partial)

**Schema:** ✅ Complete
**Logic:** 🔴 Not Implemented

The `blob_chunk_archives` table is created but the archiving logic needs to be implemented:
- [ ] Implement archive operation to move chunks to S3
- [ ] Implement restore operation to fetch chunks from S3
- [ ] Add background archival task
- [ ] Update read path to check archive status and fetch from S3 if needed

### Phase 6: Anti-Entropy

**Status:** 🔴 Not Started

The anti-entropy protocol for replica synchronization needs to be implemented:
- [ ] Implement blob comparison by `blob_id` (ULID ordering)
- [ ] Implement missing blob sync between replicas
- [ ] Implement tombstone propagation
- [ ] Add background sync task

---

## Implementation Details

### Current API

```
# Write a blob (auto-version if not specified)
PUT /objects/{path}?version={version}
Response: { path, version, blob_id, chunks_stored }

# Read a blob (latest version if not specified)
GET /objects/{path}?version={version}&start={byte_start}&end={byte_end}

# Soft delete a blob (tombstone)
DELETE /objects/{path}?version={version}
Response: { deleted: true, path, version, tombstoned: true }

# List blobs (excludes tombstoned by default)
GET /objects?prefix={prefix}&limit={limit}&include_tombstoned={true|false}
```

### Storage Layout

```
<disk>/
└── slots/
    └── <slot_id>/
        ├── meta.sqlite3          # Blob metadata
        └── blobs/
            └── <blob_id>/
                └── chunks/
                    ├── <chunk_id_1>
                    └── ...
```

### Database Schema

```sql
-- Main blobs table
CREATE TABLE blobs (
    pk INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    version INTEGER NOT NULL,
    blob_id TEXT NOT NULL,
    size INTEGER NOT NULL,
    chunks TEXT NOT NULL,  -- JSON: [{"id": "...", "len": 64MB}, ...]
    created_at TEXT NOT NULL,
    tombstoned_at TEXT,
    UNIQUE (path, version)
);

-- Chunk archives table
CREATE TABLE blob_chunk_archives (
    pk INTEGER PRIMARY KEY AUTOINCREMENT,
    blob_id TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    length INTEGER NOT NULL,
    archived_at TEXT NOT NULL,
    target_path TEXT NOT NULL,
    target_version INTEGER,
    target_range_start INTEGER NOT NULL,
    target_range_end INTEGER NOT NULL,
    UNIQUE (blob_id, chunk_id)
);
```

---

## Notes

### Backward Compatibility

- HTTP API paths remain `/objects/*` for backward compatibility
- Legacy type aliases (`ObjectMeta`, `ChunkInfo`) are provided
- `ObjectNotFound` error is still used (equivalent to `BlobNotFound`)

### Migration from Old Schema

Existing deployments need data migration:
1. Rename `objects` table to `blobs`
2. Add new columns (`version`, `blob_id`, `tombstoned_at`)
3. Remove old columns (`seq`, `modified_at`, `archived`, `archive_location`)
4. Migrate chunk storage from shared directory to per-blob structure
5. Generate `blob_id` for existing records (using ULID or hash)

### Testing

All changes include unit tests:
- `test_chunk_store` - Tests per-blob chunk storage
- `test_compute_hash` - Tests SHA256 hash computation

---

*Last updated: After Phase 1-4 implementation*
