# Amberio 设计文档（RFC 0002）

> 基于文件系统的简化架构（接近 MinIO），并引入预分片 Slot

## 背景

RFC 0001 偏向“按 blob_id/chunk 管理”的实现路径，工程上可行，但实现复杂度偏高（chunk 生命周期、跨副本一致性、孤儿清理路径都较重）。

本 RFC 调整为更直接的对象存储模型：

- **对象路径优先**（`blobPath` 是一等键）
- **每个对象按“目录+文件”组织**（逻辑上接近 MinIO）
- **引入预分片 Slot**（写入前先哈希路由）
- **SQLite3 仅管理本地元信息**，数据块落盘为外部文件

---

## 设计目标

1. **尽可能简单**：优先保证可实现、可运维、可恢复。
2. **架构接近 MinIO**：对象按路径组织，元信息与数据文件解耦。
3. **保留 Slot 预分片能力**：通过 `hash(blobPath) -> slot` 固定路由。
4. **SQLite3 作为本地元信息真相源**：使用 `file_entries` 表管理逻辑文件。

## 非目标（本 RFC 不解决）

- 不引入复杂纠删码。
- 不引入 Raft/Paxos 等复杂共识协议。
- 不做在线重分片（slot 数量固定）。

---

## 总体架构

### 1) 预分片 Slot

- 集群固定 `slot_count`（默认 2048）。
- 每次请求先对 `blobPath` 做标准化，再哈希路由到唯一 slot。
- slot 到节点/磁盘的映射沿用现有路由层（可来自静态配置或注册中心）。

### 2) 每个 Slot 独立存储单元

每个 slot 目录：

```text
<disk>/slots/<slot_id>/
├── meta.sqlite3
└── objects/
    └── <blobPath>/
        ├── part.<sha256>
        ├── part.<sha256>
        └── ...
```

说明：

- `meta.json` 与 `tombstone.<sha256>` **逻辑上属于** `<blobPath>/`，但**物理上存 SQLite3**。
- `part.<sha256>` **物理上是文件系统外部 blob**（由 SQLite 记录其外部路径）。

### 3) 对象逻辑布局（每个 blob）

```text
{blobPath}/meta.json
{blobPath}/part.{sha256}
{blobPath}/part.{sha256}
{blobPath}/tombstone.{sha256}
```

- `meta.json`：当前可读版本的元信息（覆盖写）。
- `part.{sha256}`：对象数据分片文件（可多个）。
- `tombstone.{sha256}`：删除标记（追加写，支持保留历史删除事件）。

---

## Slot 路由规则

### Path 标准化

写入、读取、删除前，统一：

- 去掉前导 `/`
- 合并重复 `/`
- 拒绝 `..` 路径逃逸
- UTF-8 NFC（可选，但建议）

### 路由公式

```text
slot_id = hash64(normalized_blob_path) % slot_count
```

建议实现：`xxh3_64` 或 `sha256(path)` 前 8 字节转 `u64`。

> 若 `slot_count` 是 2 的幂（如 2048），可用按位与优化：`hash & (slot_count - 1)`。

---

## SQLite3 元信息模型（每 Slot）

`meta.sqlite3` 至少包含一张 `file_entries` 表。

```sql
CREATE TABLE IF NOT EXISTS file_entries (
    slot_id        INTEGER NOT NULL,
    blob_path      TEXT NOT NULL,
    file_name      TEXT NOT NULL, -- meta.json | part.<sha256> | tombstone.<sha256>
    file_kind      TEXT NOT NULL CHECK (file_kind IN ('meta', 'part', 'tombstone')),
    storage_kind   TEXT NOT NULL CHECK (storage_kind IN ('inline', 'external')),

    inline_data    BLOB,          -- 仅 meta/tombstone 使用
    external_path  TEXT,          -- part 的本地路径（相对 slot 根目录）
    archive_url    TEXT,          -- part 的归档位置（可选），如 s3://bucket/key?range=bytes=0-1048575

    size_bytes     INTEGER NOT NULL,
    sha256         TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,

    PRIMARY KEY (slot_id, blob_path, file_name),

    CHECK (
      (storage_kind = 'inline'  AND inline_data IS NOT NULL AND external_path IS NULL AND archive_url IS NULL)
      OR
      (storage_kind = 'external' AND inline_data IS NULL AND (external_path IS NOT NULL OR archive_url IS NOT NULL))
    )
);

CREATE INDEX IF NOT EXISTS idx_file_entries_blob_kind_time
ON file_entries(slot_id, blob_path, file_kind, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_file_entries_prefix
ON file_entries(slot_id, blob_path);

CREATE INDEX IF NOT EXISTS idx_file_entries_archive_url
ON file_entries(slot_id, archive_url)
WHERE archive_url IS NOT NULL;
```

### 记录规则

- `meta.json`
  - `file_kind='meta'`, `storage_kind='inline'`
  - `inline_data` 为 JSON 文本
  - 对同一路径执行 **UPSERT 覆盖**
- `tombstone.<sha256>`
  - `file_kind='tombstone'`, `storage_kind='inline'`
  - 追加写（不覆盖），文件名由内容哈希决定
- `part.<sha256>`
  - `file_kind='part'`, `storage_kind='external'`
  - `external_path` 指向 `objects/<blobPath>/part.<sha256>`（本地仍在热数据时）
  - `archive_url` 可选；当 part 进入冷存（如 S3）时记录远端地址和 range

`archive_url` 格式建议：

- `s3://<bucket>/<object_path>?range=bytes=<start>-<end>`
- 示例：`s3://amberio-archive/slot-731/pack-00042?range=bytes=1048576-2097151`

### 执行层约束

- **可见性真相源**：`file_entries` 中的 `meta.json` / `tombstone.*`。
- **写入状态**：不引入持久化事务日志，写入状态仅存在于单次请求生命周期。
- **反熵输入**：基于当前 head 快照（而非操作日志）做差异修复。

---

## `meta.json` / tombstone 内容建议

### `meta.json`（示例）

```json
{
  "path": "images/2026/a.png",
  "slot_id": 731,
  "generation": 12,
  "size_bytes": 1049600,
  "etag": "b4d6...",
  "parts": [
    {"name": "part.a1...", "sha256": "a1...", "offset": 0, "length": 1049600}
  ],
  "updated_at": "2026-02-08T10:00:00Z"
}
```

### `tombstone.<sha256>`（示例）

```json
{
  "path": "images/2026/a.png",
  "slot_id": 731,
  "generation": 13,
  "deleted_at": "2026-02-08T10:05:00Z",
  "reason": "api-delete"
}
```

---

## 核心流程（重点）

### PUT / 写入路径（重点）

> 目标：尽量接近 MinIO 的无状态模型：请求结束即“无事务状态残留”，收敛依赖后台 healing。

#### 角色

- **Ingress 节点（协调者）**：接收客户端请求，驱动一次写入事务。
- **Slot 副本集合**：该 `slot_id` 的 N 个副本节点（如 3 副本）。
- **Quorum**：`W = floor(N/2) + 1`，写入成功至少需要 W 个副本提交。

#### 时序（PUT）

1. **路由**：标准化 `blobPath`，计算 `slot_id`，获取健康副本集合。
2. **生成写入上下文**：创建 `write_id`（ULID/UUID），读取当前 head 得到 `next_generation`。
3. **数据阶段（无状态）**：
   - 请求体按 `part_size` 切片；
   - 每片写 `part.<sha256>.tmp`，`fsync + rename` 到最终路径；
   - 已存在同名 part 则复用（内容寻址去重）。
4. **数据副本 fanout**：协调者把 part 写到所有目标副本；当 `part_acks >= W` 进入提交阶段。
5. **构造新 head**：生成 `meta.json`（包含 `generation`、`write_id`、parts、size、etag）。
6. **元信息提交（并行发到副本）**：每个副本在一个 SQLite 事务内：
   - 可选 CAS：仅当本地 head generation < `next_generation` 时覆盖；
   - UPSERT `file_entries` 的 `meta.json`；
   - UPSERT 本次涉及的 `part.<sha256>` external 引用；
7. **成功返回**：`commit_acks >= W` 即对客户端返回成功；未提交副本后续由 anti-entropy 修复。

#### 失败与恢复（无状态）

- **协调者崩溃/超时**：不依赖持久化事务日志；客户端使用同一 `write_id` 重试，副本按 `write_id + generation` 幂等处理。
- **部分副本已提交**：读写以 quorum 成功为准，落后副本依赖 anti-entropy 拉齐 head 与 parts。
- **遗留 part 文件**：若最终未被任何 `meta.json` 引用，交由 GC 延迟回收（如 24h 宽限）。

可见性规则：**以 `file_entries` 中 head（`meta.json` / tombstone）提交成功为准**。

### GET / 读取路径

1. 路由到 `slot_id`，优先读本地副本。
2. 查询该 `blobPath` 最新 head：
   - 若 head 是 `tombstone.*`，返回 `404/410`；
   - 若 head 是 `meta.json`，解析 `parts`。
3. 顺序读取 `part.<sha256>` 文件并流式返回；若本地缺 part，可按策略回源同 slot 其他副本。

### DELETE / 删除路径

1. 路由并决议 `next_generation`。
2. 生成 tombstone JSON，计算 `tombstone.<sha256>`。
3. 并行提交 tombstone 到副本，`commit_acks >= W` 即成功：
   - 在 `file_entries` 插入 tombstone（inline）；
4. part 不同步删除，交由 GC 延迟回收。

---

## Anti-Entropy 执行流程（重点）

> 目标：让 slot 副本在“节点故障、网络抖动、部分提交”后最终收敛。

### 触发方式

- 周期任务（默认每 30s 每 slot 一轮）。
- 节点重启后对本机持有 slot 立即补跑。
- 读路径发现本地缺 part 时可触发该路径的快速修复。

### 三阶段流程（无操作日志）

#### 阶段 A：头部快照与摘要

每个副本基于本地 `file_entries` 计算 slot 快照，不依赖任何 oplog：

- 对每个 `blob_path` 提取当前 head：`(head_kind, generation, head_sha256)`；
- 将路径按前缀分桶（例如前 2 字节）；
- 计算每个 slotlet 的 `slotlet_digest`。

#### 阶段 B：slotlet 级 diff

1. 与 peer 交换 `slotlet_digest`。
2. 仅对不一致桶，交换该桶的 head 列表：
   - `blob_path -> (head_kind, generation, head_sha256)`。
3. 对每个冲突路径做确定性裁决：
   - 优先比较 `generation`；
   - generation 相同时，`tombstone` 胜过 `meta`；
   - 若仍相同，按 `head_sha256` 字典序定胜负。

#### 阶段 C：对象修复（healing）

1. 若胜出 head 是 `meta.json`：
   - 拉取 `meta.json` inline 数据；
   - 按 `meta.parts` 校验本地 `part.<sha256>` 是否存在且哈希正确；
   - 缺失 part 从胜出副本拉取。
2. 若胜出 head 是 `tombstone.*`：
   - 仅拉取 tombstone inline 数据并写入本地。
3. 本地应用使用 UPSERT，保证幂等；修复可重复执行。

> 可选增强：后台 `scrub` 周期校验 part 文件哈希，发现坏块后触发同路径 healing。

### Anti-Entropy 的幂等与收敛保证

- `part.<sha256>` 内容寻址，重复拉取可安全跳过。
- `file_entries` 通过 `(slot_id, blob_path, file_name)` 主键 UPSERT，天然幂等。
- 差异检测仅依赖“当前状态快照”，不依赖请求期状态或持久化事务日志。
- 在网络恢复后，所有副本最终收敛到同一 head 集合（最终一致）。

### 关键参数建议

- `anti_entropy_interval`: 30s
- `anti_entropy_batch_objects`: 1000
- `anti_entropy_slotlet_prefix_len`: 2
- `repair_part_parallelism`: 8

---

## 垃圾回收（GC）

按 slot 周期执行：

1. 扫描所有“可见 `meta.json`”构建 `reachable_parts` 集合。
2. 列举 `objects/<blobPath>/part.*` 实际文件。
3. 删除不在 `reachable_parts` 且超过宽限期（如 24h）的 part 文件。
4. 清理过旧 tombstone（保留窗口可配置）。

---

## 与 RFC 0001 的主要差异

| 维度 | RFC 0001 | RFC 0002 |
|---|---|---|
| 数据组织 | `blob_id/chunks` 目录 | `blobPath` 逻辑目录 + `part.<sha256>` |
| 元信息模型 | `blobs` 表 + chunk JSON | 统一 `file_entries`（meta/tombstone/part 引用） |
| 对象定位 | path/version -> blob_id | path -> meta.json -> part 列表 |
| 删除语义 | `tombstoned_at` 字段 | `tombstone.<sha256>` 逻辑文件 |
| 复杂度 | 偏高（版本/chunk 生命周期） | 更直接，接近 MinIO 心智模型 |

---

## 实施建议（最小落地顺序）

1. 先落地单机单副本：`slot hash + file_entries + part external file`。
2. 完成 PUT/GET/DELETE/LIST 的本地一致语义。
3. 增加 slot 级 GC。
4. 最后叠加跨节点复制与反熵。

---

## 结论

RFC 0002 将 Amberio 的核心从“chunk/事务驱动”收敛为“对象路径驱动”：

- 路由层保留预分片 slot；
- 存储层采用 MinIO 风格对象布局；
- 元信息统一进入 SQLite `file_entries`，part 数据落文件系统 external blob。

该方案实现路径更短、运维心智更统一，并为后续复制与反熵保留扩展空间。

---

## HTTP API 设计（对外 / 对内）

> 本节定义 RFC 0002 的 API 契约。目标是：客户端接口稳定、节点间接口最小化、支持无状态写入与后台 healing。

### 通用约定

- 协议：HTTP/1.1（可升级 HTTP/2）。
- 编码：
  - 对象内容使用原始二进制流。
  - 控制面请求与响应使用 `application/json`。
- 认证（建议）：
  - 外部 API：AK/SK 或 Bearer Token（后续 RFC 细化）。
  - 内部 API：mTLS + `X-Amberio-Node-Id`。
- 幂等键：`X-Amberio-Write-Id`（PUT/DELETE 建议必传）。
- 追踪：`X-Request-Id`（可选，服务端透传）。

### 对外 API（Client-Facing）

#### 1) 健康与路由

- `GET /api/v1/healthz`
  - 用途：节点存活与就绪检查。
  - `200 OK` 示例：

```json
{
  "status": "ok",
  "node_id": "edge-node-001",
  "group_id": "edge-cluster-001"
}
```

- `GET /api/v1/nodes`
  - 用途：返回当前 group 节点视图。
  - `200 OK` 示例：

```json
{
  "nodes": [
    {"node_id": "edge-node-001", "address": "127.0.0.1:8080", "status": "healthy"}
  ]
}
```

- `GET /api/v1/slots/resolve?path=<blobPath>`
  - 用途：按 path 解析 `slot_id` 与副本列表（便于调试和集成测试）。
  - `200 OK` 示例：

```json
{
  "path": "images/a.png",
  "slot_id": 731,
  "replicas": ["edge-node-001", "edge-node-002", "edge-node-003"],
  "write_quorum": 2
}
```

#### 2) 对象读写

- `PUT /api/v1/blobs/{blobPath}`
  - 请求头：
    - `Content-Type: application/octet-stream`
    - `X-Amberio-Write-Id: <uuid>`（幂等）
    - 可选 `If-Match` / `If-None-Match`
  - 语义：
    - 无状态写入，写入流程遵循本 RFC 的 quorum 语义。
    - 同一 `(blobPath, write_id)` 重试必须幂等。
  - `201 Created`（首次提交）示例：

```json
{
  "path": "images/a.png",
  "slot_id": 731,
  "generation": 12,
  "etag": "b4d6...",
  "size_bytes": 1049600,
  "committed_replicas": 2
}
```

  - `200 OK`（幂等重试命中）示例：

```json
{
  "path": "images/a.png",
  "generation": 12,
  "etag": "b4d6...",
  "idempotent_replay": true
}
```

- `GET /api/v1/blobs/{blobPath}`
  - 支持 `Range: bytes=start-end`。
  - 成功返回对象二进制流。
  - 返回头建议：
    - `ETag: <etag>`
    - `X-Amberio-Generation: <generation>`
    - `Content-Length`
  - 错误：`404 Not Found`（不存在）、`410 Gone`（被 tombstone 覆盖）。

- `HEAD /api/v1/blobs/{blobPath}`
  - 不返回 body，仅返回元信息头：`ETag`、`X-Amberio-Generation`、`Content-Length`。

- `DELETE /api/v1/blobs/{blobPath}`
  - 请求头：`X-Amberio-Write-Id`（建议）。
  - 语义：写入 `tombstone.<sha256>` 到 quorum。
  - 成功：`200 OK` 或 `204 No Content`。

- `GET /api/v1/blobs?prefix=<p>&limit=<n>&cursor=<c>&include_deleted=false`
  - 用途：按前缀列举。
  - `200 OK` 示例：

```json
{
  "items": [
    {
      "path": "images/a.png",
      "generation": 12,
      "etag": "b4d6...",
      "size_bytes": 1049600,
      "deleted": false,
      "updated_at": "2026-02-08T10:00:00Z"
    }
  ],
  "next_cursor": null
}
```

### 对内 API（Node-to-Node）

> 对内 API 只服务节点间 fanout、读取补洞、anti-entropy 与 healing。

#### 1) 写入 fanout

- `PUT /internal/v1/slots/{slot_id}/blobs/{blobPath}/parts/{sha256}`
  - 请求头：
    - `X-Amberio-Write-Id`
    - `X-Amberio-Generation`
    - `X-Amberio-Part-Offset`
    - `X-Amberio-Part-Length`
  - 请求体：part 原始 bytes。
  - 语义：
    - 若本地已有同 hash part，返回复用。
    - 重试幂等。
  - `200 OK` 示例：

```json
{
  "accepted": true,
  "reused": false,
  "sha256": "a1..."
}
```

- `PUT /internal/v1/slots/{slot_id}/blobs/{blobPath}/head`
  - 请求体：`meta.json` 或 `tombstone.*` 的 JSON 内容。
  - 关键字段：`head_kind`（`meta`/`tombstone`）、`generation`、`head_sha256`。
  - 语义：CAS 覆盖（仅新 head 覆盖旧 head）。
  - `200 OK` 示例：

```json
{
  "applied": true,
  "head_kind": "meta",
  "generation": 12
}
```

- `GET /internal/v1/slots/{slot_id}/blobs/{blobPath}/head`
  - 返回当前 head 摘要与内联内容（meta/tombstone）。

#### 2) 对象拉取与补洞

- `GET /internal/v1/slots/{slot_id}/parts/{sha256}`
  - 用途：拉取缺失 part。
  - 返回：part 原始二进制流。

#### 3) Anti-Entropy / Healing

- `GET /internal/v1/slots/{slot_id}/heal/slotlets?prefix_len=2`
  - 返回该 slot 的桶摘要。
  - `200 OK` 示例：

```json
{
  "slot_id": 731,
  "prefix_len": 2,
  "slotlets": [
    {"prefix": "0a", "digest": "f1...", "objects": 42}
  ]
}
```

- `POST /internal/v1/slots/{slot_id}/heal/heads`
  - 请求体：`{"prefixes": ["0a", "0b"]}`
  - 返回：每个前缀下 `blob_path -> (head_kind, generation, head_sha256)` 列表。

- `POST /internal/v1/slots/{slot_id}/heal/repair`
  - 请求体示例：

```json
{
  "source_node_id": "edge-node-001",
  "blob_paths": ["images/a.png"],
  "dry_run": false
}
```

  - 返回示例：

```json
{
  "slot_id": 731,
  "repaired_objects": 1,
  "skipped_objects": 0,
  "errors": []
}
```

### 错误码约定

- `400 Bad Request`：参数非法、路径非法。
- `401/403`：认证或鉴权失败。
- `404 Not Found`：对象不存在。
- `409 Conflict`：`If-Match` / generation CAS 冲突。
- `410 Gone`：对象被 tombstone 删除。
- `412 Precondition Failed`：前置条件失败。
- `429 Too Many Requests`：节点过载限流。
- `503 Service Unavailable`：quorum 不满足或节点不可用。

### 与核心流程的映射

- 写入路径：`PUT /api/v1/blobs/*` -> 多次 `PUT /internal/.../parts/*` -> `PUT /internal/.../head`。
- 反熵路径：`GET /internal/.../heal/slotlets` -> `POST /internal/.../heal/heads` -> `POST /internal/.../heal/repair`。

---

## Integration Test 设计（integration/）

> 测试目标：验证 RFC 0002 HTTP API 契约，覆盖外部 API、内部 API、以及节点故障后的 healing。

### 目录结构

```text
integration/
├── _harness.py
├── 001_cluster_bootstrap.py
├── 002_external_blob_crud.py
├── 003_internal_healing.py
└── run_all.py
```

### 自举策略

- 每个 case 都通过 `integration/_harness.py`：
  - 生成唯一 `group_id`。
  - 为每个节点生成配置文件和数据目录。
  - 启动多节点 `amberio server` 进程。
- Redis 不由脚本拉起，默认直连：`redis://127.0.0.1:6379`。

### Case 说明

- `001_cluster_bootstrap.py`
  - 验证 `GET /api/v1/healthz`、`GET /api/v1/nodes`、`GET /api/v1/slots/resolve`。

- `002_external_blob_crud.py`
  - 验证 `PUT/GET/HEAD/LIST/DELETE`。
  - 验证 `X-Amberio-Write-Id` 幂等重试语义。

- `003_internal_healing.py`
  - 先下线一个节点制造数据滞后。
  - 写入对象后重启滞后节点。
  - 通过 `heal/slotlets + heal/repair` 修复并校验读取成功。

### 运行方式

- 跑全部：`python3 integration/run_all.py --build-if-missing`
- 跑单个：`python3 integration/002_external_blob_crud.py --build-if-missing`

> 这些测试是 API 契约测试：当实现完成后，应作为回归测试长期保留。
