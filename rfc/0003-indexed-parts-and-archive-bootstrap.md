# Amberio 设计文档（RFC 0003）

> 在 RFC 0002 基础上，进一步收敛为「小头信息 + 可选分片索引 + 按需回源」模型。

## 背景

在集群初始化时，节点可能仅能从 archive 后端（如 S3）扫描得到“对象列表”，并将对象头信息写入各 slot 的 `meta.sqlite3`。此时通常**没有每个 blob 的 chunks/parts 明细**。

RFC 0002 中 `meta.json` 内携带 `parts[]` 的方式有两个问题：

1. **大对象放大**：例如 2TB 对象，64MiB 分片需要 32,768 条 `parts` 记录，head 过大。
2. **导入路径不自然**：archive scan 阶段拿不到 `parts[]`，难以直接落地一致的 head。

本 RFC 解决上述问题，并明确 range 读取语义。

---

## 设计目标

1. `meta.json` 保持小且稳定，不再携带 `parts[]` 大列表。
2. part 命名支持固定序号：`part.{index:08}.{sha256}`。
3. 支持“仅对象清单导入”（无 part 列表）后仍可正确 range 读取。
4. 保持 MinIO 风格无状态提交：可见性由 head 提交决定，修复依赖 anti-entropy。

## 非目标

- 不引入分布式事务日志。
- 不在本 RFC 引入纠删码。

---

## 核心决策

### 决策 1：part 命名改为 index + hash

从：

- `part.{sha256}`

改为：

- `part.{index:08}.{sha256}`（示例 `part.00000001.abcd...`）

说明：

- `index` 明确表示逻辑顺序（range 计算更直接）。
- `sha256` 保持内容校验与幂等复用能力。

### 决策 2：BlobMeta 移除 `parts[]`

`meta.json` 仅保存“对象头”，建议字段：

```json
{
  "path": "videos/a.mp4",
  "slot_id": 731,
  "generation": 12,
  "size_bytes": 2199023255552,
  "etag": "...",
  "part_size": 67108864,
  "part_count": 32768,
  "part_index_state": "none",
  "archive_url": "s3://bucket/key",
  "updated_at": "2026-02-08T10:00:00Z"
}
```

`part_index_state`：

- `none`：无本地 part 索引（典型于 archive scan 导入）。
- `partial`：部分 part 已索引/本地化。
- `complete`：该 generation 的 part 索引完整。

### 决策 3：part 明细下沉到 SQLite 行级索引

继续使用 `file_entries`，但将 part 明细放为行级记录，而不是放进 `meta.json`：

- `file_kind='part'`
- `generation`（已存在）表示所属 head 代际
- 新增 `part_no INTEGER`（建议）
- `sha256/external_path/archive_url` 记录该分片位置与校验

建议索引：

```sql
CREATE INDEX IF NOT EXISTS idx_file_entries_part_lookup
ON file_entries(slot_id, blob_path, generation, part_no)
WHERE file_kind='part';
```

---

## 存储布局（推荐）

为避免同一 blob 多次改写后出现重复 `part.{index:08}` 混杂，建议按 generation 隔离目录：

```text
slots/{slot_id}/objects/{blobPath}/
├── g.{generation}/part.{index:08}.{sha256}
├── g.{generation}/part.{index:08}.{sha256}
└── ...
```

说明：

- 同一路径不同 generation 的 part 不混放，简化回收与排障。
- 可见版本由 `meta.json`（head）决定，不由目录扫描决定。

---

## 集群初始化：Archive Scan Import

### 导入输入

- 对象路径（`blob_path`）
- 对象总大小（`size_bytes`）
- ETag/版本信息（若可用）
- `archive_url`（例如 `s3://bucket/key`）

### 导入写入

每个对象仅写入：

1. `meta.json`（inline，`part_index_state='none'`）
2. 可选 tombstone（如果源端对象已标删）

**不要求**立即写入任何 `part` 行。

这样可以快速完成初始化，并允许后续读路径按需物化。

该初始化操作，应当有一个配置选项控制是否可选。

---

## Range 读取语义（重点）

给定 `Range: bytes=start-end`：

1. 读取当前 head（`meta.json`）。
2. 用 `part_size/part_count/size_bytes` 计算涉及的分片区间：
   - `first_part = start / part_size`
   - `last_part  = end / part_size`
3. 对每个 `part_no` 按优先级读取：

### 读取优先级

1. **本地 part 索引命中 + 本地文件存在**
   - 直接读 `part.{index:08}.{sha256}`。
2. **本地未命中或文件缺失，但有 archive_url**
   - 直接对 archive 对象发起子 Range 请求（按绝对字节区间）。
3. **可选：peer 补洞**
   - 若 archive 不可达，可尝试从同 slot 副本拉取。

### Read-through（建议）

对从 archive 拉取到的分片可执行懒物化：

- 写本地 `g.{generation}/part.{index:08}.{sha256}`（sha 可当场计算）
- 写/更新 `file_entries` 的 part 行
- 将 `part_index_state` 从 `none -> partial -> complete` 渐进推进

### 错误处理

- head 不存在：`404`
- head 为 tombstone：`410`
- archive 与 peer 都不可读取：`503`（或 `500`，建议 `503`）

---

## 写入路径（更新后）

PUT 过程改为：

1. 路由到 slot，生成 `write_id` 与 `next_generation`。
2. 按固定 `part_size` 切分；每片写：
   - `part.{index:08}.{sha256}`
   - `file_entries(part_no, generation, sha256, external_path)`
3. 构造小 `meta.json`（不含 `parts[]`，含 `part_count/part_size`）。
4. quorum 提交 head（`meta.json` UPSERT）。

可见性规则不变：**以 head 提交成功为准**。

---

## Anti-Entropy（更新后）

head 对齐逻辑保持 RFC 0002：

1. 先对比 slotlet digest（按 head 快照）。
2. 对不一致桶交换 `(path, head_kind, generation, head_sha256)`。
3. 按 generation/tombstone 规则裁决胜者。

差异点在 part 修复：

- 若 `part_index_state='complete'`：可按 `(part_no, sha256)` 精确补洞。
- 若 `part_index_state in ('none','partial')`：
  - 优先保证 head 收敛；
  - part 通过读路径懒物化或后台 materialize 任务逐步补齐。

这样可避免导入初期强制构建超大 part 清单。

---

## GC 与回收

建议按 generation 回收：

1. 每个 `blob_path` 找到当前可见 `generation`。
2. 清理旧 generation 的 part 文件目录（超过保留窗口）。
3. 清理无引用 part 行（或 archive 只读对象的冗余缓存行）。

generation 目录隔离下，GC 可以按 `g.N` 粒度执行，复杂度更低。

---

## 与 RFC 0002 的关系

RFC 0003 是对 RFC 0002 的收敛与扩展，不是推翻：

- 保留：slot 路由、file_entries、无状态提交、anti-entropy 主流程。
- 调整：`meta.json` 不再携带 `parts[]`，part 采用 `index+hash` 命名，支持 archive scan 无分片导入。

---

## 落地顺序（早期版本，直接迭代）

当前处于早期版本阶段，不引入双轨版本与复杂兼容策略，直接演进到 RFC 0003 目标形态。

1. 直接更新 `BlobMeta` 为小头模型（`part_count` / `part_size` / `part_index_state`），移除 `parts[]`。
2. 写入路径直接切换为 `part.{index:08}.{sha256}` 命名。
3. `file_entries` 直接增加 `part_no` 并建立 `(slot_id, blob_path, generation, part_no)` 索引。
4. 读取路径直接以新模型实现：
   - 先按 `(path, generation, part_no)` 查本地 part 索引；
   - 未命中则 archive range 回源；
   - 可选 read-through 落本地并补齐 part 索引。
5. anti-entropy 优先收敛 head，part 通过读路径懒物化或后台任务渐进补齐。
6. 不要求维护旧格式回退，减少实现分叉，优先快速闭环。

## 结论

对于“初始化只拿到 archive 对象列表、无 chunks 明细”的场景，推荐采用：

- `part.{index:08}.{sha256}` 命名；
- `meta.json` 去掉 `parts[]`，保留小头字段；
- part 明细下沉到 SQLite 行级索引（可懒物化）；
- range 读取支持直接 archive 回源 + read-through 缓存。

该方案在可扩展性、实现复杂度和运维可解释性之间取得更稳妥平衡。
