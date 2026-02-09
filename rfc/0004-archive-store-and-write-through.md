# Amberio 设计文档（RFC 0004）

> 在 RFC 0003 基础上，补齐 archive 的完整实现闭环：统一抽象、写入归档、读取回源、初始化扫描对齐。

## 背景

目前代码已具备两段能力：

1. `init_scan` 可从外部列表导入对象头（`meta.json`）；
2. 读取路径可基于 `archive_url` 执行按需回源（目前主要是 Redis URL）。

但 archive 仍存在缺口：

- 缺少统一的 `ArchiveStore` 读写接口（当前偏“只读+散落实现”）；
- PUT 路径不会自动写入 archive，导致 `archive_url` 主要依赖外部注入；
- 初始化扫描、读取回源、归档写入未形成同一抽象层；
- 列表接口未定义分页契约，不利于大规模初始化导入。

本 RFC 定义一个可落地的“完整 archive 实现（V1）”。

---

## 设计目标

1. `storage` 层提供统一 `ArchiveStore` 抽象，覆盖 list/read/write。
2. list 接口提供分页能力（cursor + limit）用于初始化扫描。
3. PUT 成功提交后可将对象写入 archive，并在 head 中写入 `archive_url`。
4. 读取路径在本地 part 缺失时可稳定回源 archive，并保留 read-through 行为。
5. `init_scan` 继续复用 `ArchiveStore` 抽象，不再散落 backend 细节。
6. 不改变“head 为可见性真相源”的语义。

## 非目标

- 本 RFC 不引入跨对象打包（pack）与压缩。
- 本 RFC 不引入后台迁移/分级策略（如 LRU、冷热策略执行器）。
- 本 RFC 不强制引入新的分布式事务机制。

---

## 核心决策

### 决策 1：`ArchiveStore` 接口统一为分页 list/read/write

`ArchiveStore` 位于 `amberio-core/src/storage/`，提供：

- `list_blobs_page(list_key, cursor, limit)`：供 `init_scan` 分页扫描；
- `list_blobs(list_key)`：便捷方法（内部迭代分页）；
- `read_range(object_key, start, end)`：供 GET range 回源；
- `write_blob(object_key, body)`：供 PUT 归档落地；
- `archive_url_for_key(object_key)`：构造可持久化的 `archive_url`。

说明：

- V1 保持“对象级归档”（whole-object），range 回源时在 archive 侧切片；
- 归档定位由 `archive_url` 承载，head 只保存 URL，不保存 archive 内部私有状态。

### 决策 2：PUT 路径支持可选 write-through archive

当配置了 archive backend 时：

1. 正常执行本地 part 落盘与副本复制；
2. 在 head 提交前完成 archive 写入；
3. `BlobMeta.archive_url` 写入最终归档地址。

若 archive 未配置，则行为与当前一致（`archive_url = None`）。

### 决策 3：读取路径继续使用 archive fallback + read-through

读取 part 失败后：

1. 先尝试 archive range 回源；
2. 回源成功则继续 read-through：本地写 part + upsert part 索引；
3. archive 失败再尝试 peer 拉取。

与 RFC 0003 语义保持一致。

---

## 配置约定（V1）

`archive` 作为初始化期配置写入 registry，并在运行期从 bootstrap state 读取。

示例（Redis backend，便于测试）：

```yaml
archive:
  archive_type: redis
  redis:
    url: "redis://127.0.0.1:6379"
    key_prefix: "amberio:archive"
```

说明：

- `key_prefix` 用于 PUT 归档键名前缀；
- 原有 `s3` 字段保留，作为后续 backend 扩展入口。

`init_scan` 分页参数示例：

```yaml
init_scan:
  enabled: true
  redis:
    url: "redis://127.0.0.1:6379"
    list_key: "amberio:init-scan:list"
    page_size: 500
```

---

## 错误语义

- 若对象 head 存在但本地 part 缺失：
  - archive 可读：返回成功，并触发 read-through；
  - archive 不可读且 peer 不可读：返回 `503/500`（现有约定）。
- PUT 若 quorum 已满足但 archive 写入失败：
  - V1 选择“请求失败优先”（返回失败，不写入带 archive_url 的 head）；
  - 以避免返回成功但 `archive_url` 不可用的隐性不一致。

---

## 实施步骤

1. 扩展 `ArchiveStore` trait 与 Redis 实现（分页 list/read/write）。
2. 在 `PutBlobOperation` 注入可选 archive writer，写入并设置 `meta.archive_url`。
3. `cluster::state` 初始化扫描改为分页遍历 list。
4. 增加集成用例覆盖“PUT→archive→删除本地part→GET回源”。

---

## 验证标准

新增集成 case（`011`）需覆盖：

1. 配置 archive(redis) 后，PUT 对象后 head 中 `archive_url` 非空；
2. Redis archive key 中可读取完整对象；
3. 删除本地 part 文件后，GET 仍可返回正确数据（archive fallback 生效）；
4. 回源后 part 会重新物化到本地（read-through）。

---

## 与 RFC 0003 的关系

RFC 0003 定义了“小头 + 可选分片索引 + 按需回源”的模型。

RFC 0004 不改变模型，只补齐工程实现：

- 从“可回源”提升为“可写入 + 可回源 + 可初始化导入”完整闭环；
- archive 能力集中到 storage 抽象，减少 operations/cluster 中的 backend 细节泄漏。
