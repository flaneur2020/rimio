# AmberBlob 设计文档 (RFC 0001)

> 面向边缘云节点的轻量级 blob 存储

## 设计目标

- **牺牲扩展性，换取维护简单性**
- **无中心 Leader**：任意节点可处理请求，对活跃副本执行 2PC
- **固定拓扑**：组内 2048 slot，机器数量固定，不可动态扩容

适用于小规模边缘集群（3-10 台机器）。

## 核心设计

### Slot（固定存储单元）

| 属性 | 说明 |
|------|------|
| 数量 | 每组固定 2048 个 slot |
| 扩展性 | **组内不可扩展** |
| etcd 存储 | slot 路由表（slot → [机器/盘] 列表）+ Slot 副本健康状态、每个 path 的最大版本号及对应的 blob_id |
| 元信息 | 每个 slot 本地 SQLite3 自管理，**不存 etcd** |

### 本地存储格式

每个 slot 在本地磁盘上的目录结构：

```
<disk_path>/slots/<slot_id>/
├── meta.sqlite3          # blob 元信息数据库
└── blobs/              # blob 存储根目录
    └── <blob_id>/      # 每个 blob 独立目录
        └── chunks/       # blob 的 chunk 存储目录
            ├── <chunk_id_1>
            ├── <chunk_id_2>
            └── ...
```

#### ChunkID 格式

ChunkID 采用 chunk 内容的 SHA256 哈希值：

- **格式**: `<chunk_sha256>` (64 字符十六进制)

示例：
```
a1b2c3d4e5f6789012345678901234567890abcd1234567890abcdef12345678
```

#### meta.sqlite3 表结构

```sql
-- blob 元信息表
-- blob_id 本身即为 ULID，天然有序，用于 slot 同步
-- 每个 (path, version) 对应唯一的 blob_id
CREATE TABLE blobs (
    pk INTEGER PRIMARY KEY AUTOINCREMENT,  -- 自增主键
    path TEXT NOT NULL,              -- blob 路径
    version INTEGER NOT NULL,        -- 版本号，从 1 开始递增
    blob_id TEXT NOT NULL,         -- blob 唯一ID (ULID)
    size INTEGER NOT NULL,           -- blob 大小
    chunks TEXT NOT NULL,            -- chunk 列表 (JSON: [{"id": "...", "len": 64MB}, ...])
    created_at DATETIME NOT NULL,     -- 创建时间戳
    tombstoned_at DATETIME,          -- 删除标记时间（NULL 表示未删除）
    UNIQUE (path, version)
);

-- 索引：通过 blob_id 查找 blob
CREATE INDEX idx_blobs_blob_id ON blobs(blob_id);

-- Chunk 归档状态跟踪表（仅存储已归档的 chunk）
CREATE TABLE blob_chunk_archives (
    pk INTEGER PRIMARY KEY AUTOINCREMENT,  -- 自增主键
    blob_id TEXT NOT NULL,                -- blob ID
    chunk_id TEXT NOT NULL,                 -- chunk ID (SHA256)
    length INTEGER NOT NULL,                -- chunk 实际长度
    archived_at DATETIME NOT NULL,          -- 归档时间
    target_path TEXT NOT NULL,      -- 归档目标路径（如 S3 URL）
    target_version INT,
    target_range_start INTEGER,     -- 归档文件中的起始位置
    target_range_end INTEGER,       -- 归档文件中的结束位置
    UNIQUE (blob_id, chunk_id),
    FOREIGN KEY (blob_id) REFERENCES blobs(blob_id)
);

-- 索引：用于 chunk_id 查询
CREATE INDEX idx_blob_chunk_archives_chunk ON blob_chunk_archives(chunk_id);
```

### 数据模型

- **Chunk**: 64MB 固定分块，ChunkID 为 chunk 内容的 SHA256
- **ChunkID**: 内容寻址（64字符十六进制 SHA256）
- **blob 元信息**: (path, version) 对应唯一的 blobID(ULID)、大小、chunk 列表
- **一致性**: blob_id 本身为 ULID，天然有序，用于判断副本一致性

### 写入流程

```
1. Client → 任意节点（携带 path 和可选的 version）
2. 查询 etcd 获取 slot 副本位置，筛选健康节点
3. 若健康节点数 < 最小写入副本数（如 3），拒绝写入
4. 生成新的 blob_id (ULID)，确定 version（未指定则取当前最大 version + 1）
5. 异步复制 chunk 数据到筛选出的副本，chunk 保存在 blobs/{blobID}/chunks/ 下
6. 执行 2PC 提交元信息；若失败，chunk 成为孤儿数据（后台清理）
```

### 读取流程

- 查询 etcd 路由表定位副本
- 指定 version 则读取对应版本，未指定则读取最新版本
- 本地有数据则直接返回，否则代理转发

### 故障恢复（Anti-Entropy）

副本下线恢复后，自主查询其他副本获取 blob_id 最新的 blob 列表，复制缺失数据，无需中心协调。

## 归档

- **策略**: Pick-of-Two LRU 策略选择归档目标（按 blob 维度）
- **流程**: chunk 级别归档到 S3 → 写入 blob_chunk_archives 表记录归档信息
- **读取**: 按需从 S3 拉取指定 chunk，本地未归档的 chunk 直接读取
- **版本管理**: 归档可以针对特定版本进行，旧版本 blob 可整体归档到冷存储

## 关键权衡

| 方面 | 选择 | 原因 |
|------|------|------|
| 扩展性 | 固定 2048 slot，组内不可扩 | 边缘规模固定，简化一致性 |
| 架构 | 无 Leader，任意节点 2PC | 消除单点，提高可用性 |
| 一致性 | 2PC 提交元信息为写入成功标准 | 数据与元信息强一致 |
| 恢复 | 副本自主反熵 | 无需中心协调 |

## 配置示例

```yaml
node:
  node_id: "edge-node-001"
  group_id: "edge-cluster-001"
  disks:
    - path: /data/disk1
    - path: /data/disk2

etcd:
  endpoints: ["etcd1:2379", "etcd2:2379", "etcd3:2379"]

archive:
  type: s3
  s3:
    bucket: "amberblob-archive"
    region: "us-east-1"
    credentials:
      access_key_id: "YOUR_ACCESS_KEY_ID"
      secret_access_key: "YOUR_SECRET_ACCESS_KEY"
```

## 参考

- [Content Addressable Storage](https://en.wikipedia.org/wiki/Content-addressable_storage)
- [Two-Phase Commit](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
