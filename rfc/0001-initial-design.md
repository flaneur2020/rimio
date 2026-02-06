# AmberBlob 设计文档 (RFC 0001)

> 面向边缘云节点的轻量级对象存储

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
| etcd 存储 | slot 路由表（slot → [机器/盘] 列表）+ Slot 副本健康状态、slot 中元信息的最新 seq 位置 |
| 元信息 | 每个 slot 本地 SQLite3 自管理，**不存 etcd** |

### 数据模型

- **Chunk**: 64MB 固定分块，SHA256 内容寻址
- **对象元信息**: 路径、大小、chunk 列表、ULID(seq)
- **Seq**: 每个副本独立维护 ULID 序列号，用于一致性判断

### 写入流程

```
1. Client → 任意节点
2. 查询 etcd 获取 slot 副本位置，筛选健康且 seq 最新的节点
3. 若健康且 seq 最新节点数 < 最小写入副本数（如 3），拒绝写入
4. 异步复制 chunk 数据到筛选出的副本，chunk 保存在文件系统
5. 执行 2PC 提交元信息；若失败，chunk 成为孤儿数据（后台清理）
```

### 读取流程

- 查询 etcd 路由表定位副本
- 本地有数据则直接返回，否则代理转发

### 故障恢复（Anti-Entropy）

副本下线恢复后，自主查询其他副本获取最新 seq，复制缺失数据，无需中心协调。

## 归档

- **策略**: Pick-of-Two LRU 策略选择归档目标
- **流程**: 整对象同步到 S3 → Slot 保留元信息 + 归档标记（记录 s3 path + chunk range）
- **读取**: 按需从 S3 拉取指定 range

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
