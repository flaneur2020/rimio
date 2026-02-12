# Rimio 设计文档（RFC 0008）

> 提议引入“Redis Cluster 风格（无中心）+ CRDT 收敛 + Gossip 传播”的 registry 子系统，逐步替换当前对外部 Redis 的强依赖。

## 背景

当前 README 与本地集成流程都依赖可达的 Redis（例如 `redis://127.0.0.1:6379`）。

在现有实现中，registry 承担了三类职责：

1. 节点注册与心跳可见性（`register_node` / `get_nodes`）
2. slot 元数据与副本健康状态查询
3. 集群 bootstrap 状态（`set_bootstrap_state_if_absent`）

这使部署路径对单个外部组件有显式依赖，和 Rimio 在小型边缘集群“低运维依赖”的目标并不完全一致。

Redis Cluster 的核心启发是：

- 每个节点都持有槽位视图
- 通过 gossip 传播状态
- 通过 epoch 与故障投票完成最终收敛

本 RFC 在该思想基础上，进一步引入 CRDT 语义，约束合并规则，确保并发更新与网络分区后的可预测收敛。

---

## 设计目标

1. **去中心化 registry**：slot->node 映射与节点成员关系不再依赖外部 Redis/etcd 才能工作。
2. **最终一致收敛**：在短时分区、节点抖动、重启后，状态可自动收敛。
3. **保持当前固定 slot 模型**：继续使用 `total_slots`（默认 2048）与确定性路由。
4. **兼容现有写入语义**：不改变 quorum 写与 generation/head 语义。
5. **可迁移落地**：支持与现有 Redis registry 双写/双读阶段，降低切换风险。

## 非目标

1. 本 RFC 不实现完整 Redis Cluster 协议兼容（不是 Redis 的 wire-compatible 替代）。
2. 本 RFC 不引入自动扩缩容与在线重分片（与当前 non-goal 保持一致）。
3. 本 RFC 不改变对象数据面（part/head/tombstone）读写协议。
4. 本 RFC 不替代归档层 Redis/S3 逻辑（仅聚焦 registry）。

---

## 方案概览

新增 `gossip` 类型 registry backend，核心由三部分组成：

1. **本地状态机（Registry State Machine）**
   - 每个节点维护本地完整 registry 视图（membership + slot map + bootstrap state）
2. **CRDT 数据模型（Merge-safe）**
   - 节点成员与 slot 分配采用可交换、可结合、幂等的合并规则
3. **Gossip 传播层（Redis Cluster 风格）**
   - 周期 PING/PONG + 增量状态传播 + 周期性 anti-entropy 全量校对

该模型下，客户端路由读取本地视图，不再每次请求访问外部集中式注册中心。

---

## 数据模型（CRDT）

### 1) Membership CRDT（节点成员）

使用 OR-Map + LWW-Register 组合：

- key: `node_id`
- value: `NodeRecord`

```text
NodeRecord {
  node_id: String,
  address: String,
  status: Alive | Suspect | Failed | Leaving,
  incarnation: u64,
  heartbeat_hlc: HlcTimestamp,
  metadata: { group_id, tags... }
}
```

合并规则：

1. 先比较 `incarnation`，更大者胜
2. `incarnation` 相同则比较 `heartbeat_hlc`
3. 再相同则以 `node_id` 字典序做稳定 tie-break

> 说明：`incarnation` 用于“节点重启后推翻旧失败结论”，与 Redis Cluster 的 epoch/任期思想一致。

### 2) SlotMap CRDT（slot 分配）

使用 AWOR-Map（Add-Wins Observed-Remove Map）：

- key: `slot_id`
- value: `SlotRecord`

```text
SlotRecord {
  slot_id: u16,
  replicas: Set<NodeId>,
  primary: NodeId,
  slot_epoch: u64,
  state: Stable | Migrating | Importing,
  last_updated_hlc: HlcTimestamp
}
```

合并规则：

1. 更高 `slot_epoch` 的记录胜出
2. `slot_epoch` 相同，`state` 采用 `Migrating > Importing > Stable`（避免丢迁移中状态）
3. `replicas` 采用 add-wins 集合合并
4. `primary` 在同 epoch 下按确定性函数选择（`max(node_id)` 或固定哈希顺序）

### 3) BootstrapState CRDT（一次初始化）

保持现有 first-wins 语义，但落在去中心化传播链路上：

- 字段包含 `initialized_at`, `nodes`, `replication`, `archive`, `initialized_by`
- 以 `bootstrap_epoch` 标识版本
- 首次成功提案后通过 gossip 广播
- 新节点 join 后若本地缺失则主动拉取

---

## Gossip 协议（Redis Cluster 风格）

### 消息类型

1. `MEET`：新节点加入握手
2. `PING`：定期探活 + 摘要交换
3. `PONG`：探活应答
4. `UPDATE`：增量 CRDT op 传播（delta）
5. `FAIL_REPORT`：故障怀疑投票
6. `STATE_SYNC`：反熵全量快照（低频）

### 传播机制

- 周期 gossip：默认每 `500ms` 向 `fanout=3` 个随机节点发送 `PING`
- 增量传播：每条本地状态更新打包为 op-log delta，并 piggyback 到 `PING/UPDATE`
- 全量校对：每 `10s` 做 digest 对比，不一致触发 `STATE_SYNC`

### 故障判定（借鉴 PFAIL/FAIL）

1. 节点在 `suspect_timeout`（默认 15s）内无心跳 -> 标记 `Suspect`
2. 收到多数节点的 `FAIL_REPORT`（`> N/2`）-> 标记 `Failed`
3. 节点重启后提升 `incarnation` 并广播，可覆盖旧 `Failed` 结论

---

## Slot 主从与故障转移

在 `slot_epoch` 维度引入 fencing：

1. 每次 slot primary 变更必须 `slot_epoch + 1`
2. 写请求携带 `slot_epoch`，副本拒绝陈旧 epoch 的提交
3. primary 失效后由存活副本发起提案并竞争：
   - 候选优先级：`latest_seq` 最大，其次 `node_id` 稳定排序
   - 胜出者发布新 `SlotRecord(slot_epoch++)`

这样保证分区恢复后，旧主即使恢复，也会因 epoch 落后而被拒绝继续提交。

---

## 路由与读写语义变化

### 当前行为（简化）

- `resolve_replica_nodes` 基于当前可见 node 列表旋转计算副本
- node 可见性来自 registry 心跳键值

### 新行为

- 路由优先读取本地 `SlotMap` 快照
- 若 slot 记录缺失，按 bootstrap 初始布局进行确定性回退
- 写路径将 `slot_epoch` 下传到内部复制 API（`internal_put_part/head`）
- 读路径优先本地 primary/healthy 副本，必要时按 slot 副本集合降级

---

## 持久化与重启恢复

每个节点本地持久化 registry 状态：

- `registry/snapshot.json`：最近一致快照
- `registry/oplog.log`：增量 op（可截断）

重启流程：

1. 读取 snapshot + oplog 恢复本地视图
2. 与 seed 节点执行 `STATE_SYNC`
3. 合并后进入常规 gossip

---

## 配置草案

```yaml
registry:
  backend: gossip # options: gossip | redis | etcd
  namespace: default
  gossip:
    seeds:
      - "node-1@127.0.0.1:19080"
      - "node-2@127.0.0.1:19081"
      - "node-3@127.0.0.1:19082"
    gossip_interval_ms: 500
    full_sync_interval_sec: 10
    suspect_timeout_sec: 15
    fail_timeout_sec: 45
    fanout: 3
    persist_dir: "./demo/node1/registry"
```

建议新增配置结构：

- `RegistryBackend::Gossip`
- `GossipRegistryConfig`
- 可选 `dual_write_redis_url`（迁移阶段）

---

## 与现有代码的对接点

1. `rimio-core/src/registry/mod.rs`
   - 新增 `gossip` backend 实现
2. `rimio-core/src/registry/factory.rs`
   - 支持 `backend = gossip`
3. `rimio-server/src/config.rs`
   - 新增 `gossip` 配置解析
4. `rimio-server/src/server/mod.rs`
   - `resolve_replica_nodes` 改为优先读取 slot map
5. internal API
   - 写复制接口增加 `slot_epoch` 头/字段

---

## 分阶段实施

### Phase 0：实现与观测（不切流量）

- 实现 `GossipRegistry` 与 CRDT merge 单测
- 节点启动时可选开启 gossip，但仍使用 Redis 作为主 registry

### Phase 1：双写（Redis + Gossip）

- 所有 registry 更新同时写入 Redis 与 gossip state machine
- 增加一致性对账指标（slot map hash、member hash）

### Phase 2：双读（优先 Gossip）

- 读路径优先 gossip，本地异常时回退 Redis
- 跑全套 integration + 注入故障测试

### Phase 3：移除 Redis 前置依赖

- README / integration 默认不再要求 registry Redis
- Redis backend 保留为可选兼容模式（非默认）

---

## 测试与验证策略

1. **CRDT 属性测试**
   - 交换律、结合律、幂等性
2. **故障注入测试**
   - 网络分区、单节点重启、消息乱序、重复消息
3. **收敛时延测试**
   - N=3/5 集群下 slot map 收敛时间 P50/P99
4. **写安全测试**
   - 验证 `slot_epoch` fencing 防止旧主提交
5. **兼容回归**
   - 现有 API 与 S3 integration 不回归

建议补充 TLA+ 模型检查：

- 不变量 1：同一 slot 在同一 `slot_epoch` 下最多一个 primary
- 不变量 2：`slot_epoch` 单调不回退
- 不变量 3：最终在无新写入时各节点 slot map 收敛

---

## 风险与缓解

1. **风险：分区期间视图不一致导致路由抖动**
   - 缓解：读本地快照 + 写入携带 epoch fencing + anti-entropy 加速
2. **风险：Gossip 参数不当导致误判故障**
   - 缓解：默认保守超时，暴露观测指标后再调优
3. **风险：双写阶段状态漂移**
   - 缓解：hash 对账 + 自动告警 + 可回退 Redis 主读

---

## 验收标准

1. 在 3 节点集群中，不依赖 Redis/etcd 也可完成初始化、路由、写入与读回。
2. 单节点故障后，`fail_timeout` 内完成主副本收敛，写路径可恢复。
3. 网络分区恢复后，slot map 在 `2 * full_sync_interval` 内收敛。
4. 发生主切换后，旧主无法以过期 `slot_epoch` 写入成功。
5. README 与 integration 默认流程不再要求“registry Redis 必须可达”。

---

## 开放问题

1. `slot_epoch` 与现有 `generation` 的边界是否需要统一版本向量表达？
2. Gossip 传输层首版采用 HTTP 复用还是独立 UDP/TCP bus？
3. 是否保留 etcd backend 作为长期选项，还是与 Redis 一并降级为兼容模式？

