# Rimio 设计文档（RFC 0008）

> 提议引入“Redis Cluster 风格（无中心）+ CRDT 收敛 + Gossip 传播”的 registry 子系统，替换当前对外部 Redis 的强依赖；其中 membership 与 gossip 传播直接复用 `memberlist` crate，而非自研协议栈。

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
5. **复用成熟实现**：membership、failure detection、gossip 传播优先采用 `memberlist`，避免自研网络协议。
6. **直接切换落地**：采用一次性切换到 gossip registry，不引入双写/双读迁移阶段。
7. **统一引导流程**：通过 `start/join` 子命令显式区分“建群”和“入群”，并约束节点命名唯一性。

## 非目标

1. 本 RFC 不实现完整 Redis Cluster 协议兼容（不是 Redis 的 wire-compatible 替代）。
2. 本 RFC 不引入自动扩缩容与在线重分片（与当前 non-goal 保持一致）。
3. 本 RFC 不改变对象数据面（part/head/tombstone）读写协议。
4. 本 RFC 不替代归档层 Redis/S3 逻辑（仅聚焦 registry）。
5. 本 RFC 不支持混部运行（不考虑 gossip 与 redis/etcd backend 混跑）。
6. 本期（MVP）不追求覆盖所有边界一致性细节；允许少量短时不一致，以实现简洁优先。

---

## 方案概览

新增 `gossip` 类型 registry backend，核心由三部分组成：

1. **本地状态机（Registry State Machine）**
   - 每个节点维护本地完整 registry 视图（membership + slot map + bootstrap state）
2. **CRDT 数据模型（Merge-safe）**
   - 节点成员与 slot 分配采用可交换、可结合、幂等的合并规则
3. **Gossip 传播层（Redis Cluster 风格）**
   - 基于 `memberlist` 的成员传播与故障探测 + Rimio CRDT 增量同步

该模型下，客户端路由读取本地视图，不再每次请求访问外部集中式注册中心。

说明：本 RFC 不再建议实现自定义 gossip wire 协议，改为将精力集中在 CRDT 数据模型与业务语义（slot_epoch fencing、路由回退、反熵）上。

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

## Memberlist 集成方案（替代自研 Gossip 协议）

### 复用边界

由 `memberlist` 负责：

1. 节点发现（join/leave）
2. 存活探测与故障怀疑（alive/suspect/dead）
3. gossip 扩散与反熵底座

由 Rimio 负责：

1. CRDT 结构定义（Membership/SlotMap/BootstrapState）
2. slot 主从切换规则（`slot_epoch`）
3. 写入 fencing 与路由语义

### 事件与数据流

1. 节点启动后通过 seed 列表 `join` 到 memberlist 集群。
2. memberlist 成员变化事件驱动本地 Membership CRDT 更新。
3. Rimio 将 slot/bootstrap 的 delta 作为应用层 payload 广播。
4. 节点收到 payload 后按 CRDT merge 规则合并并持久化。
5. 周期 digest 比对仍保留；发现偏差后触发全量 `STATE_SYNC`（Rimio 内部 RPC）。

### 编码与兼容

- Gossip payload 使用带版本号的 envelope（例如 `version`, `kind`, `payload`）。
- 首版建议 `kind` 至少包含：`slot_delta`、`bootstrap_snapshot`。
- 对未知版本 payload 执行“忽略并记录指标”，避免集群升级期间硬失败。

### 故障判定映射

- memberlist 的节点状态映射为 Rimio `NodeRecord.status`：
  - alive -> `Alive`
  - suspect -> `Suspect`
  - dead/left -> `Failed` / `Leaving`
- 节点重启仍通过 `incarnation` 递增覆盖旧状态，保持 CRDT 可收敛。

### 入群地址格式（REGISTRY_URL）

在支持 gossip/memberlist 后，`join` 命令接收统一 `REGISTRY_URL`：

- 形式：`cluster://seed1:8400,seed2:8400,seed3:8400`
- 语义：只要其中任意一个 seed 可连接，节点即可完成入群
- 解析：`cluster://` 仅用于 CLI 语义层，底层仍转换为 memberlist seed 地址列表
- 约束：`join` 不读取本地 `config.yaml`，节点配置以 registry 中的 bootstrap state 为唯一真相源

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

## 启动与入群流程（CLI）

本 RFC 采用 `start` 自动模式：优先检查 registry 中是否已存在 cluster bootstrap state，
不存在则建群，存在则按 `join` 语义启动；`join` 保留为无本地配置的纯入群入口。

### 1) 启动（start，自动模式）

```bash
rimio start --conf config.yaml --node node1
```

语义：

- `start` 先连接 registry 并检查是否已有 bootstrap state
- 若 registry 不存在 bootstrap state：按“建群”处理，使用 `config.yaml` 写入首个 `BootstrapState`
- 若 registry 已存在 bootstrap state：按“join”处理，registry 为唯一配置真相源
- 在“已建群”路径下，`start` 提供的本地参数（如 `--node`）与 registry 对应项必须一致，不一致直接拒绝启动

### 2) 入群（join）

```bash
rimio join REGISTRY_URL --node node2 --listen 0.0.0.0:482 --advertise-addr 192.19.0.1
```

语义：

- `REGISTRY_URL` 支持 `cluster://seed1:8400,seed2:8400`（任一可达即可）
- `join` 不接受 `--conf`；节点拓扑、disks、replication、archive 等全部从 registry 读取
- `--node` 必须在 registry 保存的 bootstrap state 中预定义，且集群中不允许两个活跃同名节点
- `--listen`、`--advertise-addr` 为可选一致性校验参数（若提供必须与 registry 一致）

参数约束：

1. `join` 启动时必须先从 `REGISTRY_URL` 拉取 bootstrap state；拉取失败直接报错退出。
2. `--node` 若不在 bootstrap state 的 `nodes[]` 列表中，直接报错退出。
3. 若提供 `--listen` 或 `--advertise-addr`，其值必须与 bootstrap state 中该 `node_id` 的配置一致；不一致则拒绝启动。
4. 若未提供 `--listen` / `--advertise-addr`，则直接使用 bootstrap state 中的值。
5. 若存在同名 `Alive` 节点，`join` 直接拒绝，返回冲突错误。
6. 若存在同名 `Suspect` 节点，默认拒绝；仅在显式提供 `--force-takeover` 且旧实例 `last_heartbeat_age >= fail_timeout` 时允许接管。
7. 若同名旧实例已是 `Failed` / `Leaving`，仅当新实例 `incarnation` 更高时允许接管。

`start` 与 `join` 一致性规则：

- 当 bootstrap state 已存在时，`start` 与 `join` 走同一套校验逻辑。
- 所有 join 阶段可覆盖参数都必须与 registry 中保存配置一致；不一致则拒绝启动。
- 不允许通过 `start` 或 `join` 在入群时修改集群拓扑、replication、archive 配置。

`--force-takeover` 约束：

- 仅对同名 `Suspect` 节点生效，对 `Alive` 节点始终无效。
- 触发 takeover 时必须记录审计日志（`node_id`、旧状态、旧心跳年龄、发起时间）。

---

## 配置草案

```yaml
registry:
  backend: gossip # options: gossip | redis | etcd
  namespace: default
  gossip:
    transport: "internal_http"
    seeds:
      - "192.19.0.1:482"
      - "192.19.0.2:482"
      - "192.19.0.3:482"
    gossip_interval_ms: 500
    full_sync_interval_sec: 10
    suspect_timeout_sec: 15
    fail_timeout_sec: 45
    fanout: 3
    persist_dir: "./demo/node1/registry" # 可选；未配置时默认使用 /tmp/rimio-registry/<node_id>

initial_cluster:
  nodes:
    - node_id: "node1"
      bind_addr: "0.0.0.0:482"
      advertise_addr: "192.19.0.1"
      disks:
        - path: ./demo/node1/disk
    - node_id: "node2"
      bind_addr: "0.0.0.0:482"
      advertise_addr: "192.19.0.2"
      disks:
        - path: ./demo/node2/disk

archive:
  archive_type: s3
  s3:
    bucket: rimio-archive
    region: us-east-1
    endpoint: http://127.0.0.1:9000
    allow_http: true
    credentials:
      access_key_id: xxx
      secret_access_key: yyy
```

建议新增配置结构：

- `RegistryBackend::Gossip`
- `GossipRegistryConfig`
- `gossip.provider = memberlist`
- `persist_dir` 为可选项，未配置时自动落到 `/tmp` 默认目录
- `rimio start --conf ... --node ...` 启动方式
- `rimio join REGISTRY_URL --node ...` 入群方式（不带 `--conf`）

---

## 与现有代码的对接点

1. `rimio-core/src/registry/mod.rs`
   - 新增 `gossip` backend 实现
2. `rimio-core/Cargo.toml`
   - 新增 `memberlist` 依赖
3. `rimio-core/src/registry/gossip_memberlist.rs`
   - 基于 `memberlist` 封装 `Registry` trait
4. `rimio-core/src/registry/factory.rs`
   - 支持 `backend = gossip` 并构造 memberlist 实例
5. `rimio-server/src/config.rs`
   - 新增 gossip/memberlist 配置解析
6. `rimio-server/src/server/mod.rs`
   - `resolve_replica_nodes` 改为优先读取 slot map
7. `rimio-server/src/main.rs`（或命令层）
   - 增加 `start` / `join` 子命令与参数校验（`join` 无 `--conf`、参数与 registry 配置一致、节点名唯一、`--force-takeover` 约束）
8. internal API
   - 写复制接口增加 `slot_epoch` 头/字段

---

## 分阶段实施

### Phase 0：实现与验证

- 实现 `GossipRegistry(memberlist)` 与 CRDT merge 单测
- 完成 memberlist 集成冒烟（join/leave/suspect/dead）
- 实现 `start` / `join` CLI 以及节点命名唯一性校验
- 完成故障注入与收敛压测（分区、乱序、重启）

### Phase 1：一次性切换

- 默认 `registry.backend` 改为 `gossip`
- `README` 与 `integration` 默认流程移除“registry Redis 必须可达”的前置条件
- `start` 作为唯一建群入口，`join` 作为唯一入群入口
- 不支持混部：切换窗口内必须统一到 gossip 方案，不允许 registry backend 混跑

### Phase 2：收敛与清理

- 在验证稳定后，将文档与配置示例全面收敛到 gossip-only 推荐路径

---

## 测试与验证策略

1. **CRDT 属性测试**
   - 交换律、结合律、幂等性
2. **故障注入测试**
   - 网络分区、单节点重启、消息乱序、重复消息、memberlist 节点 flap
3. **收敛时延测试**
   - N=3/5 集群下 slot map 收敛时间 P50/P99
4. **写安全测试**
   - 验证 `slot_epoch` fencing 防止旧主提交
5. **CLI/配置约束测试**
   - `start`/`join` 参数校验、`join` 无 `--conf`、参数与 registry 一致性校验、同名活跃节点拒绝、`cluster://` URL 解析
6. **兼容回归**
   - 现有 API 与 S3 integration 不回归

建议补充 TLA+ 模型检查：

- 不变量 1：同一 slot 在同一 `slot_epoch` 下最多一个 primary
- 不变量 2：`slot_epoch` 单调不回退
- 不变量 3：最终在无新写入时各节点 slot map 收敛

---

## 本期延后 TODO（后续迭代）

以下能力明确记录为 TODO，本期先不实现：

1. `cluster://` 的完整语法与错误模型（含 IPv6、重复节点、非法端口等）。
2. `--force-takeover` 并发争抢时的胜者裁决与冷却窗口策略。

本期策略：

- 优先实现主流程可用性（start/join、registry 收敛、基础 takeover）。
- 对上述 TODO 场景采用 best-effort 行为，必要时通过运维介入恢复。

---

## 风险与缓解

1. **风险：分区期间视图不一致导致路由抖动**
   - 缓解：读本地快照 + 写入携带 epoch fencing + anti-entropy 加速
2. **风险：Gossip 参数不当导致误判故障**
   - 缓解：基于 memberlist 推荐参数起步，按生产指标迭代调优
3. **风险：一次性切换后回退复杂度提升**
   - 缓解：按整集群版本回滚，不采用混部回退路径
4. **风险：第三方依赖升级带来行为变化**
   - 缓解：锁定版本区间、升级前跑故障注入回归测试
5. **风险：配置缺失导致节点无法入群**
   - 缓解：将 registry bootstrap state 作为唯一配置真相源，并在 `join` 入口执行严格一致性校验
6. **风险：误用 `--force-takeover` 导致脑裂窗口扩大**
   - 缓解：限制仅对 `Suspect` 生效 + 心跳年龄门槛 + 审计日志 + 指标告警

---

## 验收标准

1. 在 3 节点集群中，不依赖 Redis/etcd 也可完成初始化、路由、写入与读回。
2. 单节点故障后，`fail_timeout` 内完成主副本收敛，写路径可恢复。
3. 网络分区恢复后，slot map 在 `2 * full_sync_interval` 内收敛。
4. 发生主切换后，旧主无法以过期 `slot_epoch` 写入成功。
5. `join` 命令不依赖本地 `config.yaml`，且在参数提供时与 registry 配置严格一致。
6. `start/join` 命令满足节点名唯一约束，且 `cluster://` 地址可用任一 seed 入群。
7. README 与 integration 默认流程不再要求“registry Redis 必须可达”。

---

## 开放问题

1. `slot_epoch` 与现有 `generation` 的边界是否需要统一版本向量表达？
2. memberlist 传输层首版采用何种配置（仅内网 TCP，还是开启加密/鉴权）？
3. `--force-takeover` 是否需要增加二次确认机制（如 `--confirm-epoch`）以降低误操作？
