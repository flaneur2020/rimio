# Rimio 设计文档（RFC 0009）

> 目标：把 `init_cluster` 作为一个独立主题讲清楚，避免它和 `archive` 配置、`start/join` 行为、写闸门策略混在一起造成歧义。

## 背景与问题

当前讨论里，`init_cluster` 被同时用于表达三类诉求：

1. 集群首次初始化时，是否需要先做归档回补；
2. 在回补完成前，是否默认拒绝写请求；
3. 节点 `start/join` 时，配置到底以本地文件还是 registry 为准。

这三件事如果放在一个 `archive.*` 结构里，会产生两个常见困惑：

- **语义层级不对**：`init_cluster` 是“集群生命周期策略”，不是“某个 archive backend 参数”；
- **真相源不清楚**：一旦集群已存在，`join` 应该只信 registry，而不是本地 `config.yaml`。

因此本 RFC 定义：`init_cluster` 是 **Cluster Bootstrap Policy**，与 `archive` 解耦。

---

## 设计目标

1. `init_cluster` 语义单一：只控制“首次建群后的写闸门与回补流程”。
2. 配置分层清晰：`archive` 负责存储后端参数，`init_cluster` 负责生命周期策略。
3. 真相源清晰：集群存在后，runtime 配置来自 registry bootstrap state。
4. 行为可预测：`start/join` 与 API 允许/拒绝矩阵固定、可测试。
5. MVP 先简化：允许短时不一致，优先把主路径跑通。

## 非目标

1. 本期不做复杂仲裁（多控制器并发 `sync-archive` 的完美一致性）。
2. 本期不做在线重分片与混部迁移策略。
3. 本期不做跨版本自动迁移编排（仅保留最小兼容读取策略）。

---

## 核心设计决策

### 决策 1：`init_cluster` 不放在 `ClusterArchiveConfig`

`archive` 是“存储目标配置”，而 `init_cluster` 是“集群初始化阶段策略”。

推荐结构：

```yaml
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

init_cluster:
  enabled: true
  manifest_url: s3://rimio-archive/bootstrap/manifest.json
  require_sync_archive: true
```

其中：

- `archive`：告诉系统“去哪里读/写归档”；
- `init_cluster`：告诉系统“首次建群后是否先锁写、等回补完成再开写”。

### 决策 2：`init_cluster` 只在“首次建群成功者”生效

仅 bootstrap winner（`set_bootstrap_state_if_absent` 成功者）有权把 `init_cluster` 策略写入 registry。

后续节点（`start` 进入 join 语义，或显式 `join`）只读取并执行 registry 中已有策略，不得覆写。

### 决策 3：写闸门状态进入 bootstrap state

在 cluster state 中持久化：

```text
write_gate = Open | ClosedByInitCluster
```

规则：

- `init_cluster.enabled=true && require_sync_archive=true` => 初始 `write_gate=ClosedByInitCluster`
- 否则初始 `write_gate=Open`

### 决策 4：`join` 永远不以本地 conf 作为集群真相源

`join REGISTRY_URL --node ...` 只允许：

- 从 registry 拉 bootstrap state；
- 校验命令行参数与 registry 对应节点配置一致（可选参数时）；
- 启动本地节点。

不允许在 `join` 时提交 topology/replication/archive/init_cluster 变更。

---

## 状态机

定义集群写入状态机（MVP）：

```text
Uninitialized
  └─(bootstrap created)→
    Running[write_gate=Open]
    or
    Running[write_gate=ClosedByInitCluster]

Running[ClosedByInitCluster]
  └─(rimio-adm sync-archive success)→ Running[Open]
```

MVP 约束：

- `ClosedByInitCluster` 下严格拒绝写入口；
- `sync-archive` 失败不改变闸门（保持关闭）；
- `sync-archive` 成功后一次性切到 `Open`。

---

## CLI 语义（与 RFC0008 对齐）

### `rimio start --conf config.yaml --node node1`

1. 读取 `config.yaml`，只用于连接 registry + 本地进程启动参数。
2. 查询 registry 是否已存在 bootstrap state：
   - 不存在：按建群路径提交 bootstrap state（包含 `init_cluster` 策略）；
   - 已存在：按 join 语义执行（registry 为真相源）。

### `rimio join REGISTRY_URL --node node2 [--listen ...] [--advertise-addr ...]`

1. 不接收 `--conf`；
2. 必须先读取 bootstrap state；
3. `--node` 必须预定义于 bootstrap `nodes[]`；
4. 可选 `--listen/--advertise-addr` 若提供，必须与 registry 中一致；
5. 若存在活跃同名节点则拒绝（`--force-takeover` 作为受限例外策略，后续细化）。

---

## API 读写闸门约束（强约束）

当 `write_gate=ClosedByInitCluster`：

- 允许：`health` / `nodes` / `list` / `get` / `head`
- 拒绝：`put` / `delete` / 所有 internal write（`internal_put_part`、`internal_put_head`、repair write）

返回规范：

- HTTP：`503 ServiceUnavailable`
- JSON 错误码：`ClusterWriteDisabled`
- 消息建议：`cluster write gate is closed by init_cluster`

S3 网关映射：

- 返回 `503`，错误码 `ServiceUnavailable`，消息同义。

---

## Registry 数据模型（建议）

建议在 bootstrap state 中新增独立字段（示意）：

```text
ClusterState {
  initialized_at,
  initialized_by,
  nodes,
  replication,
  archive,
  init_cluster: Option<InitClusterState>,
  write_gate,
  state_version
}

InitClusterState {
  enabled,
  require_sync_archive,
  manifest_url,
  sync_status,      // Pending | Running | Succeeded | Failed
  last_sync_at,
  last_sync_error
}
```

MVP 简化可先不引入完整 `sync_status` 状态机，先落 `write_gate + init_cluster.enabled/manifest_url` 即可。

---

## `rimio-adm sync-archive` 语义（MVP）

命令目标：

1. 执行归档扫描/回补；
2. 成功后将 `write_gate` 置为 `Open`。

MVP 要求：

- 幂等：重复执行成功结果一致；
- 失败不放开写闸门；
- 闸门切换采用版本校验（CAS）优先，若暂未实现 CAS，需记录 TODO 并限制单操作者。

---

## 配置兼容策略

为降低迁移成本，建议提供一段过渡兼容（可选）：

1. 若发现旧字段 `archive.init_cluster`，在解析时映射到顶层 `init_cluster`；
2. 打印一次 warning，提示后续版本移除旧字段；
3. 新写入 bootstrap state 时只写新结构。

若项目希望更激进，也可直接不兼容旧字段（早期项目可接受）。

---

## 实施分期

### Phase 1（本期）

1. 定义顶层 `init_cluster` 配置结构；
2. bootstrap state 持久化 `write_gate`；
3. server 写路径加闸门检查；
4. `start/join` 统一按 registry 真相源校验。

### Phase 2

1. `rimio-adm sync-archive` 落地；
2. 成功后切换 `write_gate=Open`；
3. 补齐集成测试矩阵（含 S3 错误映射）。

### Phase 3

1. 增加 `sync_status` 可观测字段；
2. 增加审计日志与恢复工具（例如手动解锁流程）。

---

## 测试验收点（建议）

1. `init_cluster.enabled=true` 时，`PUT/DELETE/internal write` 全部返回 `503 + ClusterWriteDisabled`。
2. `GET/HEAD/LIST/health/nodes` 在闸门关闭时保持可用。
3. `sync-archive` 成功后，写请求恢复成功。
4. `join` 不带 conf，且任何与 registry 不一致的覆盖参数都被拒绝。
5. `start` 在已有 bootstrap 时进入 join 语义并执行相同一致性校验。

---

## 结论

`init_cluster` 应当被建模为 **集群生命周期策略**，而不是 `archive` 的子配置。

只要坚持三条原则，模型会非常清晰：

1. `init_cluster` 独立建模；
2. registry 是已建群后的唯一真相源；
3. 写闸门用明确状态机和 API 强约束表达。

这版设计适合当前项目阶段：先把主路径做简单、可运行、可测试，再逐步补强一致性细节。

