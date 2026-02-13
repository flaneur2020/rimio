# Rimio 设计文档（RFC 0010）

> 目标：不再让用户在 `registry.gossip` 中单独配置 `bind_addr/advertise_addr`，并且每个节点只暴露一个对外端口（即 `initial_cluster.nodes[*].bind_addr/advertise_addr` 的业务端口）。

## 背景

当前 `gossip` backend 依赖独立地址配置，且默认思路倾向额外 gossip 端口。这与我们希望的两点不一致：

1. 地址配置单一来源（避免重复配置）；
2. 每节点只提供一个对外端口（简化运维和安全策略）。

我们已确认 `memberlist` 支持自定义 `Transport`，因此可以保留 `memberlist` 的 membership/merge 语义，同时将传输落在现有 internal HTTP 通道上。

---

## 设计目标

1. 用户不再配置 `registry.gossip.bind_addr/advertise_addr`；
2. `initial_cluster.nodes[]` 是节点地址唯一真相源；
3. gossip/memberlist 通信复用现有对外服务端口（单端口）；
4. `start/join` 语义保持稳定，`join` 仍以 registry/bootstrap 为真相；
5. 保留 `redis://`、`etcd://` 路径兼容。

## 非目标

1. 不在本期实现动态扩缩容/重分片；
2. 不引入多种 gossip 协议并行；
3. 不为了单端口放弃 `memberlist`（除非后续验证不可行）。

---

## 技术可行性评估

## 结论（TL;DR）

- **可行**：`memberlist` 支持自定义 `Transport`，可以把 packet/stream 映射到 internal API。
- **关键约束**：不能直接使用默认 `NetTransport` 与 axum 共享同一监听 socket；但可通过自定义 transport 在应用层复用同一端口。

## 原因说明

1. 默认 `NetTransport` 需要独立网络监听（TCP/UDP/QUIC）；
2. axum/hyper 已占用业务端口，默认 memberlist 传输无法直接挂到同一个 socket；
3. 但 `memberlist::Transport` trait 允许自实现 `send_to/open/packet/stream`，因此可构建“HTTP internal transport”。

## 方案对比

### A. 默认 NetTransport + 独立 gossip 端口

- 可行性：高
- 与目标冲突：高（不满足单端口）

### B. 地址派生（service_port + offset）

- 可行性：高
- 与目标冲突：中（仍是双端口）

### C. 自定义 internal transport（推荐）

- 可行性：中高（可做）
- 复杂度：中（需要实现 transport 适配层）
- 与目标匹配：高（单端口）

---

## 提案（推荐方案 C）

## 1) 配置模型

### 用户侧配置

- 去掉（或忽略）`registry.gossip.bind_addr/advertise_addr`；
- `registry.gossip` 仅保留策略参数（如超时、fanout、加密选项）；
- 节点地址统一来自 `initial_cluster.nodes[*].bind_addr/advertise_addr`。

示例：

```yaml
registry:
  backend: gossip
  namespace: local-cluster-001
  gossip:
    transport: internal_http

initial_cluster:
  nodes:
    - node_id: node-1
      bind_addr: 127.0.0.1:19080
      advertise_addr: 127.0.0.1:19080
```

## 2) internal transport 设计

在 `memberlist::Transport` 适配层中：

1. `send_to(addr, packet)`
   - 映射为 HTTP `POST /internal/v1/gossip/packet`
2. `open(addr, deadline)`
   - 映射为 `WebSocket` 或 `HTTP/2 CONNECT`：`/internal/v1/gossip/stream`
3. `packet()` / `stream()`
   - 由 internal handler 将入站请求转发到 transport 内部 channel
4. `Connection` trait
   - 基于 websocket/h2 双向流实现 reader/writer split

说明：

- 这不是自研 gossip 协议，仍由 memberlist 上层处理消息语义；
- 我们仅替换“消息搬运层”。

## 3) internal API（最小集合）

1. `POST /internal/v1/gossip/packet`
   - body：memberlist packet payload
2. `GET /internal/v1/gossip/stream`（upgrade）
   - 双向可靠流，用于 memberlist stream 通信
3. `GET /internal/v1/cluster/bootstrap`
   - join 初始化读取 bootstrap state
4. `GET /internal/v1/cluster/gossip-seeds`
   - 返回 service 地址列表（非派生地址）

## 4) 单端口语义

- 对外仅 `bind_addr` 一个端口；
- 业务 API、internal API、gossip transport 共用该端口；
- 通过路径/upgrade 区分流量类型。

## 5) start/join 行为

### `start --conf ... --node ...`

1. 从 `initial_cluster` 查当前 node service 地址；
2. 启动 HTTP/internal server（单端口）；
3. 以 internal transport 初始化 memberlist。

### `join cluster://seed1:port,seed2:port --node ...`

1. `cluster://` 地址即 service seed 地址；
2. 先从任一 seed 拉 `bootstrap` + `gossip-seeds`；
3. 再通过 internal transport 加入 memberlist。

---

## 兼容性与迁移

1. 兼容窗口内：若读取到旧 `registry.gossip.bind_addr/advertise_addr`，打印 warning 并忽略；（不需要，请直接干掉）
2. 文档、示例统一改为单端口模型；
3. `redis://` 与 `etcd://` join 行为不变。

---

## 对代码的影响点

1. `rimio-core`
   - 新增 `registry/gossip_internal_transport.rs`（实现 `memberlist::Transport`）
2. `rimio-server/src/server/internal`
   - 增加 gossip packet/stream handlers
3. `rimio-server/src/main.rs`
   - 调整 start/join 初始化顺序：先 internal server 可用，再 join memberlist
4. `rimio-server/src/config.rs`
   - gossip 配置裁剪（移除 bind/advertise，新增 transport 策略字段）

---

## 风险与缓解

1. **风险：HTTP 封装带来额外延迟**
   - 缓解：优先内网部署、压测校准 memberlist interval/timeout。
2. **风险：stream 通道稳定性（WebSocket/h2）**
   - 缓解：实现重连与 deadline、增加 backpressure 指标。
3. **风险：单端口下内部接口暴露面扩大**
   - 缓解：internal path 鉴权（token/mTLS/allowlist）+ 默认仅集群网段可达。

---

## 验收标准

1. 每节点仅暴露一个服务端口；
2. gossip 模式下用户无需配置 `registry.gossip.bind_addr/advertise_addr`；
3. memberlist 在 internal transport 上可完成 join/suspect/dead/merge；
4. `join cluster://...` 可完成 bootstrap + 入群；
5. `redis://` / `etcd://` 路径无回归。

---

## 分期建议

### Phase 1（最小可用）

- 实现 internal packet 通道 + 基础 stream；
- 打通 3 节点 join/leave；
- 完成最小集成测试。

### Phase 2（稳定化）

- 增加鉴权、限流、观测指标；
- 压测并调优 memberlist 参数；
- 清理旧配置项。

---

## TODO 清单（执行版）

### P0：设计冻结（先定边界）

- [ ] 冻结 stream 实现方案（优先 `WebSocket`，`HTTP/2 CONNECT` 作为后备）
- [ ] 冻结 internal gossip API 契约（路径、method、payload、错误码）
- [ ] 冻结 `cluster://` 到 internal seed 发现流程（失败重试、超时、回退）
- [ ] 明确单端口安全基线（默认鉴权策略 + 网段限制）

### P1：核心实现（memberlist + custom transport）

- [ ] 在 `rimio-core` 新增 `gossip_internal_transport`（实现 `memberlist::Transport`）
- [ ] 实现 `send_to` -> `POST /internal/v1/gossip/packet`
- [ ] 实现 `open` + `Connection`（双向流，reader/writer split）
- [ ] 实现 `packet()` / `stream()` 入站分发与 backpressure
- [ ] 完成 transport 错误分类（remote failure / local failure）
- [ ] 先打通 3 节点 `join/suspect/dead/merge` 冒烟

### P1：服务端接口与启动时序

- [ ] 在 `rimio-server` 增加 `gossip packet/stream` internal handlers
- [ ] 增加 `GET /internal/v1/cluster/bootstrap`
- [ ] 增加 `GET /internal/v1/cluster/gossip-seeds`
- [ ] 调整启动顺序：internal server ready -> memberlist join -> register node
- [ ] `join cluster://...` 先 bootstrap，再进入 memberlist join

### P1：配置与兼容

- [x] 裁剪 `registry.gossip.bind_addr/advertise_addr`（配置可不再出现）
- [x] 取消兼容窗口（直接移除旧字段）
- [x] 更新 `README` 单端口示例
- [x] 从 `initial_cluster.nodes[*].bind_addr/advertise_addr` 注入 gossip 地址

### P1：测试清单

- [ ] transport 单测：packet 编解码、stream 读写、deadline、close 语义
- [ ] 集成测试：单端口 3 节点 `start + join`
- [ ] 故障测试：seed 不可达、stream 断开、乱序/重复包
- [ ] 回归测试：`redis://`、`etcd://` 路径不回归

### P2：稳定化与运维

- [ ] 压测（N=3/N=5）：收敛时延、误判率、重连成功率
- [ ] 安全加固：鉴权默认开启（token/mTLS 至少一种）
- [ ] 指标完善：packet/stream QPS、失败率、队列水位、join latency
- [ ] 清理兼容逻辑（移除旧 gossip 地址字段）
## 开放问题

1. stream 传输优先选 `WebSocket` 还是 `HTTP/2 CONNECT`？
2. internal gossip 接口是否默认强制鉴权（建议是）？
3. 是否需要额外诊断命令：`rimio adm gossip-peers`？
