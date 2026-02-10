# Amberio 设计文档（RFC 0005）

> 引入 `amberio-s3-gateway` 包，提供 S3 兼容入口；同时将 Amberio 原生 REST API 统一迁移到 `/_/api/v1/*` 前缀。

## 背景

Amberio 当前有两条需求同时推进：

1. 对外提供 S3-compatible 协议入口，便于直接使用现有 SDK（例如 Python `boto3`）接入。
2. Amberio 自身原生 REST API（`/api/v1/*`）应与 S3 路由解耦，避免顶层路径冲突。

目前服务入口仍以 `/api/v1/*` 为主，不利于与 S3 路由共存。RFC 0005 解决协议分层与路径治理问题。

---

## 设计目标

1. 新增独立 crate：`amberio-s3-gateway`，承载 S3 协议解析与 XML 响应编排。
2. `amberio-server` 继续作为统一进程与端点暴露层，挂载：
   - Amberio 原生 API：`/_/api/v1/*`
   - Amberio 内部 API：`/internal/v1/*`（保持不变）
   - S3-compatible API：根路径样式（path-style）
3. 首期 S3 兼容覆盖：`put/get/head/delete/list-v2`。
4. `multipart upload` 首期允许返回明确的 `NotImplemented`，并给出稳定错误语义，后续单独 RFC/迭代落地完整实现。
5. 提供 Python 集成测试（使用 `uv` 管理依赖）验证 S3 基础读写链路。

## 非目标

1. 本 RFC 不定义 IAM/ACL/Policy 体系。
2. 本 RFC 不引入签名校验强制（可先“接受但不校验签名”）。
3. 本 RFC 不实现 multipart 数据面与后台合并。
4. 本 RFC 不改变 Amberio 内部存储模型（slot/head/part）。

---

## 路由与协议规划

### 1) Amberio 原生 API 迁移

- 旧路径：`/api/v1/*`
- 新路径：`/_/api/v1/*`

迁移后，文档与集成测试统一使用新前缀。

### 2) S3 路由（path-style）

- `PUT /{bucket}/{key}` -> PutObject
- `GET /{bucket}/{key}` -> GetObject
- `HEAD /{bucket}/{key}` -> HeadObject
- `DELETE /{bucket}/{key}` -> DeleteObject
- `GET /{bucket}?list-type=2&prefix=&max-keys=&continuation-token=` -> ListObjectsV2

说明：
- 首期使用 path-style，避免 virtual-host-style 域名依赖。
- bucket 仅作为逻辑命名空间，落地到 Amberio path 时映射为 `bucket/key`。

### 3) Multipart（首期行为）

- `POST /{bucket}/{key}?uploads`
- `PUT /{bucket}/{key}?partNumber=N&uploadId=...`
- `POST /{bucket}/{key}?uploadId=...`

首期统一返回 S3 风格错误：`NotImplemented`（HTTP 501）。

---

## 数据映射

将 S3 对象映射到 Amberio 内部路径：

- `internal_path = "{bucket}/{key}"`

这样可直接复用当前 `PutBlobOperation` / `ReadBlobOperation` / `ListBlobsOperation`。

### ListObjectsV2 映射

- 输入 `bucket + prefix + continuation-token + max-keys`
- 转为 Amberio list：
  - `prefix = "{bucket}/" + user_prefix`
  - `limit = max-keys`
  - `cursor = decode(continuation-token)`
- 输出时去掉 `"{bucket}/"` 前缀后写入 XML `Key` 字段。

Continuation token 采用不透明编码（base64-url），避免特殊字符泄漏与兼容问题。

---

## 错误语义

S3 入口返回 XML 风格错误体：

- `NoSuchKey` -> 404
- `NoSuchBucket` -> 404（首期 bucket 视为逻辑命名空间，默认不预建；保留该错误码映射能力）
- `InvalidArgument` -> 400
- `NotImplemented` -> 501（multipart）
- `InternalError` -> 500

---

## 集成测试策略（先测后实现）

新增 Python 用例（`integration/013_*`）：

1. 使用 `boto3` 对 Amberio 发起 path-style S3 请求。
2. 覆盖场景：
   - PutObject
   - GetObject
   - HeadObject
   - ListObjectsV2（含分页）
   - DeleteObject
   - Multipart（预期 `NotImplemented`）
3. Python 依赖改为 `uv` 管理（在 `integration/` 提供 `pyproject.toml`）。

---

## 实施步骤

1. 创建 `amberio-s3-gateway` crate，定义：
   - S3 请求参数解析
   - XML 响应模型
   - `GatewayBackend` trait
   - axum router 与 handlers
2. 在 `amberio-server` 实现 `GatewayBackend`，桥接 core operations。
3. `amberio-server` 路由迁移：
   - 原生 API 改到 `/_/api/v1/*`
   - 挂载 S3 路由到根路径
4. 调整 README 与现有 integration 脚本默认前缀。

---

## 兼容性与迁移

- 对现有 `/api/v1/*` 客户端：
  - 需要迁移到 `/_/api/v1/*`。
- 对新接入客户端：
  - 推荐直接使用 S3 SDK（path-style endpoint）。
- 在完整 multipart 支持上线前，SDK 的 multipart 路径会收到明确 `NotImplemented` 错误。

---

## 验收标准

1. 所有既有集成测试（001-012）在新 `/_/api/v1` 前缀下通过。
2. 新增 S3 集成测试通过（至少 put/get/head/list/delete）。
3. multipart 用例返回稳定 `501 + NotImplemented`。
4. `amberio-server` 对外同一进程同时暴露两类接口：
   - `/_/api/v1/*`
   - S3 path-style root routes。
