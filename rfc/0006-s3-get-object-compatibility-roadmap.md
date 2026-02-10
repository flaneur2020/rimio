# Amberio 设计文档（RFC 0006）

> 聚焦已支持 S3 API 的兼容性收敛路径，优先完善 `GetObject` 参数面与行为语义。

## 背景

RFC 0005 已完成两件关键事情：

1. 引入 `amberio-s3-gateway` 并在 `amberio-server` 中统一暴露。
2. 将 Amberio 原生 REST API 迁移到 `/_/api/v1/*`，为 S3 根路径路由留出空间。

当前 S3 能力已经可用，但兼容层仍处于 V1：

- 已支持：`PutObject`、`GetObject`、`HeadObject`、`DeleteObject`、`ListObjectsV2`
- 未支持：Multipart 全流程
- `GetObject` 的输入字段虽然逐步补齐，但行为语义仍需继续对齐 SDK/协议预期。

本 RFC 给出“先覆盖兼容面，再补齐复杂语义”的收敛路线。

---

## 设计目标

1. `GetObjectRequest` 字段模型向主流 SDK 输入结构对齐（以 `s3s`/AWS SDK 字段集合为参考）。
2. 明确“已支持字段”和“已声明但未实现字段（NotImplemented）”边界，避免灰色行为。
3. 提供高信号集成测试，覆盖：
   - 条件请求
   - 范围读取与 `partNumber`
   - 响应头覆盖参数
   - 未实现能力的稳定错误语义
4. 在不破坏现有 API 的前提下，为 Multipart 与版本化读取铺路。

## 非目标

1. 本 RFC 不实现对象版本化存储。
2. 本 RFC 不实现 SSE-C 解密链路。
3. 本 RFC 不实现 ACL/IAM 权限体系。
4. 本 RFC 不实现 virtual-hosted-style 路由。

---

## 当前 API 清单（已支持）

- `PUT /{bucket}/{key}` -> `PutObject`
- `GET /{bucket}/{key}` -> `GetObject`
- `HEAD /{bucket}/{key}` -> `HeadObject`
- `DELETE /{bucket}/{key}` -> `DeleteObject`
- `GET /{bucket}?list-type=2...` -> `ListObjectsV2`

---

## GetObject 兼容矩阵（V1.1）

### 已实现（或可稳定工作）

- 基础读取：`bucket`、`key`
- 单区间 range：`Range`（不支持多区间）
- 条件请求：
  - `If-Match`
  - `If-None-Match`
  - `If-Modified-Since`
  - `If-Unmodified-Since`
- 分段读取参数：`partNumber`
- 响应头覆盖：
  - `response-cache-control`
  - `response-content-disposition`
  - `response-content-encoding`
  - `response-content-language`
  - `response-content-type`
  - `response-expires`
- 兼容透传字段（当前可接受）：
  - `checksum_mode`
  - `expected_bucket_owner`
  - `request_payer`

### 已声明但明确未实现

- `version_id`：返回 `501 NotImplemented`
- SSE-C 相关字段：
  - `sse_customer_algorithm`
  - `sse_customer_key`
  - `sse_customer_key_md5`
  - 返回 `501 NotImplemented`

---

## 错误语义约定

GetObject 关键错误映射：

- `NoSuchKey` -> `404`
- `InvalidRange` -> `416`
- 条件不满足：
  - `Not Modified` -> `304`
  - `PreconditionFailed` -> `412`
- 未实现能力：`NotImplemented` -> `501`
- 参数错误：`InvalidArgument` -> `400`

目标是让 SDK 客户端在状态码与错误码两个维度都可预测。

---

## 集成测试策略

新增 `integration/014_s3_get_object_compat.py`，覆盖：

1. `Range` 读取契约
2. `partNumber` 合法/越界路径
3. `If-*` 条件请求（304/412 分支）
4. `response-*` 响应头覆盖
5. `versionId` 返回 `501 NotImplemented`
6. SSE-C 请求头返回 `501 NotImplemented`

测试目标不是“全参数都实现”，而是“全参数都有明确契约”。

---

## 后续增强路线

### Phase A：GetObject 行为深化

- `expected_bucket_owner` 语义对齐（不匹配返回 403）
- `request_payer` 语义明确化（含响应头 `x-amz-request-charged`）
- `checksum_mode` 输出头规范化

### Phase B：版本化读取

- 引入 `version_id` 的真实读取语义（前提：底层版本索引与生命周期策略）

### Phase C：SSE-C

- 引入 SSE-C 解密路径（密钥校验、MD5 校验、错误语义）

### Phase D：Multipart

- `CreateMultipartUpload`
- `UploadPart`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- `ListParts`

---

## 验收标准

1. `GetObjectRequest` 字段模型包含目标兼容字段集合。
2. `version_id` 与 SSE-C 在未实现阶段返回稳定 `501 NotImplemented`。
3. `integration/014` 持续通过。
4. 现有 `013` 与已有 S3/原生集成用例不回归。
