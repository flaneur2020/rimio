# Amberio 设计文档（RFC 0007）

> 扩展 S3 兼容层在 `PutObject` 与 `ListObjectsV2` 上的“字段面 + 行为语义”契约，并保持 staged（分阶段）落地策略。

## 背景

RFC 0006 已把 `GetObject` 的兼容路径明确化：

- 请求字段显式建模
- 已支持/未实现能力有清晰边界
- 集成测试覆盖关键分支（含 `NotImplemented` 语义）

下一步应对同样方法推广到当前已对外提供的其它核心 API：

- `PutObject`
- `ListObjectsV2`

目标是让 SDK 使用者对成功路径与失败路径都可预测，而不是“隐式忽略参数”。

---

## 设计目标

1. 为 `PutObject` 和 `ListObjectsV2` 补齐主流 SDK 常见输入字段。
2. 保留当前已稳定行为，同时把暂未实现能力统一映射到明确错误码（优先 `NotImplemented`）。
3. 增加高信号集成测试，覆盖条件写、摘要校验、分页、分隔符与参数错误路径。
4. 为后续 Multipart、版本化、ACL/IAM、对象锁等能力预留演进位。

## 非目标

1. 本 RFC 不实现对象版本化写入。
2. 本 RFC 不实现 ACL/IAM 权限控制。
3. 本 RFC 不实现 SSE-C/KMS 的真实加解密链路。
4. 本 RFC 不实现 Multipart 全流程。

---

## API 现状（已支持）

- `PUT /{bucket}/{key}` -> `PutObject`
- `GET /{bucket}/{key}` -> `GetObject`
- `HEAD /{bucket}/{key}` -> `HeadObject`
- `DELETE /{bucket}/{key}` -> `DeleteObject`
- `GET /{bucket}?list-type=2...` -> `ListObjectsV2`

---

## PutObject 兼容矩阵（V1.2）

### 已实现（或可稳定工作）

- 基础写入：`bucket`、`key`、`body`
- 条件写（预条件）：
  - `If-Match`
  - `If-None-Match`
- 内容摘要校验：`Content-MD5`
- 常见内容头字段建模（当前接受，部分仅参与请求校验）：
  - `Content-Type`
  - `Cache-Control`
  - `Content-Disposition`
  - `Content-Encoding`
  - `Content-Language`
  - `Expires`
  - `Content-Length`
- 自定义元数据字段：`x-amz-meta-*`

### 已声明但明确未实现

- ACL/Grant 相关（如 `x-amz-acl`）
- SSE/KMS/SSE-C 相关头
- 对象锁相关头
- `x-amz-tagging`
- `x-amz-website-redirect-location`
- 高级校验算法族（`x-amz-checksum-*`，除 `Content-MD5` 以外）

上述未实现能力统一返回 `501 NotImplemented`。

---

## ListObjectsV2 兼容矩阵（V1.2）

### 已实现（或可稳定工作）

- `prefix`
- `max-keys`（含上限约束）
- `continuation-token`
- `start-after`
- `delimiter`（CommonPrefixes 折叠）
- 基础分页与 `NextContinuationToken`

### 已声明但明确未实现

- `fetch-owner`
- `optional-object-attributes`

上述未实现能力统一返回 `501 NotImplemented`。

### 参数校验约定

- 非法 `continuation-token` -> `400 InvalidArgument`
- 非法枚举参数（如不支持的 `encoding-type`）-> `400 InvalidArgument`

---

## 错误语义约定

### PutObject

- 预条件失败 -> `412 PreconditionFailed`
- MD5 编码非法 -> `400 InvalidDigest`
- MD5 不匹配 -> `400 BadDigest`
- 未实现能力 -> `501 NotImplemented`

### ListObjectsV2

- 参数非法 -> `400 InvalidArgument`
- 未实现能力 -> `501 NotImplemented`

---

## 集成测试策略

新增 `integration/015_s3_put_list_compat.py`，覆盖：

1. `PutObject` 的 `Content-MD5` 成功/失败分支
2. `PutObject` 条件写 `If-Match` / `If-None-Match`
3. `PutObject` 未实现能力（ACL / SSE-C）稳定返回 `501`
4. `ListObjectsV2` 分页与 continuation token
5. `ListObjectsV2` 的 `StartAfter` 与 `Delimiter`（含 `CommonPrefixes`）
6. `ListObjectsV2` 非法参数与未实现参数错误语义

---

## 后续增强路线

### Phase A：PutObject 深化

- 支持 `x-amz-checksum-*` 与响应校验头
- 元数据/内容头持久化与读取回显（Get/Head）

### Phase B：ListObjectsV2 深化

- `fetch-owner` 输出语义
- `optional-object-attributes` 输出
- 编码类型与边界语义进一步对齐

### Phase C：Multipart / 版本化

- Multipart 全链路
- 版本化写入与按版本读取

---

## 验收标准

1. `PutObject` 与 `ListObjectsV2` 请求模型包含目标字段集合。
2. 未实现能力返回稳定 `501 NotImplemented`。
3. `integration/015` 持续通过。
4. 已有 `013`、`014` 与其它集成用例不回归。
