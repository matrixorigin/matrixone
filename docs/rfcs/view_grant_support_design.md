# MatrixOne 视图权限管理支持 (GRANT/REVOKE ON VIEW) 最终设计与实施

## 最终设计方案

### 1. 目标与范围
为视图提供 `GRANT/REVOKE ... ON VIEW` 授权能力，使用户在拥有视图授权后能访问视图结果，同时按 `SQL SECURITY` 控制底表权限要求。

### 2. 权限对象与级别
- 引入独立的 `objectTypeView`，与 `objectTypeTable` 并列。
- 权限级别与表保持一致：`*.*`、`db.*`、`db.view`、`view`（当前数据库）。
- 权限类型与表一致（如 `SELECT/INSERT/UPDATE/DELETE/OWNERSHIP/ALL`），`SHOW GRANTS` 输出 `ON VIEW`。

### 3. 元数据与存储
- 视图对象 id 使用 `mo_catalog.mo_tables.rel_logical_id`（`relkind='v'`）。
- 权限落库 `mo_catalog.mo_role_privs`：`obj_type="view"`，`obj_id=rel_logical_id`。
- 权限查询路径与表一致，但 obj_type 区分为 view，避免与表授权混淆。

### 4. 计划阶段视图血缘
编译阶段记录视图血缘信息，写入计划节点：
- `origin_views`：视图链（`db#view` 格式）。
- `direct_view`：当前 table scan 展开的直接视图（可选）。

### 5. 执行期鉴权流程
对每个参与执行的计划节点做权限检查：
1. 先按节点对象类型校验视图权限（使用 `direct_view`，缺失则用 `origin_views[0]`）。
2. 再结合 `SQL SECURITY` 语义决定是否继续校验底表权限：
   - `DEFINER`：要求 view 权限 + definer 角色对底表具备权限。
   - `INVOKER`：要求 view 权限 + 当前用户对底表具备权限。
3. 若节点无 `origin_views`，走普通表权限检查路径。

### 6. SQL SECURITY 语义
- 通过 session 变量 `view_security_type` 控制（默认 `DEFINER`）。
- `security_type` 持久化在 viewdef JSON 中。
- 旧视图缺失 `security_type` 时按 `DEFINER` 处理。

### 7. 特殊场景与兼容
- 系统视图库（`information_schema` / `mysql`）不进行 view 权限校验，保持只读行为。
- 跨库 `db.view` 的全路径查询可在无 `USE` 权限时执行（前提是 view 权限通过）。

### 8. 可测试行为清单
- `GRANT/REVOKE ... ON VIEW` 语法与 `SHOW GRANTS` 输出正确。
- 视图多层依赖链可授权访问，撤销后失效。
- `DEFINER/INVOKER` 语义正确生效。
- 角色继承、多权限授权/部分回收、非系统租户均可用。

## 实施过程

### 1. 语法与对象模型
- 扩展 `ObjectType.String()` 支持 `view`。
- 新增 `objectTypeView` 并补齐 privilege level 映射与展示逻辑。

### 2. 元数据与计划结构
- 扩展计划节点与相关结构体序列化（`proto/plan.proto` + `pkg/pb/plan/plan.pb.go`）。
- 在查询构建阶段记录 `origin_views` / `direct_view`，贯穿计划传递。

### 3. 鉴权与执行逻辑
- 在权限提取阶段对视图节点生成 view 权限检查项。
- 执行期按 `origin_views`/`direct_view` 做 view 权限校验，并结合 `SQL SECURITY` 选择是否继续校验底表权限。
- 针对 `INSERT/UPDATE/DELETE` 等写路径补齐 privilege tips 解析，确保视图授权不误放行也不误拦截。
- 系统视图库和 cluster table 的权限检查保持原有规则。

### 4. 用例与结果集
新增/更新 BVT：
- `test/distributed/cases/zz_accesscontrol/grant_view.sql`
- `test/distributed/cases/zz_accesscontrol/grant_view_non_sys.sql`
- `test/distributed/cases/zz_accesscontrol/grant_view_complex.sql`
- 相关 `.result` 文件与错误信息对齐（如 `grant_privs_role.result`、`revoke_privs_role.result`）。
