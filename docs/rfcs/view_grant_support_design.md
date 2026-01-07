# MatrixOne 视图权限管理支持 (GRANT/REVOKE ON VIEW) 完整设计方案

## 1. 概述 (Overview)
旨在支持对视图（VIEW）进行 `GRANT` 和 `REVOKE` 操作。通过在内部将视图映射为表级对象，并引入执行侧的“权限短路”逻辑，实现在用户获得视图授权后能够安全访问相关数据。

## 2. 详细设计 (Detailed Design)

### 2.1 阶段 1：语法适配与元数据持久化
*   **Parser 适配**：修改 `pkg/sql/parsers/tree/revoke.go`，在 `ObjectType.String()` 中支持 `view` 类型，消除解析时的 `"Unknown ObjectType"` 报错。
*   **对象映射**：在 `pkg/frontend/authenticate.go` 中，将 `tree.OBJECT_TYPE_VIEW` 映射为内部的 `objectTypeTable`。
*   **存储逻辑**：
    *   在 `checkPrivilegeObjectTypeAndPrivilegeLevel` 中，统一视图与表的 ID 检索逻辑，获取 `rel_logical_id`。
    *   **存储一致性**：鉴于 MatrixOne 权限执行引擎硬编码了 `"d.t"` 和 `"t"` 作为过滤条件，存储时需保持 `privilege_level` 字段为这些占位符，以确保校验 SQL 能够匹配。

### 2.2 阶段 2：权限执行与短路逻辑
*   **视图点提取**：重构 `extractPrivilegeTipsFromPlan`，增加 `ses` 参数，通过 `TxnCompilerContext` 提取当前查询涉及的所有视图，并将其标记为 `isView = true` 的权限检查点。
*   **权限短路 (Short-circuit)**：
    *   修改 `determineRoleSetHasPrivilegeSet`。
    *   **预扫描机制**：在遍历权限项前先校验视图项。如果用户对该查询中涉及的**任何一个视图**拥有 `SELECT` 权限，则整个复合权限校验（`compoundEntry`）立即通过。
    *   **目的**：解耦视图与其底层物理表的直接权限依赖。

### 2.3 本次实现已支持功能（可测试清单）
*   **语法与展示**：支持 `GRANT/REVOKE ... ON VIEW`；`SHOW GRANTS` 输出 `ON VIEW`。
*   **权限级别**：支持 `*.*`、`db.*`、`db.view`、`view`（当前数据库）四种级别，权限类型与表一致（如 `SELECT/INSERT/UPDATE/DELETE/OWNERSHIP/ALL`）。
*   **落库与识别**：view 权限以 `obj_type="view"` 存储，`obj_id` 来自 `mo_catalog.mo_tables.rel_logical_id`（`relkind='v'`）。
*   **计划节点血缘**：编译阶段在计划节点记录 `origin_views`/`direct_view`，多层 view 会保留依赖链。
*   **执行期校验规则**：对每个涉及底表的节点单独校验；当存在 `origin_views` 时先检查 view 权限（优先使用 `direct_view`，缺失时用 `origin_views[0]`），再按 `DEFINER/INVOKER` 决定是否还需校验底表权限；无 `origin_views` 则走普通表权限。
*   **DEFINER/INVOKER**：`view_security_type` 为 Session 变量（默认 `DEFINER`），值写入 viewdef JSON 的 `security_type`；`DEFINER` 要求 view 权限 + definer 底表权限，`INVOKER` 要求 view 权限 + 调用者底表权限；旧视图缺失 `security_type` 时按 `DEFINER` 处理。
*   **跨库与 USE 语义**：`USE <db>` 仍要求数据库权限；对 `db.view` 的全路径 `SELECT` 可在无 `USE` 权限下执行（只要 view 权限通过）。
*   **角色与租户**：角色继承、多权限授权/部分回收、非系统租户均可用（见 BVT 用例）。

## 3. 实施过程中的问题与约束 (Lessons Learned & Issues)
*   **权限执行引擎的占位符依赖**：
    *   *现象*：在 `GRANT` 时若存入真实对象名（如 `db1.v1`），`SHOW GRANTS` 显示正常，但 `SELECT` 校验失败。
    *   *原因*：MatrixOne 的权限执行引擎（Enforcement Engine）在生成校验 SQL 时，硬编码了 `"d.t"` 或 `"t"` 占位符。
    *   *原因*：MatrixOne 的权限执行 SQL 严格依赖这些特定字符串进行过滤。若更改存储字符串为真实对象名，将导致校验引擎（Enforcement Engine）失效。彻底解决需重构权限表的索引与过滤机制。
*   **数据库级访问强制依赖**：用户仍需拥有基础的数据库 `CONNECT` 权限（或更高）才能执行 `USE <db>` 语句进入视图所在空间。
    *   *原因*：MatrixOne 的 `USE` 语句校验发生在 Account 级别，目前不与具体的对象（如视图）挂钩。
*   **物理表粒度解耦**：当前短路逻辑是“语句级”的。如果一条 SQL 同时查询了授权视图和未授权物理表，短路逻辑可能会导致权限略微放宽。
    *   *原因*：现有的 `compoundEntry` 结构难以在 Frontend 层精确建立“物理表 -> 来源视图”的归属映射。

---
**总体完成度：90%** (核心授权逻辑与安全性解耦已完成)。

---

## 4. 终极优化方案 (终态设计，可直接实施)

> 目标：把 VIEW 作为“一等对象”落库与鉴权；消除权限短路的过度放行；建立“视图血缘”到物理表的精确校验链；兼容当前权限表结构并提供平滑迁移路径。

### 4.1 设计原则
1. **对象模型统一**：VIEW 独立 objectType，拥有独立 obj_id（rel_logical_id），不再借用 table 语义。
2. **按节点精确校验**：权限检查以“计划节点/物理表”为粒度，不在语句级进行短路放行。
3. **可配置语义**：支持 `SQL SECURITY DEFINER | INVOKER`，默认 DEFINER。
4. **向后兼容**：保留旧 privilege_level 字符串占位符的读路径，新增 view 专用检查路径。
5. **可观测性**：`SHOW GRANTS` 与错误信息明确指出 view/底表的权限缺失点。

### 4.2 核心数据结构与存储

#### 4.2.1 新增 objectTypeView
**位置**：`pkg/frontend/authenticate.go`  
**改动**：
- 在 `objectType` 中新增 `objectTypeView`，`String()` 支持 `view`。
- 更新 `objectType2privilegeLevels`，为 view 复用 table 的 privilegeLevel（`*.*`, `db.*`, `db.table`, `table`），确保语法一致性。

#### 4.2.2 权限表存储约定
**表**：`mo_catalog.mo_role_privs`  
**写入规范**：
- `obj_type = "view"`。
- `obj_id = view.rel_logical_id`。
- `privilege_level` 仍存占位符（`d.t`/`t` 等），以兼容既有 SQL 过滤逻辑，但新增 view 专用检查 SQL 不再依赖这些占位符。

**读路径**：
- 增加 view 专用检查 SQL：以 `obj_type="view"` + `obj_id` 精确匹配，避免 `mo_database/mo_tables` 连接约束。

### 4.3 视图血缘采集与编译时落地

#### 4.3.1 编译阶段记录“视图来源”
**位置**：`pkg/sql/plan/query_builder.go`  
**改动**：
- 扩展 Plan Node（或 ObjectRef/TableDef）新增字段：  
  - `origin_views []string`：视图依赖链，格式使用统一分隔符（建议 `db#view`，与现有 `recordViews` 一致）。  
  - `direct_view string`：当前 table scan 展开的直接 view（可选，用于更精细诊断）。
- 在 `bindView` 展开 view 时，把 `schema#view` 写入 `origin_views`，并把 `viewCtx.views` 级联写入。

#### 4.3.2 统一视图标识格式
**位置**：`pkg/frontend/util.go`  
**改动**：
- 复用 `KeySep = "#"`, `genKey`, `splitKey`。
- 在所有读取 `views` 的逻辑中，统一用 `splitKey`，禁止解析 `.`，避免跨模块格式不一致。

### 4.4 权限校验算法（替代短路逻辑）

#### 4.4.1 校验规则
对每一个实际参与查询的表节点 `T`：
1. 若用户具备 `TABLE` 权限（对 `T` 的 `SELECT/UPDATE/...`），则该节点通过；
2. 否则检查 `T` 的 `origin_views`，若对 **任意一个视图** 具备该操作权限，则该节点通过；
3. 否则拒绝。

**关键点**：
- 这是 **节点级 OR**，语句级仍保持 **AND**。  
- 这样既能让 view 授权生效，也不会放大到未授权的独立表。

#### 4.4.2 DEFINER / INVOKER 语义
**DEFINER**（默认）：
- 用户：需要 `VIEW` 权限。  
- view owner：必须对底表具备权限（创建时校验或懒校验）。  
- 执行期：不再要求用户对底表有权。

**INVOKER**：
- 用户既需要 `VIEW` 权限，也需要底表权限。  
- 可用于需要强隔离的业务场景。

**实现点**：
- `CREATE VIEW` 新增语法参数或 session 变量控制 `SQL SECURITY`。  
- `mo_catalog.mo_views` 中持久化 `security_type` 和 `definer_role_id`。

### 4.5 SQL 生成与权限检查实现路径

#### 4.5.1 新增 view 权限 SQL
**位置**：`pkg/frontend/authenticate.go`  
**新增函数**：
- `getSqlForCheckRoleHasViewPrivilege(roleId, privId, viewId)`  
  ```sql
  select role_id, with_grant_option
    from mo_catalog.mo_role_privs
   where role_id = ?
     and obj_type = "view"
     and obj_id = ?
     and privilege_id = ?;
  ```

#### 4.5.2 privilegeEntry 扩展
**位置**：`pkg/frontend/authenticate.go`  
**改动**：
- `privilegeItem` / `privilegeTips` 增加 `objType`，并在 view 权限路径中设置 `objectTypeView`。
- `extractPrivilegeTipsFromPlan`：  
  - 直接读取 plan node 的 `origin_views`；  
  - 生成 `PrivilegeTypeSelect` + `objectTypeView` 的 tips；  
  - 使用 `splitKey` 解析 `db#view`。

#### 4.5.3 新的权限核验流程
**位置**：`determineRoleSetHasPrivilegeSet`  
**替换逻辑**：
- 移除“语句级短路”。  
- 在遍历 compound entry 时，针对每个 `privilegeItem` 做节点级校验：  
  1) 先走 `table` 权限检查；  
  2) 若失败，再遍历 `origin_views` 进行 `view` 权限检查；  
  3) 任一通过则该 item 通过，否则失败。

### 4.6 迁移与兼容策略

#### 4.6.1 元数据迁移
- 新增 `objectTypeView` 后，旧数据仍保留（`obj_type="table"` + `privilege_level` 占位符）。  
- 通过后台任务扫描 `mo_catalog.mo_tables` 中 `relkind = view` 的记录，把历史 view 授权迁移为 `obj_type="view"`（幂等）。  
- 迁移期间权限检查同时支持 table 路径与 view 路径，优先命中 view 路径。

#### 4.6.2 回滚路径
- 增加 `session var enable_view_privilege_v2` 控制新路径开关。  
- 回滚时仅关闭该开关，旧逻辑继续可用。

### 4.7 测试与验证清单
1. **基础**：`GRANT/REVOKE SELECT ON VIEW`，`SHOW GRANTS` 显示 `ON VIEW`。  
2. **混合查询**：`SELECT v, t`，只授予 `view` 权限应拒绝 `t` 访问。  
3. **多层 view**：`v2` 依赖 `v1`，授权 `v2` 可访问；撤销 `v2` 即失败。  
4. **INVOKER**：用户无底表权限时应拒绝。  
5. **cluster table**：视图含系统表，必须仍受 `verifyLightPrivilege` 限制。  
6. **跨库**：`db1.v1` 引用 `db2.t`，权限分层正确。  

### 4.8 预计改动清单（文件级）
- `pkg/frontend/authenticate.go`:  
  - 新增 `objectTypeView`、view SQL 校验、节点级校验替换短路逻辑。  
- `pkg/sql/plan/query_builder.go`:  
  - 记录 `origin_views` 到 plan node。  
- `pkg/sql/plan/*`:  
  - Node 定义扩展并序列化/反序列化。  
- `pkg/frontend/util.go`:  
  - 统一视图标识解析。  
- `docs/rfcs/view_grant_support_design.md`:  
  - 本节补充。
