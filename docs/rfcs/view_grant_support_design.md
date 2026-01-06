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

## 3. 实施变更与约束 (Lessons Learned)
*   **元数据依赖**：在执行 `SELECT` 时，MatrixOne 的 `Binder` 仍需访问底层表的定义。如果用户连数据库的 `CONNECT` 权限都没有，`USE` 语句或对象解析会提前失败。因此，BVT 测试中仍需确保用户拥有基础的数据库访问权。
*   **占位符机制**：`mo_role_privs` 中的 `privilege_level` 字段目前作为执行过滤的一部分，不可随意更改为真实对象名，否则会导致 `enforcement` 阶段查不到权限记录。

## 4. BVT 测试方案
*   **`grant_view.sql`**：验证 `GRANT/REVOKE ... ON VIEW` 的成功执行、`SHOW GRANTS` 的显示以及通过视图进行的 `SELECT` 操作。
*   **`grant_view_non_sys.sql`**：验证普通租户（Non-sys）下的视图权限隔离与应用。
*   **`grant_view_complex.sql`**：覆盖角色继承（Role Inheritance）和针对视图的 `GRANT ALL` 场景。
