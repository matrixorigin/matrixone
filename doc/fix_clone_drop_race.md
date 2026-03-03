# 修复 Data Branch CREATE TABLE 与 DROP DATABASE 并发竞态问题

## 问题描述

当 `data branch create table`（使用 Shared 锁）与 `DROP DATABASE`（使用 Exclusive 锁）并发执行时，可能出现以下竞态：

```
时间线:
T1: DROP DATABASE 开始，获取快照 SnapshotTS=100
T2: CREATE TABLE 提交，写入 mo_tables，释放 Shared 锁
T3: DROP DATABASE 获取 Exclusive 锁（此时 CREATE TABLE 已提交）
T4: DROP DATABASE 用 SnapshotTS=100 查询 mo_tables，看不到 T2 创建的表
T5: DROP DATABASE 提交，删除数据库
结果: mo_tables 中残留孤儿记录（T2 创建的表）
```

## 之前的修复方案（#23767）及其问题

之前的修复在获取 Exclusive 锁后调用 `refreshSnapshotAfterLock`，将事务的 SnapshotTS 推进到最新时间戳。

**问题**：这会绕过 workspace tombstone 转移机制，导致在 restore cluster 场景（单事务内执行多个 DDL）中出现 duplicate-key 错误。

## 新的修复方案

### 核心思路

不修改当前事务的 SnapshotTS，而是：
1. 用独立只读事务获取最新快照的表列表（用于 orphan 检测）
2. 用当前事务获取可见的表列表（用于正常删除流程）
3. 计算差集得到 orphan 表
4. 用独立事务删除 orphan 记录

### 实现细节

```go
func (s *Scope) DropDatabase(c *Compile) error {
    // ... 获取 Exclusive 锁 ...

    // 1. 获取最新快照的所有表（包括并发事务提交的）
    allTables, allHiddenTables, err := listRelationsAtLatestSnapshot(c, dbName)
    allTablesIncludingHidden := append(allTables, allHiddenTables...)

    // 2. 获取当前事务可见的表（在 Engine.Delete 之前，否则 tombstone 会影响可见性）
    visibleTables, err := listVisibleRelations(c, dbName)

    // 3. 过滤 hidden 表，只删除普通表
    var deleteTables []string
    for _, t := range visibleTables {
        if !catalog.IsHiddenTable(t) {
            deleteTables = append(deleteTables, t)
        }
    }

    // 4. 正常删除流程：删除当前事务可见的表
    for _, t := range deleteTables {
        c.runSql(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", dbName, t))
    }

    // 5. 删除数据库
    c.e.Delete(ctx, dbName, txnOp)

    // 6. 清理 orphan 记录
    orphanTables := diffStringSlice(allTablesIncludingHidden, visibleTables)
    if len(orphanTables) > 0 {
        deleteOrphanTableRecords(c, dbName, orphanTables)
    }
    // ...
}
```

### 关键函数

#### listRelationsAtLatestSnapshot
- 使用独立只读事务（不带 WithTxn）
- 查询 mo_tables 获取最新已提交的所有表
- 返回 (普通表, hidden表)

#### listVisibleRelations
- 使用当前事务（带 WithTxn）
- 查询 mo_tables 获取当前事务可见的表
- 必须在 Engine.Delete 之前调用（否则 tombstone 会影响可见性）

#### deleteOrphanTableRecords
- 使用 `exec.ExecTxn` 创建独立事务
- 调用 `engine.Database.Delete()` 删除 orphan 表
- **不能用 SQL DELETE**：TN 期望 mo_tables 的 DELETE 条目是 `GenDropTableTuple` 生成的完整 batch 格式，SQL DELETE 只生成 tombstone batch (rowid + pk)，会导致 TN panic
- **不能用 DROP TABLE IF EXISTS**：会调用 `lockMoDatabase(Shared)`，与父事务的 Exclusive 锁死锁

### 原子性分析

独立事务和父事务分别提交：
- 如果独立事务成功 + 父事务失败：orphan 记录被删除但数据库仍存在 —— 无害，orphan 本不应存在
- 如果独立事务失败：`deleteOrphanTableRecords` 返回错误，父事务不继续

## 排除的方案

| 方案 | 排除原因 |
|------|----------|
| `refreshSnapshotAfterLock` | 绕过 workspace tombstone 转移，restore cluster 场景 dup key |
| `isRestoreContext` 条件判断 | 治标不治本，其他场景可能有同样问题 |
| SQL DELETE 删除 orphan | TN 期望完整 batch 格式，会 panic |
| DROP TABLE IF EXISTS 删除 orphan | 会获取 Shared 锁，与 Exclusive 锁死锁 |
| 在当前事务 workspace 写入 tombstone | rowid 来自新快照，与旧 SnapshotTS 不匹配，compaction 时 dup key |

## 文件变更

- `pkg/sql/compile/ddl.go`
  - 移除 `refreshSnapshotAfterLock`
  - 新增 `listRelationsAtLatestSnapshot`
  - 新增 `listVisibleRelations`
  - 新增 `diffStringSlice`
  - 新增 `deleteOrphanTableRecords`
  - 修改 `DropDatabase` 流程

- `pkg/sql/compile/ddl_test.go`
  - 新增 `TestDropDatabase_ListRelationsAtLatestSnapshot`
