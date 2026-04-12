# CLONE + DROP DATABASE 竞态条件分析与修复

## 1. 问题现象

MO 重启后 checkpoint replay 阶段 panic：

```
panic: ExpectedEOB

goroutine ... [running]:
catalog.(*Catalog).onReplayCreateTable(...)
    catalogreplay.go:496
```

`onReplayCreateTable` 在 replay mo_tables 中的一条 CREATE TABLE 记录时，调用 `GetDatabaseByID(dbId)` 返回 `OkExpectedEOB`——该 database 在 mo_database 中已被删除（有 tombstone），但 mo_tables 中的表记录却没有对应的 tombstone，成为"孤儿记录"。

## 2. 复现路径

1. 启动 MO
2. 运行 `tools/repro_clone_race/repro.go`（并发执行 CLONE + DROP DATABASE）
3. 手动创建两个 checkpoint
4. 重启 MO → panic

## 3. 根因分析

### 3.1 涉及的锁机制

| 操作 | 锁对象 | 锁模式 |
|------|--------|--------|
| CREATE TABLE (含 CLONE) | mo_database 行 | Shared |
| DROP DATABASE | mo_database 行 | Exclusive |

锁的 key 由 `Serial(accountId, dbName)` 生成，锁在 lock service 中持有，事务提交时释放。

### 3.2 竞态时间线

以日志中 `clone_tgt_5` (dbId=272530) 为例：

```
T1  17:58:32.041  CREATE DATABASE clone_tgt_5 (dbId=272530)
T2  17:58:32.047  CLONE: lockMoDatabase(Shared) 成功
T3  17:58:32.048  CLONE: createWithID → 写入 mo_tables (tableId=272531)
T4  17:58:32.052  CLONE: tn.handle.create.relation 完成
T5  17:58:32.053  CLONE: 事务提交 (commitTS=1772013512052191646)，释放 Shared lock
T6  17:58:32.053  DROP:  lockMoDatabase(Exclusive) 成功 (snapshotTS=1772013512050552757)
T7  17:58:32.055  DROP:  Relations() → 看不到 t1 → 不删除表
T8  17:58:32.055  DROP:  tn.handle.drop.database (Id=272530) → 只删 database，不删 table
```

### 3.3 为什么 DROP 的 snapshot 没有被推进

`lockMoDatabase(Exclusive)` 内部调用 `doLock()`，其正常路径：

```go
if result.NewLockAdd && !result.HasConflict && snapshotTS.LessEq(lockedTS) && RC {
    newSnapshotTS = WaitLogTailAppliedAt(lockedTS)
    changed = hasNewVersionInRange(mo_database行, snapshotTS, newSnapshotTS)
    if changed {
        UpdateSnapshot(newSnapshotTS)
        return refreshTS  // → 触发 retry
    }
    // changed=false → 不更新 snapshot，继续执行
}
```

关键：`hasNewVersionInRange` 检查的是 **mo_database 表中被锁定的行**是否在 `[snapshotTS, newSnapshotTS]` 区间内被修改。

CLONE 操作只往 **mo_tables** 写入新行，**不修改 mo_database 的行**，所以 `changed = false`，DROP 的 snapshot 不会被推进到 CLONE 的 commitTS 之后。

### 3.4 结果

DROP 的 `Relations()` 使用 stale snapshot 查询 mo_tables，看不到 CLONE 刚提交的表 → 直接删除 database → mo_tables 中留下孤儿记录（有 INSERT 无 tombstone）→ checkpoint replay 时 panic。

### 3.5 为什么反方向没问题

如果 DROP 先拿到 Exclusive lock 并提交：
- CLONE 后续拿 Shared lock 时，`doLock()` 检测到 mo_database 行被 DROP **修改**了（DELETE 操作）
- `changed = true` → 返回 retry error
- CLONE 重试后发现 database 不存在 → 返回 `Unknown database` 错误

## 4. 预防修复方案（防止新增孤儿记录）

### 4.1 方案选择

| 方案 | 描述 | 评估 |
|------|------|------|
| A. createWithID 中检查 catalog cache | 在写 mo_tables 前查 catalog cache 确认 db 存在 | ❌ logtail 有延迟，DROP 可能还没 apply 到 cache |
| B. CreateTable 改用 Exclusive lock | 将 Shared 改为 Exclusive | ❌ 影响同 database 下所有 CREATE TABLE 的并发性 |
| C. DropDatabase 中刷新 snapshot | 获取 Exclusive lock 后，强制推进 snapshot 到最新 | ✅ 精准修复，影响范围小 |
| D. 修改 lock 冲突检测逻辑 | 让 hasNewVersionInRange 同时检查 mo_tables | ❌ 改动太大，影响全局 |

选择方案 C。

### 4.2 具体实现

将 snapshot 刷新逻辑直接内置到 `lockMoDatabase`（`pkg/sql/compile/ddl.go`）中，当锁模式为 Exclusive 时自动刷新 snapshot。原有的加锁逻辑拆分为 `doLockMoDatabase`（纯加锁）和 `lockMoDatabase`（加锁 + 条件刷新）：

```go
var doLockMoDatabase = func(c *Compile, dbName string, lockMode lock.LockMode) error {
    // 原有的加锁逻辑：getRelFromMoCatalog + getLockBatch + lockRows
    ...
}

var lockMoDatabase = func(c *Compile, dbName string, lockMode lock.LockMode) error {
    if err := doLockMoDatabase(c, dbName, lockMode); err != nil {
        return err
    }

    if lockMode == lock.LockMode_Exclusive {
        txnOp := c.proc.GetTxnOperator()
        if txnOp.Txn().IsPessimistic() && txnOp.Txn().IsRCIsolation() {
            latestCommitTS := c.proc.Base.TxnClient.GetLatestCommitTS()
            if txnOp.Txn().SnapshotTS.Less(latestCommitTS) {
                newTS, err := c.proc.Base.TxnClient.WaitLogTailAppliedAt(...)
                txnOp.UpdateSnapshot(c.proc.Ctx, newTS)
            }
        }
    }
    return nil
}
```

### 4.3 修复原理

```
T5  CLONE 提交 (commitTS=X)，释放 Shared lock
T6  DROP lockMoDatabase(Exclusive):
      - doLockMoDatabase 获取 Exclusive lock
      - lockMode == Exclusive → 进入 snapshot 刷新
      - 获取 latestCommitTS (≥ X)
      - WaitLogTailAppliedAt(latestCommitTS) → 确保 logtail 已 apply
      - UpdateSnapshot(newTS) → snapshot 推进到 X 之后
T7  DROP Relations() → 使用新 snapshot → 能看到 CLONE 创建的表 → 正确删除
```


## 5. 生产环境存量数据修复（已有孤儿记录）

### 5.1 问题本质

生产环境如果已经出现了孤儿记录，第 4 节的预防方案只能防止新增，无法清除已有的孤儿数据。

孤儿数据的存储位置：
- mo_tables（tid=2）的 **data object** 中的一行（或多行）
- 该行记录了一个 table 的元信息，但其 dbid 对应的 database 已被删除
- 该行 **没有对应的 tombstone**（因为 DROP DATABASE 时看不到这个表，没有为它写 tombstone）

### 5.2 为什么 ickp + gckp 无法清除孤儿数据

checkpoint（无论 ickp 还是 gckp）只收集 catalog 内存中的 **object 级别元信息**（ObjectEntry 的 ObjectStats、createTS、deleteTS 等），不会重写 mo_tables data object 内部的行数据。

数据流：
1. gckp 的 `GlobalCollector_V2` 遍历 catalog 内存中的 ObjectEntry
2. mo_tables（tid=2）属于 mo_catalog（永远不会被删除），其 data objects 每次都会被收集
3. checkpoint 文件记录的是"mo_tables 有哪些 data objects"，不改变 data object 内部的行内容
4. 下次 replay 时，`ReadSysTableBatch` → `HybridScanByBlock` 从 mo_tables 的 data objects 读行数据并应用 tombstone
5. 孤儿行没有 tombstone → 每次都会被读出来 → 每次都会触发 `onReplayCreateTable` 的 panic

### 5.3 方案对比

| 方案 | 描述 | 复杂度 | 数据准确性 | 风险 |
|------|------|--------|-----------|------|
| A. replay 时默认跳过 | `onReplayCreateTable` 遇到 `OkExpectedEOB` 直接 warn + skip | 低 | ❌ 孤儿行永久残留在 mo_tables data object 中 | 极低 |
| B. replay 时创建 phantom DB + table（已删除） | 创建已删除的 DB 和 table entry，让 catalog 结构完整 | 中 | ❌ catalog 内存中有了 entry，但 mo_tables data object 中的行仍无 tombstone | 低 |
| C. replay 后开内部事务 drop orphan table | replay 时记录 orphan tables，启动后开事务执行正常 drop | 高 | ✅ 正常 drop 会写 tombstone，merge 后彻底清除 | 中（需要事务上下文，可能与启动流程冲突） |
| D. 方案 B + C 组合 | replay 时创建 phantom 结构，启动后开事务 drop | 高 | ✅ 同 C | 中 |

### 5.4 方案分析

**方案 A（推荐）：replay 时默认跳过**

这是改动最小、风险最低的方案。核心逻辑：

```go
func (catalog *Catalog) onReplayCreateTable(dbid, tid uint64, schema *Schema, txnNode *txnbase.TxnMVCCNode) {
    catalog.OnReplayTableID(tid)
    db, err := catalog.GetDatabaseByID(dbid)
    if err != nil {
        if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
            logutil.Warn("[REPLAY-ORPHAN] skip orphan table: db not found",
                zap.Uint64("dbid", dbid),
                zap.Uint64("tid", tid),
                zap.String("table", schema.Name),
            )
            return  // 安全跳过
        }
        panic(err)
    }
    // ... 正常流程
}
```

安全性论证：
- database 已被删除（有 tombstone），其下的 table 记录是无效数据，跳过不会丢失任何有效信息
- 孤儿 table 没有 data objects（因为 `OnReplayObjectBatch_V2` 中 `GetDatabaseByID` 返回 `OkExpectedEOB` 时已经跳过了对应的 objects），所以不会有悬挂的数据文件
- 跳过后 catalog 内存中没有这个 table entry，不影响任何正常业务逻辑

缺点：
- mo_tables 的 data object 中孤儿行永久残留，占用少量空间（通常只有几行）
- 每次 replay 都会打 WARN 日志

**方案 C：replay 后开内部事务 drop**

这是唯一能彻底清除孤儿数据的方案，但复杂度较高：
1. replay 阶段：遇到 orphan table 时，创建 phantom DB entry（标记为已删除）+ 正常创建 table entry（不标记删除）
2. 记录所有 orphan table 的 (dbid, tid) 到一个列表
3. 系统正常启动后，开一个内部事务，对每个 orphan table 执行 `DropRelationByID`
4. 正常的 drop 流程会在 mo_tables 中写入 tombstone
5. 后续 merge/compaction 时 tombstone 生效，孤儿行被物理清除

风险点：
- replay 阶段创建 phantom DB 需要确保不影响其他 replay 逻辑（如 `OnReplayObjectBatch_V2` 中对已删除 DB 的跳过逻辑）
- 启动后的内部事务需要正确的事务上下文（TxnOperator、TxnClient 等）
- 如果内部事务失败，需要有重试或降级机制
- phantom DB 的 acInfo（tenantID 等）需要合理填充

### 5.5 当前实现

当前采用方案 A（环境变量控制），设置 `MO_REPLAY_SKIP_ORPHAN_TABLE=1` 启用跳过。

后续计划：去掉环境变量控制，改为默认跳过行为。理由：
- database 不存在时，其下的 table 记录 100% 是无效数据，没有理由 panic
- 这与 `OnReplayObjectBatch_V2` 中遇到 `OkExpectedEOB` 时直接 return 的处理方式一致

## 6. 修改的文件

| 文件 | 变更 |
|------|------|
| `pkg/sql/compile/ddl.go` | `lockMoDatabase` 拆分为 `doLockMoDatabase`（纯加锁）+ `lockMoDatabase`（加锁 + Exclusive 模式下自动刷新 snapshot） |
| `pkg/sql/compile/ddl_test.go` | 新增 `TestDropDatabase_SnapshotRefreshAfterExclusiveLock` |
| `pkg/vm/engine/tae/catalog/catalogreplay.go` | `onReplayCreateTable` 增加 orphan table 跳过逻辑 + debug 日志 |

## 7. 单元测试

在 `pkg/sql/compile/ddl_test.go` 中新增 `TestDropDatabase_SnapshotRefreshAfterExclusiveLock`，包含两个子测试：

| 子测试 | 场景 | 验证点 |
|--------|------|--------|
| `snapshot_refreshed_when_stale` | `snapshotTS(100) < latestCommitTS(200)` | `WaitLogTailAppliedAt` 被调用，`UpdateSnapshot` 被调用且参数为 applied 时间戳 |
| `snapshot_not_refreshed_when_fresh` | `snapshotTS(300) >= latestCommitTS(200)` | `WaitLogTailAppliedAt` 和 `UpdateSnapshot` 均不被调用 |

运行命令：
```bash
go test -v -run "TestDropDatabase_SnapshotRefreshAfterExclusiveLock" ./pkg/sql/compile/
```

## 8. 安全性审查

### 8.1 预防方案（lockMoDatabase snapshot 刷新）

- **不会与 `doLock` 内部的 `UpdateSnapshot` 冲突**：`doLock` 只在 `changed=true` 时调用 `UpdateSnapshot` 并返回 `retryError`，此时 `doLockMoDatabase` 直接返回错误，不会执行到 snapshot 刷新代码。
- **对 `CreateDatabase` 无副作用**：`CreateDatabase` 也使用 `lockMoDatabase(Exclusive)`，snapshot 推进后它只做写操作，不依赖 snapshot 读取表列表。
- **`UpdateSnapshot` 是幂等安全的**。
- **条件守卫**：`IsPessimistic() && IsRCIsolation()` + `lockMode == Exclusive` 确保只在必要时执行。
- **可测试性**：`doLockMoDatabase` 作为 `var` 函数变量，测试中可通过 `gostub.Stub` 跳过加锁。

### 8.2 存量修复方案（replay 跳过）

- **与 `OnReplayObjectBatch_V2` 行为一致**：该函数遇到 `OkExpectedEOB` 时已经直接 return，跳过已删除 DB 下的 objects。`onReplayCreateTable` 采用相同策略是合理的。
- **不会丢失有效数据**：database 已被删除，其下的 table 记录不可能被任何正常查询访问到。
- **不影响 GC**：孤儿 table 没有 data objects（已在 object replay 阶段被跳过），不会有悬挂的数据文件占用磁盘。

### 8.3 性能影响

可忽略：
- `GetLatestCommitTS()` 是纯内存读取，零开销
- `WaitLogTailAppliedAt()` 只在 `snapshotTS < latestCommitTS` 时调用，DROP DATABASE 是低频操作
- replay 跳过逻辑只增加一次 `moerr.IsMoErrCode` 判断，零开销
