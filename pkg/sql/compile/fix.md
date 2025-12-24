# Bug Fix Report: Race Condition in Data Collection

## 1. 问题描述 (Issue Description)
在 `pkg/sql/compile/scope.go` 的 `getRelData` 方法中，当手动增加 `time.Sleep(time.Second * 10)` 模拟延迟时，在多 CN (Compute Node) 场景下执行特定的 SQL（如涉及 `EXCEPT` 和聚合查询），会出现数据不一致的问题。具体表现为本应为空的结果集却返回了数据，或者部分数据丢失。

## 2. 原因分析 (Root Cause Analysis)
原有的代码逻辑在处理本地 CN (Local CN) 的数据读取时，将数据收集过程分为了两个独立的步骤：

1.  **步骤一**: 调用 `expandRanges` 使用 `Policy_CollectCommittedPersistedData` 读取**已持久化（Persisted）**的数据。
    *   *此时，部分新写入的数据可能仍位于内存（MemTable）中，因此步骤一无法读取到这部分数据。*
2.  **步骤二 (模拟)**: 执行 `time.Sleep`。
    *   *在此期间，后台进程可能触发 Flush 操作，将上述内存中的数据刷写到磁盘/S3，使其变为持久化数据。*
3.  **步骤三**: 再次调用 `expandRanges` 使用 `Policy_CollectUncommittedData | Policy_CollectCommittedInmemData` 读取**内存中（In-Memory）**的数据。
    *   *此时，由于数据在步骤二中已经被刷盘，内存中已不存在该数据，因此步骤三也无法读取到。*

**结论**: 数据在两次读取调用的“间隙”中从内存转移到了磁盘，导致两次读取都未能捕获该数据，从而引发数据丢失或结果不一致。

## 3. 修复方案 (Fix Solution)
修改 `pkg/sql/compile/scope.go` 中的 `getRelData` 函数。针对非远程 (Local CN) 的情况，不再分两步读取，而是合并为一次原子操作。

**修改前 (伪代码):**
```go
// Step 1: 读磁盘
committed = expandRanges(..., Policy_CollectCommittedPersistedData)
sleep(10s) 
// Step 2: 读内存
memory = expandRanges(..., Policy_CollectInmemData)
// Merge
return committed + memory
```

**修改后 (伪代码):**
```go
sleep(10s) // 即使这里发生 Flush，也不会影响原子读取
if isRemote {
    // Remote 逻辑保持不变...
} else {
    // Step 1: 原子读取所有数据 (内存 + 磁盘)
    data = expandRanges(..., Policy_CollectAllData) 
    return data
}
```

## 4. 修复原理 (Why this fixes the issue)
使用 `engine.Policy_CollectAllData` 策略调用 `expandRanges` 时，存储引擎会在同一个逻辑视图或事务上下文中同时扫描内存表和持久化存储。这意味着：

*   如果数据在内存中，会被收集。
*   如果数据在磁盘上，也会被收集。
*   不存在两次调用之间的时间窗口，消除了因后台 Flush 操作导致数据“由于位置转移而丢失”的 Race Condition。

即使在调用前强制休眠导致数据被刷盘，`CollectAllData` 依然能从持久化存储中正确读取到该数据，保证了数据的一致性。
