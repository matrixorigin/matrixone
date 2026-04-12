# LockService 系列 Bug 完整分析

## 生产事故概述

2026-03-16，生产集群 `freetier-01`，`mo_increment_columns` 表的行锁被一个泄漏事务持有 **7 小时**（05:17 ~ 12:14 北京时间），导致数百个事务排队等待，所有涉及自增列的 DDL/DML 全部阻塞。

### 涉及节点

| 角色 | CN UUID | IP |
|------|---------|-----|
| 泄漏事务所在 CN | `61633733-3435-3130-3162-373637636437` | `10.3.93.80` |
| 锁表 owner / 受害 CN | `39396265-3065-3437-3561-393533613162` | `10.3.92.203` |

### 涉及事务

| 事务 ID | 所在 CN | 角色 |
|---------|---------|------|
| `b9ad6f9da7c856c7189cc61b43b410fc` | CN `61633733` | 泄漏事务（锁持有者） |
| `8adeb0f5c0e2801f...` 等数百个 | CN `39396265` | 锁等待者 |

---

## 发现的 Bug 清单

本次分析共发现 **3 个独立 bug**，在生产环境中同时触发、互相加剧：

| # | Bug | 位置 | 严重程度 | 效果 |
|---|-----|------|---------|------|
| 1 | lockWithRetry 无限循环 | CN `61633733`（锁请求方） | P0 | 事务永远不退出，成为泄漏事务 |
| 2 | isRetryError 孤儿检测失效 | CN `39396265`（锁表 owner） | P0 | 无法判定泄漏事务为孤儿，锁不释放 |
| 3 | lockWithRetry 无限循环（canRetryLock 不检查 ctx） | lockop/lock_op.go | P0 | 即使 context 已过期，循环仍继续 |

> Bug 1 和 Bug 3 本质上是同一个问题的两个表现：`canRetryLock` 不检查 `ctx.Err()`，`handleError` 把 `context.DeadlineExceeded` "洗"成 `ErrBackendCannotConnect`。

---

## Bug 1 & 3: lockWithRetry 无限循环

### 根因

`remoteLockTable.lock()` 中，当 `client.Send(ctx, req)` 返回 `context.DeadlineExceeded` 时：

```
client.Send(ctx, req)
  → Future.Get() → ctx.Done() → context.DeadlineExceeded
  → handleError(err)
    → retryRemoteLockError(context.DeadlineExceeded) → true
    → 转成 ErrBackendCannotConnect
  → getLockTableBind(context.Background(), ...) → bind 没变
  → return ErrBackendCannotConnect

canRetryLock(ErrBackendCannotConnect) → sleep 1s → return true
lockWithRetry 继续循环
```

**核心问题**：`handleError` 把 `context.DeadlineExceeded` "洗"成了 `ErrBackendCannotConnect`。`canRetryLock` 只看错误类型，不检查 `ctx.Err()`，所以永远不会因为 context 过期而退出循环。

### 涉及代码

| 文件 | 函数 | 问题 |
|------|------|------|
| `pkg/lockservice/lock_table_remote.go` | `retryRemoteLockError()` | `context.DeadlineExceeded` 被判定为可重试 |
| `pkg/lockservice/lock_table_remote.go` | `handleError()` | 把 deadline exceeded 转成 `ErrBackendCannotConnect` |
| `pkg/sql/colexec/lockop/lock_op.go` | `canRetryLock()` | 不检查 `ctx.Err()`，只看错误类型 |
| `pkg/sql/colexec/lockop/lock_op.go` | `lockWithRetry()` | 不检查 `ctx.Err()`，依赖 `canRetryLock` |

### 生产环境触发路径

```
doUpdate()
  ctx, cancel := context.WithTimeout(ctx, 10s)    ← 只有 10s
  └→ store.UpdateMinValue(ctx)
       └→ SQL executor 执行 UPDATE mo_increment_columns
            └→ lockWithRetry(ctx)
                 └→ lockService.Lock(ctx)
                      └→ remoteLockTable.lock(ctx)
                           └→ client.Send(ctx, req)
                                └→ Future.Get()
                                     select {
                                     case <-ctx.Done():  ← 10s 后触发
                                         return context.DeadlineExceeded
                                     }
```

**第一次调用**（耗时 ~10s）：
- `doUpdate` 创建 10s timeout ctx
- `client.Send` 发送 lock 请求到 CN `39396265`
- 10s 后 ctx 过期，`Future.Get()` 返回 `context.DeadlineExceeded`

**后续每次重试**（每秒一次，持续 7 小时）：
- `lockWithRetry` 用**同一个已过期的 ctx** 调用 `lockService.Lock`
- `client.Send` → `Future.Get()` **立即**返回（ctx 已过期）
- `handleError` → `ErrBackendCannotConnect`
- `canRetryLock` → sleep 1s → 继续
- 间隔 = 0s（立即返回）+ 1s（sleep）= ~1s

### 日志证据

```
05:16:59  "failed to lock on remote" error="lock table bind changed"     ← 首次尝试
05:17:08  "failed to lock on remote" error="context deadline exceeded"   ← 10s 后超时
05:17:09  "failed to lock on remote" error="context deadline exceeded"   ← 每秒重试
05:17:10  "failed to lock on remote" error="context deadline exceeded"
...（持续 7 小时）
05:17:58  "found leak txn" txn-id="b9ad6f9d..."                         ← 60s 后被检测为泄漏
```

### 关于 readTimeout 的误解

之前以为 `defaultRPCTimeout = 10s`（即 morpc 的 `readTimeout`）是导致超时的原因。**这是错误的。**

`readTimeout` 是连接级别的空闲超时，不是请求级别的超时。morpc backend 每 `readTimeout/5 = 2s` 发送 ping 心跳，server 回复 pong，所以连接永远不会因为 `readTimeout` 而断开。

**真正的超时来源是 caller 的 context**：
- `doUpdate` → `time.Second * 10`（10s）
- `doAllocate` → `defaultAllocateTimeout`（3 分钟）

生产环境中是 `doUpdate` 的 10s ctx 导致超时。


### 复现方案

在 `remoteLockTable.lock()` 中注入 fault point `remote_lock_short_timeout`，将 `client.Send` 的 ctx 替换为 1s 超时：

```go
// pkg/lockservice/lock_table_remote.go
sendCtx := ctx
if _, _, isFault := fault.TriggerFault("remote_lock_short_timeout"); isFault {
    var sendCancel context.CancelFunc
    sendCtx, sendCancel = context.WithTimeout(ctx, time.Second)
    defer sendCancel()
}
resp, err := l.client.Send(sendCtx, req)
```

原理与生产环境完全一致：缩短 `client.Send` 的 ctx，使其快速超时，触发 `handleError` → `ErrBackendCannotConnect` → `canRetryLock` → 无限循环。

**关键**：fault point 只在 `remoteLockTable.lock()` 中触发，所以必须让 CN1 去锁一个绑定在 CN2 上的表。
正确做法是 CN2 创建 `seed_t1`（锁表绑定到 CN2），然后 CN1 对 `seed_t1` 执行 `INSERT`（lockop 走 remote lock）。
**不能用 CREATE TABLE**，因为 CREATE TABLE 锁的是 `mo_increment_columns`（系统表），它的锁表在 CN1 启动时就绑定到了 CN1（local lock），不走 `remoteLockTable`。

> 旧方案（在 `handleRemoteLock` 中 sleep 30s）不工作，因为 morpc 的 ping/pong 心跳让连接保持活跃，`readTimeout` 永远不会触发。

复现脚本：`doc/reproduce_lockwithretry_infinite_loop.sh`

### 修复建议

**方案 A**（推荐）：`lockWithRetry` 检查 `ctx.Err()`

```go
func lockWithRetry(ctx context.Context, ...) (lock.Result, error) {
    result, err = LockWithMayUpgrade(ctx, ...)
    if !canRetryLock(tableID, txnOp, err) {
        return result, err
    }
    for {
        if ctx.Err() != nil {       // ← 新增
            return result, ctx.Err() // ← 新增
        }
        result, err = lockService.Lock(ctx, tableID, rows, txnID, options)
        if !canRetryLock(tableID, txnOp, err) {
            break
        }
    }
    return result, err
}
```

**方案 B**：`canRetryLock` 检查 `ctx.Err()`（需要传入 ctx 参数）

---

## Bug 2: isRetryError 孤儿检测失效

### 根因

CN `39396265`（锁表 owner）检测到 waiter 等待超过 1 分钟后，尝试验证 holder 事务（txn `b9ad6f9d`，在 CN `61633733` 上）是否仍然有效。验证方式是发送 `GetActiveTxn` RPC 到 CN `61633733`。

但 RPC 返回 `context deadline exceeded`（CN `61633733` 不可达或响应超时），`isRetryError` 把这个错误判定为"可重试"，即认为事务仍然有效：

```go
// pkg/lockservice/lock_table_remote.go:326
func isRetryError(err error) bool {
    if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
        moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
        return false  // 只有这两种被认为"不可重试"
    }
    return true  // ⚠️ context.DeadlineExceeded 被认为"可重试" → 事务被认为有效
}
```

### 调用链

```
CN 39396265 的 waiter 等待超过 1 分钟
  → checkOrphan
  → isValidRemoteTxn(txn b9ad6f9d on CN 61633733)
    → 发送 GetActiveTxn RPC 到 CN 61633733
    → context deadline exceeded
  → isRetryError(err) → true  ← BUG
  → return true（事务被认为有效）
  → 锁不释放，waiter 继续等待
```

### 日志证据

```
05:17:18  "failed to valid txn" wait-txn="b9ad6f9d...(CN 61633733)"
          error="context deadline exceeded / initRemote 2"
05:17:23  同上（每 5 秒一次，持续 7 小时）
```

### 复现方案

在 `handleGetActiveTxn` 中注入 fault point `lockservice_get_active_txn_hang`，使 CN `61633733` 处理 `GetActiveTxn` 请求时 sleep，导致 CN `39396265` 的验证 RPC 超时。

复现脚本：`doc/reproduce_isretryerror_bug.sh`

### 修复建议

**方案 A**（推荐）：扩展 `isRetryError` 判断

```go
func isRetryError(err error) bool {
    if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
        moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
        return false
    }
    // 新增：context 超时也应视为不可重试
    if errors.Is(err, context.DeadlineExceeded) ||
        errors.Is(err, context.Canceled) {
        return false
    }
    return true
}
```

**方案 B**（更安全）：在 `isValidRemoteTxn` 中增加连续失败计数，N 次超时后判定为无效。

---

## 三个 Bug 如何协同导致 7 小时阻塞

```
时间线：

05:16:58  CN 61633733 上 doUpdate 创建 txn b9ad6f9d（sql-executor 内部事务）
          执行 UPDATE mo_increment_columns，需要获取 Exclusive-Row 锁
          锁表绑定在 CN 39396265 上

05:16:59  client.Send 发送 lock 请求到 CN 39396265
          （可能因为 bind 刚变更或其他原因，请求未能在 10s 内完成）

05:17:08  doUpdate 的 10s ctx 过期
          Future.Get() 返回 context.DeadlineExceeded
          handleError 转成 ErrBackendCannotConnect
          canRetryLock → true → 开始无限循环          ← Bug 1/3 触发

05:17:08+ txn b9ad6f9d 每秒重试获取锁（ctx 已过期，立即返回）
          但事务本身不退出，成为泄漏事务
          同时，txn b9ad6f9d 之前已经成功获取了锁（05:16:59 之前的某次）
          这把锁永远不会被释放（事务不退出就不会 unlock）

05:17:18  CN 39396265 上的 waiter 开始验证 txn b9ad6f9d 是否有效
          发送 GetActiveTxn RPC 到 CN 61633733
          → context deadline exceeded
          → isRetryError → true → 认为事务有效      ← Bug 2 触发
          → 锁不释放

05:17:38+ CN 39396265 上越来越多的事务排队等待
          每 5 秒验证一次 → 每次都超时 → 每次都认为有效

05:17:58  CN 61633733 的 leak checker 检测到 txn b9ad6f9d 泄漏
          → 仅打日志，不中止事务

10:36:20  CN 39396265 上 300+ 个事务排队等待

12:14:27  txn b9ad6f9d 最终超时退出（7 小时后）
          锁释放，排队事务开始执行
```

**三个 Bug 的协同效应：**

| Bug | 如果只有这一个 Bug | 三个 Bug 同时存在 |
|-----|-------------------|-----------------|
| lockWithRetry 无限循环 | 事务泄漏，但如果孤儿检测正常，锁会在几分钟内被释放 | 事务泄漏 + 锁不释放 = 7 小时阻塞 |
| isRetryError 孤儿检测失效 | 如果事务正常退出，锁会被释放 | 事务不退出 + 孤儿检测失效 = 锁永远不释放 |
| canRetryLock 不检查 ctx | 同 Bug 1 | 同上 |

---

## 相关文件索引

### 分析文档

| 文件 | 内容 |
|------|------|
| `doc/lockservice_bug_full_analysis.md` | 本文档（完整分析） |
| `doc/incrservice_lock_leak_analysis.md` | isRetryError bug 详细分析 |
| `doc/reproduce_lockwithretry_infinite_loop.md` | lockWithRetry 无限循环复现文档 |
| `doc/reproduce_isretryerror_bug.md` | isRetryError bug 复现文档 |
| `doc/incrservice_hang_analysis.md` | 早期 incrservice hang 分析（Task 1-2） |

### 复现脚本

| 文件 | 复现的 Bug |
|------|-----------|
| `doc/reproduce_lockwithretry_infinite_loop.sh` | lockWithRetry 无限循环 |
| `doc/reproduce_isretryerror_bug.sh` | isRetryError 孤儿检测失效 |

### 修改的源码（Fault Point 注入）

| 文件 | Fault Point | 作用 |
|------|-------------|------|
| `pkg/lockservice/lock_table_remote.go` | `remote_lock_short_timeout` | 缩短 client.Send ctx 为 1s，触发无限循环 |
| `pkg/lockservice/service_remote.go` | `lockservice_get_active_txn_hang` | CN2 处理 GetActiveTxn 时 sleep，触发孤儿检测失效 |
| `pkg/lockservice/service_remote.go` | `lockservice_handle_remote_lock_hang` | ⚠️ 不工作（ping/pong 导致不超时） |
| `pkg/sql/colexec/lockop/lock_op.go` | （诊断日志） | lockWithRetry 中打印 retry 计数和 ctx.Err |

### Bug 所在源码

| 文件 | 行 | Bug |
|------|-----|-----|
| `pkg/lockservice/lock_table_remote.go` | `retryRemoteLockError()` | `context.DeadlineExceeded` 被判定为可重试 |
| `pkg/lockservice/lock_table_remote.go` | `handleError()` | 把 deadline exceeded 转成 `ErrBackendCannotConnect` |
| `pkg/lockservice/lock_table_remote.go` | `isRetryError()` | `context.DeadlineExceeded` 不在排除列表中 |
| `pkg/sql/colexec/lockop/lock_op.go` | `canRetryLock()` | 不检查 `ctx.Err()` |
| `pkg/sql/colexec/lockop/lock_op.go` | `lockWithRetry()` | 不检查 `ctx.Err()` |

---

## 修复优先级

| 优先级 | 修复项 | 影响 |
|--------|--------|------|
| P0 | `lockWithRetry` / `canRetryLock` 检查 `ctx.Err()` | 防止事务泄漏 |
| P0 | `isRetryError` 对 `context.DeadlineExceeded` 返回 false | 防止孤儿检测失效 |
| P1 | leak checker 增加强制中止能力（如 5 分钟后 rollback） | 兜底防护 |
| P1 | `doUpdate` 的重试逻辑增加退出条件 | 防止无限重试 |
| P2 | 跨 CN 通信健康监控 | 早期发现半分区 |

---

## 关键技术细节

### morpc readTimeout vs caller context timeout

| 维度 | readTimeout (10s) | caller context |
|------|-------------------|----------------|
| 作用范围 | 连接级别空闲超时 | 请求级别超时 |
| 触发条件 | 连接上 10s 无任何数据 | context 到期 |
| ping/pong 影响 | 每 2s ping 重置计时器，永远不触发 | 不受影响 |
| 生产环境中的角色 | **不是超时原因** | **是超时原因** |
| doUpdate 的值 | N/A | 10s |
| doAllocate 的值 | N/A | 3 分钟 |

### handleError 的错误"清洗"

```
原始错误                    handleError 输出              canRetryLock 判断
─────────────────────────  ──────────────────────────   ─────────────────
context.DeadlineExceeded → ErrBackendCannotConnect    → true (重试)
io.EOF                   → ErrBackendCannotConnect    → true (重试)
net.Error (timeout)      → ErrBackendCannotConnect    → true (重试)
ErrBackendClosed         → ErrBackendClosed           → true (重试)
ErrLockTableBindChanged  → nil (bind 变了)            → true (重试)
正常业务错误              → 原样返回                    → false (不重试)
```

所有网络/超时错误都被转成 `ErrBackendCannotConnect`，原始的 context 超时信息丢失。`canRetryLock` 无法区分"暂时网络抖动"和"context 已永久过期"。
