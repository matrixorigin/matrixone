# BackExec 设计解析

## 核心概念

`backExec` 是 MatrixOne 中用于在后台执行 SQL 的轻量级执行器，主要用于内部元数据查询（如 subscription、publication 信息）。

## 两种事务模式

```txt
Session (父)
    │
    ├─ TxnHandler
    │      │
    │      └─ txnOp ◄─── 事务生命周期由父 session 管理
    │           │
    │           │ (共享引用)
    │           ▼
    └─ backExec (共享事务)
           │
           └─ TxnHandler
                  │
                  ├─ txnOp ──► 指向同一个 txnOp
                  └─ shareTxn = true
```

| 模式 | shareTxn | 事务管理 | Close() 行为 |
|------|----------|----------|--------------|
| 共享事务 | true | 父 session 管理 | 不 rollback，只 Reset |
| 独立事务 | false | backExec 自己管理 | 执行 rollback，完全 Close |

## 关键洞察

### 1. 共享事务不负责事务生命周期

```go
func (th *TxnHandler) rollbackUnsafe(...) error {
    if !th.inActiveTxnUnsafe() || th.shareTxn {
        return nil  // 共享事务：直接返回，不做任何操作
    }
    // 只有非共享事务才真正 rollback
}
```

**含义**：共享事务的 `backExec.Close()` 对事务没有任何影响，事务的 commit/rollback 完全由父 session 控制。

### 2. 事务引用可以安全更新

由于共享事务不管理事务生命周期，我们可以：
- 直接更新 `txnOp` 引用，而不需要 Close + 重建
- 在 autocommit 模式下，事务切换时只需 `UpdateTxn(newTxnOp)`

### 3. 悲观事务 snapshot 抬升自动可见

`backExec` 持有的是 `txnOp` 的**引用**（指针），当悲观事务获取锁后 snapshot 被抬升时：

```go
// txnOperator.UpdateSnapshot() 直接修改内部状态
tc.mu.txn.SnapshotTS = newTS
```

`backExec` 执行 SQL 时自动使用新的 snapshot，无需任何额外处理。

## 优化策略

### Before: 每次调用都创建新的 backExec

```txt
GetSubscriptionMeta()
    │
    ▼
GetShareTxnBackgroundExec()  ← 创建 backExec
    │
    ├─ newBackSession()
    │      ├─ InitTxnHandler()
    │      ├─ buffer.New()
    │      └─ FastUuid()
    │
    ▼
getSubscriptionMeta()
    │
    ▼
bh.Close()  ← 销毁 backExec
```

**问题**：频繁创建/销毁开销大

### After: Session 级别复用，事务切换时更新引用

```txt
getOrCreateBackExec()
    │
    ▼
cachedBackExec 存在?
    │
    ├─ 是 → txn 变化?
    │          │
    │          ├─ 是 → UpdateTxn(currentTxn)  ← 只更新引用
    │          │
    │          └─ 否 → 直接返回缓存
    │
    └─ 否 → GetShareTxnBackgroundExec()  ← 首次创建
              │
              ▼
          缓存并返回
```

## 总结

| 特性 | 说明 |
|------|------|
| 事务所有权 | 共享事务由父 session 管理，backExec 只是借用 |
| 生命周期 | 可以提升到 session 级别，跨事务复用 |
| 事务切换 | 只需更新 txnOp 引用，无需重建 |
| Snapshot 更新 | 自动可见，无需处理 |

## 相关文件

- `pkg/frontend/back_exec.go` - backExec 和 backSession 定义
- `pkg/frontend/txn.go` - TxnHandler 定义，shareTxn 逻辑
- `pkg/frontend/compiler_context.go` - TxnCompilerContext.getOrCreateBackExec() 实现
