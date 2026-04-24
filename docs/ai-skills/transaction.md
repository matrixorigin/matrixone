# 事务模型

## 两种事务模式

| 模式 | 说明 | 核心包 |
|------|------|--------|
| **悲观事务**（默认） | 写前加锁，冲突立即报错 | `pkg/lockservice/` |
| **乐观事务** | 提交时检测冲突，冲突则回滚 | `pkg/txn/` |

## 核心组件

### TxnClient（`pkg/txn/client/`）
- `TxnClient` 接口 — 创建和管理事务
- `TxnOperator` 接口 — 单个事务的读写操作
- `TxnTimestampAware` — MVCC 时间戳管理

### LockService（`pkg/lockservice/`）
- `LockService` 接口 — 分布式锁协调
- `LockTableAllocator` — 按表分配锁表实例
- `lockTable` 接口 — 行级/表级锁
- 死锁检测：`lockNode`, `deadlockTxn`

## MVCC 机制

```
写入 → 分配时间戳 → 写入新版本 → 提交
读取 → 使用快照时间戳 → 读取可见版本
```

- 快照时间戳由时钟服务提供
- 行版本存储在 TAE 中
- 增量变更通过 Logtail 系统传播

## 事务状态

```
Active → Committing → Committed
              ↘ Aborting → Aborted
```

## 与测试的关联

| 变更范围 | 影响的测试 |
|---------|----------|
| 悲观事务/锁服务 | BVT: pessimistic_transaction |
| 乐观事务/冲突检测 | BVT: optimistic |
| MVCC/快照隔离 | BVT: snapshot; Snapshot 测试 |
| 事务恢复 | Chaos: 杀 TN 后事务恢复; PITR |
| 高并发事务 | 稳定性: tpcc, sysbench |
