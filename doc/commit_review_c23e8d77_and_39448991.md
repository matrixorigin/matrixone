# Commit Review: c23e8d77 + 39448991 + 8927bcfa

**Author**: GreatRiver
**Date**: 2026-04-09
**Reviewer**: Code Review Assistant
**Status**: Approve — All three commits form a complete and well-hardened fix

---

## 1. 结论

**总体评价**：三个提交形成完整的三层防御链，层层递进、互相补强。第三个提交修复了一个此前 Review 中未识别到的关键边缘场景——bounded retry 被 ctx cancel 打断时，不应降级为 generic ctx error，而应保留 backend 故障本体，让 whole-txn rollback 机制能正常工作。

**推荐等级**：**Approve**

---

## 2. 问题背景

详见 [lock_long_holder_after_retry_fix_analysis.md](lock_long_holder_after_retry_fix_analysis.md)。线上根因为三层叠加：

1. lockop 层：backend/CN-restart 错误无 wall-clock retry budget
2. frontend 层：backend 连通性错误不触发显式事务 whole-txn rollback
3. sender 层：RPC 层的 backend 错误重试无 budget

---

## 3. 第一提交 Review（c23e8d77）

### 各模块改动

| 模块 | 核心改动 |
|---|---|
| `pkg/sql/colexec/lockop/lock_op.go` | 新增 `lockRetryState` + 30s wall-clock budget，防止 backend/CN-restart 错误无限重试持锁 |
| `pkg/frontend/util.go` | 5 类 backend/未知状态错误纳入 `errCodeRollbackWholeTxn`，显式事务强制 rollback |
| `pkg/txn/rpc/sender.go` + `types.go` | sender RPC 重试加 30s budget，rollback/catalog 路径不再无限等 |

**评价**：完整覆盖三个根因，向后兼容，正常路径不受影响。

---

## 4. 第二提交 Review（39448991）

### 各模块改动

| 改动 | 说明 |
|---|---|
| `shouldBypassHeldLockTableCheck` 拆分 | CN rolling-restart 时 lock table 已持有仍 retry（owner 重建需要时间），其他 backend 错误则 fail-fast |
| `getRetryWaitDuration` budget <= 0 时 fail-fast | 零值不再退化为无保护行为，而是 `return 0, false` |
| `waitToRetryLock` / `waitToRetrySend` 加注释 | 明确 `ctx.Err() == nil` 返回值的语义 |
| 新增可观测性日志 | `logLockRetryBudgetStop` / `logBackendRetryStop`，区分 budget exhausted 和 budget disabled 场景 |
| 新增 3 个测试 | `DoesNotRetryBackendErrorWhenLockTableAlreadyHeld`、`FailsFastWhenBackendRetryBudgetDisabled`（lockop + sender）、`StopsWhenBackendRetryBudgetExceeded` 断言更精确 |

### 第一提交建议处理情况

| 建议 | 处理状态 |
|---|---|
| budget <= 0 时应 fail-fast | ✅ 已处理 |
| `waitToRetryLock` 分支加注释 | ✅ 已处理 |
| 时间断言应更精确 | ✅ 已处理 |
| 补充 budget disabled 场景测试 | ✅ 已处理 |
| `shouldBypassHeldLockTableCheck` 语义拆分 | ✅ 已处理 |
| 硬编码 30s 需可配置 | ⚠️ 仍为 package-level var |

---

## 5. 第三提交 Review（8927bcfa）

### 5.1 改动概述

commit message: "fix: preserve backend failures on retry cancellation"

这个提交修复了一个此前 Review 中**未识别到**的关键边缘场景：

**问题**：bounded backend retry 循环中，当 `waitToRetryLock` / `waitToRetrySend` 因为 ctx cancel 或 deadline 而返回 false 时，原逻辑直接返回 `ctx.Err()`（generic context error）。这会导致：

- frontend 的 `errCodeRollbackWholeTxn` 看不到 backend 错误（因为错误已被 ctx error 替换）
- 显式事务不会被强制 rollback，而是继续存活
- holder 仍然无法释放

**修复**：对 bounded retry error（`isBoundedRetryLockError` / `isBackendConnectRetryError`），即使 ctx 已 cancel/deadline exceeded，也保留原始 backend 错误返回，让 whole-txn rollback 机制能正常触发。

### 5.2 具体改动

**`lock_op.go` — `getLockRetryExitError`**：
```go
if ctxErr := ctx.Err(); ctxErr != nil && isRetryLockError(err) {
    if isBoundedRetryLockError(err) {
        // Preserve the backend failure so explicit txns do not survive on a raw
        // context error after backend retry has already proven the path unhealthy.
        return err  // ← 返回原始 backend 错误，而非 ctxErr
    }
    return ctxErr
}
return err
```

**`sender.go` — `doSend`**：
```go
if !waitToRetrySend(ctx, wait) {
    if ctxErr := ctx.Err(); ctxErr != nil {
        return txn.TxnResponse{}, err  // ← 返回原始 backend 错误，而非 ctxErr
    }
    return txn.TxnResponse{}, err
}
```

### 5.3 测试改动

**3 个 lockop 测试改名并修改断言**：

| 旧测试名 | 新测试名 | 断言变化 |
|---|---|---|
| `TestLockWithRetryStopsOnDeadlineExceededContext` | `TestLockWithRetryReturnsBackendErrorWhenDeadlineExceededStopsBoundedRetry` | `context.DeadlineExceeded` → `ErrBackendCannotConnect` |
| `TestLockWithRetryStopsOnCanceledContext` | `TestLockWithRetryReturnsBackendErrorWhenCanceledContextStopsBoundedRetry` | `context.Canceled` → `ErrBackendCannotConnect` |
| `TestLockWithRetryStopsWhenContextCanceledDuringRetryWait` | `TestLockWithRetryReturnsBackendErrorWhenContextCanceledDuringRetryWait` | `context.Canceled` → `ErrBackendCannotConnect` |

**新增 sender 测试**：`TestSendReturnsBackendErrorWhenContextCanceledDuringBackendRetryWait`——验证 sender 侧 ctx cancel 打断 backend retry 时，优先返回 backend 错误。

### 5.4 评价

**这是一个重要的语义修正**。此前 Review 没有识别到这个场景，但它的确会导致一个回归风险：

- bounded retry 已证明 backend 路径不健康
- 此时 ctx 被 cancel（例如用户主动取消查询）是一个**正面的中断信号**——说明调用方也在积极终止这个不健康的事务
- 如果反而返回 `context.Canceled`，会绕过后续的 whole-txn rollback 逻辑（因为 `context.Canceled` 不在 `errCodeRollbackWholeTxn` 中）

修复后的逻辑是：只要命中 bounded retry error，就**始终**保留 backend 错误本体，ctx cancel 只控制"是否立即中断 retry 循环"，不改变最终返回的错误类型。这个语义是合理的。

### 5.5 潜在疑问

**问**：如果不希望被 ctx cancel 打断 retry（想等 budget 耗尽再返回），会不会因为这个修改而受影响？

**答**：不会。`waitToRetryLock` / `waitToRetrySend` 返回 `false` 只在 timer 触发后检查 ctx 仍然 alive 才会返回 false。如果 ctx 一直 alive，timer 触发后返回 true，retry 继续，直到 budget 耗尽。ctx cancel 的影响仅限于"用户主动取消了查询"这种场景，此时保留 backend 错误是正确的——因为 backend 路径已经不健康，没有必要用 context error 替换它。

---

## 6. 三提交综合评价

### 6.1 修复完整性

| 层次 | 修复内容 | 覆盖情况 |
|---|---|---|
| lockop 层：retry budget | c23e8d77 + 39448991 | ✅ 完整 |
| lockop 层：cancel 时保留 backend 错误 | 8927bcfa | ✅ 完整 |
| frontend 层：whole-txn rollback | c23e8d77 | ✅ 完整 |
| sender 层：retry budget | c23e8d77 + 39448991 | ✅ 完整 |
| sender 层：cancel 时保留 backend 错误 | 8927bcfa | ✅ 完整 |
| 可观测性 | 39448991 | ✅ 完整 |

### 6.2 回归风险

**极低**。所有改动仅在错误路径生效，且 8927bcfa 的修正是让 error 语义更精确——从 generic context error 变为具体的 backend error，不会破坏任何依赖 context error 做 Cancel 逻辑的正常路径。

### 6.3 剩余观察点

1. **硬编码 30s budget 长期需可配置**：建议后续从 session config / lock_wait_timeout 读取
2. **`util_test.go` 仍未补充表驱动测试**：c23e8d77 新增的 5 个 error code 的 whole-txn rollback 行为没有直接测试覆盖
3. **CN 滚动升级 P99 延迟**：新代码对 rolling upgrade 场景下 TN 侧 bind 重建的 P99 延迟提出 < 30s 的要求

---

## 7. 最终评价

三个提交配合完整，层层递进：

- **c23e8d77**：解决核心问题——三层各加 retry budget，frontend 升级 rollback 语义
- **39448991**：加固边缘行为——bypass held-lock 语义拆分、零值 fail-fast、可观测性日志
- **8927bcfa**：修正关键语义——cancel 打断 bounded retry 时保留 backend 错误而非降级为 ctx error

**强烈建议合并。**

---

*Review generated by Code Review Assistant on 2026-04-09*
