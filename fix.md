# TestIssue19551 偶发失败分析与修复

## 结论

单测写法问题，不是代码 BUG。

## CI 失败现象

txn1 断言失败（issue_test.go:687）：期望 `ErrCannotCommitOnInvalidCN`，实际得到 `ErrTxnClosed`（"the transaction ... has been committed or aborted"）。

## 根因分析

### CI 日志中的关键时序

```
05:45:07.632306  tn: inactive service detected (AddInvalidService 生效)
05:45:07.634820  mo_table_stats 后台任务 commit 失败: "cannot commit a orphan transaction on invalid cn"
05:45:07.635260  mo_table_stats 的 closeTxn 触发 markAllActiveTxnAborted（异步）
05:45:07.653603  txn2 insert (2,2) 完成
05:45:07.662083  txn1 insert (1,1) 完成
05:45:07.662xxx  txn1 commit → markAbortedLocked() == true → 返回 ErrTxnClosed
```

### 因果链

1. 测试调用 `AddInvalidService` 标记 CN 为 invalid
2. 后台 `mo_table_stats` 任务恰好在此时 commit，到达 TN 端 `lock_table_allocator.Valid()` 检测到 inactive service，返回 `ErrCannotCommitOnInvalidCN`
3. 该错误在 `txnClient.closeTxn()` 中触发 `markAllActiveTxnAborted()`，通过 channel 异步通知 `handleMarkActiveTxnAborted`
4. `handleMarkActiveTxnAborted` 遍历所有 `createAt < from` 的活跃事务，设置 `AbortedFlag`，txn1 和 txn2 都被标记
5. txn1 随后 commit 时，在 `doWrite()` 中 `markAbortedLocked()` 返回 true，直接返回 `ErrTxnClosed`，而不是走到 TN 端的 `Valid()` 检查

### 原始测试的两个竞态问题

**竞态 1：txn2 没等 `invalidMarkedC`**

txn2 在 `close(txn2StartedC)` 后立即执行 insert，可能在 `AddInvalidService` 之前就完成 insert 并跳出 for 循环（`HasInvalidService` 还是 false），先于 txn1 进入 commit，先触发 `markAllActiveTxnAborted`。

**竞态 2：后台任务抢先触发 `markAllActiveTxnAborted`**

`mo_table_stats` 等后台任务在 CN 被标记 invalid 期间也会 commit 事务。如果后台任务先于 txn1 commit 到 TN 端，后台任务的 `closeTxn` 会先触发 `markAllActiveTxnAborted`，把 txn1 标记为 aborted。

### 关键代码路径

```
txnClient.closeTxn()                          // client.go:671
  → if ErrCannotCommitOnInvalidCN:
      markAllActiveTxnAborted()                // client.go:838 - 往 abortC channel 发信号
        → handleMarkActiveTxnAborted()         // client.go:845 - 异步 goroutine
          → 遍历活跃事务, op.addFlag(AbortedFlag)
          → Resume()                           // 清除 inactiveService

txnOperator.doWrite(commit=true)               // operator.go:1009
  → if commit && markAbortedLocked():          // operator.go:1088
      return ErrTxnClosed                      // 不会走到 TN 端的 Valid() 检查
```

## 修复方案

### 修复 1：消除 txn1/txn2 之间的时序竞态

- txn1、txn2 都在 `AddInvalidService` 之前完成 insert（通过 `txn1InsertedC`/`txn2InsertedC` 同步）
- txn1 在 `invalidMarkedC` 后立即返回触发 commit，最小化被后台任务抢先的窗口
- txn2 通过 `<-txn1CommittedC` 等待 txn1 先 commit，再通过 `HasInvalidService` 轮询等待 service resume

### 修复 2：txn1 断言接受 `ErrTxnClosed`

后台任务抢先触发 `markAllActiveTxnAborted` 是无法从测试层面完全避免的（除非能暂停所有后台任务）。txn1 得到 `ErrTxnClosed` 和 `ErrCannotCommitOnInvalidCN` 语义一致：都是因为 CN invalid 导致事务无法 commit。区别仅在于是 txn1 自己触发的 abort 机制，还是被别人（后台任务）抢先触发的。

### 测试验证点保持不变

| 验证点 | 修复前 | 修复后 |
|--------|--------|--------|
| CN invalid 后事务 commit 失败 | ✅ | ✅ |
| markAllActiveTxnAborted 标记其他事务 | ✅（但时序不保证） | ✅（通过 txn1CommittedC 保证） |
| service resume 后新事务正常 | ✅ | ✅ |
| txn1 先于 txn2 commit | ❌ 无保证 | ✅ 通过 channel 保证 |

## 复现验证

在原始代码的 `AddInvalidService` 之后、`close(invalidMarkedC)` 之前，插入一个额外的写事务 commit（模拟后台 `mo_table_stats` 任务），并等待 `markAllActiveTxnAborted` 完成后重新标记 invalid：

```go
// [REPRO HACK]
allocator.AddInvalidService(lockSID)
_ = exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
    res, _ := txn.Exec("insert into "+table+" values (99, 99)", executor.StatementOption{})
    res.Close()
    return nil
}, executor.Options{}.WithDatabase(db))
for allocator.HasInvalidService(lockSID) { time.Sleep(10 * time.Millisecond) }
allocator.AddInvalidService(lockSID)
close(invalidMarkedC)
```

| 场景 | 结果 |
|------|------|
| 原始代码 + repro hack | **5/5 稳定失败** |
| 修复后代码 + repro hack | **5/5 稳定通过** |
| 修复后代码（无 repro hack） | **5/5 稳定通过** |

## Diff

```diff
--- a/pkg/tests/issues/issue_test.go
+++ b/pkg/tests/issues/issue_test.go

 			// workflow:
 			// start txn1, txn2 on cn1
+			// txn1, txn2 both do insert before CN is marked invalid
 			// mark cn1 invalid
-			// commit txn1
-			// wait abort active txn completed
-			// commit txn2
+			// txn1 commits first, gets ErrCannotCommitOnInvalidCN, triggers markAllActiveTxnAborted
+			// wait abort active txn completed (service resume)
+			// txn2 commits, gets ErrTxnClosed (already marked aborted)
 			// start txn3 and commit will success
+			//
+			// Note: txn1 may also get ErrTxnClosed if a background task (e.g. mo_table_stats)
+			// commits while CN is invalid and triggers markAllActiveTxnAborted before txn1.

 			txn1StartedC := make(chan struct{})
 			txn2StartedC := make(chan struct{})
+			txn1InsertedC := make(chan struct{})
+			txn2InsertedC := make(chan struct{})
 			invalidMarkedC := make(chan struct{})
+			txn1CommittedC := make(chan struct{})

 			// txn1
 			go func() {
 				defer wg.Done()
+				defer close(txn1CommittedC)
 				err := exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
 					close(txn1StartedC)
-					<-invalidMarkedC
 					res, err := txn.Exec("insert into "+table+" values (1, 1)", ...)
 					...
+					close(txn1InsertedC)
+					<-invalidMarkedC    // insert 完成后再等 invalid 标记，commit 紧随其后
 					return nil
 				}, ...)
 				require.Error(t, err)
-				require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOnInvalidCN), ...)
+				require.Truef(t,
+					moerr.IsMoErrCode(err, moerr.ErrCannotCommitOnInvalidCN) ||
+						moerr.IsMoErrCode(err, moerr.ErrTxnClosed), ...)
 			}()

 			// txn2
 			go func() {
 				err := exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
 					close(txn2StartedC)
 					res, err := txn.Exec("insert into "+table+" values (2, 2)", ...)
 					...
-					for { if !allocator.HasInvalidService(lockSID) { break } ... }
+					close(txn2InsertedC)
+					<-txn1CommittedC    // 确保 txn1 先 commit
+					for { if !allocator.HasInvalidService(lockSID) { break } ... }  // 等 resume 完成
 					return nil
 				}, ...)
 			}()

 			<-txn1StartedC
 			<-txn2StartedC
+			<-txn1InsertedC   // 等 insert 完成后再标记 invalid
+			<-txn2InsertedC

 			allocator.AddInvalidService(lockSID)
 			close(invalidMarkedC)
```
