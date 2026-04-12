# 修复 `0112afb96e` 后 holder 长时间不结束问题分析与修复总结

## 1. 结论

`0112afb96e56b29b8c63828380775651a753850a` 修掉的是：

- `ctx` 已经超时/取消后，`lockWithRetry` 还继续无限 retry

这次日志里的问题已经不是这一类。

这次的根因是：

1. CN `37353465-6665-3532-3930-383762663830` 在事务执行期间同时打不通远端 lockservice 和 TN/logtail
2. `ErrBackendCannotConnect` 仍然会在 lock/catalog 路径上被当成可继续重试/重入的错误
3. 该事务是一个**显式事务**（`byBegin:true`），而当前前端并不会因为这类 backend 连通性错误把整个 txn 强制打成 rollback-only
4. leak checker 只打日志，不强制 abort / rollback

所以最终表现为：

- 不再是“死循环不退出”
- 但变成了“事务本体一直活着，锁也一直活着，直到客户端/会话最终结束”

这仍然是 **bug / 设计缺陷**，因为它允许一次 backend 连通性故障把 holder 放大成数小时级别。

---

## 2. 日志证据

### 2.1 不是旧问题：不再是 `context deadline exceeded`

旧问题的特征是：

- `context deadline exceeded`
- 已过期 ctx 被继续用于 lock retry

这次不是。

`Explore-lock-detail.txt` 中同一事务 `4f1c5e9e2f54737318a45159b043004d` 的错误是：

```text
failed to lock on remote
error="can not connect to remote backend, dial tcp4 172.20.49.28:6003: connect: connection refused"
```

这说明这次是**硬连接失败**，不是 caller ctx 已过期。

### 2.2 故障不是单点 lockservice，而是 CN 对外连通性异常

同一时间窗口里，除了远端 lockservice `172.20.49.28:6003` 拒连外，还出现了：

- `rebuild the cn log tail client failed`
- `failed to get bind`
- 对 `172.20.120.132:41011/41012` 的 `connect: connection refused`

这说明当时这个 CN 不只是拿不到远端锁，连 TN/logtail 访问也一起异常。

### 2.3 事务不是自动结束，而是一直被 leak checker 报警

`Explore-lock.txt` 中从 `07:58:20` 开始，持续报：

```text
found leak txn
txn-id="4f1c5e9e2f54737318a45159b043004d"
options="... byBegin:true autocommit:true ..."
```

其中最关键的是：

- `byBegin:true`

这说明它不是纯内部短事务，而是一个**显式事务**上下文里的 txn。

### 2.4 同一个 txn 后续仍在进入 catalog 路径

`Explore-lock.txt` 在 `08:08:53` 还能看到同一个 txn：

```text
engine.database.load.from.storage
name="mo_task"
txn="4f1c5e9e2f54737318a45159b043004d/Active/..."
```

这说明该 txn 并没有在第一次 lock/connect 失败后结束，而是后续还在继续进入内部 catalog 路径。

---

## 3. 为什么会卡在内部 catalog / lock 路径里几个小时

### 3.1 `0112afb96e` 只修了“done ctx 仍 retry”，没有修“live ctx 下无限等 backend 恢复”

当前 `pkg/sql/colexec/lockop/lock_op.go` 的行为是：

- `ctx.Done()` 了，就停止 retry
- 但如果 `ctx` 还活着，而错误是：
  - `ErrBackendCannotConnect`
  - `ErrBackendClosed`
  - `ErrNoAvailableBackend`

  这些仍然是 retryable error

也就是说，现在的 stop condition 主要还是：

- **等 ctx 结束**

而不是：

- **等 retry budget 用完**

所以只要调用方上下文本身足够长，这条路径就可能反复重试/反复重进很久。

### 3.2 这次事务本身是显式事务，生命周期不短

日志里已经明确写了：

- `byBegin:true`

这意味着当前 txn 不一定会因为某一个 statement 的失败就自然消失。

对显式事务来说，如果前端没有把错误升级成“必须回滚整个事务”，txn 就可能继续留在 session 里。

### 3.3 当前前端没有把 backend 连通性错误视为“必须回滚整个 txn”

`pkg/frontend/util.go` 里的 `errCodeRollbackWholeTxn` 当前只包含：

- `ErrDeadLockDetected`
- `ErrLockTableBindChanged`
- `ErrLockTableNotFound`
- `ErrDeadlockCheckBusy`
- `ErrLockConflict`

并**不包含**：

- `ErrBackendCannotConnect`
- `ErrNoAvailableBackend`
- `ErrBackendClosed`

这会导致在显式事务里，backend 连通性错误未必会把整个 txn 打成 rollback-only。

换句话说：

- statement 可以失败
- 但 txn 可能还活着
- 如果 txn 之前已经持有锁，或者后续还能继续重入内部路径，就会出现 holder 长时间不结束

### 3.4 leak checker 只有观察，没有处置

当前 leak checker 的行为是：

- 发现长时间 active txn
- 打日志
- 保存 goroutine profile

但**不会**：

- 自动 abort txn
- 自动 rollback txn
- 自动释放 holder

所以只要客户端连接/会话不结束，这个 txn 就可能一直活着。

---

## 4. 为什么这仍然算 bug

我认为这是 **bug**，而且不是纯“业务层显式事务自己不提交”的正常现象。

原因有三点：

### 4.1 backend 连通性异常不应该允许 holder 无界存活

一次 `connect: connection refused` 不应该把一个持锁事务放大成 3~4 小时。

这不是单纯的“用户没 commit”，而是系统在 lock/catalog/internal SQL 故障场景下缺乏收敛机制。

### 4.2 当前 stop condition 过于依赖外层 ctx

现在 `lockWithRetry` 的改动，本质上还是：

- ctx 结束才停

这能修掉旧死循环，但不能保证“故障在合理时间内失败返回”。

### 4.3 前端没有把这类错误升级成 rollback-only，风险过大

对于：

- backend 不可达
- lockservice 不可达
- TN/logtail 不可达

这类错误，继续保留显式 txn 活着，风险很高：

- holder 不释放
- 后续 statement 继续重入 catalog / lock 路径
- 一个连接可以长时间放大成系统级阻塞

因此这应该被视为**事务安全性 bug**，至少也是明显的容灾策略缺口。

---

## 5. 这次问题与旧问题的关系

### 5.1 不是旧的 ctx-done 无限 retry

这次日志没有看到旧问题核心特征：

- `context deadline exceeded`
- 已过期 ctx 继续每秒 retry

所以不能说 `0112afb96e` 修复失效。

### 5.2 也不是旧文档里的 orphan 检测失效

当前 `pkg/lockservice/lock_table_remote.go` 里：

- `isRetryError` 对 `ErrBackendCannotConnect` 返回 `false`

所以这次不是“owner 端把失联 holder 误判成 alive”。

这次更像是：

- **origin txn 自己一直没有结束**

而不是：

- owner 明知道它死了还不释放

---

## 6. 修改建议

## 方案 A：给 backend-connect 错误增加 retry budget（推荐）

目标：

- 不再允许 `ErrBackendCannotConnect` / `ErrNoAvailableBackend` / `ErrBackendClosed` 在 live ctx 下无界等待

建议：

1. 在 `lockWithRetry` 上给这类错误增加 wall-clock budget
2. budget 可以取：
   - `min(statement timeout, lock_wait_timeout, 固定上限)`
   - 如果缺少 statement timeout，则至少给一个固定上限，例如 `10s ~ 30s`
3. budget 用完后直接返回终态错误，不再继续 retry

优点：

- 直接解决“为什么能挂几小时”
- 不依赖外层 ctx 是否设置得足够严格

### 风险

- 需要明确哪些错误是“短暂可等”，哪些错误应该“尽快失败”

---

## 方案 B：把 backend-connect 错误升级为 rollback-only（强烈建议一起做）

目标：

- 对显式事务，backend 连通性错误不能让 txn 带锁长期存活

建议：

1. 将以下错误纳入“必须回滚整个 txn”语义，至少在 lock/catalog/internal SQL 路径上如此：
   - `ErrBackendCannotConnect`
   - `ErrNoAvailableBackend`
   - `ErrBackendClosed`
2. 可以有两种落点：
   - 直接加入 `pkg/frontend/util.go` 的 `errCodeRollbackWholeTxn`
   - 或新增更细粒度的“txn marked rollback-only”机制，只对系统级 backend 故障生效

优点：

- 即使用户显式 `BEGIN`，这类错误后 txn 也不会继续带锁活着

### 风险

- 会改变一部分显式事务遇错后的语义
- 需要确认是否接受“这类错误后事务必须终止”

我的判断是：**值得改**，因为这类错误已经不是普通 statement-level business error，而是系统连通性错误。

---

## 方案 C：给 leak checker 增加强制处置（兜底）

目标：

- 即使前面两层都漏了，也不能让 holder 活几小时

建议：

1. 保持当前日志/profile 能力
2. 额外增加阈值，例如：
   - 超过 5 分钟：标记告警
   - 超过 10 分钟：自动 abort / rollback

优点：

- 提供最后一道保险

### 风险

- 需要确认误杀成本
- 需要明确仅对“明显卡死”的 txn 生效

---

## 方案 D：补 observability，便于后续判定

建议新增日志/指标：

1. `lockWithRetry` 对 backend-connect 错误的累计 retry 次数、累计耗时、ctx deadline
2. txn 被标记 rollback-only 的明确日志
3. leak checker 中增加当前 SQL / 最近一次 retry 原因
4. 单独指标统计：
   - `backend cannot connect retry exhausted`
   - `txn forced rollback due to backend disconnect`

---

## 7. 我的建议排序

如果要我排优先级，我建议是：

1. **方案 A + 方案 B 一起做**
   - A 解决“为什么能挂几小时”
   - B 解决“为什么显式 txn 还能继续活着”
2. **方案 C 作为兜底**
3. **方案 D 作为可观测性增强**

只做其中一个都不够完整：

- 只做 A：可能仍有显式 txn 在别的路径上长期存活
- 只做 B：live ctx 下 lock/catalog 路径仍可能等很久，只是最终事务会被打死

---

## 8. 最终判断

最终判断如下：

- **这不是 `0112afb96e` 修复失效**
- **这是修完旧问题后暴露出来的新问题 / 遗留策略缺口**
- **它应该算 bug**

更准确地说，是两个紧耦合的 bug / 缺口：

1. **backend 连通性错误缺少 retry budget**
2. **显式事务在 backend 连通性错误后没有被强制 rollback-only**

这两点叠加，再加上 leak checker 只观测不处置，最终就形成了“holder 卡 3~4 小时”的现象。

---

## 9. 最终修复内容

这次最终落地的修复不是单点补丁，而是三层一起收口：

### 9.1 `lockop`：给 backend / rolling-restart 错误加 wall-clock budget

修改点：`pkg/sql/colexec/lockop/lock_op.go`

已处理的错误：

- `ErrBackendClosed`
- `ErrBackendCannotConnect`
- `ErrNoAvailableBackend`
- `ErrRetryForCNRollingRestart`

行为变化：

1. 这些错误不再只依赖外层 live ctx 决定何时停止
2. lock retry 现在有固定 wall-clock budget
3. 一旦进入 backend retry 状态，中间即使穿插 `ErrLockTableBindChanged` / `ErrLockTableNotFound`，也不会把 budget 重置回“重新等很久”

这层修的是：

- “为什么 lock/catalog 路径可以反复卡很久”

### 9.2 `frontend`：把系统级 backend 故障升级为 whole-txn rollback

修改点：`pkg/frontend/util.go`

已纳入 `errCodeRollbackWholeTxn` 的新增错误：

- `ErrRetryForCNRollingRestart`
- `ErrBackendClosed`
- `ErrNoAvailableBackend`
- `ErrBackendCannotConnect`
- `ErrTxnUnknown`

其中 `ErrTxnUnknown` 很关键，因为它覆盖的是：

- 请求已经发出
- 但连接在响应返回前断掉
- sender 只能给出“事务状态未知”

如果不把这类错误升级成 whole rollback，那么显式事务仍然可能在 statement 失败后继续挂在 session 上。

这层修的是：

- “为什么显式事务在故障后还活着”

### 9.3 `txn/rpc sender`：给 TN/catalog/rollback 自身也加 budget

修改点：`pkg/txn/rpc/sender.go`

此前 `sender.doSend()` 对以下错误会一直 `sleep 300ms` 然后无限重试：

- `ErrBackendClosed`
- `ErrNoAvailableBackend`
- `ErrBackendCannotConnect`

这会带来一个很致命的问题：

- 即使 frontend 已经决定 whole rollback
- `Rollback()` 自己发往 TN 的 RPC 也可能无限卡住

现在这条路径也有 wall-clock budget 了。

这层修的是：

- “为什么即使决定 rollback，回滚自己也可能长时间不结束”

---

## 10. 为什么现在这组修改可以覆盖线上 active txn 打满问题

线上现象本质上是：

1. backend/CN 状态异常
2. statement 失败或长时间卡在内部路径
3. 显式事务没有被强制结束
4. 旧 holder 长时间存活
5. 活跃事务槽位逐渐被占满

现在对应关系变成：

1. **lock 路径不会因为 backend/rolling-restart 错误无限等待**
2. **frontend 遇到这些错误会强制把显式事务打成 whole rollback**
3. **即使回滚需要走 TN/catalog RPC，sender 也不会无限重试**
4. **如果请求发出后响应路径断开，`ErrTxnUnknown` 也会终止整个事务**

所以这个问题现在不会再停留在：

- statement 已经失败/异常
- 但 txn 继续长期存活
- 最终 holder 挂数小时

更准确地说，这组补丁把“长时间活着”收敛成了“有限时间内失败并结束事务”。

---

## 11. 为什么本地很难稳定复现出‘挂几个小时’

本地 `make dev-up` + `make dev-restart-cn1` 更容易打出来的是：

- 短时间 `20505 can not connect to remote backend`
- 随后 `20702 lock table bind changed`
- 然后路由恢复，statement 很快返回

这说明本地环境更容易命中：

- “backend 短暂不可达，但很快恢复”

而不容易命中线上那种：

- CN/TN/logtail/remote lockservice 一起异常
- 会话和显式事务仍然长期活着
- 最终被 leak checker 连续报很久

所以**本地复现不稳定，不代表根因判断有问题**，更不代表修复覆盖不到线上症状。它只是说明：

- 本地 restart 窗口太短
- 故障面不够宽
- 更容易恢复成 bind changed，而不是长期 backend unavailable

---

## 12. 这次修复的边界

这次修复解决的是：

- 系统级 backend/CN/TN/lockservice 故障后
- 显式事务不该继续长期持锁存活

它**不是**去改变所有显式事务的正常语义。也就是说：

- 用户正常 `BEGIN` 后长时间不 `COMMIT`，仍然可能持锁，这是原本的事务语义
- leak checker 仍然主要负责观测，不是这次修复的主路径

这次修掉的是更具体的异常语义：

- **系统已经告诉我们 backend 状态不可靠了**
- **这时不能再继续把显式事务留在 session 里赌它后面会自己恢复**

---

## 13. 最终结论

对这次线上问题，我现在的结论是：

1. **这是 bug**
2. **根因不是升级逻辑本身，而是 backend/CN 异常触发了事务清理链路的收敛缺口**
3. **这组补丁已经把最关键的长 holder 形成路径补全了**

如果线上现象确实是：

- holder 挂几小时
- 活跃 txn 被打满
- 同时伴随 remote backend / TN / logtail 访问异常

那么这组修改就是针对这个问题本身，而不是只针对某个偶发现象做表面止血。
