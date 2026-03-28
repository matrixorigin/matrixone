# Lazy Catalog Flow（coding-ready）

## 关键规则

1. **lazy subscribe / activation 不再透传 raw checkpoint**
   - 三张系统表的 `CkpLocation` 不是 account-filtered；
   - 所以 startup / reconnect subscribe 和 activation response 只转发过滤后的 row-level data，不直接把 raw checkpoint 送到 CN。

2. **共享 `PartitionState` 只保存全局基线**
   - object / metadata 持续全局推进；
   - 只有 row-level in-memory delta 按 account 过滤。

3. **account 的 ready 点是 `replayTS`，不是 `targetTS`**
   - `targetTS` 是 TN 返回的 barrier；
   - `replayTS` 是 CN 在 barrier 真正落地后拿到的确定 snapshot ts。

4. **activation 期间普通 push 仍然推进 `PartitionState`**
   - 只是 cache 侧不能直接对外可见；
   - cache apply 要先进入 `accountDCA[account]`，full replay 后再 flush。

5. **row delete 也必须按 account 过滤**
   - delete 不看 delete batch 里的 `account_id`；
   - 统一按 `cpkey` 解出的 account 判断。

6. **push 与 pull 的 account 粒度不一样**
   - steady-state push 的三表 in-memory entry 视为只属于一个 account；
   - 所以 TN 的 publish 过滤和 CN 的 `accountDCA` 路由都按整条 entry keep/drop，不做 row-splitting；
   - 但 subscribe / activation pull 回来的 entry 仍可能混合多个 account，TN 需要在发送前复制出目标行。

---

## Flow 1：首次 startup

### CN 发起

1. 建立 logtail stream。
2. `subSysTables()` 对三张系统表发送 subscribe request：

```text
lazy_catalog = true
initial_active_accounts = [0]
```

### TN 返回

3. startup subscribe response 返回：
   - object / metadata；
   - 过滤后的 sys account row-level in-memory delta；
   - 不转发 raw `CkpLocation`。

### CN 落地

4. 把 subscribe response 应用到共享 `PartitionState`。
5. 继续走当前 startup 骨架：
   - `waitTimestamp()`
   - `replayCatalogCache()`
6. startup replay 只加载 sys account。
7. replay 期间普通 cache apply 继续走现有 global DCA 语义。
   - global DCA 只负责 startup / reconnect 的共享基线；
   - `accountDCA` 只负责运行中单 account activation，不会阻塞其他 ready account 的正常 cache apply。
8. replay 完成后：
   - flush startup DCA；
   - `sys.readyTS = startupReplayTS`；
   - `sys.state = ready`。

最终结果：

- `PartitionState`：包含全局 object / metadata，以及 sys 已放行的 row-level delta；
- `CatalogCache`：只有 sys account。

说明：startup 仍沿用现有 `waitTimestamp()` / `replayCatalogCache()` 路径，不新增单独的 startup `targetTS`。

---

## Flow 2：运行中新增 account X

### A. 触发 activation

1. `Session.AuthenticateUser()` 完成租户识别。
2. 在 `tenant.SetTenantID(X)` 之后、第一条 tenant SQL 之前调用：

```go
ActivateTenantCatalog(ctx, X)
```

### B. TN 执行 catch-up

3. 若 X 已 ready，CN 直接返回。
4. 否则 CN：
   - `singleflight(key = X)`；
   - `state[X] = catching_up`；
   - 初始化 `accountDCA[X]`；
   - 分配本地 `seq`；
   - 发送 activation request。

5. TN 收到 request 后：
    - 记录 `activatingAccounts[X] = seq`；
    - phase1 worker 从 `0` 开始按表拉历史 row-level delta，实际下界由 checkpoint 逻辑处理；
    - 三张表 catch-up 可以并发执行，直到各自补到 `phase1To`；
    - `logtailSender()` 串行完成 phase2，并取 `targetTS`；
    - response 成功进入当前 session FIFO `sendChan` 后，才把 X 加入 `activeCatalogAccounts`。

这里锁死的边界是：

- `targetTS` 之前的数据属于 activation response；
- `targetTS` 之后的数据属于 steady-state push。

### C. CN 先补 `PartitionState`

6. CN 收到 `ActivateAccountForCatalogResponse{account_id, seq, target_ts, tails}`。
7. 用 `(X, seq)` 命中当前 pending activation。
8. 先把 `tails` 应用到共享 `PartitionState`。

这里不要求 `tails` 固定顺序；CN 必须按表身份消费，而不是按位置消费。

此时语义：

- X 在 `targetTS` 之前缺失的 row-level delta 已补齐；
- X 仍然不能 serve cache。

### D. CN 取确定的 `replayTS`

9. 执行：

```go
replayTS = WaitLogTailAppliedAt(targetTS)
```

按当前 waiter 语义：

- 会先等到 `latestApplied >= targetTS`；
- 返回值通常是 `latestApplied.Next()`；
- 所以 `replayTS` 一般严格大于 `targetTS`。

### E. activation 期间普通 push 的处理

10. 如果 X 的实时 update push 在 replay 完成前到达：
    - `PartitionState` 正常更新；
    - cache 不直接 apply；
    - 写入 `accountDCA[X]`。

这里依赖的是 push-entry 单 account 假设：CN 只对运行中收到的 pushed entry 按 entry 的 account 决定是立刻 apply 还是进入 `accountDCA[X]`。

### F. full replay + ready

11. 在 `catalogCacheMu` 下执行：

```go
replayCatalogCacheForAccount(ctx, e, X, replayTS)
```

12. full replay 完成后，在同一个 `catalogCacheMu` 临界区里：
    - drain `accountDCA[X]`；
    - `X.readyTS = replayTS`；
    - `state[X] = ready`。

13. `ActivateTenantCatalog(ctx, X)` 返回。
14. auth 继续执行 tenant SQL。

---

## Flow 3：X 激活后的 steady state

1. TN `publishEvent()`：
   - object / metadata 继续全局推进；
   - row-level delta 只对 `sys ∪ activeCatalogAccounts` 放行。

2. CN 对 ready 的 X：
   - `PartitionState` 正常更新；
   - cache 正常更新。

3. `CanServeAccount(X, ts)` 必须同时满足：
   - 全局 `CatalogCache.CanServe(ts)` 为 true；
   - `ts >= X.readyTS`。

---

## Flow 4：reconnect

1. CN 保存断线前已 ready 的账户集合：

```text
wantedAccounts
```

2. reconnect 后第一次 `subSysTables()` 直接发送：

```text
initial_active_accounts = wantedAccounts ∪ {0}
```

3. TN 返回：
   - object / metadata；
   - sys + wanted accounts 的 row-level in-memory delta；
   - 不转发 raw `CkpLocation`。

4. CN 继续走 startup 骨架：
   - `waitTimestamp()`
   - 拿到 `reconnectReplayTS`
   - replay sys + wanted accounts

5. replay 完成后，对 `wantedAccounts ∪ {0}` 统一设置：
   - `readyTS = reconnectReplayTS`
   - `state = ready`

这里的建模是：

> reconnect 是“带着 wanted accounts 的重新 startup”，不是“运行中新增一个 account”。

---

## 一句话版

### startup

`PS 先到位 -> startupReplayTS -> sys full replay -> sys ready`

### 新增 account

`PS 先补到 targetTS -> WaitLogTailAppliedAt 得到 replayTS -> replayTS 上 full replay -> flush accountDCA -> account ready`

### reconnect

`wanted accounts 一次性带给 TN -> reconnectReplayTS -> sys + wanted 全量 replay -> 批量 ready`
