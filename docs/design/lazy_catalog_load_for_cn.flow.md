# Lazy Catalog Flow

## 关键规则

1. **subscribe/activation 不透传 raw checkpoint**：只转发过滤后的 row-level data。
2. **`PartitionState` 只保存全局基线**：object/metadata 全局推进；row-level delta 按 account 过滤。
3. **account ready 点是 `replayTS`，不是 `targetTS`**。
4. **activation 期间 push 仍推进 `PartitionState`**：cache apply 进 `accountDCA`，replay 后 drain。
5. **delete 也按 account 过滤**：统一按 `cpkey` 解出 account。
6. **push 与 pull 粒度不同**：push entry 视为单 account（整条 keep/drop）；pull batch 可能混合 account（行级复制）。

---

## Flow 1：首次 startup

1. CN `subSysTables()` 发送 `lazy_catalog=true, initial_active_accounts=[0]`。
2. TN 返回：object/metadata + sys account row-level delta（无 raw `CkpLocation`）。
3. CN 应用到 `PartitionState`，走 `waitTimestamp()` / `replayCatalogCache()`（仅 sys account）。
4. Flush global DCA → `sys.readyTS = startupReplayTS` → `sys.state = ready`。

结果：`PartitionState` 包含全局基线 + sys delta；`CatalogCache` 只有 sys。

---

## Flow 2：运行中新增 account X

**A. 触发**：`AuthenticateUser` → `tenant.SetTenantID(X)` 后 → `ActivateTenantCatalog(ctx, X)`。

**B. CN 侧**：
1. X 已 ready → 返回。
2. Inflight dedup（sync.Map leader/waiter） → `catching_up` + `accountDCA` + 分配 `seq` + 发送 request。

**C. TN 侧**：
1. Phase1 worker 从 0 拉三表历史 delta（**立即过滤**），入 `activationTailChan`。
2. Sender 串行完成 phase2（`phase1To → targetTS`），合并，发送 response。
3. Response 入 FIFO 后才将 X 加入 `activeCatalogAccounts`。

**D. CN 落地**：
1. Apply tails → `PartitionState`。
2. `WaitLogTailAppliedAt(targetTS)` → `replayTS`。
3. `replayCatalogCacheForAccount(X, replayTS)` → drain `accountDCA[X]` → `readyTS` → `ready`。

**E. 期间 push**：`PartitionState` 正常更新；X 的 cache apply 进 `accountDCA`。

---

## Flow 3：steady state

- TN publish：object/metadata 全局推进；row-level delta 只对 `sys ∪ activeCatalogAccounts` 放行。
- CN：ready account 正常更新 PS + cache。
- `CanServeAccount(X, ts)` = `globalCanServe(ts) && readyTS[X] exists && ts >= readyTS[X]`。

---

## Flow 4：reconnect

1. CN 保存 `wantedAccounts`（断线前已 ready）。
2. Reconnect `subSysTables()`：`initial_active_accounts = wantedAccounts ∪ {0}`。
3. TN 返回 sys + wanted 的 row-level delta。
4. CN 走 startup 骨架 → `reconnectReplayTS` → replay sys + wanted → 批量 `ready`。

---

## Flow 5：存储回退（account 未激活）

当 `CanServeAccount(X) == false` 时，engine 退回 `loadTableFromStorage()`：

1. `Engine.Database("mo_catalog")` 短路（`engine.go:369`），不查 cache。
2. `relation("mo_tables")` 强制 `accountId=0`（`txn_database.go:107`），sys 始终 ready。
3. `execReadSql(SELECT ... FROM mo_tables WHERE account_id=X)` 扫描 `PartitionState`。

**数据新鲜度**：返回 subscribe-time 快照。subscribe 后 TN push 按 account 过滤，inactive account 增量不到达 CN。
**安全性**：login 在 tenant SQL 前激活；后台升级有版本守卫；同事务写入通过 workspace 可见。

---

## Flow 6：activation 被 reconnect 打断

1. `doActivateTenantCatalog` 等待 `respCh` → reconnect → `resetAllStates()` 发送 nil。
2. Sentinel `errActivationInterruptedByReconnect` → 回退 `catching_up → inactive`。
3. `ActivateTenantCatalog` 指数退避重试（500ms/1s/2s/4s，最多 4 次）。
4. 所有 goroutine 独立重试，sync.Map 自动选举新 leader。

---

## Flow 7：consumeEntry DCA 路由

```
entry 到达
  ├─ PartitionState 无条件更新
  ├─ 非 lazy catalog table → return
  ├─ Global DCA buffering? → 缓冲（startup/reconnect）
  ├─ Account catching_up? → accountDCA[X]
  └─ 直接 apply
```

---

## 一句话版

| 场景 | 流程 |
|------|------|
| startup | PS → startupReplayTS → sys replay → sys ready |
| 新增 account | PS 补到 targetTS → replayTS → replay → drain DCA → ready |
| reconnect | wanted ∪ {0} → reconnectReplayTS → 批量 replay → 批量 ready |
| 存储回退 | CanServe=false → mo_catalog 短路 + accountId=0 → 读 PS 快照 |
| reconnect 打断 | nil response → sentinel → 退避重试 → 新 leader |
