---
name: Lazy Catalog Load for CN
overview: "Keep shared catalog PartitionState semantics unchanged, add account-scoped cache readiness, and make runtime activation follow the same PS-first then exact-replay model as startup."
isProject: false
---

# Lazy Catalog Load for CN（主设计文档）

> 设计约束、协议语义、实现要点。执行时序见 `lazy_catalog_load_for_cn.flow.md`。

## 1. 目标与边界

**目标**：startup 后 `CatalogCache` 只加载 sys account；运行中新增 account 走 activation；reconnect 按批量恢复处理。

**非目标**：不改 checkpoint 按账户懒化；不改 `PartitionState` 为 per-account 隔离；不改普通表的订阅/推送/反订阅。

> **共享 `PartitionState` 保留全局基线；account 是否可见，由 per-account replay 和 per-account `readyTS` 决定。**

---

## 2. 核心不变量

1. **不透传 raw checkpoint**：三表的 `CkpLocation` 不是 account-filtered，subscribe/activation 只转发过滤后的 row-level data。
2. **account 在 full replay 后才能 serve**：activation response 只补 `PartitionState`，`readyTS` 在 replay 后发布。
3. **`targetTS` 在 TN sender 路径上确定**：保证和 steady-state push 无 gap。
4. **response 入 FIFO 后才标记 active**：不能让 post-activation update 先于 response 到达 CN。
5. **object/metadata 全局推进，只有 row-level delta 按 account 过滤**：insert/update 按 `account_id`，delete 按 `cpkey`。
6. **reconnect 视为批量恢复**：`wantedAccounts ∪ {0}` 一次性 subscribe + 统一 replay。
7. **只落在三张 catalog 系统表**：`mo_database`/`mo_tables`/`mo_columns`，不污染普通表。

---

## 3. 关键术语

| 术语 | 含义 |
|------|------|
| `targetTS` | TN activation response barrier：row-level delta 已补到此点 |
| `replayTS` | CN 调 `WaitLogTailAppliedAt(targetTS)` 后拿到的确定 snapshot ts（通常 > targetTS） |
| `readyTS` | account 开始可 serve 的时间点 = replayTS |
| `seq` | activation 相关键（不复用 stream id） |
| `accountDCA` | per-account 延迟 cache apply 暂存区 |

CN account 状态：`inactive → catching_up → ready`。

---

## 4. 协议

```protobuf
message SubscribeRequest {
  api.TableID table = 1;
  bool lazy_catalog = 2;
  repeated uint32 initial_active_accounts = 3;   // startup=[0], reconnect=wanted∪{0}
}
message ActivateAccountForCatalogRequest  { uint32 account_id = 1; uint64 seq = 2; }
message ActivateAccountForCatalogResponse {
  uint32 account_id = 1; uint64 seq = 2;
  timestamp.Timestamp target_ts = 3;
  repeated TableLogtail tails = 4;                // 三表 row-level delta，不要求固定顺序
}
```

---

## 5. TN 侧

### Session 状态

`lazyCatalogMode` / `activeAccounts` / `activatingSeqByAccount`。`activeAccountsSnapshot`（copy-on-write `atomic.Pointer`）供 publish 热路径无锁读。

### 过滤

- **subscribe/activation（pulled batch）**：可能混合 account → 用 `batch.Union(sels)` 按 `account_id`/`cpkey` 行级复制。Phase1 在 pull 后**立即**过滤，不进串行 send 路径。Filtered batch 用 cleanup callback 保活，不做 proto-batch deep clone。`stripObjectMeta` 参数控制 object metadata 是否保留（subscribe 保留，activation 去除）。
- **steady-state push**：entry 级单 account 假设 → `prepareLazyCatalogPublishWrapsFromIndex` 读 entry summary 整条 keep/drop。无 summary 时退回行级扫描。
- **entry-level summary**（`txn_handle.go`）：`api.Entry` 携带 `lazy_catalog_account_id` + validity bit，仅当 batch 内所有行属于同一 account 时设置。混合 batch（如 restore）不设置。

### Activation worker

有界 `activationReqChan` + 固定 worker pool（默认 1）。Phase1（worker 并发拉三表）→ Phase2（sender goroutine 串行：补齐 gap、合并、发送）。`targetTS = waterline.Waterline()`，response 进入 FIFO 后才 `completeActivation`。

---

## 6. CN 侧

### 状态

`lazyCatalogCNState`：per-account `{state, readyTS}`、`pendingSeq`、`accountDCA`、`wantedAccounts`、`inflightActivations`（sync.Map）、`activationHistory`（circular buffer）、`catchingUpCount`（atomic fast path）。

### Activation 流程

1. 已 ready → 直接返回。
2. Inflight dedup（sync.Map leader/waiter）。
3. `catching_up` + 初始化 `accountDCA` + 分配 `seq` + 发送 request。
4. Response → apply tails 到 `PartitionState` → `WaitLogTailAppliedAt(targetTS)` → `replayCatalogCacheForAccount` → drain `accountDCA` → `readyTS` → `ready`。

失败回退：`catching_up → inactive`，丢弃 `accountDCA`，TN `abortActivation`。

### Reconnect 重试

`resetAllStates()` 向 pending channel 发送 nil → sentinel `errActivationInterruptedByReconnect` → 指数退避重试（500ms/1s/2s/4s，最多 4 次）。所有 goroutine 独立重试，新 leader 自动选出。

### DCA 两级架构

- **Global DCA**：startup/reconnect 期间阻止 cache apply 抢跑。Replay 后 drain。
- **Per-account DCA**：runtime activation 期间隔离一个 tenant 的 catch-up。

两级互不阻塞。`consumeEntry` 路由：`PartitionState` 无条件更新 → global DCA 判断 → per-account DCA 判断 → 直接 apply。

### Serve gate 与存储回退

```go
CanServeAccount(X, ts) = globalCanServe(ts) && accountReadyTS[X] exists && ts >= readyTS[X]
```

回退时 `loadTableFromStorage` → `execReadSql` → 扫描 `PartitionState`（subscribe-time 快照）。安全性：
- `mo_catalog` 数据库短路（`engine.go:369`），不查 cache。
- 系统表强制 `accountId=0`（`txn_database.go:107-113`），sys 始终 ready。
- login 路径在任何 tenant SQL 前激活；后台升级有版本守卫。
- **注意**：subscribe 后 TN push 按 account 过滤，inactive account 的 `PartitionState` 不含后续增量。

---

## 7. Frontend

`activateAccountCatalogIfNeeded`（`snapshot.go:533`）：对 `accountID==0` 直接返回。`EntireEngine` 必须转发 `TenantCatalogActivator` 接口。

| 调用位置 | 场景 |
|---------|------|
| `session.go:1395` | 登录 |
| `authenticate.go:3289/4048` | ALTER/DROP ACCOUNT |
| `show_account.go:717` | SHOW ACCOUNTS |
| `snapshot.go:623,631,2385,2394` | RESTORE |
| `clone.go:257,378` | CLONE TABLE/DATABASE |

---

## 8. Debug 诊断

| 端点 | 用途 |
|------|------|
| `/debug/status/catalog?account=X` | 全局状态 + per-account readiness |
| `/debug/status/catalog-cache?account=X` | 直接查看 catalog cache 内容 |
| `/debug/status/catalog-activation?account=X` | 最近 activation 事件（进程级，重启后重置） |
| `/debug/status/partitions` | 分区状态摘要 |

---

## 9. 实现落点

**协议**：`proto/logtail.proto`、`pkg/pb/logtail/`
**TN**：`tae/logtail/service/{server,session,lazy_catalog_session,catalog_filter}.go`、`tae/logtail/txn_handle.go`
**CN**：`disttae/{lazy_catalog_cn,logtail_consumer,logtail,engine,txn_database,debug_state}.go`
**Frontend**：`frontend/{session,snapshot,authenticate,show_account,clone}.go`
**其他**：`engine/{engine,entire_engine}.go`、`catalog/types.go`、`util/status/server.go`

---

## 10. 核对项

1. startup 后 cache 只有 sys account
2. activation response 只补 PS，不直接宣告 ready
3. `replayTS` 来自 `WaitLogTailAppliedAt(targetTS)`
4. activation 期间 cache 暂存，replay 后 drain
5. `CanServeAccount` 依赖 per-account `readyTS`
6. reconnect 批量恢复，不逐个 activation
7. auth 路径在 tenant SQL 前完成 activation
8. delete 和 insert/update 同样按 account 过滤
9. 存储回退读 subscribe-time 快照，login 路径保证不触发
10. TN push 对三表做 per-session account 过滤
11. phase1 在 pull 后立即过滤，不进串行路径
12. `EntireEngine` 必须转发 `TenantCatalogActivator`
