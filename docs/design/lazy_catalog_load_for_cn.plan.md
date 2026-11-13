---
name: Lazy Catalog Load for CN
overview: "Keep shared catalog PartitionState semantics unchanged, add account-scoped cache readiness, and make runtime activation follow the same PS-first then exact-replay model as startup."
isProject: false
---

# Lazy Catalog Load for CN（主设计文档）

> 设计约束、协议语义、实现要点。执行时序见 `lazy_catalog_load_for_cn.flow.md`。

Owning issue: [#23777](https://github.com/matrixorigin/matrixone/issues/23777).

## 1. 目标与边界

**目标**：startup 后 `CatalogCache` 只加载 sys account；运行中新增 account 走 activation；reconnect 按批量恢复处理。

**非目标**：不改 checkpoint 按账户懒化；不改 `PartitionState` 为 per-account 隔离；不改普通表的订阅/推送/反订阅；不提供每租户硬内存配额或 activated-account eviction。

> **共享 `PartitionState` 保留全局基线；account 是否可见，由 per-account replay 和 per-account `readyTS` 决定。**

---

## 2. 核心不变量

1. **保留共享 checkpoint 基线**：三表的 `CkpLocation` 和 object metadata 仍是全局的；只对 row-level delta 和 cache replay 做 account 过滤。activation 也保留 object metadata，以清理 flush 与 activation 重叠时重复到达的内存行。
2. **account 在 full replay 后才能 serve**：activation response 只补 `PartitionState`，`readyTS` 在 replay 后发布。
3. **`targetTS` 在 TN sender 路径上确定**：保证和 steady-state push 无 gap。
4. **response 入 FIFO 后才标记 active**：不能让 post-activation update 先于 response 到达 CN。
5. **object/metadata 全局推进，只有 row-level delta 按 account 过滤**：insert/update 按 `account_id`，delete 按 `cpkey`。
6. **reconnect 视为批量恢复**：`wantedAccounts ∪ {0}` 一次性 subscribe + 统一 replay。
7. **只落在三张 catalog 系统表**：`mo_database`/`mo_tables`/`mo_columns`，不污染普通表。
8. **失败不发布 ready**：tail apply、barrier、replay、drain 任一失败都不能成功完成 activation。重连前的 seq 不能将新连接的 account 标记 ready。
9. **共享 sender 不等慢客户端**：activation response 采用与 subscription/update 相同的非等待队列准入；队列满时释放 response 并触发该 session 重连。

---

## 3. 关键术语

| 术语 | 含义 |
|------|------|
| `targetTS` | TN activation response barrier：row-level delta 已补到此点 |
| `replayTS` | CN 调 `WaitLogTailAppliedAt(targetTS)` 后拿到的确定 snapshot ts（通常 > targetTS） |
| `readyTS` | account 开始可 serve 的时间点 = replayTS |
| `seq` | activation 相关键（不复用 stream id） |
| `accountDCA` | per-account 延迟 cache apply 暂存区 |

CN account 状态：`inactive → catching_up → ready_draining → ready`。seq 在 ready 发布前保持有效；reset 清除 seq，旧 completion 必须被拒绝。

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

- **subscribe/activation（pulled batch）**：可能混合 account → 用 `batch.Union(sels)` 按 `account_id`/`cpkey` 行级复制。Phase1 在 pull 后**立即**过滤，不进串行 send 路径。Filtered batch 用 cleanup callback 保活，不做 proto-batch deep clone。subscribe 和 activation 都保留共享 object metadata。
- **steady-state push**：有效 entry summary 走整条 keep/drop；无 summary 时按行分组，只输出 allowed account 的单租户 entry。CN 的 per-account DCA 因此不依赖混合 batch 的首行来决定归属。
- **entry-level summary**（`txn_handle.go`）：`api.Entry` 携带 `lazy_catalog_account_id` + validity bit，仅当 batch 内所有行属于同一 account 时设置。混合 batch（如 restore）不设置。

### Activation worker

有界 `activationReqChan` + 固定 worker pool，数量来自 `PullWorkerPoolSize`，与普通 pull 共享 token。Phase1（每个 token 并发拉三表）→ Phase2（sender goroutine 串行：补齐 gap、合并、非等待入队）。`targetTS = waterline.Waterline()`，response 进入 FIFO 后才 `completeActivation`。pull 受 session/server cancellation 和 request timeout 约束；失败通知 session error owner，不能只记录日志后让 CN 无限等 response。

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
response apply 失败进入现有接收错误/重连路径；已失效 seq 的 replay completion 进入同一有限重试路径，而不是重新创建 ready 状态。

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

回退时 `loadTableFromStorage` → `execReadSql` → catalog reader。共享 push
`PartitionState` 缺少冷租户的后续 row delta，不能作为任意 sys catalog SQL 的完整数据源。
reader 保留已有订阅、pending logtail 和 snapshot coverage 检查；普通 sys SQL 以及未就绪
租户的 catalog 读取，复用 TN `OpGetLogTail`，按事务 snapshot TS 获取完整 checkpoint + delta。
结果仅属于本次 reader，不写回共享 push partition，不发布 cache ready，也不添加 wanted account。
CN 仅额外保留三张表的固定 bootstrap 基线；其 COW fork 在 engine 锁内串行，RPC/I/O 在锁外。
历史 snapshot 仍走原有 checkpoint coverage 路径；同事务写入仍由 workspace 合并。

只有 startup/reconnect/activation 在完整 tail 已落地后的、限定 account 的 replay 可以跳过
这个读取回退；普通 SQL 不得借用 replay 标记。系统表的物理 `accountId=0` 不代表调用方
具有完整 catalog，判断使用实际 caller account。

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

`SHOW ACCOUNTS` 先从 sys catalog 枚举账号，再激活结果中的租户并重新查询库表数量，
避免将冷租户的缺失增量当作零。全量查询会加载所有返回账号的 catalog，不保证操作之后
仍保持只加载少量租户。默认 StorageUsage 定时执行全量 `SHOW ACCOUNTS` 会触发该行为；
该后台任务的收益与代价仍需单独决策，本次正确性修复不改变其激活策略。

---

## 8. Debug 诊断

| 端点 | 用途 |
|------|------|
| `/debug/status/catalog?account=X` | 全局状态 + per-account readiness |
| `/debug/status/catalog-cache?account=X` | 直接查看 catalog cache 内容 |
| `/debug/status/catalog-activation?account=X` | 最近 activation 事件（进程级，重启后重置） |
| `/debug/status/partitions` | 分区状态摘要 |

---

## 9. Alternatives, Cost And Operations

- Status quo: eager replay and full catalog push minimize protocol state, but every CN pays for all tenants. This is the comparison baseline.
- Cache-only lazy replay keeps the wire unchanged, but still sends and applies every inactive tenant's live catalog delta. It does not meet the bandwidth and steady-state replay goal.
- Per-account PartitionState/checkpoints could isolate more memory, but require a separate persisted-object ownership and historical-read design. They are deliberately not implemented here.
- The selected design retains the existing shared checkpoint/object owner and adds account-scoped live rows, cache replay and readiness. Full-cluster shutdown upgrades remain the repository contract; no persisted catalog layout or old-peer compatibility mode is added.

Resource owners and bounds:

- TN owns at most `PullWorkerPoolSize` pull tokens; request capacity equals the activation worker count and completed phase-1 capacity is 64. Each admitted activation has three catalog pulls, not a goroutine per waiting request. Source/filter batches transfer through phase-1, merger and session response cleanup exactly once, including rejection and shutdown.
- CN deduplicates one in-flight activation per account. Its seq, catching-up state and pending-response waiter publish under one lock, so reset cannot miss a late-registered waiter. Its pending response and DCA belong to that attempt and are released on success, failure or reset; waits observe cancellation and TN collection has a finite timeout. Reconnect retries are capped at four with 500ms/1s/2s/4s backoff. Recent diagnostic history is capped at 128 events.
- Ready/wanted account identifiers remain until CN restart, and the TN active set until session destruction. Capacity is therefore proportional to distinct accounts served during that lifetime, not just currently connected sessions. Table-definition memory follows the existing catalog GC. This design is not an eviction policy or a guarantee that RSS stays below a fixed value.
- Replay cost is proportional to selected tenants' metadata plus shared checkpoint I/O. A cold activation can still scan large `mo_columns` history; filtering after pull does not remove TN collection cost. Preserve bounded column-batch replay and validate high-DDL pressure separately from functional UT/BVT.
- Authentication/privilege checks remain authoritative. An internal activation populates metadata but grants no SQL privilege. Remote pipelines, publication/snapshot reads, restore/clone and quota admission must activate before tenant name lookup. Debug status endpoints inherit the existing administrative deployment boundary and must not be exposed as tenant APIs.

Acceptance: verify sys-only startup; independent cold-account activation; mixed-account insert/update/delete filtering; flush and checkpoint completeness; rejected/canceled activation without ready publication; reconnect with stale completion; nonblocking sender rejection and exactly-once cleanup; cross-CN SQL/restore/privilege outcomes. Use deterministic component tests and the shared authenticated two-CN fixture. TKE results from a different source SHA or shared-process vector-cache behavior are not evidence for this catalog protocol's final revision.

## 10. 实现落点

**协议**：`proto/logtail.proto`、`pkg/pb/logtail/`
**TN**：`tae/logtail/service/{server,session,lazy_catalog_session,catalog_filter}.go`、`tae/logtail/txn_handle.go`
**CN**：`disttae/{lazy_catalog_cn,logtail_consumer,logtail,engine,txn_database,debug_state}.go`
**Frontend**：`frontend/{session,snapshot,authenticate,show_account,clone}.go`
**其他**：`engine/{engine,entire_engine}.go`、`catalog/types.go`、`util/status/server.go`

---

## 11. 核对项

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
