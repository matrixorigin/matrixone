---
name: Lazy Catalog Load for CN
overview: "Coding-facing design reference for lazy catalog load: keep shared catalog PartitionState semantics unchanged, add account-scoped cache readiness, and make runtime activation follow the same PS-first then exact-replay model as startup."
todos:
  - id: proto-contract
    content: "proto/logtail.proto + pkg/pb/logtail: add `SubscribeRequest.lazy_catalog`, `initial_active_accounts`, and dedicated `ActivateAccountForCatalogRequest/Response{account_id, seq, target_ts, tails}`; keep request_id/response_id as stream ids and use `seq` for activation correlation"
    status: done
  - id: stream-plumbing
    content: "pkg/vm/engine/tae/logtail/service/{client.go,server.go,request.go,response.go}: plumb activation messages over the existing segmented logtail stream and dispatch them separately from subscribe/update"
    status: done
  - id: tn-session-filter
    content: "pkg/vm/engine/tae/logtail/service/session.go + helpers: track lazyCatalogMode, activeCatalogAccounts, activatingAccounts, and reuse one catalog entry-account filter helper for subscribe/publish/activation (delete still decodes account from cpkey)"
    status: done
  - id: cn-replay-refactor
    content: "pkg/catalog/types.go + pkg/vm/engine/disttae/logtail_consumer.go: split `replayCatalogCache()` into reusable startup/reconnect/account replay helpers, and add account-filtered SQL templates so replay no longer assumes a global all-account load"
    status: done
  - id: startup-sys-baseline
    content: "pkg/vm/engine/disttae/logtail_consumer.go: make `subSysTables()` send `lazy_catalog=true` with `initial_active_accounts=[0]`, and change startup replay/cache ready so only sys account becomes ready after startup"
    status: done
  - id: tn-activation-sender
    content: "pkg/vm/engine/tae/logtail/service/server.go: implement activation phase1 worker plus sender-serialized phase2/targetTS/response enqueue, and only add an account to `activeCatalogAccounts` after the activation response enters `sendChan`"
    status: done
  - id: cn-account-state
    content: "pkg/vm/engine/disttae/{logtail_consumer.go,engine.go,types.go}: add pending(account, seq), inactive/catching_up/ready, per-account readyTS, per-account accountDCA, and wantedAccounts that survives reconnect"
    status: done
  - id: cn-activation-flow
    content: "pkg/vm/engine/disttae/logtail_consumer.go + pkg/vm/engine/tae/logtail/service/client.go: send activation, apply activation tails to shared PartitionState, call `WaitLogTailAppliedAt(targetTS)`, replay one account at replayTS, flush accountDCA, and remember the account in wantedAccounts"
    status: done
  - id: serve-gate-audit
    content: "pkg/vm/engine/disttae/{engine.go,txn_database.go,txn.go,mo_table_stats.go} + cache/catalog.go: replace global-only `CatalogCache.CanServe(ts)` decisions with account-aware readyTS checks and storage fallback"
    status: done
  - id: reconnect-restore
    content: "pkg/vm/engine/disttae/logtail_consumer.go: preserve wantedAccounts across reconnect, send them in the first system subscribe, replay sys + wanted accounts at one reconnectReplayTS, and batch-mark the whole set ready"
    status: done
  - id: frontend-auth-hook
    content: "pkg/frontend/session.go: trigger activation immediately after `tenant.SetTenantID(...)` and before the first tenant-context SQL in `Session.AuthenticateUser`; return activation errors directly"
    status: done
  - id: tests-audit
    content: "Add coverage for startup sys-only baseline, activation seq/ordering, global/per-account delayed cache apply, subscribe-time mixed-entry row copying, readyTS serve gate, reconnect batch restore, auth-path failure handling, and lazy-off compatibility"
    status: done
isProject: false
---

# Lazy Catalog Load for CN（主设计文档）

> 这份文档只保留做代码前必须锁死的设计约束、协议语义、状态机和落点。更细的执行时序见 `lazy_flow.md`。

## 1. 目标与边界

### 目标

1. **startup** 保持现有系统表启动骨架，但 `CatalogCache` 在 startup 阶段只加载 sys account。
2. **运行中新增 account** 复用 startup 语义：先把数据补进共享 `PartitionState`，再在一个确定的 snapshot ts 上做 full replay，最后再开放 cache serve。
3. **reconnect** 按“批量恢复已 ready account”处理，而不是逐个 post-reconnect activation。

### 非目标

- 不优化 system-table checkpoint 内存。
- 不把 checkpoint 改成按账户懒化。
- 不把 catalog `PartitionState` 改成 per-account 隔离。
- 不改普通用户表的订阅 / 推送 / 反订阅流程；lazy catalog 只针对 `mo_database` / `mo_tables` / `mo_columns`。

一句话原则：

> **共享 `PartitionState` 保留全局基线；account 是否可见，由 per-account replay 和 per-account `readyTS` 决定。**

---

## 2. 必须保留的基线语义

当前 startup 的真实语义是：

```text
PS first -> exact replay ts -> full replay -> flush delayed cache apply -> ready
```

本轮实现只是在这个基线上扩展：

- startup 继续走现有 `waitTimestamp()` / `replayCatalogCache()` 骨架；
- runtime activation 改成显式拿 `targetTS`，再通过 `WaitLogTailAppliedAt(targetTS)` 取得确定的 `replayTS`；
- reconnect 沿 startup 骨架一次性恢复 sys + wanted accounts。

---

## 3. 核心不变量

1. **lazy subscribe / activation 不直接透传 raw checkpoint**
  - 三张系统表的 `CkpLocation` 不是按 account 过滤的；
  - startup / reconnect subscribe 和 activation response 都只转发过滤后的 row-level data，不把 raw checkpoint 直接送到 CN。
2. **account 只有在 full replay 完成后才能 serve**
  - activation response 只负责把缺失 row-level delta 补进 `PartitionState`；
  - account 真正 ready 的时点是 full replay 完成后的 `readyTS`。
3. `**targetTS` 必须在 TN sender 路径上确定**
  - 只有这样，`targetTS` 才是和 steady-state push 无 gap 的真实边界。
4. **account 变 active 必须晚于 activation response 成功入 FIFO send queue**
  - 先 response enqueue，再打开 `activeCatalogAccounts`；
  - 不能让 post-activation update 先于 activation response 到达 CN。
5. **object / metadata 全局推进，只有 row-level in-memory delta 按 account 过滤**
  - insert / update 按行内容里的 account 过滤；
  - row delete 按 `cpkey` 解出的 account 过滤；
  - startup / reconnect subscribe、activation response、steady-state push 共用同一套过滤契约。
6. **reconnect 视为批量恢复，不视为单账户新增**
  - reconnect 时第一轮 subscribe 直接带 `wantedAccounts ∪ {0}`；
  - 用一个统一 replay ts 恢复 sys + wanted accounts。
7. **lazy catalog 逻辑必须只落在三张 catalog 系统表**
  - `SubscribeRequest.lazy_catalog`、activation、row-level catalog filter、account readyTS 都只服务 `mo_database` / `mo_tables` / `mo_columns`；
  - 普通表的订阅 / steady-state push / 反订阅语义不能被这轮改动污染。

---

## 4. 关键术语与状态

### 时间戳语义

- `**targetTS`**
  - TN 在 activation response 中返回的 barrier；
  - 只表示“目标 account 缺失的 row-level delta 已补到这个点”。
- `**replayTS`**
  - CN 在确认 `targetTS` 已应用后拿到的确定 snapshot ts；
  - 用于 full replay，也作为该 account 的 `readyTS`；
  - 按当前 waiter 语义，通常满足 `replayTS > targetTS`。
- `**readyTS`**
  - 某个 account 在某个 CN 上开始可以安全使用 cache serve 的时间点；
  - sys account、新增 account、reconnect 恢复的一批 account 都各自依赖这个语义。

### 相关键

- `**seq**`
  - activation 的相关键；
  - 不能依赖 `RequestId/ResponseId`，因为它们在当前协议里承担 stream ID 角色。

### CN account 状态

```go
type accountCatalogState struct {
    state   catalogReadyState
    readyTS timestamp.Timestamp
}

const (
    accountInactive catalogReadyState = iota
    accountCatchingUp
    accountReady
)
```

语义固定为：

- `inactive`：不能 serve，也不能直接把该 account 的增量写入 cache。
- `catching_up`：activation 中，`PartitionState` 正常推进，但 cache 更新进入 `accountDCA`。
- `ready`：full replay 完成，`readyTS` 已确定，cache 可正常 serve / apply。

---

## 5. 协议语义

### `SubscribeRequest`

```protobuf
message SubscribeRequest {
  api.TableID table = 1;
  bool lazy_catalog = 2;
  repeated uint32 initial_active_accounts = 3;
}
```

用途：

- startup：`initial_active_accounts = [0]`
- reconnect：`initial_active_accounts = wantedAccounts ∪ {0}`

### `ActivateAccountForCatalogRequest/Response`

```protobuf
message ActivateAccountForCatalogRequest {
  uint32 account_id = 1;
  uint64 seq = 2;
}

message ActivateAccountForCatalogResponse {
  uint32 account_id = 1;
  uint64 seq = 2;
  timestamp.Timestamp target_ts = 3;
  repeated TableLogtail tails = 4;
}
```

锁死两条语义：

1. activation 是 **account 级** 协议，不是 table 级协议；
2. `tails` 只承载三张 catalog 表的 **row-level in-memory delta**，但**不要求固定顺序**；TN 内部可以并发拉取三表，CN 必须按 table identity 消费，而不是按位置消费。

---

## 6. TN 侧职责

### session 侧状态

```go
type Session struct {
    // existing fields...
    lazyCatalogMode       bool
    activeCatalogAccounts map[uint32]struct{}
    activatingAccounts    map[uint32]uint64
}
```

### startup / reconnect

- checkpoint 继续正常返回并进入共享 `PartitionState`；
- object / metadata 继续全局推进；
- row-level in-memory delta 只对 `initial_active_accounts` 放行。

### runtime activation

activation 保持“两阶段、sender 收口”的模型：

1. `onMessage()` 只做 request 分发和 session 定位；
2. worker 执行 phase1，按表拉历史 row-level delta；
3. `logtailSender()` 串行完成：
  - 校验 `activatingAccounts[account] == seq`；
  - 取 `targetTS`；
  - 拉 `phase1To -> targetTS` 的 phase2；
  - 合并 phase1 + phase2；
  - 发送 response；
  - **只有 response 成功进入 session FIFO `sendChan` 后** 才把 account 加入 `activeCatalogAccounts`。

phase1 的下界固定为从 `0` 开始；实际有效下限由现有 checkpoint 处理逻辑决定，不在 activation 协议里额外维护 per-table catch-up 起点。

三张 catalog 表的 catch-up 可以并发执行；response 里的 `tails` 只要求完整覆盖三表，不要求固定顺序。

### steady state

- `publishEvent()` 继续全局推 object / metadata；
- row-level catalog delta 只对 `sys ∪ activeCatalogAccounts` 放行。

### 过滤契约

TN 侧必须复用一套 catalog filter helper，用在：

- startup / reconnect subscribe response（过滤 row，且去掉 raw `CkpLocation`）
- activation response（过滤 row，且去掉 raw `CkpLocation`）
- steady-state `publishEvent()`

过滤规则固定为：

- steady-state push 的三张 catalog 表 in-memory entry batch 仍假定各自只属于一个 account，因此 TN publish 过滤和 CN `accountDCA` 路由都可以按 entry 整体 keep/drop；
- startup / reconnect subscribe response 和 activation response 里的 pulled entry batch 仍可能混合多个 account，因此 TN 在发送前必须复制出目标行；
- steady-state push 的 insert / update：按 entry 首行里的 `account_id` 判断整个 entry；
- steady-state push 的 row delete：按 entry 首行 `cpkey` 解出的 account 判断整个 entry。

---

## 7. CN 侧职责

### 需要维护的状态

- `pending(account, seq)`：request / response 关联；
- `state[account]`：`inactive / catching_up / ready`；
- `readyTS[account]`；
- `accountDCA[account]`：activation 期间普通 push 的暂存区；
- `wantedAccounts`：reconnect 时要恢复的账户集合。

### startup

- 继续走当前 startup 骨架；
- `replayCatalogCache()` 仍只加载 sys account；
- startup replay 完成后设置 `sys.readyTS = startupReplayTS`。

### runtime activation

同步路径固定为：

1. 如果 account 已 ready，直接返回；
2. `singleflight(key=accountID)`；
3. 置 `state[account] = catching_up` 并初始化 `accountDCA`；
4. 发送 `ActivateAccountForCatalogRequest{account, seq}`；
5. 收到 response 后，先把 `tails` 应用到 `PartitionState`；
6. 调 `WaitLogTailAppliedAt(targetTS)` 得到确定的 `replayTS`；
7. 执行 `replayCatalogCacheForAccount(account, replayTS)`；
8. 在 `catalogCacheMu` 下先 drain `accountDCA[account]`，再发布 `readyTS[account] = replayTS`；
9. 最后把状态切到 `ready`。

### cache apply 规则

- `inactive`：不直接写 cache；
- `catching_up`：普通 push 继续更新 `PartitionState`，cache apply 写入 `accountDCA`；
- `ready`：正常更新 cache。

这里依赖的是 steady-state push-entry 单 account 假设：CN 只对运行中收到的 pushed entry 按 entry 归属 account 决定是立即 apply 还是进入 `accountDCA`，不再按 row 拆分 batch。

### serve gate

最终读路径必须按 account 判断：

```go
func CanServeAccount(account uint32, ts timestamp.Timestamp) bool {
    return globalCatalogCacheCanServe(ts) &&
           !accountReadyTS[account].IsEmpty() &&
           ts.GreaterEq(accountReadyTS[account])
}
```

不能只依赖全局 `CatalogCache.CanServe(ts)`。

### 失败清理

如果 activation 失败或超时，且没有新的 seq 接管，必须：

- 清理 `pending(account, seq)`；
- 丢弃 `accountDCA[account]`；
- 把 `state[account]` 从 `catching_up` 回退到 `inactive`。

---

## 8. Frontend 与 reconnect 钩子

### Frontend

`pkg/frontend/session.go::(*Session).AuthenticateUser` 中：

- 在 `tenant.SetTenantID(...)` 之后触发 activation；
- 在第一个 tenant-context SQL 之前完成 activation；
- 出错时直接返回，不能吞错后继续 tenant SQL。

### reconnect

- CN 记住断线前哪些 account 已 ready，记为 `wantedAccounts`；
- reconnect 后第一次 `subSysTables()` 直接携带 `wantedAccounts ∪ {0}`；
- 用一个 `reconnectReplayTS` 重建 sys + wanted accounts cache；
- 为这一批 account 统一设置 `readyTS` 和 `state=ready`。

---

## 9. 主要实现落点

### 协议

- `proto/logtail.proto`
- `pkg/pb/logtail/logtail.pb.go`

### TN

- `pkg/vm/engine/tae/logtail/service/session.go`
- `pkg/vm/engine/tae/logtail/service/server.go`
- `pkg/vm/engine/tae/logtail/service/client.go`
- `pkg/vm/engine/tae/logtail/service/` 下新增 catalog row-level filter helper

### CN

- `pkg/vm/engine/disttae/logtail_consumer.go`
- `pkg/vm/engine/disttae/logtail.go`
- `pkg/vm/engine/disttae/engine.go`
- `pkg/vm/engine/disttae/cache/catalog.go`
- `pkg/catalog/types.go`

### Frontend

- `pkg/frontend/session.go`

---

## 10. 代码前最后核对项

1. startup 后 `CatalogCache` 只有 sys account；
2. activation response 只补 `PartitionState`，不直接宣告 cache ready；
3. `replayTS` 是确定值，并且来自 `WaitLogTailAppliedAt(targetTS)`；
4. activation 期间实时 cache 更新会暂存，replay 后再 flush；
5. `CanServeAccount(account, ts)` 依赖 per-account `readyTS`；
6. reconnect 一次性恢复 wanted accounts，而不是逐个 activation；
7. auth 路径必须在 tenant SQL 前完成 activation；
8. row delete 和 insert / update 一样，保持严格 account-scoped 过滤。
