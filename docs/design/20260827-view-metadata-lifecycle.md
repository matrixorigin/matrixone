# View 元数据生命周期与分布式激活设计

- **状态**：Pending re-approval
- **历史获批语义 checkpoint**：PR #27734 commit `351397e59a286ff13cef6113904f24313310a03c`，包含 cursor per-FETCH epoch fencing、E0 presence、disabled same-epoch semantics、metadata-only authority containment、独立 sealed provisional epoch gate，以及 multi-CN/frontend admission/hot-path evidence
- **历史审批记录**：reviewer `fengttt` 于 `2026-08-30T00:00:03Z` 对包含该语义 checkpoint 的 exact head `310ad16bdb87ee74ac57f25f1d5a0ee3c2b2fe19` 提交 GitHub `APPROVED` review（[review 5059506799](https://github.com/matrixorigin/matrixone/pull/27734#pullrequestreview-5059506799)）。该 approval 不覆盖后续 semantic changes。
- **重新审批原因**：`bc6f03e17e` 将 catalog capability 与 admission lease 解耦，使 catalog 已就绪但 admission-disabled 的 capable CN 仍维护当前 DDL metadata；后续 restore 修复改变 table/database restore 的 invalidation 与 rolling-upgrade catalog-readiness fallback。最终协议不使用 whole-account reset：relation-removal 在 restore 事务内将受影响 reverse closure 推进到非 `CURRENT` generation；disabled 但 catalog-ready 的 capable CN 同样发布 durable marker 与 affected-closure generation。事务末 reconciliation 按 restore scope 限定：table restore 只扫描目标 table identity/name，database restore 只扫描目标 database，只有 account restore 才扫描 account；各 scope 只删除 orphan targets/dependencies 并 seed missing restored Views。
- **待审批语义 checkpoint**：本设计修订对应的 executable implementation commit。审批必须覆盖包含本修订的 exact PR head；该 head 将记录在 PR conformance 区域。
- **稳定版本**：PR 正文必须链接获批 checkpoint、审批记录与当前 conformance head；后续任何 semantic change 都重新进入 Pending re-approval
- **Owning issue**：#26227
- **实现系列**：#27267、#27370、#27430、#27734
- **完整变更分类**：feature / distributed lifecycle change，而不是普通 bug fix
- **强制门禁触发项**：跨 HAKeeper、LogService、CN、bootstrap、frontend、planner、catalog；修改 wire protocol、持久化 RSM、catalog lifecycle、分布式 epoch、混版本升级、重启/回滚和后台 worker

## 1. 问题、证据与成功标准

View 查询会按当前依赖重新绑定，但 `DESC`、`SHOW COLUMNS`、`information_schema.columns` 和 CTAS 会消费持久化
`TableDef`。依赖列类型变化后，直接查询结果与公开 metadata 可能不一致。#27267 提供权威 schema regeneration，#27370
提供 inactive durable lifecycle，#27430 提供 durable membership/capability epoch 与 ingress fence；这些基础能力合入后仍故意保持
inactive，直到最后一层能够证明所有 admitted CN、catalog 和持久化重校验都满足同一个 epoch。

成功标准：

1. 对任意 admission epoch `E`，公开 metadata 读取只能在 `E` 的 lease 内返回。
2. epoch 改变时必须满足：**关闭新读 → drain 旧 lease → durable required marker → revalidate → durable completion → reopen**。
3. 旧 CN late join、binary rollback、CN UUID generation replacement、Proxy/HAKeeper/CN 观察偏差均不能让旧 metadata fail open。
4. worker 重启、取消、租约丢失、事务冲突或 catalog 暂时不可用时，不得提交错误 completion，也不得无界重试或增长。
5. clean install、rolling upgrade、restart、rollback fence、snapshot/restore 和 publication/subscription 均有确定性证据。

不以“测试未报错”、日志顺序、一次空扫描或本地 MOCluster 快照作为成功证明。

## 2. Invariant、negation 与 owner

### 2.1 核心 invariant

对每个公开 lifecycle-sensitive statement `S`：若 `S` 观察 epoch `E` 的 View metadata，则 HAKeeper durable state、CN epoch
fence、catalog required/completion marker 与事务 epoch 必须同时属于 `E`。在任一条件未知、过期或迁移中时，`S` 必须等待或返回
可重试错误，不能返回旧 schema。

最小 negation：一个未持有 refresh lease 的合法 `information_schema.columns` 查询在 epoch advance drain 之前取得旧事务快照，并在
`RequireViewMetadataRevalidation` 提交后返回旧 View metadata。

### 2.2 唯一 owner

| 状态/资源 | 第一 owner | 禁止的第二 authority |
|---|---|---|
| membership/capability epoch | HAKeeper replicated RSM | CN 本地 MOCluster cache |
| CN 进程代次 | HAKeeper 分配的 non-zero admission generation | UUID-only 本地判断 |
| 公开读 quiescence | CN `ViewMetadataEpochFence` | SQL 文本启发式或 planner 局部 flag |
| durable lifecycle | `mo_view_refresh` / `mo_view_dependencies` | worker 内存队列 |
| target claim/retry/completion | catalog generation + lease epoch 条件写 | 单次扫描为空 |
| worker lifecycle | CN stopper/context | detached goroutine/ticker |

## 3. 标准、先例与 MatrixOne 约束

该设计不新增外部 SQL 标准或客户端互操作协议。`information_schema.columns` 的可见结果继续遵循 MatrixOne 已有 MySQL-compatible
surface。内部协议采用以下已验证先例：

- HAKeeper replicated RSM 作为 membership 与 capability 的 durable source of truth；
- generation/epoch fencing 作为分布式 ownership transfer 的 stale-writer 防护；
- 与 RCU/quiescent-state 类似的 reader lease drain：新 generation 发布前等待旧 reader 退出；
- transactional outbox/conditional-update 类似的 durable required/completion marker，而不是进程内“已完成”布尔值；
- additive protobuf 字段与 unknown-field tolerance 支持 mixed binary。

MatrixOne 的限制是 SQL transaction snapshot、catalog upgrade offset、CN 无状态重启和 HAKeeper heartbeat 观察不同步。因此不能仅依赖
请求路由、进程启动顺序或 periodic cluster cache。

## 4. 方案比较

### 4.1 选定方案：durable epoch + CN lease drain + catalog completion

HAKeeper 选择 epoch；CN 先 drain lifecycle-sensitive readers，再持久化 required marker。worker 使用既有 bounded claim/retry
协议完成 current View revalidation；HAKeeper 仅在 capability、catalog fence 和 completion 同 epoch 时开放。

优点：authority 唯一；mixed-version fail closed；restart 后可恢复；公共读 hot path 只增加本地 lease。代价：协议状态较多，要求真实
混版本与 unhappy-path 验证。

### 4.2 备选 A：本地 MOCluster 周期扫描后激活

拒绝。不同 CN/Proxy 的 cache 可观察不同 membership；late old-CN 可在扫描间隙进入，无法形成 admission 线性化点，也无法在 rollback
发生时先关闭公开读取。

### 4.3 备选 B：每次公开读取同步 regenerate

拒绝。把 catalog discovery、dependency bind 与 schema replacement 放入公开读 hot path，增加事务冲突、尾延迟和可用性风险；不能解决
多个 CN 对“全体 binary 是否支持”的 authority 问题。

### 4.4 备选 C：升级期间停机并一次性重写全部 View

拒绝。违反 rolling upgrade 目标；大 catalog 上无明确时延上界；失败后的 partial rewrite 与 restart recovery 更复杂；rollback 仍缺少
membership fence。

## 5. 状态机与线性化点

1. **INACTIVE**：binary/catalog 未 ready 或存在 unsupported member；refresh 未开放，DDL 仍写 durable invalidation。
2. **PREPARING(E)**：HAKeeper 请求 `E`；CN 设置 requested epoch，阻止新 refresh lease。
3. **DRAINING(E)**：CN 等待旧 generation lease 归零。取消时保留旧 ordinary epoch，但 refresh 保持 sealed。
4. **CATALOG_FENCED(E)**：系统事务提交 `REVALIDATE_REQUIRED`。这是 catalog fail-close 线性化点；失败不得 heartbeat ack。
5. **REVALIDATING(E)**：bounded worker discovery/claim/regenerate/retry；所有写验证 generation 与 lease epoch。
6. **REVALIDATED(E)**：不存在本 epoch 未完成 current target，durable completion 已提交。
7. **ENABLED(E)**：HAKeeper admission、CN capability、catalog fence、refresh-ready 和 completion 全部匹配 `E`；公开读取可取得 lease。
8. **EPOCH CHANGE / ROLLBACK**：进入 `PREPARING(E+1)`；先 seal/drain，再更新 durable marker。旧 completion 不能开放新 epoch。

`Advance` 发布新 epoch、required marker commit、completion commit 和 HAKeeper reopen 是四个不同线性化点；任何实现不得合并为一次本地观察。

## 6. API、wire 与 catalog contract

### 6.1 Wire/RSM

使用 #27430 的 additive heartbeat/RSM fields 表达 admission generation、catalog fenced epoch、refresh support 与 revalidated epoch。
旧 binary 解码未知字段时保持兼容，但因不报告 refresh capability，不能满足 activation predicate。字段编号不可复用；RSM snapshot/replay
必须保留相同状态。

### 6.2 Catalog

- upgrade entry 仅在 exact final version/offset 与 `READY` 状态广告 capability；
- seed/enable upgrade 幂等；旧 offset 或 catalog table 尚未出现时只接受既有 typed readiness error；
- required/completion/claim 更新必须在事务内验证 target generation 与 lease epoch；
- snapshot/restore、PITR、publication/subscription 保留 binding account 与 identity，不把 subscriber metadata 写入 publisher owner。

### 6.3 Public SQL

- `DESC`、`SHOW COLUMNS`、`information_schema.columns` 和 current-catalog CTAS 必须取得同一个 refresh lease；
- txn snapshot 创建前不得通过 catalog 展开用户 View 定义，否则 fence 自身会引入旧 snapshot 与 catalog I/O；因此先对任一未被 CTE scope shadow 的读取 relation 取得不检查 metadata authority 的 provisional epoch lease；它等待 active advance，并在 canceled advance 留下 `requestedEpoch > current.epoch` 时继续 sealed，直到 requested epoch 真正发布；planner Resolve 展开 direct/nested View 后记录完整 `information_schema.columns` 依赖，普通 base-table 与普通 prepared/cached plan 在 Compile 完成后立即释放并不受 authority expiry 影响；真实 metadata consumer 在 publication/execution 前同步验证 authority 并保留 lease 到语句终点；
- relation 分类基于解析后的 AST identity，不能依赖原始 SQL 文本；Insert/Update/Delete 必须先建立 statement-owned CTE scope，再检查 source/subquery，并排除纯写 target；
- `SHOW COLUMNS`/`DESC` 与 current-catalog CTAS 是 terminal consumer：即使 planner 改写为 `mo_catalog.mo_columns` 且不再 Resolve `information_schema.columns`，仍保留 lease 到结果终点；该标记随 prepared/binary/text Execute 与 plan rebuild 传播；
- prepared/binary execute 在 planning 前保守取得 lease，避免 cached AST/plan 跨 epoch；planning 后按缓存的 terminal/dependency 标记决定 retain/release；
- 显式事务记录 snapshot 创建时的 epoch；metadata-sensitive statement 若 transaction epoch 过旧则要求 rollback/retry；
- direct View query 继续按当前依赖 rebind，不被错误地改成持久化 schema consumer。

## 7. 生命周期、失败与资源边界

| 失败 | 必须行为 | 禁止行为 |
|---|---|---|
| HAKeeper unavailable | authority deadline 到期时仅 seal View metadata ingress、唤醒等待者并使在途 terminal validation 失败；普通 SQL/CN 保持运行 | 本地 cache 授权、继续返回 metadata，或关闭整个 CN |
| catalog unavailable | lifecycle-sensitive metadata 保持 sealed 并返回可重试错误 | 开放旧 metadata 或影响普通 base-table SQL |
| `Advance` cancel/timeout | ordinary old epoch 可继续，refresh 不重开 | canceled transition 恢复旧 enabled |
| lease owner error/panic | statement defer exactly-once release | lease 泄漏导致永久 drain |
| worker restart | 从 durable required/claim 恢复 | 以内存“已扫描”跳过 |
| claim/lease lost | stale write 条件失败 | 覆盖新 generation |
| unsupported CN join/rollback | 新 epoch seal + revalidation | 等 periodic scan 后再 fence |
| empty discovery page | 继续 durable terminal check | 直接报告 completion |
| transaction conflict | bounded retry/backoff | unbounded spin |

每个 CN 最多一个 stopper-owned worker；无 per-View goroutine。沿用既有 page size、claim lease、retry/backoff 与每 tick timeout；不引入跨
tick 无界 map/queue。ticker 创建后立即注册 stop，SQL context 有 deadline。

## 8. 性能与容量预算

量化 acceptance threshold（同机、同 Go/toolchain、`-benchmem -count=5`）：

- provisional refresh lease acquire+release：p50 必须 `< 1 µs/op`，且 `<= 64 B/op`、`<= 1 alloc/op`；
- AST detector 的 simple TP read 与 metadata-sensitive statement：p50 必须 `< 5 µs/op`，且 `<= 512 B/op`、`<= 4 allocs/op`；
- prepared/cached `EXECUTE` fast path：p50 必须 `< 100 ns/op` 且零分配；
- 10-way parallel provisional lease contention：p50 必须 `< 1 µs/op`，且 allocation budget 不恶化；parallel detector 不共享可变状态，per-operation allocation 必须与 serial 一致。

`351397e59a286ff13cef6113904f24313310a03c` 的本地 before/after 基准（darwin/arm64, Apple M1 Pro, Go repository toolchain）。同一 harness 预解析同一 AST；`baseline` 精确代表 base branch 没有 admission detector 时的 dispatch 操作，`admission` 只增加 head 引入的 detector，因此差值不混入 parser/catalog/I/O 噪声：

| 场景 | baseline p50 | admission 五次范围 | admission p50 | alloc |
|---|---:|---:|---:|---:|
| refresh lease serial | 0 | 77.64–79.63 ns/op | 78.10 ns/op | 32 B, 1 alloc |
| refresh lease parallel (10 CPU) | 0 | 298.6–301.2 ns/op | 299.7 ns/op | 32 B, 1 alloc |
| simple TP detector | 0.8094 ns/op | 847.8–936.8 ns/op | 916.8 ns/op | 208 B, 2 alloc |
| simple TP detector parallel | N/A | 125.4–145.7 ns/op | 138.0 ns/op | 208 B, 2 alloc |
| prepared/cached EXECUTE detector | 0.8151 ns/op | 2.081–2.178 ns/op | 2.095 ns/op | 0 B, 0 alloc |
| prepared/cached detector parallel | N/A | 0.2710–0.3126 ns/op | 0.2787 ns/op | 0 B, 0 alloc |
| metadata-sensitive detector | 0.8178 ns/op | 891.6–1141 ns/op | 1065 ns/op | 208 B, 2 alloc |
| metadata-sensitive detector parallel | N/A | 196.1–531.2 ns/op | 244.3 ns/op | 208 B, 2 alloc |

可重复命令：

```text
mo-cgo-test -run '^$' -bench '^BenchmarkViewMetadataRefreshLease$' -benchmem -count=5 ./pkg/sql/compile
mo-cgo-test -run '^$' -bench '^BenchmarkViewMetadataStatementNeedsLease$' -benchmem -count=5 ./pkg/frontend
```

这些 before/after mechanism benchmark 直接隔离本 PR 新增的 mutex/AST 成本，并覆盖 simple TP、prepared/cached、metadata-sensitive 与并发 contention。它们不声称替代系统级 workload benchmark；如果后续 profile 显示该机制达到 query latency 的 1%，再以固定 Linux workload 建立独立 performance gate。

- 任意可能解析用户 View 的 relation-read path：一次进程内 mutex provisional lease acquire/release；relation detector 对当前 statement AST 做 O(AST nodes) 无 I/O 扫描；
- 普通 base-table read 仅在 snapshot 创建与 planning 的有界阶段持有 lease，Compile 确认无 metadata dependency 后、pipeline 与客户端结果传输前立即释放；direct/nested/cached View consumer 才保留到终点；纯 literal、CTE-only 与不读取 relation 的 statement 不取得 lease；
- detector 不访问 catalog、不查询 MOCluster，也不依赖 plan cache；
- heartbeat 增加固定宽度字段，无高基数 payload；
- worker 每 tick 的 target 数、SQL 次数、retry 与 lease duration 使用 #27370 的既有硬上限；
- 不允许以扩大 page、缩短 polling interval 或新增并发 worker 解决完成延迟；若容量不足，进入独立性能设计。

## 9. Upgrade、restart、rollback 与 rollout

### 9.1 Rolling upgrade

1. 旧 HAKeeper/CN 运行时功能 INACTIVE。
2. HAKeeper 升级后可理解新状态，但任一旧 CN 不报告 capability，仍不得 activation。
3. CN 逐个升级；最后一个 admitted CN 支持后进入新 epoch，drain + durable revalidation。
4. 全部 completion 满足后才 ENABLED。

### 9.2 Restart 与 rollback

- 新 CN 同 UUID 重启必须取得新 generation，并重新证明 catalog fence/completion；
- 每个成功 HAKeeper heartbeat response 同时续租 fence 内的 authority deadline 与本地 metadata-seal timer；本地 deadline 比 HAKeeper CN-store owner expiry 至少提前一个 replicated tick。deadline 到期只 seal lifecycle-sensitive metadata：provisional planning lease 仍可取得并让普通 base-table/prepared/cached plan 继续；规划确认的 metadata consumer 在 finalization authority validation 返回可重试错误，等待 refresh fence 的 caller 被唤醒，在途 consumer 在 packet/commit 前的 synchronous validation 失败；下一次成功 heartbeat 续租后恢复 metadata admission。普通 SQL、frontend、pipeline、QueryService 与 CN process 不关闭，RTO 为一次成功 heartbeat 往返加本地调度延迟。只有 HAKeeper 明确返回更高 admission generation（证明本进程已失去 UUID ownership）才执行全 CN revoke/Close。每个 metadata packet/flush 前把绝对 deadline 安装到真实 net.Conn；prepared metadata cursor 用独立 presence flag 保存 materialization epoch（包括合法 E0），不跨请求持有 lease；每次 FETCH 重新取得短生命周期 current lease、仅比较 epoch并重装 deadline；同 epoch 的 enabled=false 仍是有效 generation，沿用 materialization 时的 durable-predicate结果。消费 metadata 的事务把 authority deadline 再扣除 CN runtime MaxClockOffset 后安装到 TxnOperator.Commit context，抵消 TN 对普通 deadline 增加的 clock-offset grace。过期响应关闭连接并丢弃 buffered output；成功 metadata response 在 lease release 时先以 sensitive 状态把 connection write deadline 重置为零，再清除 statement sensitivity，避免旧 absolute deadline 污染后续普通响应。显式 COMMIT 重新取得并持有 epoch lease、比较 transaction snapshot epoch。timer 只负责唤醒 metadata waiter，synchronous deadline validation 才是正确性边界；
- authority lease wire state 必须区分“尚未启用”与“已经过期”：admission 尚未启用时，generation-scoped heartbeat 合法携带零 `AuthorityLeaseTicks/TickPerSecond`，不得因此 arm 或 seal metadata authority；一旦 snapshot 声明 admission/refresh 已启用，或本地 fence 已被有效 lease arm，缺失 lease 参数仍必须 fail closed，且后续有效 heartbeat 可以恢复；
- 新 binary 回滚为旧 binary 后，缺失 capability 的 heartbeat 触发新 epoch，公开 metadata 在 revalidation 前 fail closed；
- old binary 不需要理解新 refresh fields 才能保持 wire compatibility，但它不能成为 activation authority；
- 再升级后必须重新完成 epoch，而不是复用 rollback 前 completion。

### 9.3 Rollout/fallback/removal

默认只有 durable predicates 全满足才自动启用，不提供绕过 authority 的 session/global switch。故障 containment 是仅让 lifecycle-sensitive metadata 保持 sealed 并返回
可重试错误；普通 SQL 继续服务。operator 通过 HAKeeper/CN heartbeat 与 catalog state 定位卡在哪个 phase。回退 binary 会触发 fence，不删除新 catalog 字段。
协议字段与 catalog state 只有在所有支持版本退役并完成单独兼容设计后才能移除。

## 10. 可观测性与安全

需要能关联 service UUID/generation、requested/current epoch、catalog fenced epoch、refresh-ready/revalidated epoch、worker terminal error 与 retry。
日志不得按 View 每 tick 无界刷屏；metrics label 不含 View SQL、tenant object name 或高基数 epoch history。

本设计不改变 privilege 判定。AST detector 只决定 lease，不授予 relation 访问；绑定/鉴权仍使用既有路径。系统事务继续携带 account context；
publication/subscription 与 snapshot binding 不能跨 tenant。恶意复杂 SQL 最多触发 O(AST nodes) 本地遍历，不引入额外 catalog I/O。普通长查询与停止读取结果的客户端在 planning 后不再持有 provisional
lease，不能跨租户阻塞 epoch drain；真实 lifecycle consumer 的终点 lease 是正确性所需边界。

## 11. 验证矩阵与 acceptance gate

| Contract | 最便宜的确定性证据 | 额外证据 |
|---|---|---|
| comment/quoting 不绕过 I_S relation | parser + frontend focused UT | public SQL BVT |
| old reader 必须先 drain | public Acquire barrier，无 sleep | focused `-race` 重复 |
| HAKeeper partition 超过 authority deadline | frontend provisional/finalization 入口证明 base SELECT 与普通 binary prepared/cached 成功，SHOW COLUMNS/I_S 失败并在续租后恢复；3 个 CN 同时 metadata fail closed、ordinary ingress 保持；stale timer 不覆盖续租 | frontend focused UT + deterministic multi-CN callback + focused `-race -count=100` |
| provisional lease / AST hot path | 同一 AST/harness 的 baseline-vs-admission ns/op、B/op、allocs/op | serial + 10-way parallel contention，`-count=5` |
| canceled advance 保持 sealed | refresh 与 provisional acquire 均等待 requested epoch 发布 | focused fence `-race -count=100` |
| metadata response deadline cleanup | successful sensitive release 记录 non-zero → zero write deadline | frontend focused `-race -count=100` |
| stale generation/lease 不提交 | HAKeeper/catalog UT | multi-CN replacement |
| required → completion durable | embedded issue regression | exact BVT 同实例重复 |
| rolling upgrade/rollback fence | state-machine UT | 真实旧/新 binary multi-CN sequence |
| restart recovery | worker UT | 新 CN process restart |
| restore commit → recovery reader barrier | `TestIssue27734RestoreCommitFencesSecondCNBeforeRecovery`：真实双 CN/shared catalog；两个 recovery worker 均停在 tick barrier，CN-A commit table restore，CN-B 真实 `DESC` fail closed；释放 worker 后同一 reader 成功 | embedded base-cluster issue regression，禁止用纯 predicate 替代 |
| restore orphan terminal cleanup | database restore 保存 pre-restore target relation ID；terminal recovery 后当前 identity=`CURRENT`，旧 ID 的 refresh/dependency rows 均为 0 | `view_metadata_dependency_refresh.sql` public BVT |
| snapshot/pub-sub binding | existing + new focused UT | publication/subscription BVT |
| generated wire compatibility | proto regeneration/diff | old/new binary heartbeat |

合入条件：tracked design exact revision 已链接；设计审批可追踪；所有 owning package normal/race 证据终态；公开 BVT 通过且 teardown 已证明；
真实 binary sequence 覆盖 all-old、partial upgrade、all-new activation、restart、rollback fail-closed 与 recovery；无 unresolved self-review blocker。
CI 中硬编码 `if: false` 的 Upgrade jobs 只能记录为 SKIPPED，不能替代证据。

## 12. Decision log

1. **接受** HAKeeper replicated epoch 作为唯一 authority；**拒绝** MOCluster cache authorization。
2. **接受** reader lease drain 在 required marker 前；**拒绝** 先写 marker 后等待旧 snapshot。
3. **接受** durable terminal completion；**拒绝** empty scan completion。
4. **接受** prepared execute 保守 lease；其固定本地成本优于 cached plan 绕过风险。
5. **接受** AST relation identity；**拒绝** raw SQL 字符串匹配，因为合法 comment/quoting 会改变文本但不改变 relation。
6. **接受** rollback 后 fail closed + 再 revalidate；不承诺旧 binary 可独立开放新 lifecycle。
7. **实现偏差**：原 prototype 使用 SQL 文本识别 `information_schema.columns`，review 发现可绕过；本版本将 section 6.3 固化为 AST contract。

阻塞性开放问题：当前 affected-closure restore 修订尚未获得覆盖 exact head 的人工 approval。`351397e59a286ff13cef6113904f24313310a03c` 的历史人工 approval 仅作为审计记录，不代表当前实现已获批。若真实 mixed-version binary evidence 与上述 sequence 不一致，设计进入 REQUEST_CHANGES，不以修改测试预期解决。
