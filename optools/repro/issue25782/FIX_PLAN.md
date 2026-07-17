# Issue #25782 修复方案与实施计划

更新时间：2026-07-17（Asia/Shanghai）

状态：单 PR、多 commit 的本期 no-OOM 与 Shuffle 有界 spill/re-spill 已完成实现、代码级验收和 fresh-runtime 业务验收；本机 132,096×132,096 多 CN SQL 已验证 Broadcast 与 Shuffle 结果正确、Shuffle 正 spill、hard query budget reject 后转入 spill，且零 OOM/零 swap。第 5 节所述跨 CN pre-dispatch lease 协议仍是后续增强项，不是本次业务验收的前置条件；当前由每 CN statement generation、pipeline 限额传递和有限 remote backstop 兜底。证据路径记录在 HANDOFF。

### 2026-07-17 实施进度

已完成：

- CN aggregate + execution-generation HashBuild budget、实时 ceiling refresh 和有限 remote compile mpool backstop；
- retained batch 在复制前 admission，expression join key 在缺少可靠峰值估算器时 fail closed；
- Int/String HashMap 事务化 resize 和 old+new peak admission；
- Broadcast `JoinMap | BuildError` 唯一终态，多消费者收到一致的受控失败；
- runtime-filter 序列化独立 reservation，并把 token 生命周期转移给 MessageBoard message；
- `ReallocZero` temporary peak cap 检查和 rollback；
- spill/re-spill scratch、reader、磁盘字节和文件描述符均进入有限预算；无法继续时受控失败。

当前业务语义：Shuffle 在有限预算内执行有界 spill/re-spill并返回正确结果；无法取得进展、达到深度/队列/磁盘/FD 上限时返回受控 query error。Broadcast 在预算内成功，超预算时所有消费者收到同一受控错误，不会伪装成 empty build。

Fixed E2E 与 hard-budget E2E 均已完成：Broadcast 和 Shuffle 在 132,096×132,096 的受控物理数据上返回 `132096`；hard-budget run 将 `join_spill_mem` 提高到 1 GiB，并在两个 CN 上同时观测到 query memory reject 与 initial spill 增长，最终仍返回精确结果。所有运行均为 `oom=0, oom_kill=0, swap=0` 并完整停止清理；完整路径和计数见 HANDOFF。

Issue：[HashBuild can bypass spill and trigger system OOM during large hash table growth](https://github.com/matrixorigin/matrixone/issues/25782)

## 1. 结论

本问题不能只通过删除 `!IsShuffle` 修复。最终方案由两层组成：

1. 建立强制执行的、按 statement execution generation 隔离的共享 HashBuild 内存预算，在 HashMap 扩容前完成 admission；未经批准的内存增长不得开始。
2. 预算拒绝后，Shuffle HashBuild 进入有界 spill/re-spill；Broadcast HashBuild 在本 PR 中返回稳定的受控 SQL 错误。Broadcast 多消费者共享 spill 文件的完整实现不在本 PR 范围内。

本修复以一个 PR 提交，拆成九个可以独立测试、独立验收的 commit。

## 2. 问题边界

当前代码存在三条相互独立但会叠加的缺口：

- `process.Limitation.Size` 会随 pipeline 发送到远端，但没有在 HashBuild 分配路径上强制执行；本地 session mpool 默认上限为 1 TiB，远端 compile mpool 目前无有效 cap。
- HashBuild 只在 `BuildHashmap` 前按 retained input batches 判断是否 spill；HashMap 构建和 `ResizeOnDemand` 的增长没有 admission。
- Broadcast HashBuild 被 `!IsShuffle` 条件排除在 spill 之外，而且没有正确接收 build spill threshold。

大规模 Int/String HashMap 进入 block 模式后，每个增长块为 256 MiB。现有 resize 代码会先修改内部状态，再逐块分配；分配失败时可能留下部分发布的状态。因此，预算判断必须在任何状态变更和分配之前完成，resize 本身也必须事务化。

## 3. 明确的范围与非目标

### 完整 PR 目标保证

- 每个 statement 在每个 CN 上有一个 generation-scoped 的共享 HashBuild budget。
- 同一 CN 上并发 query 的 HashBuild 还受一个 CN-wide aggregate budget 约束。
- retained build batches、HashMap cells 以及 HashBuild 自有的主要 scratch/spill buffers 都必须进入预算。
- HashMap projected build 和每次 resize 都有 admission。
- Shuffle 在预算拒绝后安全 spill/re-spill，或在无法继续时受控失败。
- Broadcast 超预算时所有消费者收到同一个受控错误，不会把失败误判为空 build side。
- 成功、错误、取消、retry、Reset/Free 和 pipeline reuse 都不会泄漏 reservation、batch、map 或 spill 文件。

### 完整 PR 不声称

- 这不是所有 operator、所有 Go heap 分配的通用 query memory accounting。
- 单个 budget 不跨 CN 做全局原子协调；契约是“每 statement、每 CN”。
- 不实现 Broadcast 的多消费者 spill 文件共享。
- 不能仅靠进程内代码获知 Kubernetes node allocatable 或其他容器的实时占用；pod/container limit 不得超过 node allocatable 仍需独立的部署门禁。

因此，“无 OOM”的承诺限定为：不允许未通过 admission 的 HashBuild 增长，并且受控验收 workload 不发生 process/node OOM。

## 4. Budget 模型

### 4.1 有效上限

所有值均为 bytes，使用 checked/saturating arithmetic：

```text
effectiveCN = minNonZero(
    finite cgroup memory.max,
    host physical MemTotal,
    explicitly configured mpool global limit,
)

requestedReserve = max(4 GiB, effectiveCN / 5, fileCacheHint)
minimumHashCap   = minSafeAllowance(effectiveCN)  # normally max(5%, 64 MiB)
reserve          = min(requestedReserve, effectiveCN - minimumHashCap)
cnHashBudget     = effectiveCN - reserve

if ProcessLimitationSize > 0:
    queryCap = min(ProcessLimitationSize, cnHashBudget)
else:
    queryCap = cnHashBudget
```

语义约束：

- unavailable、unlimited 和配置值 `0` 不参与 `minNonZero`。
- `ProcessLimitationSize == 0` 表示没有更窄的 query override，不表示无限。
- 解析后的 `queryCap == 0` 必须 fail closed；HashBuild 不得把它解释为无限。
- production 至少要得到一个有限的 `effectiveCN` 来源，否则 budget 功能初始化失败。

### 4.2 两级 reservation

每次 reservation 固定按以下顺序执行：

```text
CN aggregate TryReserve
        |
        v
Query TryReserve ---- rejected ---> rollback CN reservation
        |
        v
return exactly-once token
```

Token 同时记录 CN 和 query 两笔 charge，并使用原子状态机保证：

- rollback/release/transfer 只能成功一次；
- 旧 generation 的 late release 不会影响新 generation；
- 成功发布 JoinMap 时 reservation ownership 只转移一次；
- JoinMap 最后一个引用释放时，内存与 reservation 一起释放。

### 4.3 Persistent 与 temporary peak

- 已发布并仍存活的 batches、HashMap cells、sels 等属于 persistent charge。
- resize 替换旧表时，旧表保持 persistent charge；完整新表先作为 temporary peak charge。
- 新表全部分配并发布、旧表真正 free 后，释放旧 charge，将新表转为 persistent。
- block 模式必须在第一次分配之前，一次性 reserve 本次计划增加的所有 256 MiB blocks。
- 任一中间分配失败时，释放全部中间对象和完整 temporary charge，旧表状态不得变化。

## 5. Execution generation 与远端共享

### 5.1 身份

Coordinator 为每次 statement execution 创建新的 `BudgetGenerationID`。Retry 和 prepared statement 的下一次执行必须使用新 generation，禁止 reset/reuse 旧 budget。

预算身份包含：

```text
account + statement ID + BudgetGenerationID + target CN
```

### 5.2 Pre-dispatch lease 协议

远端 fragment 不能通过“首次到达”创建 budget，避免 cancel/cleanup 后的迟到消息重建旧 generation。

流程如下：

1. Coordinator 在发送 pipeline fragments 前，对每个目标 CN 调用 `OpenBudgetLease`。
2. Open 请求携带 generation identity、预算、预期 FragmentID 集合/数量和 `AcquireExpiresAt`。
3. 目标 CN 预创建 registry entry，返回包含 registry epoch 的随机 `LeaseID`。
4. 所有目标 CN Open 成功后才能 dispatch；部分失败时，对已成功 CN 发送 Close。
5. Fragment 只能携带已有 `LeaseID + FragmentID` acquire，不能创建 registry entry。
6. Duplicate FragmentID 不增加引用；重复或未知 delivery 被拒绝。
7. Query cancel/end 进入 Closing，取消尚未到达的 fragment leases，并拒绝新 acquire。
8. `AcquireExpiresAt` 到期后自动取消 missing fragments；已 acquire 的 fragment 可以完成并 release。
9. Open/Acquire 消息携带的 expiry 已过期时，即使 registry metadata 已清理，也必须拒绝。
10. CN 重启会改变 registry epoch，重启前的 LeaseID 全部失效。

`AcquireExpiresAt` 约束 pipeline dispatch 的最大消息生命周期，不限制 query 自身运行时间。

必须覆盖 partial-open rollback、never-arrived fragment、finish/cancel 与 late arrival、duplicate delivery、metadata cleanup 后旧消息以及 service restart。

## 6. HashBuild allocation inventory

Commit 3 必须在代码和测试中维护一份 ownership/inventory 表，不允许出现未分类的 HashBuild 自有大内存。

| 类型 | 预算类别 | 生命周期 |
|---|---|---|
| copied build batch vectors/areas | persistent | HashBuild，或随 JoinMap 转移 |
| HashMap cells | persistent + resize peak | HashBuild，或随 JoinMap 转移 |
| sels、dedup bitmap、unique keys | persistent/temporary，按实际用途 | partial map cleanup 或 JoinMap |
| replacement table old+new overlap | temporary peak | resize publish 后收缩 |
| spill bucket vector buffers | temporary | flush 后释放/复用 |
| hashValues | temporary | 当前 scatter pass |
| bucketRowIDs 及 row-index backing arrays | temporary | 当前 scatter pass |
| spill write/marshal buffer | temporary | flush 完成 |
| spill expression outputs | temporary | Eval/flush 完成 |
| re-spill readers、buffers、bucket rebuild scratch | temporary | 当前 bucket/pass |
| 小型固定 operator metadata | bounded overhead | operator lifetime |

Go heap scratch 有两种允许的实现：

- 改为 mpool-backed，并与 reservation ownership 绑定；或
- 在 capacity growth 前按保守上界 reservation，完成后按实际 capacity reconciliation。

Expression Eval 的临时结果必须在 Eval 前按输入 row/type 计算保守上界；完成后用 mpool delta reconciliation。无法得到安全上界时必须拒绝，而不是先分配后补记账。

Mpool cap 是最后的物理 backstop，不代替 query/CN shared reservation。

## 7. Retained batch ingress

上游已经创建的当前输入 batch 不可能被 HashBuild 的 budget 追溯阻止，因此边界定义为：

- 上游拥有正在交付的至多一个 batch；它受现有 batch-size 控制，但不计为 HashBuild retained memory。
- `CopyIntoBatches` 前，HashBuild 必须 reserve projected copy size。
- Shuffle reserve 失败时，不复制该 batch，直接把上游 batch 写入 spill 并进入 spill mode。
- Broadcast reserve 失败时，不复制该 batch，进入受控错误路径。
- Copy 完成后按 `Batches.MemSize` 实际 delta reconciliation。

这项 bounded ingress exception 必须写入测试和指标说明，验收不得暗示预算能追溯阻止上游分配。

## 8. Transactional HashMap admission

Int/String hashtable 增加 SQL-agnostic 的可选 admission callback 和纯计算 `ResizePlan`：

```text
ResizePlan:
    current bytes
    retained bytes
    additional bytes
    projected peak bytes
    new cell count
    new block count
```

约束：

- admission 必须在任何 allocation 和状态变更前执行一次。
- pre-block replacement 的 peak 为 old + full new allocation。
- block mode 包含本轮所有新增 blocks。
- 所有中间对象私有分配成功后，才能一次性 publish cells/masks/counts。
- admission rejection 或真实 allocation failure 后，旧 lookup、cardinality、cell/block counts、`Size()`、mpool bytes 必须完全不变。
- common hashmap iterator 遇到 insert error 后不得继续更新 row/group count 或使用 stale values。
- typed `NeedSpill` 必须原样传播：

```text
ResizeOnDemand
  -> InsertBatch / PreAlloc
  -> common/hashmap iterator
  -> HashmapBuilder.BuildHashmap
  -> HashBuild / SpillEngine policy
```

测试必须在 pre-block replacement 和 multi-block growth 的每个 allocation point 注入失败，而不仅仅测试 admission rejection。

## 9. HashBuild 与 Shuffle recovery

执行路径：

```text
receive batch
  -> reserve projected retained copy
  -> copy/reconcile
  -> retained-batch soft spill check

EOF
  -> projected hash peak admission
  -> BuildHashmap
      -> ResizePlan admission for every growth
      -> admitted: private allocate + publish
      -> NeedSpill: discard partial hash state, keep original batches
```

Shuffle 收到 `NeedSpill` 后：

- initial build 将保留的 batches partition/spill，清理 retained memory 后发布 spilled JoinMap；
- bucket rebuild 收到 `NeedSpill` 时，仅在 depth 未达上限且 repartition 有可测进展时 re-spill；
- 进展定义为 `largestChildRows < parentRows`，全 skew/no-progress 直接受控失败；
- fanout 固定 32；并发打开的 spill FDs 不超过两个 fanouts 加固定 overhead；
- 开始处理 child 前关闭 parent build/probe files；
- I/O error、cancel、max depth、disk budget 或 FD budget 失败时，清理 parent/children/queue、reservation 和 mpool memory。

新增 byte-only `process.Limitation.SpillSize`：

- 显式正值为 query spill disk cap；
- `0` 解析为 saturating `min(1 TiB, 8 * queryCap)`；
- 达到上限返回稳定受控错误。

## 10. Broadcast 的安全行为

本 PR 不把 Broadcast 接到现有 spilled JoinMap 路径。原因是 Broadcast JoinMap 被 `mcpu` 个消费者共享，而 `TakeSpillBuildFds()` 当前是一次性所有权转移；直接启用会造成只有第一个消费者拿到文件、共享 offset 或重复关闭。

Broadcast admission 失败时：

1. 创建 immutable、typed 的 JoinMap dependency terminal result；它必须在 JoinMap 与 BuildError 之间二选一。
2. BuildError 携带稳定 moerr code 和安全错误元数据；不能用 nil JoinMap 表示预算失败。
3. Exactly once 地把 BuildError 广播给所有 `mcpu` consumers。
4. Runtime filter 发送 PASS/完成终态，绝不能因 unique keys 不存在而发送 DROP。
5. 记录 dependency-finalized 状态后，HashBuild 才返回同一个受控错误。
6. Reset/Free 不得再发布 nil 或重复错误。

`ReceiveJoinMap` 必须让所有消费者收到相同 BuildError；任何消费者都不得继续 probe 或返回成功 rows。Nil JoinMap 继续只表示真实 empty build/finalization compatibility。

## 11. `join_spill_mem` 与 hard budget

- 保留 `join_spill_mem` 现有兼容语义：`<= 100000` 表示 rows，更大值表示 bytes；它是 soft spill policy。
- 新的 query/HashBuild hard budget 永远使用 bytes，由 process limitation 与有效 CN ceiling 解析。
- 禁止把 `join_spill_mem` 同时当 hard query budget，避免 rows/bytes 歧义和配置行为变化。

## 12. 单 PR 的 commit 计划

| Commit | 内容 | 独立验收 |
|---|---|---|
| 1. `memory budget: add generation and hierarchical HashBuild budgets` | Budget primitives、CN/query 两级 transaction、ceiling resolver、process/wire fields、Open/Close lease RPC、remote registry/epoch/expiry | boundary、并发 CAS/rollback、transfer/free、generation isolation、partial-open、duplicate/late/expired fragment、cancel 和 restart tests |
| 2. `hashtable: make resize transactional and admissible` | ResizePlan、optional admission、private allocate/publish、iterator error propagation | Int/String、PreAlloc/Insert、old+new peak、256 MiB block math、每个 allocation point failure、状态/mpool 不变 |
| 3. `hashbuild: publish deterministic broadcast build failures` | immutable `JoinMap | BuildError | Empty` dependency、runtime-filter PASS/finalize、controlled moerr | under-budget compatibility、所有 mcpu receivers 同错、无成功 rows、Reset/Free/cancel/reuse 无 hang/leak |
| 4. `hashbuild: enforce the no-OOM hard-fail slice` | retained batch、projected build/resize admission、partial map discard；Shuffle recovery 暂未启用时同样受控失败 | exact/limit+1、Int/varchar、prebuild/mid-resize、Broadcast/Shuffle controlled error、全部 baseline |
| 5. `mpool: retain a hard physical backstop` | 远端 pool 使用有限 effective cap；审计并修复 `ReallocZero` cap bypass；budget admission 仍为首选恢复信号 | Alloc/Grow/ReallocZero exact limits、错误补偿和 cleanup |
| 6. `hashbuild: add bounded initial shuffle spill` | budget rejection 转入初次 spill；per-execution 与 CN-global disk/FD token | row 无丢失/重复、disk/FD cap、I/O/cancel、全部 baseline |
| 7. `hashbuild: add bounded shuffle re-spill` | bucket re-spill、progress/depth/skew guards | 结果正确、same-partition no-progress、max-depth、cleanup |
| 8. `observability: report HashBuild budget decisions` | structured metrics/logs：identity、budget、reserved/current/projected、block、action、spill rows/bytes；rate limit/redaction | 字段、频率和无 SQL/业务数据泄漏 tests |
| 9. `issue25782: add fixed acceptance harness and docs` | 保留显式 historical reproduction mode；默认 fixed acceptance 使用无歧义 byte budget | Broadcast 受控错误、两 CN 后续健康；Shuffle 结果 132096 且两 CN spill；零 OOM/swap/timeout；classifier/provenance fixtures 和 fresh E2E |

Commit 4 的 Shuffle 在 spill recovery 尚未合入时必须 fail closed；Commit 6 只接入有界 initial spill，Commit 7 才接入 re-spill。Broadcast 始终不得进入现有 spill FD ownership；因此每个 commit 都能保持绿色和生命周期闭合。

## 13. 测试矩阵

### Budget 与 registry

- exact limit allow / limit+1 reject；
- CN reserve 成功、query reserve 失败后的完整 rollback；
- 并发 reserve/release/race；
- temporary peak shrink、JoinMap transfer/final free；
- old generation late release 与新 generation 并发；
- Open/dispatch partial failure、missing fragment、duplicate delivery、expiry、cancel、restart epoch。

### Hashtable

- Int/String projected resize math；
- pre-block old+new peak；
- 256 MiB transition 和 multi-block plan，不实际申请巨大内存；
- rejection before mutation；
- 每个真实 allocation point fault injection；
- 原有 lookup/cardinality/size/mpool accounting 不变；
- iterator error 不更新 rows。

### HashBuild / SpillEngine

- int/varchar keys；
- retained batches 未超 soft threshold，但 projected hash 超 hard budget；
- initial PreAlloc 和后续 ResizeOnDemand 分别拒绝；
- partial map cleanup 后原 batches 完整；
- Shuffle initial spill 和 bucket re-spill 结果无丢失、无重复；
- same-partition skew/no-progress；
- disk/FD/depth limit、injected I/O failure、cancel；
- 多 fragment 并发 spill 同时受 query/CN caps；
- Reset/Free/reuse 后 reservation、mpool、FD 回到 baseline。

### Broadcast

- under-budget 行为不变；
- over-budget 返回稳定 BuildError；
- 所有 `mcpu` consumers 同时 unblock 并观察相同错误；
- 无 consumer 把失败解释为空 build 或返回成功 rows；
- runtime filter 不产生错误 DROP；
- error/cancel/Reset/Free races exactly once。

### E2E

- Planner stats 故意低估，hard budget 使用大于 `100000` 的 byte 值；
- Broadcast 保持 non-shuffle plan，并受控报错；
- 报错后两个 CN 均能执行 follow-up query；
- Shuffle 返回 `132096`，两个 routed attempts 都有 threshold-eligible positive spill；
- cgroup 无 OOM、无 swap growth、无 timeout/watchdog failure；
- source commit、运行 binary hash、evidence provenance 与 final telemetry 全部闭合。

## 14. 验证命令层次

根据实际修改包自底向上执行：

```bash
# 先按仓库要求构建 thirdparties，并设置 CGO 环境
make thirdparties
export CGO_ENABLED=1
export CGO_CFLAGS="-I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -Wl,-rpath,$(pwd)/thirdparties/install/lib"
export LD_LIBRARY_PATH="$(pwd)/thirdparties/install/lib:${LD_LIBRARY_PATH}"

go test -count=1 ./pkg/container/hashtable ./pkg/common/hashmap
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s ./pkg/sql/colexec/hashbuild
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=180s \
    ./pkg/sql/colexec/spillutil ./pkg/sql/colexec/hashjoin \
    ./pkg/sql/colexec/dedupjoin ./pkg/sql/colexec/rightdedupjoin
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=180s \
    ./pkg/sql/compile ./pkg/vm/process ./pkg/vm/pipeline

bash -n optools/repro/issue25782/*.sh optools/repro/issue25782/tests/run.sh
optools/repro/issue25782/tests/run.sh
```

Budget、registry 和 dependency lifecycle 需要额外 targeted `-race`。每次 semantic edit 后必须重新生成测试证据；没有最终 PASS/FAIL 或仍有测试进程存活时，不得判定通过。

## 15. PR 最终验收

PR 只有在以下条件全部满足时才能合并：

- 未通过 admission 的 HashBuild growth 从未开始；所有 admitted peak 在成功或失败后正确收缩/释放。
- Shuffle projected/resize rejection 能有界 spill/re-spill，并保持结果正确。
- Broadcast under-budget 结果不变；over-budget 受控失败，所有消费者收到同一错误。
- 并发 fragments/operators 同时遵守 query 和 CN caps；无 generation ABA 或 late-release 污染。
- 成功、错误、取消、retry、reuse 路径无 reservation/map/batch/FD leak、double cleanup 或 hang。
- 指标能定位 statement/operator/CN、预算、projected peak 和最终 spill/reject 动作，且不泄露 SQL/业务数据。
- 最终受控 E2E 中进程存活、零 OOM、零 swap growth、follow-up query 健康。

## 16. Rollback 边界

- 新的 Shuffle spill/re-spill recovery 必须有单一 emergency gate，默认开启；关闭时仍保留 hard admission，只把 recovery 降级为稳定的受控错误，绝不能恢复旧的无界增长路径。
- Broadcast controlled-error 与 HashMap hard admission 属于安全底线，不提供回到旧 OOM 风险行为的运行时开关。
- Historical reproduction mode 只用于确认旧问题，不作为 fixed build 的默认验收模式。
- 任一中间 commit 不得留下“部分启用 Broadcast spill”状态。
