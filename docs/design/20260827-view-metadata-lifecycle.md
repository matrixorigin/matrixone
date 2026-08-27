# View 元数据生命周期与分布式激活设计

- **状态**：Approved
- **设计审批**：issue owner 于 2026-08-27 的 implementation 前 design checkpoint 明确批准唯一 authority、fail-closed 线性化顺序与 activation-only scope
- **稳定版本**：本文件在 PR #27734 中的 exact revision；PR 正文必须链接具体 commit 的 blob URL
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
- `information_schema.columns` 基于解析后的 AST relation identity 判断，不能依赖原始 SQL 文本，因 comment/quoting 是语义等价表示；
- prepared/binary execute 保守取得 lease，避免 cached AST/plan 跨 epoch；
- 显式事务记录 snapshot 创建时的 epoch；metadata-sensitive statement 若 transaction epoch 过旧则要求 rollback/retry；
- direct View query 继续按当前依赖 rebind，不被错误地改成持久化 schema consumer。

## 7. 生命周期、失败与资源边界

| 失败 | 必须行为 | 禁止行为 |
|---|---|---|
| HAKeeper/catalog unavailable | deadline 后失败，保持 sealed | 本地 cache 授权 |
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

- public metadata path：一次进程内 mutex lease acquire/release；relation detector 对当前 statement AST 做 O(AST nodes) 无 I/O 扫描；
- ordinary非 metadata statement 不访问 catalog，不查询 MOCluster；
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
- 新 binary 回滚为旧 binary 后，缺失 capability 的 heartbeat 触发新 epoch，公开 metadata 在 revalidation 前 fail closed；
- old binary 不需要理解新 refresh fields 才能保持 wire compatibility，但它不能成为 activation authority；
- 再升级后必须重新完成 epoch，而不是复用 rollback 前 completion。

### 9.3 Rollout/fallback/removal

默认只有 durable predicates 全满足才自动启用，不提供绕过 authority 的 session/global switch。故障 containment 是保持 sealed 并返回
可重试错误；operator 通过 HAKeeper/CN heartbeat 与 catalog state 定位卡在哪个 phase。回退 binary 会触发 fence，不删除新 catalog 字段。
协议字段与 catalog state 只有在所有支持版本退役并完成单独兼容设计后才能移除。

## 10. 可观测性与安全

需要能关联 service UUID/generation、requested/current epoch、catalog fenced epoch、refresh-ready/revalidated epoch、worker terminal error 与 retry。
日志不得按 View 每 tick 无界刷屏；metrics label 不含 View SQL、tenant object name 或高基数 epoch history。

本设计不改变 privilege 判定。AST detector 只决定 lease，不授予 relation 访问；绑定/鉴权仍使用既有路径。系统事务继续携带 account context；
publication/subscription 与 snapshot binding 不能跨 tenant。恶意复杂 SQL 最多触发 O(AST nodes) 本地遍历，不引入额外 catalog I/O，避免新的
DoS 放大边界。

## 11. 验证矩阵与 acceptance gate

| Contract | 最便宜的确定性证据 | 额外证据 |
|---|---|---|
| comment/quoting 不绕过 I_S relation | parser + frontend focused UT | public SQL BVT |
| old reader 必须先 drain | public Acquire barrier，无 sleep | focused `-race` 重复 |
| canceled advance 保持 sealed | fence UT | CN lifecycle race |
| stale generation/lease 不提交 | HAKeeper/catalog UT | multi-CN replacement |
| required → completion durable | embedded issue regression | exact BVT 同实例重复 |
| rolling upgrade/rollback fence | state-machine UT | 真实旧/新 binary multi-CN sequence |
| restart recovery | worker UT | 新 CN process restart |
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

阻塞性开放问题：无。若真实 mixed-version binary evidence 与上述 sequence 不一致，设计重新进入 REQUEST_CHANGES，不以修改测试预期解决。
