# #29005：View metadata 持久恢复设计与缺口审查

- 修订：v2，2026-09-20；保留已批准合同，补充具体存储与默认关闭期间的失效约束。
- 状态：用户已批准推荐设计方向与三项合同决策；catalog 侧完整实现已落地，具体约束见第10–11节。验证证据与明确边界由 PR #29139 汇总，不把内部 transport seam 描述为生产 activation。
- Owning issue：#29005，父任务 #26227；实现 PR #29139。
- 设计时基线：`d8ddce92b1c5c172111b50aefe6b6b200b2589cb`；交付前通过 merge 合入 `mo/main` 的 `f9a6eb363b`。
- 前置合同：[wire/snapshot](CLAUDE_20260916-catalog-metadata-barrier.md)、[RSM 修订](CLAUDE_20260917-catalog-barrier-review-resolution.md)。
- 前置审批：#29049 revision `2d7420cdfc2835adec7cbe20e2351420c478c3e6`，XuPeng-SH review 5233521645；文件头仍标 Draft，但 GitHub 审批已核实。#29060 已合并 #29004 runtime。
- 风险：R3；catalog 格式、跨事务恢复、跨 CN 竞争、restore、租户上下文。必须通过设计门禁后实施。

## 1. 设计时事实与缺口（由后文实现闭合）

| 已核对的 owner/路径 | 已有事实 | 本任务缺口或必须验证的条件 |
|---|---|---|
| `pkg/catalog/view_metadata.go` | 两张 cluster table，逐 View 的 target/completed generation、lease epoch、deadline、状态与依赖图 | 没有独立的 barrier E/R/C marker 或提交后 outbox；不能把逐 View generation 当作 HAKeeper R |
| `compile/view_metadata_recovery.go:recoverPendingViewMetadataTarget` | claim RUNNING、rebind、发布均借用同一 `proc` 事务；冲突/写入后失败返回错误 | 该 claim 是事务内互斥，不是已提交的 coordinator claim；需要区分协调器接管与单 View 重试 |
| `compile/view_metadata.go:persistViewDependenciesWithContext` | completion 更新匹配 account/relation/target_generation，并检查 affected rows | 发布没有显式匹配 lease owner/epoch/deadline；若拆成持久 claim，必须同步加强所有写入边界 |
| `updateViewRefreshFailure` | failure 更新匹配 target generation/lease epoch | 不检查 affected rows；不能将 CAS 失效当作成功处理任务 |
| `discoverLegacyViewPage` | keyset cursor + generation CAS，每页 32 | sentinel 的 scan generation 不是 barrier R；轮回 discovery 不等于本 R recovery 完成 |
| `runViewMetadataRecoveryPage` | discovery 一次、最多 31 个 refresh，30 秒调用截止 | 局部页计数有界不代表 SQL 扫描、反向闭包或事务总写入有界 |
| `enqueueDependentViewClosure` | 递归反向闭包，每条写入 SQL 分页 16 | 一个 DDL 事务循环所有页，且每页重新递归/offset；不能声称事务总工作有界 |
| `frontend/snapshot.go:doRestoreSnapshot` | restore 与 recovery 共享 lifecycle 锁序；table/database removal 有失效路径 | post-restore reconciliation 调用只在 account 分支；需给 table/database 建立 scoped reconciliation，不能复用 account-wide reset |
| `ReconcileAccountViewMetadataSQL` | 清 orphan，给缺状态 View seed DISCOVERING | 已有 CURRENT 行不能单凭存在就视为恢复完成；须核对恢复/重建是否保留旧状态 |
| `recoveryCompilerContext` | 复用权威 generator，有 account/subscription/snapshot 适配 | subscription 查询忽略传入 snapshot；需验证历史绑定合同；查询错误前写 looked-cache 可能将后续查询误当作已完成的负结果 |
| HAKeeper `CATALOG_ACTION_CLAIM` | CATALOG_REQUIRED 阶段分配不可复用 ClaimID | RECOVERING 不接受重新分配 ClaimID；物理 worker 接管不能直接假定此接口支持 |
| HAKeeper `CatalogMetadataReceipt` | 固定 required/started/completed 三槽，E/R/ClaimID/action/digest | 文档 Sequence 在实际 wire 中没有单独字段；应按固定 action 槽映射，不能偷偷增加或改号 |

以上是源代码核对结果，不是已运行的回归证明。特别是同事务 claim 目前有事务回滚保护，不能仅因缺 deadline 条件就声称已经发生并发覆盖。

## 2. 不变量、所有权与计数域

采用三层身份，互不比较大小：

1. HAKeeper owns `(membership_epoch E, required_generation R, ClaimID K)`；RSM 是 barrier 的唯一授权来源。
2. catalog coordinator owns `(E,R,K,lease_epoch L,lease_owner,deadline)`；L 只用于同一逻辑 claim 的物理执行者接管。
3. 每个 View owns `(account,relation,target_generation V,completed_generation)`；V 跟踪该目标失效与重绑定，不用时间戳冒充 R。

核心条件：

- 同一 catalog scope 的 E/R/C 不回退，`C <= R`；完成必须精确绑定 RSM 当前 E/R/K。
- 每次 recovery 写事务必须锁定并核对当前 E/R/K/L/owner，不能仅在事务外预读。
- View definition、依赖替换、目标完成与该批进度在同一事务提交。任何部分写入失败必须回滚。
- 页游标只在本代内单调；新代游标重置与新 required marker 原子提交；旧页不更新新代。
- 对 restored target，缺行、DISCOVERING、PENDING、RUNNING、INVALID 或 completed 落后均不能解释为 current。
- 完成 catalog recovery 不自动授予 metadata authority；public gate 仍固定 false。

## 3. 提议的持久状态

### 3.1 独立协调行，而非继续复用依赖 sentinel

建议新增 system-account 的内部 catalog recovery 状态表与 outbox 表，不修改既有 View 表字段含义。首版只支持一个固定 global barrier scope，不允许任意用户创建 scope。

协调行字段合同（实际 SQL 类型/迁移编号在实现前的 schema review 固化）：

- 固定 scope 主键；schema/protocol version。
- E、required R、completed C、逻辑 ClaimID K。
- lease owner（有界 CN incarnation 标识）、lease epoch L、lease deadline。
- recovery phase：REQUIRED → DISCOVER → REFRESH → VERIFY → COMPLETE；错误/接管不能倒退 R/C。
- 当前 discovery keyset `(account,database,relation)`、所属 R、扫描是否完成。
- 当前重试时间、尝试数与有限错误码；不持久化无限错误文本。

现有 `mo_view_refresh` 保持唯一逐 View 队列，`mo_view_dependencies` 保持唯一依赖图。清理 cursor 属于 coordinator 的有界阶段，不另建内存全量任务副本。

### 3.2 Outbox

每条 evidence 固定身份 `(scope,E,R,K,action)`，required 阶段 K=0，started/complete 使用 K；同身份内容不可改变。digest 基于版本化、固定顺序的 marker 内容，而非不稳定 SQL 文本或 map 遍历顺序。

- marker 与对应 outbox 在同一 catalog 事务提交。
- 仅 committed outbox 可发送；不从调用栈里的待提交结果构造成功证据。
- 每 scope 最多三条、每条编码后最多 4 KiB；首版固定单 scope，总量不超过三条/12 KiB，比既有设计的全局 1024 条/4 MiB 更严格。
- 同身份重试不占新额度；旧代占满时先做 receipt/readback 回收，不绕过额度建立新 scope。
- ACCEPTED/readback 必须匹配完整 identity/digest；永久退休须有已提交的新 E/R/K 水位，普通 timeout/STALE 不能作为删除依据。
- 协调行长期保留；不能把删除 outbox 当作删除 required/completed marker 的许可。

### 3.3 物理 worker 接管选择

推荐：**同一 RSM ClaimID K 内，用 catalog L 接管**，不扩大 #29004 的 CLAIM 状态机。

1. K 在 CATALOG_REQUIRED 分配并持久安装，started evidence 提交后进入 RECOVERING。
2. 执行者 crash 后，successor 在同一协调行上 CAS 过期 lease，增加 L；保持 E/R/K、已提交游标与 View 进度。
3. 旧执行者每次写事务必须验证 L/owner，接管后不能再发布。
4. 重试 deadline 影响何时尝试接管，不承担正确性 fencing；即便时钟偏差导致提前接管，L/CAS 也保证单一有效发布者。
5. 所有计数器显式检查溢出，不 wrap；禁止靠旧 owner 心跳恢复 L。

备选：扩展 RSM 在 RECOVERING 分配新 K，需要重置 started receipt、定义 phase/receipt 迁移及 snapshot 合同，影响更大。若 reviewer 认为 B.2 必须要求每次物理接管更换 K，则需要选择该方案并单独审查，不能混用两种身份。

## 4. 事务与阶段流转

| 操作 | 同一事务内必须发生 | 提交后动作 |
|---|---|---|
| 安装 required | 生命周期 gate → 协调行；核对 E/R 单调；设置本代进度；插入 required outbox | dispatcher 发 required receipt |
| 安装 claim | 核对 E/R/K；持久 owner/L/deadline；插入 started outbox | 发 started receipt；不得提前处理未安装 claim |
| discovery 一页 | 核对 claim；读有界 keyset；seed 缺状态目标；推进本代 cursor | 下一 tick 继续 |
| refresh 一个目标 | 核对 claim → target lock；核对 V；rebind；ReplaceDef、依赖、目标状态原子更新 | 不在事务外缓存 current 判定 |
| 临时失败 | 无部分 metadata 写入时，CAS 设置有限 backoff 并检查 affected rows | 让其他到期目标继续，不能卡在第一页 |
| cleanup 一页 | 核对 claim；只清可证明 orphan 的目标/边；推进有界 cursor | 不删当前有效 claim 的进度 |
| completion | 锁定 claim/生命周期边界；本代 discovery/cleanup 已闭合且无未完成目标；写 C=R 与 complete outbox | receipt 成功仍不打开 public gate |
| supersede | 安装新的 E/R 前永久 fence 旧授权；新 marker 与进度重置原子化 | 旧 evidence 经退休握手清理 |

沿用现有锁序：feature-registry catalog gate（需要替换该 catalog 的 restore）→ SNAPSHOT lifecycle → View lifecycle → coordinator row → target object → target state/依赖写入。不能让 cleanup 先锁范围行再请求 lifecycle gate。

取消结束本次调用，不删除 durable claim/outbox；事务失败只丢未提交进度；崩溃后重读 catalog，不继承进程内 cursor。transaction commit 成功但响应丢失，通过相同 identity readback/retry 判定，不能重新递增 generation。

## 5. Restore 与有界闭包

### 5.1 Scoped reconciliation

引入显式范围值：account、database、table；account 必须来自 restore 解析后的目标租户，不能来自快照源租户。database/table 使用物理身份与规范名称来覆盖 COPY/restore 换 ID，不跨租户仅按 relation ID 匹配。

- table：删除旧目标生命周期记录、seed 恢复后的该 View（如果是 View），并处理该源的反向受影响闭包；无关同库 View 保持原状态。
- database：只 reconcile 该库内目标，库外依赖仅通过反向图失效。
- account：reconcile 该目标 account，跨 account 依赖依然通过真实 source account/发布订阅边失效。
- cluster/system catalog reset：需要恢复后全局重新 discovery；不可恢复出来的旧 CURRENT/marker 不能自动完成当前 barrier。

scoped pending/fence 与 restore 修改在同一事务可见。不要为了在下一 tick 再 seed 而留下 committed restored target 没有恢复状态的窗口。

### 5.2 有界性必须分开证明

现有每条 SQL limit 16 不限制整个反向闭包。拟将后台 discovery/cleanup/refresh 限制为每页最多 32、单 tick 至多 32 项、30 秒截止；单个 View 的 rebind 成本受其定义/依赖规模约束，不声称常数。

**已批准决策 B1（保留原比较）**：源 DDL/restore 的完整反向闭包若跨多页，选择：

- A：同一事务同步失效完整闭包，保持现有语义，但不能满足“事务总工作固定有界”；只能承诺 recovery tick 有界。
- B：事务先发布 durable scoped invalidation intent，后台持久 frontier 分页扩展闭包；在该 intent 未闭合时，后续 authority 层必须把潜在受影响目标判为不可读 current。表恢复只扩展其受影响图，不重置整个 account。

推荐 B，但它需要 #29006 消费 intent/fence；必须明确接口后才能实现，不能只把递归循环挪到 goroutine。frontier 的逐代数据有上限和清理 owner，达到额度时暂停新 intent，不能丢任务。具体额度/分页索引与 completion 查询需要下一版设计固化。

## 6. Context、失败分类与安全

- View rebind 继续走 `RegenerateViewDefinition`，保留 identity、SQL mode、default DB、security/owner/privilege 元数据，不用 DROP/CREATE。
- 身份匹配至少包含 account；订阅的 binding 名称与 publisher physical database 分离；不可用订阅不能错误回落到当前租户同名表。
- 命名 snapshot/时间戳 binding 遵守历史数据与租户语义，snapshot 参数不能无解释地被忽略。
- cache 仅记录成功的查询（包括已确认不存在）；查询失败不得写入负缓存。每次 rebind 私有 cache，退出即释放。
- INVALID 与 temporarily unavailable 区分。INVALID 不是正常 CURRENT；“本代扫描终结”与“所有 metadata 可用”不等价。

**已批准决策 B2（保留原比较）**：completion 是否允许存在已分类 INVALID View？建议允许“恢复工作已收敛”但不允许该 View admission；这要求 #29006 按目标验证状态。若 barrier completion 定义是所有 View 可用，则 INVALID 必须阻止 C=R。应由系列合同明确选定，不能按测试方便决定。

**已批准决策 B3（保留原比较）**：提交后 evidence 的可信生产入口。当前未发现可直接供 CN coordinator 调用的已认证 catalog receipt RPC。建议首先实现不自动启用的内部 coordinator/dispatcher API，用真实 catalog + 受控 RSM transport 测试证明事务边界；不能新增普通 SQL/heartbeat 可伪造 completion 的入口。正式跨进程入口需要明确现有服务认证机制或专用控制面 owner，完成前不广告 RecoveryProtocol=1、不声称生产端到端闭环完成。

## 7. 兼容、部署与可观察性

- 新 schema additive；不得只修改已执行过的 v4_0_6 upgrade 列表冒充迁移。检查当前 v4_0_7 offset/升级制度后确定新 upgrade step，并覆盖 fresh bootstrap、旧库升级和幂等重试。
- 系统 restore 不能把 coordinator marker 当作普通业务历史数据回滚后继续签发 evidence；需保留活 barrier fencing，重新安装权威 E/R 并恢复扫描。
- mixed version 保持已有维护切换前提；本 issue 不自动 Begin、不增加公开激活开关，ViewDependency/Recovery capability 未验收前保持零。
- 无新的常驻无界队列。dispatcher 首版单 scope 单 in-flight；调用者拥有 context，取消立即停止新提交；有限指数 backoff，最高 5 分钟。
- 诊断输出只含 E/R/K/L、阶段、有限错误码和条数；不输出 SQL/租户敏感 payload，不给 metrics 添加逐 View label。
- 失败可阻塞 metadata recovery，不以忽略 marker 写入错误绕过；不改变普通 SQL admission。上线后是否限制 metadata 由 #29006/#29007 决定。

## 8. 替代方案与选择

| 方案 | 优点 | 拒绝/接受原因 |
|---|---|---|
| 把全局状态塞进依赖 sentinel | 无新表 | 拒绝：字段语义复用、restore orphan cleanup 易删协调状态，难以表达独立 E/R/K/receipt |
| HAKeeper 直接保存逐 View 队列和扫描结果 | 单 owner | 拒绝：把业务 catalog/I/O 放进 RSM，破坏 #29004 边界且状态随 View 数量增长 |
| 新协调行/outbox + 复用现有 View 队列与依赖图 | 边界明确、可事务提交 | 推荐：只新增缺失的协调事实，不复制逐 View 元数据 |
| 每物理 worker 在 RSM 新分配 K | token 所有权直观 | 备选：RECOVERING takeover 与 receipt 改动需要新协议审查 |
| 逻辑 K + catalog lease L | 无 RSM 新转移 | 推荐：RSM 控授权代，catalog 控物理执行权；每次写必须校验两层身份 |

采用事务 outbox 与 fenced worker 的常见设计原则，但不假定消息 exactly-once；安全来自幂等 receipt/CAS 与原子 catalog 事务，不来自 RPC 成功。外部没有要求新增 MySQL 语法，SQL 表本身仅为内部持久化实现。

## 9. 验证映射与执行顺序

| 合同 | 最低有效证据 |
|---|---|
| marker/outbox 原子 | 真实 SQLExecutor 事务：提交前不读到、回滚无残留、提交后 dispatcher 崩溃可重放 |
| 接管不回退 | 两 worker，显式时钟/事务屏障，旧 worker 分别在 claim、页推进、ReplaceDef 前后恢复；检查 durable 状态 |
| completion stale fencing | 旧 R、旧 K、旧 L、owner mismatch、deadline/接管边界、受影响行数 0；错误不误报成功 |
| receipt 有界幂等 | RSM 真 Update/readback + catalog outbox，ACCEPTED 丢响应、删除失败、退休/身份冲突、三槽上限 |
| 两页公平 | 页长+1 的最小目标集；第一页含 retry/INVALID/conflict，对下一页有实际推进断言，不用 sleep |
| scoped restore | table、database、account；保留同名不同租户和无关目标；第二 CN/独立事务观察恢复前后状态 |
| context | account/publisher/snapshot 正例及不可用/重新可用反例；错误不得污染下一次查询 |
| metadata 原子 | guarded ReplaceDef 后故障，定义、边、状态全部回滚；保留 View 身份与属性 |
| schema/restore | fresh bootstrap、升级幂等、系统 restore 后 marker 不能回退并伪造 completion |
| 默认禁用 | public helper 固定 false，generic barrier 不启用 View 生命周期、不发送虚假 RecoveryProtocol |

先复用 `view_metadata_recovery_test.go`、`snapshot_test.go` 与现有 catalog/升级集成 fixture；不为每条 case 启新 cluster。UT 用最少对象、注入 clock/barrier；真实 persistence/MVCC 不能仅用 SQL substring mock 代替。

后续按 Go 1.26.4 + worktree `mo-cgo-test` 执行 focused tests、owning packages、相关 race/restart/upgrade，再增量静态检查；代码修改覆盖率至少 75%。public activation BVT 属于后续子任务，但本任务仍需真实 catalog/restore 消费者证据；如增加 SQL BVT，用 mo-tester 生成并验证 result。

## 10. 审查结论与下一步

- 已批准：工作计划；#29003/#29004 前置设计。
- 本设计方向已获用户明确批准：逻辑 K + 物理 L 接管；B1 采用持久 intent 分页扩展；B2 INVALID 可结束恢复但不得 admission；B3 保持内部默认关闭 producer，不新增公开激活入口。
- 实施按契约分组；先闭合已有 recovery/scoped restore 的局部缺口，再固化并实现协调 schema/intent/outbox；未完成的跨事务证据必须明确记录，不将局部修复报告为整个 issue 完成。
- public admission 始终不提前开启。

## 11. v2 实现落点与边界

- `mo_view_recovery` 是系统私有控制表：唯一行、格式版本 1 的 JSON、revision CAS、独立 mutation revision、SQL lease deadline。JSON 保留 E/R/C、K/L/owner、不可变 scope、work 配额和固定三槽 receipt。JSON 总长度限制 16 KiB；它不是按事件无限追加的日志。
- `mo_view_recovery_work` 是同一 owner 管理的持久 work/visited set。四个初始任务负责源反向边、范围内 View 扫描、refresh 孤儿、孤立 dependency 边；节点任务扩展反向闭包。使用 `(account_id,relation_id)` keyset，每页 32，按 visits 轮转；整个存储至多 65,536 行。已完成节点保留至本代结束，以消除环和菱形重复。
- 初次恢复必须完整发现 catalog。尚未完成的旧代不能被更小或不相交的 scope 覆盖；新代必须包含其未完成范围。旧代与完成代的 work 逐页回收；不会通过一次无界 DELETE 清空。队列达到上限时，新代安装在同一事务先回收一页旧代 work，避免因没有初始任务名额而无法替换旧代。
- `RequireViewMetadataRecoveryInTxn` 必须在对象修改后、同一事务提交前调用。调用者取得权威 E/R，并负责 scope 覆盖本次已封闭的变更。单独的 Require 用于先前已由 durable mutation clock 拦截的默认关闭路径。没有 SQL/heartbeat 公开调用入口。
- 默认关闭时的已有 DDL/restore 失效路径同时递增独立 mutation revision。Require 记录当前值；后续 claim/page/complete/current 比较该值。恢复之外的对象修改使旧代失效，不回退 C，也不能沿用旧 CURRENT。实际 table restore 在第二个 CN 上验证这一行为。
- 每次 Claim acquisition 都递增物理 L，包括重启后复用相同 owner 名称；同一逻辑 K 的 receipt 不改变。已有 token 的续租由 Page 完成，不能把重复 acquisition 当成允许旧 worker 继续运行的心跳。
- 协调事务锁序保持 lifecycle gate → coordinator → 目标；最终 CAS 再检查原 lease deadline，不能在页执行期间过期后续租或完成。错误与取消由 SQL 事务回滚；冲突退避使用另一个完整 token 与目标 V 双重 fencing 的事务。
- 仅在 frontier 完成后刷新目标；coordinator context 禁止旧路径在一个 refresh 事务内再次展开整个 fanout。CURRENT 必须 V 已完成；INVALID 可以使工作收敛，但 `IsCurrent` 必须返回 false。缺失表、订阅/RPC 暂时不可用保持重试；已有源表缺失绑定列的明确 ErrBadFieldError 属于本代无效定义。
- `IsCurrent` 仅提供 catalog 证据，不替代 HAKeeper authority/response fence。所有公开 admission 开关依旧返回 false；不注册后台 goroutine、public producer 或自动 protocol advertisement。
- 历史订阅读取借用完整 snapshot transaction（包含 logical timestamp），结构化缓存键包含租户、数据库、历史标志与完整时间戳，避免名字与拼接后缀碰撞；不能用只有 physical timestamp 的 SQL hint 代替。恢复对象重新绑定使用目标租户上下文，订阅边同时保留 publisher 物理身份和 subscriber 绑定命名空间。
- Receipt 只能来自已提交 outbox。恢复后续租不改变逻辑 receipt digest；digest 包含 mutation revision。网络失败保留槽，丢失响应可重放；线性一致的接受或退休证据才允许回收。已被新 DDL 弄脏的 completion 不再提交，只允许可信只读 readback 回收旧证据。
- 4.0.8 独立 additive migration 与 fresh bootstrap 注册两张表；系统 restore 排除控制表，不能将控制真相回滚到业务快照。4.0.6/4.0.7 已执行的迁移不修改。
- 真实验证使用一个必要的私有双 CN fixture：本包旧用例保留单 CN shared fixture，不能同时启动另一种 shared base；显式并存预算避免隐式破坏其资源 owner。数据量为跨越 32 行边界的最小 33 个同级 View，加两级反向依赖及同名不同租户对照。最终验证状态记录于本地证据文档，不将尚未跑完的检查写成通过。
