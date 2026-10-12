# 完整多层关联子查询：现行设计 r4

- 需求：[#7559](https://github.com/matrixorigin/matrixone/issues/7559)；设计交付：[PR #29751](https://github.com/matrixorigin/matrixone/pull/29751)。
- 状态：**r4 待技术评审，未批准；BLOCKED FOR IMPLEMENTATION。** 本 PR 仅修改文档，不实现或关闭需求。这里给出开发侧选定合同，不将“已回答意见”当作 reviewer 批准或运行证据。
- 源码基线：`mo/main@c2c1031d0ba4b1034849f6bd3d267252a11eb6c9`，2026-10-09 merge；Go 1.27.1，最新 MORPC=107。实现时重新检查协议分配，不预占版本号。
- r3 四项评审分别由第 3–5 节（执行/frame）、第 2/6 节（Steps/transport）、第 7 节（入口）、第 8–9 节（资源/性能）回答；第 10 节给出共同 trace，第 12 节记录取舍。
- [r0–r3 历史调查/基准附录](CLAUDE_multilevel_correlated_subquery_history.md)保留原始事实和已废弃候选。**本正文是唯一现行合同**；附录不是另一组可任选接口或当前 head 验收。

## 1. 目标与不变量

合法任意多层、跨级、多祖先、多 binding 关联统一执行，不按 depth、SQL shape、arm 数或预计 O(E·I) 拒绝。覆盖 scalar/行 scalar、EXISTS/NOT EXISTS、IN/NOT IN、ANY/ALL，JOIN ON、聚合/HAVING/GROUP BY、窗口、CTE、集合运算及现有 DML 子查询入口。

独立语言约束仍有效：同层非 LATERAL FROM 不可见、非法递归源重读、窗口结果循环依赖、RETURNING 的现有非关联子查询禁令。分区、charset/collation、存储过程、UDF、warning 兼容性不扩展；既有合法值的完整类型/NULL/provenance 必须保真。

| 不变量 | 否定它的最小状态 |
|---|---|
| 按实际语义 stage/occurrence 求值，不按参数值共享 | 两个不同候选 pair 有相同 payload，第二次求值被省略 |
| 未需求分支零 program/reader/producer 启动 | CASE 全 FALSE，但顶层 Steps 已启动其 CTE |
| 完整 native 类型与 operands 单次求值 | slots 经 ParamRef TEXT；行比较展开重算易变 lhs |
| 决定真值不等于 producer 成功完成 | 第一 TRUE 后取消掩盖已冻结读错/cardinality/budget |
| 每个 acquire 一个 cleanup owner，旧代静止才换代 | child Reset/Free 后 parent 仍借用 tuple；迟到 completion 写新 frame |
| 同一 statement/snapshot/会话副作用与 query-CN cap | 子程序复制 Base mutex/map，或每 child 新开 query cap |
| 优化是等价实现，不是通用机制的覆盖边界 | mutating flatten 失败后执行已被改坏的“fallback”树 |

选 dependency IR + 已证明集合 lowering + 通用 dependent 执行。无证明的 equal-value/domain 去重不启用。优化证明必须含需求、错误、易变性、operand、bag、snapshot；不能仅以 CannotFold 或参数相同判定。

scalar EOF 后零行全字段 NULL、一行完整 tuple、真实第二行 cardinality 错误；EXISTS 第一输出行可决定；IN/ANY 三值 OR、ALL 三值 AND、空集分别 FALSE/TRUE；NOT IN 是三值 NOT。使用绑定的比较 overload，`(1,NULL)=(2,NULL)` 为 FALSE，不复用单 key NULL 摘要。无完整排序 LIMIT/易变值用合法结果集合与性质 oracle，不引入文本顺序 WHERE/AND/OR 短路保证。

## 2. 唯一 IR 与 step catalog 合同

下面字段是拟增加的正式 plan schema，不是现有 Go API。生成代码从 proto 源生成；不得用 ExtraOptions、SQL string、callback 或 ParamRef 偷传。无新 IR 的 Query 不分配 registry，按旧路径处理。

```text
CorrelationSpec(version=1):
  main_output_step: StepId
  step_owners: RegionId[len(Query.Steps)]
  regions: RegionSpec[]
  programs: ProgramSpec[]
  stages: StageSpec[]
RegionSpec: region_id, ordered_owned_steps[], activation[ENTRY | FIRST_READER],
            shared_source_reads[step_id, sharing_proof_id, reader_lease_kind]
ProgramSpec: program_id, region_id, root_node, output_types[], inputs[], callee_ids[]
SlotSpec: ordinal, semantic_binding_id, column_ordinal, full_type, provenance,
          origin = StageAccessor(stage_id, side, image_version, physical_column)
                 | ParentSlot(parent_program_id, ordinal)
DependentExpr: program_id, occurrence_id, stage_id, reducer, lhs_operands[],
               comparison_overloads[], single_evaluation_ids[]
SlotExpr: program_id, slot_ordinal             // type 仍在 Expr.Typ
StageSpec: stage_id, owner_node, stage_kind, accessor_layout
```

StepId/NodeId 为现有 int32 索引；RegionId/ProgramId/OccurrenceId/StageId 为Query-local uint32，运行epoch/lineage为checked uint64。SlotOrdinal为program-local uint32，slot origin是oneof；缺schema/非法ID/不完整owner长度拒绝publish。`RegionId=0` 为 main；program region ID 非零，ID 在当前 Query 内唯一；初版ProgramId等于它的RegionId，不做未声明的转换。所有关系仍在 `Query.Nodes`，表达式与程序之间是可遍历显式边。callee 图必须无环；递归 CTE 的 iteration 回边不是程序调用回边。冻结语义 binding owner 后，用显式 work stack 求 direct+descendant slot 闭包；中间程序以 ParentSlot 转发跳层引用，运行时不减 Corr.Depth、不建 schema/tag map。

### 2.1 索引、初始化与编译

1. `Query.Steps` 是**全 Query 稳定 catalog**，不是默认全部可执行 roots。绑定完成后冻结；每个 index 恰归一个 region，程序 root 若不是 step 也必须在其 region 的节点闭包内。
2. owner 空的旧 Query：全部 step 为 main，输出仍是原最后 step。owner 非空：输出显式使用 main_output_step，不能让追加的 program step 被误当最后输出或 LIMIT 0 的判定根。
3. `Node.SourceStep` 继续引用全局 catalog index。region 内 source 必须同 owner；唯一跨 owner 边是带 sharing proof 的 `shared_source_reads`。从 parent live pipeline 读参数或偷接另一 region 的 receiver 是 plan 校验错误。
4. `CompileMain(Q)` 编译 main 的有序 executable list；`CompileRegion(Q, rid, snapshot)` 只编译指定 region 为不可变 template。sink/recursive receiver 发现只遍历该 region 的 root/source 闭包。它们不创建 execution edge、reader、producer 或执行 expression。仅被program引用的共享main producer标FIRST_READER，直到实际reader取得lease才初始化；不能让main的all-source initializer又把它提前Begin。ENTRY只包含main输出的真实启动闭包。
5. main literal LIMIT 0 的既有优化作用于 main_output_step 的需求闭包；不对 catalog 最大 index 作算术推断。未选 program 不调用 Activate，故没有待收数据的动态 receiver。
6. `stepRegs`、materialized reader 索引、recursive iteration 索引的运行 key 是 `(AttemptId, DomainId, InvocationSeq, globalStepId, destNodeId)`，不是孤立 step index。fresh generation 不复用旧的 edge/receiver。
7. append 不重排旧 index；必须压缩/复制/切 fragment 时生成完整 old-to-new step/node/region/occurrence map，原子重写 Steps、SourceStep、main_output_step、owned_steps、shared source、program roots、slots/accessors、sink/recursive consumers。映射缺失拒绝 publish，不猜相邻 index。

### 2.2 绑定、授权和发布顺序

完整 bind Query -> 冻结 owner/type -> 只读选择优化 -> 构造独立候选 -> 完整图校验 -> 原子发布 -> 完整授权/对象依赖/控制函数检查 -> statement 元数据锁/definition fence -> compile immutable templates -> 按需求 Activate。

权限和 locks 看**所有 bound nodes/expressions/programs/frame operands**，即使 CASE 不选；授权不能惰性绕过。catalog/metadata 观察本身不是 SQL 表达式求值。datasource reader、materialized Begin、prepare-time runtime cast/副作用则只能在所属 region 激活后发生。

旧 pull-up/depth/CTE lowering 是破坏性变换：只对已证明适用的候选副本使用，失败丢弃副本；不可 catch NYI 后执行它。新 IR demand 屏障不被 fold、join-key extraction、post-join FILTER、prune 或 common-expression dedup 穿越。纯 syntax/type/可见性错误仍在 bind 返回；有运行期错误/副作用的表达式 factory 不在 compile/Prepare 中偷偷 Eval。

## 3. 执行选型：同 lane 显式 continuation，不用嵌套 Scope workers

**选择协作式 continuation。废弃 r3 的 reusable Scope task-slot 候选。** 原因：要计量 Go stacks 且父等待不能占 child 容量；只在每层准备专属 goroutine 仍增加 depth 相关 stacks，并未解决 legacy submission/terminal/reuse 的组合。

只有含 general dependent 的 stage 和 program region 接这一 runner；普通证明式计划不改变 `Scope.MergeRun`/`Pipeline.Run`。初版需求决策本地单 lane，输入可用普通并行 gather。一次 statement attempt 借用当前 consumer goroutine 驱动；不新建全局池、每层/per-E goroutine 或“后台逃生 worker”。

### 3.1 具体接口与唯一责任

```text
CompileRegion(...) -> RegionTemplate                 // bind/物理选择/expr opcode 一次
OpenOwner(template, StatementServices) -> RegionOwner // 借 template，不执行 SQL
AcquireFrame(stage, input_lease, logical_N) -> FrameLease
Submit(owner, frame, occurrence, row, typed_slots, lhs) -> Ticket
Pump(ticket) -> Completed | substantive_error         // 只在最外 consumer 调用
Step(task) -> Progress | NeedInput(edge) | NeedExpr(pc)
            | NeedProgram(ticket) | EOF | Failed(error)
Finish(ticket, EOF | decision_stop | error | cancel) -> FrozenOutcome
ReleaseFrame(frame)                                  // tickets 静止后
CloseOwner(owner)                                    // 不 Free shared Base/template
```

`RegionTemplate` 是 immutable operator/expr factory 与 topology；`RegionOwner` 只持借用 template、attempt services 与当前 invocation handle。**不跨 generation 复用 mutable operator/executor/Scope state**：每次 Activate 从 factory 创建 fresh state，Prepare 一次，Finish 后 Reset/Free 一次。复用的是 template 和已分配、清零的 pointer-free控制记录容量，不是被 TP Reset 拒绝的 AP 算子对象。重建 runtime state/reader 不是重新 parse/bind/优化/compile；factory 不做这些步骤。

`Pump` 是 trampoline：task 的 PC、当前 batch lease、pair/group/assignment cursor、待完成 native function arguments 存在显式记录中。NeedProgram 压入 child activation record并让出；child 返回后只唤醒 parent PC。内层 Step 不调用 Pump/Eval/Scope.Run 递归等待，所以相关层数只增加已记账 frame records，不增加 Go 调用栈。

main普通Pipeline的桥接点是拥有Frame的stage adapter：Call获取/保留input Frame，启动expr task并在当前goroutine调用最外Pump，完成后返回旧CallResult；输出恢复复用Frame，不在dependent leaf新建Frame。已处于Pump中的program stage只返回Step outcome，绝不再调用这个synchronous bridge。factory使用预编译native overload/expr opcode，新Prepare只造execution scratch，不递归NewExpressionExecutor重新编译expression树。

### 3.2 如何适配现有 VM，避免平行 lifecycle

| 现有 owner | r4 的唯一改动/保留边界 |
|---|---|
| `Compile.Reset` | 仍仅是顶层 prepared TP API，绝不用于 Submit/Finish。普通 prepared/AP rebind 路线保留；新 immutable template 被 execution owner 借用 |
| `Scope.MergeRun` | main 普通 scopes 仍管 ants/通知/wg/结果仲裁；program region 不进入此方法、不提交 PreScope worker。program task topology 用相同 producer/source 边，不复制一套 SQL 执行计划 |
| `vm.Operator` 的 ctr/cursors | continuation PC 存在同一个实例的 ctr；已有 matched、group、sort/window、recursive state 仍权威。不能另建一份 shadow matched/cache 状态 |
| `Pipeline.Run` | 普通路径原样；region driver替代其递归/阻塞执行循环。region operator 的 Step 与现有 Call 共用 kernel，不靠 ErrYield unwinding 丢弃 Call locals |
| `Reset/Free`/reader Close | 原资源释放函数仍权威；实例 lifecycle wrapper 记录 constructed/Prepare-entered/ready/Reset/Free bits。新 runner只组织调用顺序，不第二次关闭相同 reader或 spool |
| frozen scope outcome/MarkPipelineFailure | 提取既有冻结/取消归因/主次错误仲裁为可供两种 driver 调用的 helper。旧 wg 不用虚假 task count；region使用sealed task completion slots |
| `Process.Free` | 顶层原合同不变；child只 CloseDomain。不能调原 Free/ResetQueryContext 去清 Base MessageBoard/prepare params/会话状态 |

这是实现必须完成的 adapter 闭包，不是说现在的所有 Call 已经可以 yield：

| 算子类 | suspension 点与保存值 |
|---|---|
| scan/value/function-scan/APPLY | 输入/表函数游标和本次 owned batch；storage I/O 是 context-aware leaf，不等待本 driver 的 task |
| filter/project/limit/offset | input lease、选择 mask、expr PC、output row cursor；resume 不重算已求值 rows |
| group/distinct/set operations | 当前输入与 key/aggregate operands PC；hash/group state 原 owner；blocking drain 的 producer 由 driver 推进 |
| sort/top/window | 输入/排序 key已拥有值；window bound/setup/输出 row PC；不在 sort comparator 内新启动程序 |
| HashJoin/LoopJoin/ASOF/DEDUP | build/probe/candidate/key/action cursor、condition window、matched state；第 7 节规定实际 stage |
| merge/connector/dispatch/source/recursive/lazy UNION | TryReceive/TrySend、独立 terminal slot、active arm/iteration；禁止阻塞等本 driver 下游 |

local exchange 的 TrySend/TryReceive 返回 NeedInput/credit，不占 goroutine 等待；terminal slot 先冻结并封边，满 data queue 不能阻止 Error/Abort/End 记录。普通 blocking Go/native leaf 只允许无本地 wait-for 依赖且受 context 退出控制的 I/O；含子查询的 arguments 必须先由 expr continuation 求齐，才调用 native function，因此不持 sequence/session gate 等 child。

编译校验所有 region opcode 有 adapter；开发期间缺 adapter 是实现门禁失败，不在发布后按 SQL operator 退回 NYI 或开临时 goroutine。相关-sensitive binder/closure/codegen 遍历也用显式 work stack。

### 3.3 shared Base / domain 的具体边界

`StatementServices` 借用原 Base 的 identity/time/settings、txn/snapshot/services、MPool/query-CN budget、prepare params/callbacks、sequence gate/会话 maps、insert-ID/user-lock mutex、warning/IGNORE profile。合法 nextval/LAST_INSERT_ID/session effects仍走这些原 owner；不做 child session、副作用副本或独立 retry/commit/workspace statement。

`ExecutionDomain` 仅拥有 pipeline/query-runtime overlay、MessageBoard/owned txn clone、GROUP_CONCAT iterator counters、collector、slots、edges/readers、epoch。GetCtx/GetAtRuntime/GetMessageBoard/GetGroupConcat… 改为 domain-aware；direct Base consumers按这张责任表分类迁移，nil-domain走原路径。Base setting/事务副作用 accessor绝不因 overlay 改读另一份 map/mutex。

child collector不写 frontend Buf/ResultColTypes/found_rows/affected_rows/OK packet/post-DML；不重置 root prepare vectors、statement profile或 offsets。只有所属 clone可 Close，root txn只借。Window/aggregate 等本地计数按 domain+node+epoch，而不与 root相同 NodeId 混用。

## 4. Frame / memo / selection：一份权威状态

```text
FrameKey = (AttemptId, OwnerLaneId, StageId, InputEpoch)
RowIdentity = (FrameKey, LineageSeq)                 // real pair/group/row version
EvalKey = (RowIdentity, OccurrenceId)
Generation = (AttemptId, DomainId, InvocationSeq)
Frame = input_lease + logical_N + accessor_layout
      + occurrence_state[done/pending bits, aligned result, owned operands]
```

`AcquireFrame` 由真实 stage owner调用，不由 leaf猜 batch pointer。projection/filter 保留输入 lease与输出恢复 epoch；group新 group身份，ON新 pair身份，window setup/row、DML route/assignment分别建身份。换 input、group、pair或 row version必须 Advance；计数实际溢出报身份容量耗尽，不回绕、不限制相关深度。

FrameLease 在该输入最后一次消费/恢复结束后释放；先 seal、Finish所有pending tickets、退还借用，才 Free operands/results/input。原 batch复用前不得留下未拥有 varlen 值。

### 4.1 与现有 memoRoot 的接口

现有 `memoRootExpressionExecutor.Eval` 每 call清 cache，保留用于无 dependent 的旧 root。**含 dependent 或其单次 lhs/memo alias 的 root** 由 build context提升为 `FrameExpressionProgram`；其 SingleEvaluationId 状态就是 Frame.occurrence_state，不再套 legacy memoRoot reset或另建 executor cache。

`EvalInFrame(frame, mask)` 用 expr PC提交 demand并yield；普通 Scalar/Function native kernel在arguments完成后执行。一次 row比较的完整 lhs只存一次；prepared cast/function wrapper共享该语义ID，而两个文本相同的独立 occurrence不 dedup。重复 mask命中done bits，partial mask只填新增 identities；pending 同一 ID挂接同 Ticket，不启动第二代。error使frame封闭，不能把pending/失败行标done再重试其易变函数。

Dependent/Slot executor的对齐分类为 `InputRows`：返回 N logical rows，未选位置不可消费，不是compact结果，也不被父函数当Const第0行广播。native ancestor environment是每slot一个typed值，但在caller stage的读取仍按明确 row accessor/对齐合同。

### 4.2 空 mask 与拥有责任

- CASE/IFF/COALESCE可实际调用leaf传全FALSE；Submit之前检查mask，空需求不创建domain/edge/source，返回适配N的未消费NULL占位。
- factory/Prepare不求值 runtime cast/constant-error/sequence。未选分支语义错误不因初始化暴露；bind语言/权限错误不被此规则隐藏。
- 每个新 demanded row先拥有lhs和需要复制的当前stage slots，再Activate。借父 immutable slot必须持父lease到Finish；没有寿命证明就 native复制并记账，不经TEXT。
- scalar第一tuple/归约bool写caller-owned provisional result，不能借child batch跨下一次Eval/Free；收到EOF/decision并Finish成功后才置done。第二行/读错误丢弃provisional；parent result/memo由Frame释放，child cleanup不Free它。

## 5. Activate / Finish 的线性化与终态

只有 task/control ownership 与全部receiver/source责任登记完成、资源admission成功，才能将invocation从private发布给driver。每项acquire即时记入其现有operator/domain owner，不是等Prepare成功后才登记。

| 输入状态/事件 | 唯一动作与下一状态 |
|---|---|
| 无需求 | 无invocation、无dynamic edge；仅template/授权/元数据责任 |
| 私有初始化/admission失败 | 不publish、不等待未提交任务；Free已construct实例，Reset已进入Prepare的实例并retire它的edges，Release已acquire reservation；原错误返回 |
| Ready | publish epoch并使root可runnable；只推进实际需要的lazy arms/producer |
| Running -> NeedProgram | parent保留lease/PC但不占child worker/持gate；driver推进callee |
| EOF/decision | 锁定归约；request本generation pipeline stop，**不取消statement query ctx** |
| error/external cancel | 先Mark/freeze substantive或query deadline；seal新demand、abort所有owned siblings/edges；取消owned reader pipeline ctx |
| Draining | terminal独立于data queue；推进started task cleanup，收齐frozen outcomes/归还borrow；never-started责任由controller完成，无假wg wait |
| Quiesced | 所有本地continuations terminal，所有leaf I/O已返回，borrow归还；Reset/Free实例、Close自己的reader/domain，释放slots与ticket records |
| 下一generation | 只能从Quiesced后fresh实例发布新epoch；late event检查epoch并退还其payload，不能设置新代EOF/结果 |

Reset发terminal，Call/Step不直接冒充Reset发EOF；成功typed End与paired cleanup后reclaim，Error/Abort/delivery failure即时Abort spool，沿既有typed协议，不调用legacy CloseWithTimeout。

Finish保留read/cardinality/budget/retry具体moerr；归因于本地stop且query ctx仍live的cancel回声可规范化。独立实质错误叶不能一并消去；外部query deadline保持可见。使用现有结果仲裁，而非channel到达顺序或任意errors.Join。scalar不能在一行后当EOF；IN/ANY决定TRUE、ALL决定FALSE、EXISTS第一行均只请求stop；所有已启动producer仍须完成/收齐错误。未启动lazy arm不启动、不制造它的SQL错误。

`CloseOwner`先Finish活动ticket，再释放owneddomain/control容量及template借用引用；prepared/cache template的最终Free仍属于plan/cache owner。外部取消结束整个attempt，不能在同一个已cancelled attempt启动“第二代”；下一execute/retry使用新AttemptId和snapshot fence。

## 6. 远端、copy、prepare、View 的具体合同

### 6.1 首版 placement 与 closed transport projection

通用program及需求stage本地；**program内部不调用RemoteRun/MergeRun/新远端query**。普通program扫描用local distengine Reader读相同远端存储snapshot；inner O(E·I)重复读取/网络是真实代价，不伪称参数往返。这是物理选择，不拒绝任何SQL或要求用户单CN。

main可保留独立普通remote输入。若所谓“program普通输入remote”必须依赖program slots/occurrence或产生跨owner producer，编译将必要计算闭包relocate到协调CN，远端存储读不变；不临时给old CN开一个独立child cap。现有已证明fast path可继续真正multi-CN计算，不强制所有相关SQL串行。

`ProjectTransport(Q, fragment_roots)`生成新的普通Query：

1. 只收集闭合Children与同fragment SourceStep依赖；重新编号Nodes/Steps、SourceStep、tags/column refs并保留普通operator必须的grouping/type/function/snapshot/txn元数据。
2. 不带CorrelationSpec/regions/programs/stages，不含DependentExpr/SlotExpr/Corr/Sub、cross-region SourceStep或未特化的环境引用。prepared runtime值只能按已有typed协议完成绑定，不能把slot伪装ParamRef。
3. `Scope.Plan`、grouping attach、`generatePipeline.p.Qry`都使用该projected Plan，不再任意附full original Plan。沿全部PreScope、pipeline instructions、Qry/expression树验证，无新字段才能encode。
4. 原先type/function/protocol gates继续检查projection后的真实需求。无法投影且保留grouping等必要合同的fragment整体本地，不删除Plan蒙混过关。
5. main的full Query仍供coordinator授权/依赖/诊断，不能将transport剪枝误当permission剪枝。

旧CN收到的正是旧schema普通Plan/InstructionList/batch与现有protocol metadata；没有新occurrence、epoch、slot或新opcode。新IR local roundtrip必须有named `GeneralCorrelationV1` capability；实现时映射正式MORPC版本/feature contract并更新生成代码，不把107当已具备新能力。

### 6.2 全部消费方的同一遍历/映射

规范入口 `WalkQueryGraph` 访问Steps、program roots、SourceStep、expr-owned operands/window/frame、slots/stages/sharing proof。permission与dependency看全图；execution编译带region filter；prune/explain/copy用全图。`VisitPlan`/`VisitExprTree`/structural hash/reflect owner visitor接此合同，不能一个只走Children另一个省oneof。

`DeepCopyQuery/Expr`深拷贝全部描述和ID maps；Expr equality/hash含occurrence/stage/single-eval身份，不因fallback proto hash一致合并不等价occurrences。fresh mutableowner不在plan/cache。prepare/rebind在完整Query上重新验证表定义、权限、参数类型/metadata、capability、stage布局；需要特化则compile模板一次，执行每Eval不重新compile。

View沿既有SQL/parser mode/required-protocol字段保存最低coordinator capability，不新增catalog格式；expand/rebind重新绑定所有program依赖，不能只读取旧View wrapper的外表。旧binary/旧coordinator不支持新能力时给明确required capability错误或路由到capable coordinator，不忽略要求再走旧NYI。普通View/SQL无新requirement不受影响；downgrade不会产生新on-disk数据格式，但不能承诺旧binary执行新feature View。

## 7. 入口接受矩阵与算法（不是等待实施才选型）

### 7.1 普通 JOIN、ASOF 与 keyed DEDUP

普通JOIN保留HashJoin/LoopJoin匹配owner。安全普通keys可限制candidates；否则合法loop逐pair，不物化整个product。ON在matched update之前运行，FALSE/NULL不匹配、error abort；NULL-extension不再Eval ON。RIGHT交换children只remapaccessors，不交换语义binding。FULL两侧matched、SEMI/ANTI/SINGLE/MARK、spill condition/cursor属于现有ctr；输出恢复沿同pair/frame，不重开ticket。

| 入口/输入 | 接受与stage/算法 | 独立对照/拒绝依据 |
|---|---|---|
| ASOF equality args 含合法scalar/ancestor slot | 接受；依slot的join-side dependency取left/right side。无totality/demand证明不放hash build；采用local candidate engine：对backward temporal候选求完整ON，维护每left identity最大合格right time的winner |
| ASOF strict/inclusive、tolerance、NULL | 保持`>`/`>=`与date_sub comparison；NULL时间/UNKNOWN不合格；无winner的ASOF_LEFT一次NULL-extension，INNER不输出。相同time按该物理输入的最先encounter ordinal保留，不因resume重选；不伪造无完整排序时SQL保证的跨执行唯一row |
| ASOF temporal expression/反向/缺key或多个时间不等式 | 不开放；`configureAsofJoin`已有同形非关联parser UT与明确parse/type合同：temporal必须同类型DATE/DATETIME/TIMESTAMP/TIME列、恰一个backward inequality、至少一个cross-input equality |
| ASOF tolerance | 保留constant interval合同；无当前join行依赖、满足既有statement-constant/稳定性合同的ancestor参数在该invocation中可作为constant，按setup occurrence计算一次；row/pair或volatile依赖仍按同形nonconstant控制拒绝 |
| public DEDUP equality keys + ancestor/one-sided scalar | 接受；它是keyed duplicate/action关系，不是普通INNER。key args在各自input-key stage计算一次、保持NULL/key equality/duplicate policy，然后交给现有dedup action/finalize kernel。无值域共享，不将action表达式提前当key |
| DEDUP key形状 | 延续`constructDedupJoin/extraJoinConditions`的typed-plan keyed合同：每等值arg只依一个输入及ancestor参数。`l.a>r.b`非等值控制由extraJoinConditions明确归residual，现有DEDUP constructor不能消费它；该独立keyed前提不等同于新加Sub/Corr拒绝。不能仅凭HasColExpr首个relpos或一次compiler panic宣称mixed-side表达式非法：必须完整semantic dependency校验，并以同形非关联控制证明typed-key合同。不能证明者留作该语言合同review反例，禁止在实现中悄悄排除。普通JOIN的同形非等值谓词仍接受general pair路径 |
| DEDUP FAIL/IGNORE/UPDATE/keep-last/self-delete | 按原action kernel、pessimistic/optimistic错误归属、NULL跳过、目标row-id、自更新deleted marker、REPLACE source ordinal保真。不把public DEDUP默认为DISTINCT或SEMI；无DedupJoinCtx不得虚构内部UPDATE action |

ASOF不以first TRUE就返回：需要搜索所有仍可能改善predecessor的候选；只有safe时间排序/索引证明可停止。writer更新best只在合格且time更大时，equal time不覆盖已有ordinal；tolerance对best的date_sub残余比较沿当前`findAsofBest`合同，不能把失败candidate当outer match。dependent equality在候选stage逐pair求值；只有已证明one-sided total/stable且相同需求的key才允许input-stage计算。candidate/winner只保留当前left的best payload与cursor，已有输入/spill另计，不保留L×R bitmap。

DEDUP的key stage是它本身已有的实际关系阶段，不套普通ON的pair identity。两侧dependency必须从SlotSpec的semantic owners分析，不能让旧getJoinSide忽略新oneof。key diagnostics的激活仍遵守right-preservation/DeferredJoinDiagnostic合同；build自重复、probe conflict、ordered incoming action、final output各有不同identity。action运行到第i个赋值时让出，恢复从i继续；只有本action最终版本交给原write/index/FK/affected-row逻辑。

### 7.2 窗口 frame

普通frame grammar的直接offset仍只允许literal/marker/interval；不改parser来新增任意numeric Subquery。**INTERVAL expression unit**内scalar、祖先slot与当前输入依赖走general表达式，不把现有bind-time literal假设当禁令。

- `BoundSpec(quantity_expr, literal_unit, dependency_class)`保留到runtime，授权扫描可见；任何mo_ctl/fault_inject仍绑定期拒绝，所有账户一样。
- 无本window输入引用：在window **setup stage** 的一个identity上求一次bound，首次真正需frame时触发；不同program invocation不共享，即使ancestor值相同。volatile/subquery仍只在该setup occurrence求值一次，不在bind未授权时执行。
- 有本window输入引用：在对应window output-row identity求bound，使用排序/分组后的实际input accessor；悬挂与输出恢复memo同一row。不能未经独立契约以“constant frame”拒绝interval表达式。
- bound native结果交同一个normalize kernel，与literal/现有prepared bound共享cast/unit/decimal/timestamp规则。不是把slots输成TEXT；合法string interval自身的normalize是SQL转换。当前interval NULL归一为MaxInt64、负值使用既有frame error；numeric prepared bounds按其既有NULL/非整值错误合同，不能把两种helper混为一个规则。
- RANGE N仍要求单个numeric/temporal ORDER；frame若依赖本window的输出则循环，`rejectWindowResultDependency`既有非关联控制仍拒绝；GROUPS保持现有独立不支持合同。prepared interval markers的独立禁令见`TestPreparedWindowIntervalFrameMarkersAreUnsupported`/nested控制，不借correlation任务偷偷开放它；Subquery不再被误识别成marker。
- window无需要frame的输出时没有bound需求；空input不为bound启动program。PARTITION/ORDER的子查询在实际输入/key stage，不用bound/setup身份替代。

### 7.3 DML、CTE、集合

| 入口 | slots、stage、需求与写入边界 |
|---|---|
| sequential UPDATE | 第i rhs看到第i-1 current image；OLD sidecar独立保留。每赋值一个stage/version，结果cast/约束后推进；不能把所有rhs绑OLD |
| ODKU | existing/OLD/current与incoming VALUES两套独立accessors。只在conflict action触发；rhs序列更新current image，VALUES始终incoming，重复incoming/source ordinal保持原action次序；final action metadata/FK/affected-row由原owner计算 |
| INSERT ALL/FIRST WHEN | statement CTE+source-output alias，不暴露source-private CTE。ALL按各WHEN决定route；FIRST用unclaimed mask推进，后续WHEN不提前执行；NULL WHEN不匹配 |
| conditional INTO VALUES/ELSE | 只对selected route rows求表达式；未选branch不Activate其program。每target occurrence/row有自己的身份，不按相同source值共享 |
| REPLACE/VALUES/default | 一个compact VALUES source + row ordinal，不为每含Sub的row展开UNION分支；删除32-row表示guard。default依赖按现有可见列规则形成row-local DAG，前驱materialize后rhs捕获；非法列引用/真实default cycle沿非关联合同拒绝，不把“有Sub+default”一概拒绝 |
| generated/functional index columns | 在最终new row image形成后由现有投影/写pipeline维护，program不自行写隐藏索引；保持最新main的functional-index默认/更新依赖 |
| DELETE/多目标UPDATE/INSERT SELECT | source计算stage共享一般机制；程序只读原snapshot，不推进workspace/statement offset、不开txn/SQL session；写入唯一归原DML owner |
| declaration CTE/multiple refs | 参数按声明owner冻结，实例按invocation。初版只采用既有deterministic/total/drain的sharing proof；其他按现有inline/独立实例语义，不跨E缓存 |
| recursive CTE | 同env的seed/member/iteration/source由region驱动；当前r.n作为值可传Sub。`FROM r nested`非法重复读recursive source仍拒绝；iteration completion先于源换代 |
| set operations | arms有自己的bind/stage，env内ALL bag/DISTINCT/INTERSECT/MINUS、order/limit保持，不合并跨env相等payload |
| GROUP BY/aggregate | input stage的scalar key、postgroup的rhs/HAVING区分；global empty aggregate每env产生empty值，grouped empty无group；不保留correlation-only GROUP BY guard |
| RETURNING | 同形非关联Sub已被`returning_test.go`独立拒绝，不在此任务重新定义整个RETURNING语言 |

共享statement CTE producer必须独立runnable；其storage采用可spill的materialized source，不靠暂停parent消费队列给child credit。child不引用parent live output。producer/source有一个有效Close owner，多reader显式lease；unused producer不因catalog出现而Begin。recursive source的fixed-point由同ctr/iteration状态驱动，不新设第二个递归引擎。

## 8. 实际准入、容量与Go stack政策

### 8.1 唯一ledger和默认值

执行借用该statement attempt已有`ExecutionResourceGeneration`与allocation registry；**Submit/OpenOwner不OpenGeneration**。Q为现有ResolveExecutionMemoryCeiling.QueryCap，CN为其CNMemoryCap：E=min(有效host/cgroup/global MPool上限)，requestedReserve=max(4 GiB,E/5,FileCacheHint)，按现有小CN minimumExecutionCap clamp后CN=E-reserve，Q=min(CN,正的Process.Limitation.Size)。live runtime safety=max(1 GiB,E/20)且不超过reserve，是reserve的一部分，不重复扣减。不增加猜测的64 MiB/node或每child cap。spill disk/FD沿现有generation和CN ledger。

初版 capture window=1；没有额外task数、depth、arms上限。task/continuation records只按**已构造且实际需要的topology**分配，实际byte申请进同ledger；caller等待不消耗一个child worker。N是实际caller frame logical rows，不是1：aligned结果、owned operands、done/pending bitmap、input、window/group/匹配state都计量。

planning只在需新IR时开启一个PlanLease，cap同该Process的Q/CN政策，不按每region开放。它拥有新registry/候选与新增node/expr的真实容量；一次parse/bind只有一份lease。失败候选立即退还retained bytes，但cumulative构造/访问次数保留诊断。首版每region只做一次只读准入与一次已证明lowering，不新增参数域候选枚举/search framework。候选真实内存admission不成功就释放它并选择compact general，不能把可选优化失败变query资源错。cumulative visits/node/expr记录只作诊断，非hard generation-count/visit/depth拒绝；所有循环检查原query cancellation。

PlanLease是planning allocation generation的单一拥有引用；构造/优化时仅这一份cap，发布时将retained physical账户随immutable template移交plan/cache owner（不close仍持bytes的generation），临时reservation当场退还。普通非cache计划在statement结束释放；copy需新actual容量并单独登记，不复用悬挂原账户。immutable prepared/cache template的PlanLease随最终plan引用释放，不挂在已结束execute的frame上；execute只借用template，模板payload在CN只记一次，新增borrower metadata/复制按execute Q记。一次execute的所有root/child共享一份execution cap，不把cache PlanLease变成每环境的额外执行额度。此契约需接plan/cache eviction，不能close账本但留未收费template。

### 8.2 按什么实际字节记账

| storage | 申请/计量/释放 |
|---|---|
| pointer-free IR/PC/cursor/ID/bitmaps/ready records | checked element-size×actual capacity，用`mpool.MakeSliceAccounted`/physical account；overflow或真实allocator/capacity拒绝才报资源错误 |
| native vectors/value area/varlen captures/results | 原allocation account，真实capacity/value bytes；borrow payload不双计，copy另计；child结果转parent时转own responsibility |
| GC-visible refs/objects、proto materialization/copy、reader metadata scratch | 禁止未记账Go map累积；固定typed registry，实际struct/slice capacity在分配/增长前ReserveTransientMemory；增长要计old+new临时峰值，Go对象不得藏off-heap指针让GC失去root |
| spool/inputs/sort/window/group/join | 原算子account/spill ownership；本机制不因B=1删掉它们的保留成本 |
| 新task Go stacks | 不创建program task goroutines，related nesting在explicit frame arena；Go/native leaf调用仍使用既有consumer stack/runtime，沿**现有CN runtime headroom**，不伪报为ReserveTransient已追踪goroutine stack |

Managed ledger计实际owner申请容量，不等于RSS/Go allocator全部页/fragmentation。Go stack/runtime、allocator overhead和共享服务metadata明确属于现有CN runtime reserve/physical headroom政策，不能将它们冒记为query精确payload。测量记录实际StackInuse/RSS/native peak；要求随相关嵌套深度新增的是charged frame bytes，非线性增长的stack/task或未收费heap是实现失败。普通remote main workers/services仍按既有CN政策，无per-E新增helper预算豁免。

峰值是所有**active owners**之和：captures + N×(result/operand/memo) + task/expr records + 本代operator状态 + retained shared input/producer/spill decoder；不留历史E map、parallelGenerations或completion列表。allocator拒绝记录component/requested/used/cap；不因估算SQL复杂、运行慢或有很多相同参数而报资源错。唯一合法timeout是已有用户query deadline，不额外设“复杂关联超时”。

## 9. 固定验收门槛与old-success corpus

以下是r4请求review的**具体准入标准**，不是已经通过或留待实施时决定的数字。

| corpus | 复用数据/计划合同与最低指标 |
|---|---|
| no-subquery与shallow/depth_two_old/scalar planning | 两个既有benchmark family；4个flatten no-subquery控制仍0 B/op/0 allocs/op，未引入registry；保留其他控制节点/expr/alloc不变，除列明必须纠正旧语义的案例 |
| old-success scalar/row/IN/EXISTS/aggregate | `row_scalar_subquery_test.go`、原subquery BVT的已成功最小fixture；保留typed tuple/cardinality/NULL/duplicate结果及一次operand，scan/startup不新增N+1 |
| ON ordinary/CTE/set/window | `outer_join`/subquery BVT、`cte_lazy_binding_test.go`、ASOF/window已有控制；同plan路径的runtime filter/scan/CTE共享和多CN布局保留，不额外全域product |
| DML/prepare | sequential update、ODKU action/affected-row、multi-insert/REPLACE与prepared integration现有fixture；count/source order/write atomicity/cache miss与锁/权限依赖保真 |
| multi-CN | 现有`pkg/tests/sqlintegration/multicn`与实际remote placement；主要control的bytes/scans/peak/spill固定；只本地执行不算multi-CN证据 |

固定small execution corpus（基线/候选共用，不为本doc运行）：`a(k,v)={(1,10),(1,20),(2,NULL),(NULL,30)}`，`b(k,v)={(1,100),(1,NULL),(3,300),(NULL,400)}`；已有integration/BVT fixture承载，性能批量复制用固定seed，无随机oracle。

| ID | 必测SQL形状/独立合同 |
|---|---|
| C0 | `SELECT k,v FROM a WHERE k IS NOT NULL ORDER BY k,v`：无Sub控制 |
| C1 | `SELECT a.k,(SELECT COUNT(*) FROM b WHERE b.k=a.k) FROM a`：global空aggregate与duplicate outer |
| C2/C3 | `a.k IN (SELECT k FROM b)` / `EXISTS(SELECT 1 FROM b WHERE b.k=a.k)`：IN/EXISTS不duplicate witness |
| C4/C5 | `NOT EXISTS(SELECT 1 FROM b WHERE b.k=a.k)` / `(a.k,a.v) IN (SELECT k,v FROM b)`：empty/NULL/row三值 |
| C6 | `SELECT a.k,b.v FROM a LEFT JOIN b ON a.k=b.k`：pair bag与NULL-extension |
| C7 | `WITH q AS(SELECT k FROM b WHERE k IS NOT NULL) SELECT a.k FROM a WHERE EXISTS(SELECT 1 FROM q WHERE q.k=a.k)`：旧CTE成功路径 |
| C8/C9 | `SUM(COALESCE(v,0)) OVER(PARTITION BY k ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)` / `SELECT k FROM a UNION ALL SELECT k FROM b`：旧window/set控制 |
| C10 | prepare/execute C1外层加`WHERE a.k > ?`，交替native整数/NULL；不按参数值cache result |
| C11/C12 | 主键u两行：sequential UPDATE先改x后用x与scalar aggregate算y；ODKU `x=VALUES(x)+x,y=x+1`；每次rollback/reset，核对最终行与affected action而不计fixture启动进statement latency |

C0–C10和旧multi-CN同形SQL必须在单CN与2CN各测，记录plan/实际placement、scans/bytes/peak；C11/C12另沿已存在sequential/ODKU fixtures固定schema/动作。旧深层成功aggregate、ASOF与REPLACE/multi-insert复用其既有命名fixture，实际SQL/hash/行数/session与样本一起记录，不能在candidate后改baseline query。若预检发现上述某cell并非旧成功，将它列new-feature而非拿NYI耗时比较；同一合同的既有成功control仍必须保留，不能删除整个维度。

无新语义或纠错需求的old-success路径：planner nodes/expr/allocs、scan/remote requests/net bytes不增加；无子查询热路径零新allocation。planner和execution分别做同head依赖、同Go/JSONbackend/native、索引stats/session/拓扑数据的baseline/candidate成对warm轮转。

**latency相对回归95% paired区间上界≤5%**；每cell至少10对独立sample，测量区间太宽不PASS，继续校准测量而非改阈值；小查询批量计per-statement。新增retained native/accounted bytes仅允许实际必需frame/capture/result/ctr（第8节公式），既有fastpath为0；RSS/StackInuse、observer peak与公式解释不符不得靠平均值掩盖。不能自批豁免slow控制，超门槛重新设计或提交独立有证据的tradeoff供批准。

旧成功但需求/错误语义原本错误的SQL单独列纠错cell，必须有独立oracle和成本来源，不拿旧错误输出当等价baseline。新general类与语义相同逐环境参考比较；集合高效类与已证明的SEMI参考比较。E/D/I各1x/4x，wide/NULL/duplicate、无key pair、nested active owners，计Σinner work、真实重复storage transfer、spill/FD。原NYI耗时不是execution baseline。

本PR不新增runnable代码，不重跑无关Go suites。附录dce/83d的7×3 planner样本仅历史作者证据，c2c新main新增functional-index/DML与protocol changes；不能称新head/candidate/统计门槛通过。

## 10. 一个端到端worked trace

语义fixture（设计oracle，尚未执行）：

```sql
SELECT o.k, CASE WHEN o.want THEN
  (WITH RECURSIVE r(n) AS
     (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 2)
   SELECT COUNT(*) FROM r
   WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k
     AND EXISTS (SELECT 1 WHERE r.n = o.k)))
  ELSE 0 END AS v
FROM o;
```

`o={(1,TRUE),(1,FALSE),(NULL,TRUE)}`，`i={1,1,NULL}`。正确bag为`{(1,1),(1,0),(NULL,0)}`。两个i witnesses不能复制r或o；递归FROM r只在CTE member出现一次，子程序传r.n值，不再次FROM r。

Q有main region0/output stepS0；P1拥有scalar aggregate与CTEseed/member/source stepsS1/S2/S3，输入o.k；P2在r filter中输入r.n+转发o.k；P3是更深scalar predicate输入两者。stepOwners=[0,P1,P1,P1]；更多source roots用同规则append，不按depth建parent map。

### 10.1 未选分支与正常partial masks

1. 授权/锁看o/i和全部递归/程序表达式；compile P1/P2/P3 immutable templates，无reader/materialized Begin/receiver。main普通o scan可以是旧CN闭合remote fragment；其Qry只含o扫描/普通投影和旧字段，coordinator CASE/程序IR不跨wire。
2. local main stage获取3行Frame F。第一次mask={row0}：row1 FALSE不Submit，P1的其他环境未创建。full FALSE mask甚至不会构造domain；没有P1 receiver需要发假EOF。
3. row0先拥有typed o.k=1，私有Activate P1：admit/control记录、fresh instances、owned edges/CTEsource、Prepare逐项登记，最后publishG1。原statement Base/budget/snapshot借用，domainboard/iterator独立。
4. driver推进seed和member r；r row1滤波stage Submit P2，P2保留i输入cursor，P3拿已捕获r.n+o.k。parent PC悬挂，无worker/sequence lock被占；driver直接推进P3。P3 TRUE，P2的EXISTS决定但先Finish started i producer/必要诊断，归约一个TRUE。r row2 FALSE，P1 COUNT得1并真实EOF。
5. P1 scalar tuple先写F的owned provisional slot，再收齐typed/frozen终态、Reset/Free已激活实例、Close自己的reader/source/domain、释放captures；成功后F[row0]标done=1。shared Base/session/prepare/template不Free。
6. 第二次mask={row0,row2}：row0命中同frame memo不启动任何P；row2开始fresh G2，native NULL slots不转TEXT。i.k=NULL不为TRUE，CTECOUNT=0；G2同样Finish后commit F[row2]。FALSE row1由CASE取0。最后ReleaseFrame F退还result/memo/input lease。

i的物理数据来自远端存储时，P2用local distengine reader读该snapshot；不让旧CN启动带P3的计算scope或得到整个Q。若planner先选remote i computation，CompileRegion将其必要闭包relocate本地；只main的独立o输入按ProjectTransport发送。此限制不把该SQL或多CN环境判不支持，代价计入重复remote storage bytes。

### 10.2 admission/init failure、stop、external cancel与第二代

| 故障/时点 | acquire/start/quiesce/release trace |
|---|---|
| capture reservation拒绝 | F尚无done，P1未publish，退已有lhs/capture；没有reader/receiver/task可wait；原budget错误返回 |
| Prepare第k项失败 | constructed实例的Free即时责任已登记；Prepare-entered项按failed Reset，未进入Prepare项只Free；retire已登记edges/reader/source，冻结原cause；禁止启动后续或将Row0标成功 |
| P3 FALSE/EXISTS local decision stop | pipeline stop只属于该invocation；collect started producer outcome，未启动lazy arm保持未启动。query ctx活着，P2/P1可继续，G1 Quiesced后G2可开始 |
| 已开始producer冻结读错后TRUE | 弃provisional TRUE/tuple，原读错不能被stop/cancel归因抹掉；所有siblings先abort后收齐，错误回root |
| P3 suspended时外部query cancel | seal整个attempt demand；ctx独立取消I/O，parent/child通过driver completion而非等待同池worker退出；所有active域/edges/tickets排空Reset/Free，F不再发布，root返回deadline/cancel |
| 外部cancel后的下一execute | 前attempt已Quiesced/Release；新AttemptId/根ctx/fence，借同immutable prepared template，新Frame、新domain、新operators/receivers。旧event mismatch只能释放payload，不能关闭新G或清shared params |

registry里的未激活program只有template metadata，没有动态edge；已claim但没start的edge由activation cleanup发/记录terminal并退payload。shared CTE需要时采用独立driver producer+可spillsource，不靠paused parent的输出恢复，因此不存在parent->child->parentcredit环。所有正常/error/panic/部分启动路径都调用同一ownedcleanup表。

## 11. 实现/验证变更闭包与门禁

| 闭包 | 必須一次完成的反向consumer与证据 |
|---|---|
| schema/binder/slots | proto生成、semantic owners、全部clauses/DML、WalkQueryGraph、原子publish/slot closure/cycle/非法同层UT；proto roundtrip与owner-empty兼容 |
| step/main/program compiler | SourceStep remap/main_output/receiver/sharing/recursive/transport projection；unselected CASE含失败/递归producer零启动，main remote Qry无IR的真实检查 |
| region VM/expr/frame | 每个operator class的Step adapter、shared memo build/reset、alignment、Native invocation单次；partial mask/resume、fresh non-TP generations、empty mask、init/cancel/late event与资源余额UT/race |
| JOIN/frame/DML kernels | actual input-key/candidate/winner/action/window-row identity、spill/input recovery；第7节每个accept/reject的同形非关联控制与独立public结果oracle |
| lifecycle/base/budget/cache | 每个direct Base consumer、reader/spool/source、default/generation accounts、template refs/eviction、prepare/View权限fence；allocation/FD/spill rollback、Q1–Q3、真实dependent endpoints |
| old/new执行/performance | 第9节明确controls与成对阈值、multi-CN实际placement/bytes/peak，不以planner或纯local证明runtime/mixed-version |

原始`test/distributed/cases/subquery/subquery-with-in.sql:1031–1045`的skip必须移除，result在**同目录**。原fixture key7/parent10正确为空；另加parent `(7,'2002-02-22')`同形二列IN应只返回一个7，两个cc witness不duplicate outer。补NULL/不同字段/未命中/outer duplicate bag，不只检空集。

结果由mo-tester生成+独立oracle审查+normal compare两次、teardown健康；SQL注释不新增issue号。新行为focused UT/owning packages、触发的race/typed terminal/多CN/fault/性能证据与75% changed-code coverage均是**实现交付硬门禁**，未在本设计PR执行。hang/zero selection/pending/存活进程不是PASS。

### 11.1 当前源码锚点（结构核验，不是运行证据）

- `pkg/sql/compile/compile.go`: Compile.Reset、prePipelineInitializer、runPipelineAttempt、compileQuery；`scope.go`: MergeRun、resetForReuse、collectMergeRunResults；`pkg/vm/pipeline/pipeline.go`: Prepare/Exec loop；`pkg/vm/process/process2.go`: shared-Base Free。
- `pkg/sql/colexec/evalExpression.go`: ExpressionExecutor只读/下一Eval有效期、memoRoot逐Eval reset；`pkg/sql/compile/remoterun.go`: generatePipeline的Qry；`grouping_transport_protocol.go`: wholePlan attach。
- `pkg/sql/plan/asof_join.go`、`colexec/hashjoin/join.go`: configureAsofJoin/findAsofBest；`compile/operator.go`: constructDedupJoin，`colexec/dedupjoin/join.go`: probe/action/finalize；`plan/utils.go`: join-side分类当前不认识新slot。
- `pkg/sql/plan/window_binder.go`: makeWindowFrameConstValue/resetWindowIntervalExpr/setWindowIntervalValue；`window_binder_test.go`: prepared interval controls；`ondup_update_binder.go`、`bind_multi_insert.go`、`bind_insert.go`、`bind_replace.go`: 当前row-image/route/default与旧表示guards。
- `pkg/vm/process/execution_resource_budget.go`: ResolveExecutionMemoryCeiling/GetExecutionResourceBudget/ReserveTransientMemory；`pkg/common/mpool/mpool.go`: MakeSliceAccounted。off-heap slice仅pointer-free，不能套到含Go refs的operator structs。

## 12. 决策日志与批准范围

| r3 finding | r4 选定决策与取舍 |
|---|---|
| P1 D1/D2 | 同lane trampoline/Step、fresh mutable state、现有kernel/Reset/Free/outcome权威；frame-owned promoted memo替代含dependent root的per-Eval reset。拒绝nested ants、专属Go taskslot stacks与TP Reset模拟AP重用；代价是完整adapter闭包，实施前不可删成few-op fallback |
| P1 D5 | 全局stable step catalog+唯一owner+明确main output、CompileRegion与atomic remap；old-CN仅closed main transport、新program local。拒绝whole Plan旁路和“移除Qry但丢grouping” |
| P2 D3/D6 | ordinary candidate matching、ASOF predecessor winner、DEDUP原key/action阶段、window setup或row bound、DML row-image/route/default DAG；独立拒绝以第7节noncorrelated控制/现有key/type/递归契约为依据，不按correlation形状 |
| P2 D4 | 无per-child cap/Go stacks，actual capacities入原query/CN账本、existing runtime reserve明确非query RSS；N与active-owner和；5% paired95%标准、明确old-success corpus。拒绝臆测node/depth/task数预算和baseline-only性能结论 |

实现仍G-FEATURE-DESIGN blocked：等待对**r4精确修订**的技术review，不请求把附录r3的未知项自动批准，不自签架构正确或benchmark PASS。若reviewer发现具体反例，修合同与受影响closure后再review；不能留给coding时另选task model/transport/entry语义或容量策略。

本次交付差异只含现行设计与历史附录；没有production/test/proto/native/config变更。R0文档发布检查与整个R3 feature设计批准是两种判断。issue保持open，最终完整实现及上述证据通过才可宣称#7559完成。
