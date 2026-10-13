# PR #30012：4.2 差量设计、批准修订与实现对照

## 1. 审阅入口与时间顺序

- 实现 PR：[#30012](https://github.com/matrixorigin/matrixone/pull/30012)，目标 `4.2-dev`；关联 [#23158](https://github.com/matrixorigin/matrixone/issues/23158) 的 SQL1，不覆盖 SQL2/SQL3。
- **事前设计原文**：[4.2-dev 重新实现方案 v1](CLAUDE_20261012-vector-query-4.2-reimplementation-v1.md)。这是本次独立评估，不是 #29493 主线设计的批准复用。
- **对应批准记录**：[本次任务的 v1 批准记录](CLAUDE_20261012-vector-query-4.2-approval.md)。记录了先展示设计、用户回复 `go ahead`、再实施的顺序。
- **获批设计的精确内容 SHA-256**：`03c8b3878093fe4e6336cef1becc10e5c875870d00efb8fcafd8197ceb18aedd`。
- 设计初始基线：`276938065e831d66e01027a2290f18780563571b`；本对照核验的实现：`1b03471c649d352a8940095d60f353be7ffcb43d`；其目标基线为 `7ca3113da93b39b820236b22d31b73a1c81c3ba0`。

原文中的“待批准”“尚未创建 PR”“没有运行测试”是**提交用户审阅时的历史状态**，为保持获批内容 hash 不变不回写它们；批准状态见对应记录，实施后的验证见本文及 PR 正文。原设计覆盖两项独立功能，其中 §4.4 的普通等值 JOIN 属于 [#30013](https://github.com/matrixorigin/matrixone/pull/30013)，不是本 PR 的代码依赖。

本文是在实现后为公开审阅补充的映射，不是倒签的新批准，也不把实现细节自动视为另一次设计批准。批准记录是本地任务所有者的决定记录，不是 GitHub `APPROVED` review，也不替代维护者的实现审阅。若后续发现偏离 v1 的实质设计变更，应重新审阅受影响决定。

## 2. 相对主线实现的明确差量

本改动新增专用执行域、跨 planner/compile/colexec 归属边界和计划协议，独立触发设计门禁，不以非生成行数或拆分 PR 来规避。

| 决定 | 4.2 的约束与选定方案 | 事前设计 |
| --- | --- | --- |
| 不搬主线依赖链 | 复用既有 FUNCTION_SCAN、IVF/HNSW provider、materialized.Source；不引入完整 logical vector-index 架构 | §2、§4 方案比较 |
| 不扩展通用惰性调度 | 普通 MergeRun 假定所有 PreScopes 都启动；仅为固定三个分支建立局部执行域，并保留普通外层 merge 承载 JOIN 附加依赖 | §4.2、§4.3 |
| 不使用旧 AUTO 代替 NULL 选择 | 4.2 AUTO 可整语句重放；新形状只增加普通 LIMIT/PRE 支持，其他模式保留原行为 | §4.1 |
| 不把空/NULL provider 当成零条最终结果 | 非 NULL 走 ANN；空/NULL 走原关系分支；失败不是 fallback 条件 | §3、§4.2 |
| 不传输本地 source | 整查询固定入口协调 CN，禁止 selector 域远端派发，允许既有 TN/fileservice RPC | §5 |

不新增 CDC、索引磁盘格式、全局后台 worker、跨执行向量缓存或 JOIN 成本模型；不声称经过混合版本部署或性能/召回率专项验证。

## 3. 所有权和状态边界

```text
Compile 创建 Source(2)，每次 prePipelineInitializer 执行 Begin
  普通 merge 包装：上层消费者和 JOIN build 依赖不归 selector 私有域
    selector 私有域：P(provider)、A(ANN)、E(原关系)，恰好三个分支
      初始：merge 的活动输入范围为空
      LIMIT 0：不激活 P/A/E，直接进入清理
      非零需求：安装 P receiver -> 初始化/提交 P -> 消费到 EOF
        -> 等待 P 完成成功并复核取消 -> Source.Finish(nil)
        -> 安装且只启动 A 或 E -> 消费结果 -> EOF 后等待该分支完成
      错误/取消/提前结束：终止继续调用 -> 清理 -> join -> 关闭本代 source
```

- **Compile 是 source 总生命周期 owner**，持有 source/reader 映射；selector 负责写入与 Finish，Scope 执行域负责本代完成屏障，Compile 释放资源时还会 Close 兜底。
- 每个私有域固定三个完成槽和三个关闭通知通道；最多激活 provider 和一个结果分支，不创建无界任务队列或持续后台 worker。分支内部仍使用现有 Scope/pipeline。
- `claimed[i]` 表示该激活已被处理，**不等同于 goroutine 已启动**。初始化/提交失败也会同步清理并记录 claimed；WaitGroup 只跟踪实际提交工作，并在提交失败时回退计数。
- 启动声明由父 pipeline 单一调用链发出。错误/结束后该链不再启动分支；域的 defer 清除 start/wait 回调。每个 claimed 槽先写入结果再关闭 done，WaitGroup 另行保证 worker 退出。
- 向量只保留零或一行、一列，规模 O(D)，沿用物化内存/溢写预算。只有选中 reader 读取 source；额外输出批副本由该 Merge reader 持有到 Reset/Free，TVF 借用，普通 CTE 批转移规则不变。

## 4. 审查所要求的五条闭包

### 4.1 启动失败与部分初始化

`Scope.InitAllDataSource` 不穿透 `VectorQueryScope`。被选择后，先激活对应 receiver，再由 start 回调初始化该分支。锁与原有权限/事务上下文仍由普通语句入口处理，不因延迟 datasource 初始化而跳过。

- 已取消：不初始化；按失败激活清理。
- 初始化返回错误或 panic：在激活边界捕获并转为错误，自底向上 `cleanUnstartedVectorScope`，再发布该槽终态。
- `ants.Submit` 失败：平衡已增加的 WaitGroup 计数，执行同一同步清理/终态发布路径。
- 成功提交后：正常 Run/MergeRun 负责 pipeline 清理，再发布完成结果；域不二次使用“从未启动”路径清理它。

先收敛初始化失败，才能避免父 pipeline Cleanup 等待根本未启动的 producer。不能依靠父层 recover 后再排空，也不能启动未选分支来制造一个清理发送者。

### 4.2 EOF 后的完成与错误

provider 的 EOF 只是输入消费结束。selector 随后等待 `done[0]`，确认 Scope 完成结果成功，再封存 source 并选分支；结果分支的 EOF 也必须等待其完成槽。`finish` 在分支 Run/MergeRun 返回后发布结果，因此能携带 prepare/call/cleanup 阶段错误。

`ChildrenCall` 可能把取消表现为空结果，所以 provider EOF、完成等待之后、结果 EOF 均检查取消原因。非 NULL provider 如果在 EOF 后报错，不得启动 ANN；错误/取消不得被转换为空 provider 的 fallback。

### 4.3 取消、清理与 join

分支错误经现有 `cancelMergeSiblingsOnError` 打破 sibling 等待；父 ParallelRun 的 pipeline 清理负责取消子 context。完成等待同时监听 done 和 context，因此取消可先终止消费，但**不能绕过最终 join**。

域 defer 清除启动/等待回调，清理未 claimed 子树，然后 `wg.Wait()`，再关闭本代 source，最后合并各槽结果并优先保留原始错误。未启动子树从叶向父发布清理终态，父 merge 不应等待一个没有 producer 的输入。普通 merge 包装保持在域外，避免把上层 JOIN hash-build 误认为第四个可不启动的私有分支。

### 4.4 source 代际与 reader 批所有权

`Compile.prePipelineInitializer` 在运行 scope 前执行 `Source.Begin`；Begin 检查旧 owner 已释放并推进 generation。selector 的 Reset 清空选择、done、LIMIT executor 状态和生命周期回调，merge 的活动输入重新置空。

不能仅凭 reader 上一次执行的 `materializedReleased` 标志结束新一代：在连续 LIMIT 0 时，reader 从不 Prepare，旧标志不会刷新。因此执行域必须在所有已提交分支退出后调用 `Source.Close`，显式结束本代；下一次 Begin 不能与旧 worker/回调重叠。Compile 最终释放时的 Close 是同一 owner 的兜底，不是新的一套并发释放者。

`MaterializedSingleBatch` 使标量 reader 保有 Source.Next 返回的独立批副本，Reset/Free 清理它；Source 负责自己的存储，二者不是同一块批所有权。复制算子时保留该标记。该局部规则不扩大到普通 CTE reader。

### 4.5 协调 CN 与协议边界

`vectorQueryExecType` 将含 `VECTOR_QUERY_TOP` 的多 CN AP 查询限制为 `ExecTypeAP_ONECN`，保留本来就协调 CN 本地执行的 `ExecTypeTP`/`ExecTypeAP_ONECN`；现有 scheduler 的这两种本地路径都选用当前 CN。保留 TP 是实际 PREPARE 编译缓存 admission 的必要条件，修正见 §7。`holdAnyCannotRemoteOperator` 递归拒绝包含 selector 的 scope，RemoteRun 在远端发送前执行此检查。只有地址匹配本 CN 的 Remote scope 才转回本地 MergeRun。

新 source 指针和生命周期回调不序列化到远端 pipeline。逻辑计划使用节点 58/59、source 字段 93；VM opcode 67 保留 63–66 的编号空档。protobuf 由工具生成，配套 deepcopy/remap/EXPLAIN/compile 消费者同步更新。保留 TN/fileservice 网络访问不等于把 selector 派发到另一 CN。

## 5. 代码与验证对应

以下源码链接固定在本次文档核验的实现提交，而非可漂移的分支名：

- [执行域、激活/等待、未启动清理](https://github.com/matrixorigin/matrixone/blob/1b03471c649d352a8940095d60f353be7ffcb43d/pkg/sql/compile/vector_query.go#L112-L231)。
- [selector 状态、EOF/取消检查、Reset](https://github.com/matrixorigin/matrixone/blob/1b03471c649d352a8940095d60f353be7ffcb43d/pkg/sql/colexec/vectorquery/vector_query.go)。
- [Compile 的 Begin](https://github.com/matrixorigin/matrixone/blob/1b03471c649d352a8940095d60f353be7ffcb43d/pkg/sql/compile/compile.go#L616-L647)、[source 的 Close 契约](https://github.com/matrixorigin/matrixone/blob/1b03471c649d352a8940095d60f353be7ffcb43d/pkg/sql/internal/materialized/source.go#L453-L471)。
- [Merge 批与 reader 释放](https://github.com/matrixorigin/matrixone/blob/1b03471c649d352a8940095d60f353be7ffcb43d/pkg/sql/colexec/merge/types.go#L95-L123)、[远端拒绝](https://github.com/matrixorigin/matrixone/blob/1b03471c649d352a8940095d60f353be7ffcb43d/pkg/sql/compile/types.go#L242-L256)。

| 不变量 | 已提交的测试 |
| --- | --- |
| 真实 Scope 初始化 panic/prepare/call 错误、未选 datasource 零初始化、每分支一次 Reset/Free | `pkg/sql/compile/vector_query_test.go::TestVectorQueryScopeLifecycle` |
| 三分支/双 reader 归属、外层普通 merge、同物理计划连续三代零 LIMIT | `TestVectorQueryCompileOwnership` |
| EOF 后错误、非 NULL/NULL/空、LIMIT 0、基数证明违例 | `pkg/sql/colexec/vectorquery/vector_query_test.go::TestSelectorBranches` |
| 取消与启动失败、非 NULL/NULL 代际切换 | `TestSelectorCancellationAndStartFailure`、`TestSelectorGenerationReuse` |
| 活动 receiver 切换、批内存回落、复制标记 | `pkg/sql/colexec/merge/vector_query_test.go`、`TestVectorQueryReaderClone` |
| AP_ONECN、直接及嵌套 scope 的远端拒绝 | `TestVectorQueryExecutionBoundary` |
| 公开 SQL、IVF/HNSW、PREPARE、NULL/空、多行错误、分页 | `test/distributed/cases/vector/vector_scalar_query.sql` 及已核对的 `.result` |

对上述实现仍有效的既有证据（按相关输入未变复用，并非所有命令都在同一提交重复执行）：六个 owning 包正常测试及 vet、聚焦生命周期 race 连跑三次、compile/merge/vectorquery/materialized 四包完整 race 一次、新 BVT 两 CN 各 92/92；与 #30013 组合验证各 440/440。组合 merge 已撤销，不包含在任一独立 PR 中。覆盖率为修改生产行相交语句块 458/543（84.35%），不是包总覆盖率。

以上描述的是首次设计材料发布：`b75a76eda3` 只发布文档和更新 PR 链接，没有修改 Go/protobuf/测试/构建输入；当时逐文件确认运行输入不变后复用证据。后续代码修复和新验证如下，不把历史证据冒称为在新修订上重跑。

## 6. Review 修复：多个 selector 的 Fold 共享状态（2026-10-12/13）

ONECN 只约束执行位置，不会串行化本 CN 的 selector。两个 `InitAllDataSource(c)` 可同时通过 ReplaceFoldExpr 分配 Fold ID、追加 `Compile.filterExprExes`；正在运行的 scope 同时通过 `handleRuntimeFilters` 求值该表，远端 scope 还会注册 Fold。修复前的回归已复现读写/写写 race，并发现应为 **11** 的 Fold 值读成 **23**。

修正归属遗漏：由 Compile 的统一互斥边界保护注册、ID 发布、求值及 executor 可变状态，覆盖 Reset/Free。所有生产入口统一使用 [Fold 封装](../../pkg/sql/compile/fold_expr.go)，不再直接访问 registry；表达式仍归各 scope，executor 仍归 Compile。TypeName 改为指针接收者，避免复制 mutex。

- 不采用每个 selector 的独立锁，也不只锁两个启动入口，否则漏掉运行中的普通/远端消费者。
- 只锁 Fold 操作，不跨 datasource relation 获取、runtime-filter 消息等待、分支提交或 join；不提前初始化或求值未选分支。
- **Q1**：不新建第二套 executor 所有者；已注册项仍由 Compile Reset/Free，Free 清空 registry。
- **Q2**：error/panic 经 defer 释放锁。Reset/Free 仍必须在旧 scope 全部 join 后执行，新锁不允许执行中跨代重置。
- **Q3**：仅新增一个 Compile mutex，无新队列、worker 或缓存；本地 datasource 复用已注册 Fold ID。

验证基于 `b75a76eda3` 加本节所述修复，Go 1.26.4、仓库 CGo wrapper，完整退出状态均已核对：

| 契约 | 新验证 |
| --- | --- |
| 两个惰性初始化与既有本地读者/远端注册互不污染 | `TestVectorQueryConcurrentDataSourceFolds`：启动屏障，Fold ID/值、raw plan 不变、Reset 复用与 mpool 回落；修复前 race 失败，修复后 race 单次及精确 `-count=100` 通过 |
| 错误与 panic 解锁 | `TestCompileFoldLockTerminalPaths`：Replace/Eval 四种终态；race 单次及精确 `-count=100` 通过 |
| 多代 Reset、一次释放及清空后的重复清理 | `TestCompileFoldExecutorLifecycle` |
| 公共查询确有两个独立 selector | `TestScalarVectorJoinConsumers`：IVF/HNSW × 有/无不同过滤条件 |
| SQL 值不会串用，原路径兼容 | scalar BVT 各 **96/96**，原 IVF membership/mode/mode_advanced/retry 各 **275/275**；同一个双 CN 实例从两个入口分别运行，总计每入口 **371/371**。新增结果已人工核对再执行 comparison，IVF 为 `(b,b)`，HNSW 为 `(2,2)`，均匹配 FORCE |
| owning、静态与交付 | 完整 compile/plan normal + vet、完整 compile race、native build、Go 1.26.4 gofmt 和 diff check 通过；自有服务停止后日志无 mpool 泄漏或 pipeline cleanup timeout |

本修复增量语句块覆盖 **63/65（96.92%）**；以新 compile/plan profile 替换旧 profile 的对应文件，其他未变文件复用原证据，整个 PR 为 **521/608（85.69%）**。此前四包 race 和组合 JOIN 的 440/440 仍标明原验证修订，未冒称本轮重新执行组合 merge。设计 v1 原文及批准 hash 未变；本修正不新增 SQL 能力或改变 selector/source/CN 生命周期设计，也不等于 reviewer 已通过整体设计门禁。

## 7. 新增 review 修复：快照 admission、TP 缓存及统一派发（2026-10-13）

XuPeng-SH 已审阅冻结设计及批准记录并认可设计方向；其 `CHANGES_REQUESTED` 指出两条具体 P2 和一条非阻塞 P3。本节记录这些修复，不替代 reviewer 后续复审或冒称 GitHub APPROVED。开始前以 merge 合入最新 `4.2-dev` 的 `02b0689fa9354cb6e957a04d0769aff9ffcd1f9a`（fulltext 引号修复）。

### 快照的明确回退边界

Fold 锁没有、也不应顺带保护 Process 的 `CloneTxnOperator` check/create/store。采用 reviewer 明确提出的保守方案：`scalarVectorContext` 拒绝候选表上的 ScanSnapshot，`scalarVectorProviderSafe` 拒绝 provider 中的 ScanSnapshot。两处同时作用于预探测和最终改写，快照扫描不进入任何新 selector 的惰性分支；原普通计划保留快照，在执行 goroutine 激活前初始化 datasource。混合语句中的无快照区域仍可使用 selector。

不宣称通用的 CloneTxnOperator 已变为并发安全，也不引入新事务 owner/锁。公共 planner 的 IVF/HNSW × 双快照 provider/混合/快照候选表/无快照对照，验证可达 selector 数及 ScanSnapshot 保留。既有 Fold 并发 fixture 增加普通域两个同快照扫描先初始化、再启动两个非快照惰性扫描，验证真实 InitAllDataSource 只请求一个 clone、过滤值不污染。

### 真实 prepared compile 缓存

TP 本来就在协调 CN，不能为 locality 把它升级到 AP_ONECN，否则 `compileQuery` 拒绝 prepare-time compilation，frontend 不保留 Compile。现在只收敛 AP_MULTICN；TP/AP_ONECN 原分类不变，远端 selector 拒绝规则仍保留。

新增 `pkg/frontend/prepared_scalar_vector_test.go::TestPreparedScalarVectorCompileCache`：只 stub catalog/engine 元数据，通过公共 HNSW SQL planner、真实 `createPrepareStmt → createCompile → Compile.Compile/compileQuery` 建立缓存，再经生产 `initExecuteStmtParam` 三次取回**同一个非空 Compile**并 Reset。它不注入假 Compile、不强改 execType；实际 pipeline 执行与参数/NULL/零 LIMIT 代际由公开 BVT 补足。修复前同一测试在“缓存必须非空”断言失败。

### 一个派发策略 owner

普通 `applyIndicesForProject` 改用已有 `applyVectorIndexForSortContext`，移除重复的 index 收集、prepared 依赖记录、vector/context 筛选及 ApplyForSort 循环。未命中仍执行 exact-sort stabilization；共享 helper 继续区分 applied（不能只看 node ID），保留错误传播及改写前依赖捕获。现有 prepared IVF index-only dependency、FORCE/descending/project-child stable sort、HNSW 和插件相关 owning 测试通过。

### 验证与归属闭包

- 修复前证据：公共 snapshot admission 与 TP 分类断言失败；真实 PREPARE 缓存为空。Snapshot 的底层并发写反例另由 reviewer 在 `bc5193120a` 演示，本轮不把保守回退说成修好了该通用并发 API。
- Go 1.26.4、仓库 CGo wrapper：完整 plan/compile/frontend normal + vet、完整 compile/frontend race 通过；新/扩展两个生命周期测试各精确 `-race -count=100` 通过。单次 race 测试体约 0.01 秒，采用 30 秒预算、100 次上限。
- native build、gofmt、diff 检查通过。BVT 数据目录先完整保留到本地证据目录，再以新数据启动自有双 CN；两个入口各 scalar **103/103** + 原 IVF 四套 **275/275**，合计 **378/378**。新增双旧快照 `(2,2)`、混合当前/旧快照 `(3,2)` 均与 FORCE 一致，结果生成后人工核对并正常 comparison。服务已停止，检查至 shutdown 没有 mpool 泄漏、cleanup timeout 或 panic。
- 本轮修改语句块 **11/11（100%）**；以新 profile 替换相应文件、未变文件复用既有证据，整个 PR **528/611（86.42%）**。frontend 最后补齐测试外层 AST 释放后，聚焦/重复/full normal/full race 均重新通过；不影响生产 profile 或 native/BVT 输入。
- Q1：快照不增加 owner；缓存 Compile 仍归 PrepareStmt，AST/Compile 清理由唯一归属承担。Q2：不新增锁或等待，cache Reset 仍在旧执行退出后；不改变取消/join。Q3：不新增状态或积累，反而删除重复 dispatch 策略及临时 key slice。无协议、持久化、权限、租户或原 v1 内容/hash 变化。

## 8. Review 修复：候选向量 NULL 与排序/分页（2026-10-13）

非 NULL provider 不能证明候选列非空。IVF/HNSW 索引都不包含 NULL 向量候选，直接选 ANN 会丢掉升序中本应排在前面的行。修复前，在 `b930f9dc05` 的二进制上以一条 NULL、两条不同距离的向量复现：两个索引的 `LIMIT 1` 返回 **2**、`LIMIT 1 OFFSET 1` 返回 **1**，而同数据 FORCE/删除索引后的结果分别为 **0/2**。

在 guard 预探测与最终改写共享的 `scalarVectorContext` 中，按距离参数的扫描绑定和真实 ColPos 找到候选列，要求 **schema `Typ.NotNullable`**，否则不建立 selector、保留原关系路径。不以 provider 或表达式类型标注代替 schema 证明，不依赖参数顺序或固定物理列位置。即使 nullable 列在查询中有 `IS NOT NULL`，本轮仍保守回退；没有新建 NULL 补偿分支。符合条件的 NOT NULL 候选仍优化，provider 的 NULL/空分支语义不变。

- `TestScalarVectorCandidateNullability`：公共 SQL planner 验证 IVF/HNSW × nullable/NOT NULL、参数交换、真实索引列位置 2 与非空性相反的无关列，并覆盖 OFFSET、PRE、PREPARE、IS NOT NULL。修复前 nullable 断言失败，正例通过；修复后全部通过。
- 扩展非变异 guard 测试：即使表达式标注 NOT NULL，schema nullable 时必须拒绝；两种准入结果都不修改原计划。
- Go 1.26.4/CGo wrapper 的 scalar 聚焦测试、完整 planner normal + vet、实际 PREPARE Compile 缓存聚焦测试，以及 native build/gofmt/diff 检查通过。仅新增无状态 planner 准入检查，不修改并发/生命周期；§7 的 runtime race 证据按未变实现复用，不声称本轮重跑。
- 新数据双 CN，每入口 scalar **133/133** + 原 IVF 四套 **275/275**，共 **408/408**，无忽略或异常。新增普通/PRE/参数化 LIMIT、OFFSET 与 FORCE/无索引对照的结果已生成、人工检查，再正常 comparison；NULL 行为 **0**，OFFSET 行为 **2**。自有服务已停止，shutdown 日志无 mpool 泄漏、cleanup timeout 或 panic。
- 本修复生产语句块 **6/6（100%）**；以新 planner profile 替换对应文件、未变文件复用旧证据后，整个 PR 为 **534/617（86.55%）**。Q1–Q3 均未新增资源、等待或状态；原设计 v1 字节/hash、协议、持久化格式不变。本节不是新的设计批准或 GitHub APPROVED。
