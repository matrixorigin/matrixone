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

`vectorQueryExecType` 将含 `VECTOR_QUERY_TOP` 的查询设为 `ExecTypeAP_ONECN`；现有 scheduler 的该路径选用 `c.addr`。`holdAnyCannotRemoteOperator` 递归拒绝包含 selector 的 scope，RemoteRun 在远端发送前执行此检查。只有地址匹配本 CN 的 Remote scope 才转回本地 MergeRun。

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

本轮只发布文档和更新 PR 链接，不修改 Go/protobuf/测试/构建输入；逐文件确认这些输入与上面实现提交一致后复用既有证据，不把文档发布描述为新一轮运行或新的设计批准。
