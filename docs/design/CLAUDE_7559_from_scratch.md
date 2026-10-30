# Issue #7559 从头实现：已批准设计 r1

## 状态与基线

- 需求：<https://github.com/matrixorigin/matrixone/issues/7559>，支持多层关联子查询。
- 状态：r1 在实施前已获用户批准（2026-10-13，原文 “go ahead”）；S1–S7 实施与本机验收已完成。下文保留批准时的设计内容。
- 本次分支：`issue-7559-from-scratch-main`，通过 `gwt-add` 新建。
- 基线：`mo/main@175a645a85994f2c95992d3aa8d6dacb6d9e167f`；已 fetch 并在新 worktree 执行 `git pull --ff-only mo main`，结果 Already up to date。
- 当前 `go.mod`：Go 1.27.1；构建、测试、格式化使用该版本。
- 从最新主线重新实现，不继承旧 `issue-7559-implementation-main` 的未提交修改，不以旧实现或旧验证作为本次完成证据。
- 本文是本次独立的实施提案；已有 `docs/design/CLAUDE_multilevel_correlated_subquery.md` r4 仅作风险与接口参考。该文仍标注未批准，不能视作本次授权。
- 本文作为 PR 的版本化设计依据；本机 TODO 与验证辅助记录保持不提交。发布整理仅更新状态与追溯信息，不改变已批准的 r1 架构。

## 1. 当前问题与完成定义

`base_binder.go`、`bind_context.go` 能表达祖先引用，`flatten_subquery.go` 通过降低 Corr.Depth、上提谓词与改写 JOIN 实现部分情况。当前源码在多层 scalar、非等值聚合、外连接 ON 等处仍明确拒绝。

`TestNestedCorrelatedScalarStillRejectsUnsafeShapes` 以及 `test/distributed/cases/subquery/nested-correlated-scalar.sql` 明确保留了拒绝用例：COUNT/APPROX_COUNT 空集值、COALESCE 观察 NULL、LIMIT/OFFSET 等情况不能由现有局部上提保证正确。这里是源码调查结论，尚未运行新基线 UT 或服务验证。

目标是合法关联引用跨越任意祖先层级时，结果与逐个外层求值环境执行完整内层关系一致。完整交付必须覆盖 scalar/行 scalar、EXISTS/NOT EXISTS、IN/NOT IN、ANY/ALL，以及已有合法表达式入口中的聚合、JOIN、窗口、CTE、集合运算和 DML。不能仅把两层 MAX 示例跑通便关闭 issue。

仍遵循独立的语言边界：不新增同层非 LATERAL FROM 可见性、不开放非法递归源重读、不扩大 RETURNING 的既有子查询语法。分区、charset/collation、存储过程、UDF 和 warning 兼容性不扩展；已有合法类型的值与元数据必须保真。

外部语义参考：

- [MySQL 8.4 关联子查询](https://dev.mysql.com/doc/refman/8.4/en/correlated-subqueries.html)：词法作用域及关联名称解析。
- [MySQL 8.4 标量子查询](https://dev.mysql.com/doc/refman/8.4/en/scalar-subqueries.html)：空结果与标量值语义。
- [MySQL 8.4 子查询错误](https://dev.mysql.com/doc/refman/8.4/en/subquery-errors.html)：标量多行错误。

这些文档约束外部结果，不要求复制 MySQL 的优化限制。具体复合语义使用手算小数据和 MySQL 对照建立独立 oracle；MO 特有语法使用其既有合同。

## 2. 核心不变量

| 合同 | 必须能识别的反例 |
| --- | --- |
| 引用绑定到声明它的语义作用域和列 | 同名 alias 遮蔽、跳过直接父层、同时引用两个祖先 |
| 每个实际外层行、候选 pair、group 或 DML 行版本形成独立求值环境 | 重复外层值被错误合并，丢失 bag 行或副作用 |
| 只有被消费的表达式启动内层执行 | CASE 未选分支仍报多行错误，或提前启动 CTE reader |
| scalar 零行返回 NULL，一行返回完整 tuple，第二行报错 | LIMIT 1 掩盖原本应报的 cardinality 错误 |
| 聚合在各环境内保持完整关系语义 | 全局 COUNT 空输入的 0 被 JOIN 缺行替换成 NULL |
| IN/ANY/ALL 保留空集与三值逻辑 | NOT IN 含 NULL，或行比较过早把整行判 UNKNOWN |
| 内层沿用本 statement 的事务、snapshot、授权和会话 | 另建 SQL session，或延迟权限检查绕过未选分支授权 |
| 每份运行状态有唯一 owner 和终态 | 取消后 reader 不释放，复用时旧结果进入下一环境 |
| 可选优化不能改变需求、错误、类型或求值次数 | flatten 失败后复用已经被破坏性改写的树 |

不定义 WHERE AND/OR 的文本求值顺序；按实际消费 stage 和现有条件函数语义处理 demand。

## 3. 方案选择

| 方案 | 正确性与实现代价 | 决策 |
| --- | --- | --- |
| 持续添加谓词上提规则 | 对可证明的形状有效；聚合、非等值、按需错误与多祖先组合需要越来越多特判 | 作为优化，不能承担完整能力 |
| 每个参数环境重新生成 SQL 或重新编译运行 Scope | 原型容易，但反复编译、嵌套 worker 等待、事务与会话所有权复杂 | 不采用 |
| 显式参数化关系程序，加按需执行；允许等价集合 lowering | 统一作用域与结果归约；需打通表达式、执行、资源、计划消费者 | 采用 |

从 binder 的作用域和求值阶段出发重新实现第三种方案。参考已有 r4 的 continuation 思路，但所有接口均以当前源码核对并测试，不复制未经证明的实现闭包。

### 3.1 绑定与计划表示

1. 为每个 query block 保留稳定语义 owner；绑定结束后收集直接与传递祖先依赖。跳层依赖由中间程序转发 typed slot，不在运行时猜测 depth 或列 tag。
2. Query 显式描述程序 root、输入 slots、结果类型、表达式 occurrence、消费 stage 与 step owner。原 `Query.Steps` 是全局目录；主输出 step 明确记录，子程序 step 不在 statement 开始时全部启动。
3. 参数是原生类型与 NULL 值，不经 SQL 字符串或 TEXT ParamRef；保留 decimal、时间、binary、JSON 等现有元数据。
4. 通用路径直接消费绑定后的完整关系。优化先只读判定资格，再改候选副本；失败释放副本。禁止捕获旧 flatten 的 NYI 后直接继续使用其被修改的计划。
5. 计划发布前校验 ID、slot 类型、step/region 所属、调用图和所有显式依赖；所有 visitor/copy/remap/hash/explain/权限/对象依赖入口覆盖新字段。
6. proto 是唯一 schema 来源，生成 `.pb.go`；新增字段和 capability 的具体编号实施时按最新分配登记，不预占历史版本号。

### 3.2 执行与需求

- 编译一次不可变程序模板，每个实际需求创建独立运行实例；不按参数相等跨行缓存。
- stage owner 持有输入 lease、逻辑行身份、selection mask、已求值 operands 和结果。未需求 mask 不创建 reader、producer 或子程序实例。
- 采用同执行 lane 的显式 continuation：算子/表达式保存 PC 与游标；请求 child 时挂起父记录，driver 推进 child。父任务不占 worker 等待同池 child，不按相关深度创建 goroutine。
- 普通无 dependent 的路径继续使用现有 pipeline。需要可恢复的算子复用现有计算 kernel 与权威状态，不建立第二份 join matched/group/window 状态。
- 接入顺序：value/table scan、filter/project、表达式归约，然后 group/sort/limit/window、JOIN、set/CTE/source，再 DML stage；缺少任一合法入口的 adapter 属于未完成工作，不能以新 NYI 作为最终交付。
- EXISTS 第一输出行可决定真值；scalar 必须确认没有第二行；ANY/IN/ALL 使用绑定好的逐行比较 overload 归约。提前决定真值仍需结束已启动的工作并保留其真实错误。
- lhs tuple 和祖先 slots 按 occurrence 求值一次，跨挂起保持有效；结果归调用者所有，不借用即将释放的 child batch。

### 3.3 语义 stage

| 入口 | 求值环境 |
| --- | --- |
| SELECT、WHERE、排序 key | 当前输入行；被条件表达式选择的 demand |
| GROUP BY、aggregate arguments、HAVING | 分别为聚合前输入与聚合后 group，不能混淆 |
| JOIN ON | 实际候选 pair，在 matched 更新前执行；NULL 扩展行不再次执行 ON |
| window | 输入 key、窗口 setup、输出 row 按现有合法语义区分 |
| CTE/set operations | 声明作用域与 invocation；各 arm 保持 bag/去重/order/limit |
| UPDATE/冲突更新 | 相应赋值阶段的 current image；OLD 与 incoming values 独立 |
| 条件 INSERT | 已选 route 与对应 source row；未选 route 无需求 |

ASOF、DEDUP、递归 CTE、窗口 bound 等沿当前独立语法合同；实施时补充同形非关联控制，防止把原代码的 Sub/Corr 限制误当语言规则。

### 3.4 生命周期、资源与失败

运行实例状态为：私有构造 → ready → running/suspended → stopping → quiesced → released。只有构造与资源准入完成后才发布；部分初始化失败按已获得的资源反序释放。

- statement 拥有 txn/snapshot/服务与总预算；子实例仅拥有 slots、执行 scratch、reader、结果归约、局部 edge 和取消域，不能 Free 共享 Process.Base。
- reader、vector、batch、spool 和运行记录取得后立即登记 cleanup。父输入在 child 完成前持 lease；下一 generation 必须在上一代 quiesced 后启动。
- cancel/deadline、存储错误、第二行、预算不足都进入同一终止闭包；保留真实错误，不能把所有取消统一当作正常 EOF。
- Call/Step 不替代 Reset 发送终态；typed End 成功交付并完成接收方清理后回收，失败使用 Abort。满数据队列不能阻塞终态记录。
- 复用现有 query/CN 资源账本，所有 child 共用原预算；新增 slots、frame、控制记录和复制值在分配前准入，记录真实容量及扩容峰值。无每 child 新额度。
- 初版同时推进一个需求环境；额外运行存储随活动嵌套深度和当前 frame 增长，不保留历史全部参数环境或物化整个外内笛卡尔积。现有 group/sort/window 状态和 spill 另按其 owner 计入总预算。
- 最坏执行成本仍可能是外层需求数 E × 内层执行成本 I。显式接受通用路径的该代价；已支持快速路径不得无故新增每行扫描/编译。优化仅在等价性可证明后引入。

### 3.5 分布式、prepare 和兼容性

通用程序与 demand stage 首版放在协调 CN，使用同 snapshot 的现有 reader 读取存储；独立普通上游可继续多 CN 执行。合法 SQL 不以连接 CN 数量作为支持门槛。

远端 fragment 只携带其完整普通依赖闭包；不得夹带不可执行的新 slots/program。无法安全切分时将必要计算闭包放回协调 CN。新增计划 capability 必须在 coordinator、缓存/prepare、View 重绑与实际序列化入口统一校验；旧节点不能静默忽略新字段。混合版本采用明确能力错误或既有可用路由，不虚构旧节点支持。

prepare/cache 仅持不可变模板；每次 execute/retry 使用新的运行 owner 和 snapshot fence。权限与对象依赖检查覆盖所有绑定程序，即使它们最终未被选择。子执行不自行 retry/commit、不写 frontend OK/affected-row 状态。无新数据库数据格式；回退版本不能执行新能力 SQL 的边界需要明确文档与测试。

## 4. 改动与风险地图

| 闭包 | 预计 owner/消费者 | 风险与证据 |
| --- | --- | --- |
| 作用域、参数程序与安全 lowering | `pkg/sql/plan`、`proto/plan.proto`、`pkg/pb/plan` | R3；绑定/复制/remap/权限/prepare 与生成代码 |
| continuation 与资源所有权 | `pkg/vm`、`pkg/vm/process`、`pkg/vm/pipeline` | R3；取消、partial init、复用、资源耗尽、race |
| demand 表达式与各 stage | `pkg/sql/colexec` 及受影响算子 | R3；mask、NULL、错误、输入 lease、真实消费者 |
| 编译、分布式与入口消费者 | `pkg/sql/compile`，按实际闭包延伸 frontend/reader | R3；step 激活、授权、远端投影、混合版本 |
| SQL 公开合同 | 现有 subquery/CTE/JOIN/window/DML BVT | R2；独立结果 oracle 与多 CN |

设计门禁适用：跨 planner/compiler/runtime 边界，新增执行状态与计划协议，影响热路径和资源边界。本文批准后才能开始代码；若发现需要改变上述架构，先更新对应设计决定。

## 5. 验证矩阵与完成标准

1. 先把基线支持与拒绝矩阵记录清楚；最小数据覆盖 0/1/2 行、重复值、NULL、跳层与多祖先，避免以旧 issue 评论代替当前证据。
2. UT 使用已有 planner fixture，typed-plan 校验内部 owner/dependency；executor UT 使用可控制 reader/failpoint/barrier 验证 mask、单次求值、部分初始化、第二行、取消、预算拒绝和下一代复用。不能只断言不 panic。
3. 扩展现有 `nested-correlated-scalar`、`scalar_correlated_projection`、`correlated-limit`、`subquery-with-any/in/exists` 等覆盖；真正不同拓扑/生命周期才新建 fixture。将旧 NYI 用例改成独立计算出的结果或合法运行时错误。
4. 新增区分性 SQL：两层标量非聚合；三层跳级；两个祖先同时引用；无匹配 COUNT/COALESCE；多行 scalar；row NULL 比较；未选 CASE 内多行 scalar；外连接 ON 两侧引用；CTE/集合/窗口/顺序赋值的真实 stage。
5. CGo 包使用仓库 `mo-cgo-test` 与 Go 1.27.1；先非空 focused selection，再 owning package。增量 gofmt/vet/lint；并发生命周期按 race-validation 合同执行。
6. 新建测试专属服务，确认 native artifacts 对应本分支；mo-tester 生成 result 后人工核对，再 comparison 模式运行，并核对 teardown 后重跑。不得删除旧 worktree 数据或终止无关进程。
7. 多 CN 验证经 Proxy、直连 CN 的结果、远端普通片段和协调节点本地程序边界；prepare 连续执行、取消后下一条查询、资源故障注入均需真实终态。
8. 性能用独立 benchmark：普通 SQL/旧成功查询的 allocation、扫描和编译次数；一般路径的 E/I/嵌套深度增长、峰值与清理后账本。不得把性能负载塞进 BVT，也不把超时当正确停止机制。
9. 最终 `mo-self-review`：完整改动闭包无未解决 blocker；修改行覆盖率达到仓库要求 75% 以上；生成文件、交付 diff、工作目录与证据逐项核对。测试通过、CI 状态和设计批准分别报告。

## 批准记录

2026-10-13 用户在实施前批准 r1，原文：“go ahead”。实施分支为 `ck89119:issue-7559-from-scratch-main`，目标分支为 `matrixorigin:main`；实施 PR：[matrixorigin/matrixone#30116](https://github.com/matrixorigin/matrixone/pull/30116)（draft）。实现与验证摘要随 PR 发布，本机 TODO 和原始日志不提交。
