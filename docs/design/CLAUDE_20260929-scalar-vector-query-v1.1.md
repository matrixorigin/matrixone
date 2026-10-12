# SQL1 标量查询向量驱动 ANN 索引：设计 v1.1

- 版本：**v1.1，SQL1 发布定稿**，2026-09-29。
- 所属问题：[issue #23158](https://github.com/matrixorigin/matrixone/issues/23158)。
- 实现：[PR #29493](https://github.com/matrixorigin/matrixone/pull/29493)，实现快照 `c642c7e46b21fff1bb2882b38720a0248d19ecc3`。
- 设计批准：[会话批准记录与版本绑定](CLAUDE_20260929-scalar-vector-query-approval.md)。批准对象是设计讨论中的 SQL1 子集，不是历史 SQL2 扩展提案。
- 来源：实施前讨论的本地 `CLAUDE_DESIGN_issue-23158.md` v1.1；本次将既有机制及实施细化收敛为可版本化交付的规范。原档来源、发布日期与批准时间分别记录，不能混同。
- 本文是 PR 的规范性设计；本地原档中的 SQL2 扩展、全局 provider step、第三个物化读游标和仅固定局部子图的文字均不作为本版契约。

## 1. 目标、非目标与强制设计门禁

目标：对独立、可由 PK/UK 等值条件证明至多一行的标量子查询，允许其查询向量驱动现有 CPU IVF-FLAT/HNSW 访问路径，同时保留 NULL、空来源、多行错误、投影及分页语义。

```sql
SELECT id FROM items
ORDER BY l2_distance(v, (SELECT v FROM items ref WHERE ref.id = 'b'))
LIMIT 2;
```

必须创建真正的向量索引；`experimental_ivf_index=1` 本身不会创建索引。IVF 与 HNSW 各自的主键、向量元素类型、维度和距离函数限制继续有效。

本版不做：

- SQL2 的 nullable INNER provider 或距离投影扩展；现有非 NULL INNER provider 快路径不得回归。
- SQL3 的采样集合两两比较/全局最近向量对优化。
- 多行 provider、相关子查询、通用 InitPlan、全局参数槽、跨执行向量缓存或新 GPU 能力。
- 索引存储格式、CDC/ISCP、catalog 或数据迁移；旧分支移植另行评估。

本能力跨 planner、compile、物化来源和算子生命周期，新增计划节点及惰性状态机，并限制查询布局，因此触发强制设计审查，不是普通局部 bugfix。批准记录区分先设计、再授权实施与后续交付校订；本文的提交修订和内容 hash 绑定在批准记录中。

## 2. 事实、根因与最小反例

[前置 PR #24394](https://github.com/matrixorigin/matrixone/pull/24394) 只接受平凡 INNER JOIN 的保守单行、非 NULL provider。标量子查询降低为 SINGLE 或特定 LEFT JOIN，不能通过移除这些 guard 直接接入原快路径。

PK/UK 证明的是**至多一行**，并非恰好一行；来源列 NOT NULL 也不能推出标量子查询非 NULL。现有向量搜索对 NULL 查询向量返回空候选，因此无条件替换标量关系会改变结果。

最小反例：主表过滤后的关系 A 有两行，标量来源 P 查不到主键，查询 `LIMIT 2`。原关系仍可返回两行，距离为 NULL；改成 P 驱动的 INNER/APPLY 会错误返回零行。

| 来源状态 | 本版 SQL1 行为 |
|---|---|
| 0 行 | 保留原 SINGLE/LEFT 的 NULL 扩展，不直接返回空外表 |
| 1 行、向量 NULL | 保留原关系计划和距离表达式语义 |
| 1 行、向量非 NULL | 使用既有 ANN 路径及其模式/过滤/召回规则 |
| 无法证明至多一行 | 不进行新改写，由原标量执行路径保留多行错误 |
| 已证明的 provider 运行时意外超过一行 | 输出前报内部契约错误，不偷偷取第一行 |
| 来源执行失败或取消 | 终止本语句，不选择另一条结果分支重试 |

参考 PostgreSQL 的[标量子查询语义](https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-SCALAR-SUBQUERIES)和 [pgvector 查询接口](https://github.com/pgvector/pgvector#querying)，但不假设 MatrixOne 已有 PostgreSQL 的通用 InitPlan/参数传递框架，也不由“SQL 能执行”推断“必然使用 ANN”。

## 3. 不变量与替代方案

### 不变量

1. provider 与主查询属于同一语句、租户、权限和既定事务/快照，不拆成客户端查询或新事务。
2. 每次执行最多求值 provider 一次；PREPARE 缓存计划而非向量。LIMIT 0 不启动 provider 或结果分支。
3. 选择发生在结果分支启动前；未选分支不得启动扫描、搜索、远程 Scope 或有副作用的表达式。
4. 保留原过滤、投影、LIMIT/OFFSET 和列绑定，不给 SQL 增加 `vector IS NOT NULL` 谓词。
5. 沿用 ANN 模式的既有语义；不把召回不足、零候选或搜索错误当成 NULL 来源。`mode=force` 不进入此改写。
6. 一个固定查询向量对应本次 Top-K，不按主表的每一行重新发起搜索。
7. **Query 中含 selector 时，整条查询在协调 CN 执行**，包括其上层排序/JOIN、provider 和两个候选结果子树。不存在“仅局部子图固定、上层继续跨 CN 并行”的例外。

### 方案比较

| 方案 | 正确性、复杂度及成本 | 决策 |
|---|---|---|
| 维持现状/应用拆两次查询 | 无内核变更，但 SQL1 不获索引能力；往返和快照责任外移 | 不作为本次交付 |
| 放开 SINGLE/NULL guard 后直接接索引 | 改动小，但空来源/NULL 最小反例即错误 | 拒绝 |
| 规划期读取来源并折叠成常量 | 会使 EXPLAIN/计划缓存读取数据或保留陈旧值，破坏快照/权限边界 | 拒绝 |
| 通用 InitPlan 与分布式 statement 参数槽 | 更通用，但增加表达式、缓存和分布式消费者协议，远超 SQL1 范围 | 留给独立需求 |
| 局部单次物化与输入状态选择器 | 复用现有索引插件及惰性调度，NULL 可回原关系；代价是新状态机和整查询单 CN 限制 | **采用** |

## 4. Planner 与计划契约

### 4.1 静态准入

仅接受一个受现有插件支持的向量距离排序键、有 LIMIT、可保留 OFFSET 的形状。索引主表与 provider 独立；SINGLE 或 LEFT 必须是平凡、无关联匹配条件的标量关系。

沿用 PK/UK 全键等值证明，不由统计 `Outcnt <= 1` 或裸 `LIMIT 1` 推断唯一性。provider 只穿过允许的扫描、列投影及排序；拒绝 volatile 表达式、非列 provider 投影、不能保持比较唯一域的类型转换和非安全重放区域。当前保守 guard 拒绝等式参数类型 ID 不匹配的来源，不扩大 charset/collation 范围。

相关来源、未证明单行、第二排序键、`SQL_CALC_FOUND_ROWS`、强制精确模式以及不能安全重映射的投影留在原计划。已有非 NULL INNER provider 仍走旧路径，不引入新 source/selector。

### 4.2 逻辑节点和数据流

```text
整条 Query：协调 CN（保留 CN 内并行）
  上层消费者（若有排序/JOIN，仍在同一协调 CN）
    VECTOR_QUERY_TOP
      child 0：原 provider 投影 P ──selector Append/Finish──> M
      child 1：ANN 分支   <── VECTOR_QUERY_SOURCE（reader 0）── M
      child 2：原关系分支 <── VECTOR_QUERY_SOURCE（reader 1）── M
```

- `VECTOR_QUERY_TOP` 三个孩子依次为 `[provider, ANN, exact]`。结果分支列类型/顺序一致，provider 规范化为一列查询向量。
- M 是现有 `materialized.Source`，只保存本语句的一行向量；有**两个**独立结果读者。selector 直接消费 provider，不另建第三个物化控制读者。
- `VECTOR_QUERY_SOURCE` 是带单列 schema 的叶节点，通过语句局部 `vector_query_source_id` 绑定 M。编译映射键为 `-sourceID - 1`，不与非负全局 CTE step 键相撞。
- **不向全局 `qry.Steps` 插入 provider，不使用全局 CTE 的立即启动队列。** provider 与两条结果分支都由此 selector 的 lazy pre-scopes 管理，启动所有者唯一。
- 在复制的候选子树上尝试已有插件。插件拒绝不得修改原关系；构建错误按原错误路径传播，不能暴露半改写计划。
- ANN 分支沿用 IVF APPLY/`VECTOR_INDEX_SCAN` 或 HNSW table function；仅该分支启动时才已知查询向量非 NULL，不伪造持久列的 NOT NULL 属性。
- 原关系分支只把 P 换成 source reader，保留 SINGLE/LEFT、过滤、投影和 Top-K，并禁止再次触发相同向量改写。来源为空时仍由原关系完成 NULL 扩展。
- 来源内部分页保留；结果分页只影响结果，候选预算沿用 `LIMIT+OFFSET` 与既有 over-fetch。

### 4.3 列类型、PREPARE 与计划消费者

TOP 输出显式引用结果 child 1，而不是 provider child 0；frontend 结果列来源沿 exact child 2 查找。FUNCTION_SCAN/VECTOR_INDEX_SCAN/VECTOR_QUERY_SOURCE 输出列取各自 TableDef，不能由提供查询向量的输入 child 覆盖结果类型。

沿用 binder 的元素类型、维度和距离验证，不能通过覆盖 `Expr.Typ` 偷换数据表示。每次 EXECUTE 重新执行来源、重建执行代际；沿用现有表/索引依赖、权限、snapshot 和 prepared 失效处理，不新增绕过这些边界的后台 SQL executor。

新增节点/字段仅在 `proto/plan.proto` 定义并生成 Go 消费者；同步 remap、deepcopy、统计、EXPLAIN、编译和 VM 名称。现有 VM 操作码不重编号。节点 `VECTOR_QUERY_TOP=58`、`VECTOR_QUERY_SOURCE=59`，字段 `vector_query_source_id=93`；新 VM `VectorQuery=67` 追加在 `MinusAll=66` 后。

## 5. 执行、所有权与 Q1—Q3

状态：`未启动 → 读取来源 → 来源成功封存 → 选定一条结果分支 → 输出 → 完成`。任何活动状态可转入失败/取消并清理，不能重新选择结果分支。

1. 安装并激活 provider receiver，再启动 provider；逐批检查至多一行并由 Source 独立保留所需向量。
2. **provider EOF 加上 producer 完成屏障成功**后，Finish source 并固定选择；EOF 本身不能掩盖 producer 后续清理错误。
3. 先激活被选分支 receiver，再启动该分支。非 NULL 选 ANN；NULL/空来源选 exact。
4. 流式转发结果，使用既有 Top-K/传输协议，不再物化 K 行结果。结果 EOF 后也等待相应分支终态。
5. LIMIT 0 不启动任何孩子；未启动分支仍必须清理编译资源和读者。错误不通过“打开另一分支排空”解决。

| 资源/等待 | 所有者及终态 | 上界 |
|---|---|---|
| Source | Compile 创建、Begin/Close；selector Append/Finish；两个 Merge reader 各自释放 reader | 至多一行向量，O(D)，使用现有 allocation/spill 边界 |
| provider 输入 batch | 既有扫描/merge 所有者保有批次；Source 独立保留，不保存下层借用切片 | 与既有输入批次及一个向量有关，不随主表增长 |
| 三个 lazy pre-scopes | Compile/Scope 唯一负责启动和清理；最多启动 provider 加一个结果分支 | 固定三个分支、两个 source reader |
| ANN reader/table function | 既有 search/APPLY 的 Prepare/Reset/Free 所有者 | 不新增常驻 worker 或全局缓存 |
| 完成等待 | 既有 branch waiter 与当前 statement context | 不等待未启动分支；取消通过既有调度路径终止发送者 |
| PREPARE 代际 | 上一代完成/清理后复用；Reset 清除选择、LIMIT executor 状态与回调 | 不保留跨执行向量或旧 reader |

Q1：成功、来源/搜索错误、启动或部分 Prepare 失败、取消、提前结束及重复清理，均归还到一个有效回收所有者。

Q2：安装 receiver 后才启动 producer；取消不能依赖被取消的结果发送继续推进；完成屏障覆盖真正的 producer 终态。

Q3：新增保留状态为 O(D) 向量及固定控制状态，不物化主表、不缓存两份 Top-K，不随查询次数或 prepared 执行代际积累。ANN 内部既有 probe/AUTO 行为仍由原插件管理。

## 6. 整查询协调 CN 边界、兼容与回退

**布局约束是整查询，而不是局部子图。** `vectorQueryExecType` 检查 Query 是否含 `VECTOR_QUERY_TOP`；存在时整条查询使用 `ExecTypeAP_ONECN`，保留 CN 内现有并行。provider、selector、ANN/exact 和所有上层消费者均留在入口协调 CN，不能把任何依赖 M 的新算子或状态下发给另一个 CN。

未含 selector 的查询布局不变；这不是关闭整个集群或所有向量查询的 multi-CN。单 CN 执行也不禁止正常 TN/Log/fileservice RPC。

- Source 是进程局部状态，新节点不以“protobuf 新字段可忽略”为由发送给旧 CN。跨 CN 共享来源/远程新算子不在本版能力内。
- 不改 catalog、磁盘索引或数据，不需索引重建、数据迁移或新的恢复格式；执行状态不持久化，失败/重启后由新语句重新建立。
- 沿用既有索引开关和 plugin 能力，不新增用户参数。回退可用现有强制精确模式或撤回此优化；prepared 按既有生命周期失效/重建。
- 所有分支继承同一语句租户、权限、事务与 snapshot；不通过旁路连接传递向量，不新增向量内容日志。
- 本版接受失去**该整条查询**跨 CN 并行的代价。解除这一限制属于新的分布式状态/兼容设计，必须另行批准，不能在实现中悄悄放开上层消费者。
- 未进行混合版本集群或生产规模性能验证，不能由单/双 CN 同版本测试推导出这些结论。

## 7. 成本、诊断与验收

非 NULL 快路径新增一次 provider 求值、O(D) 来源保留和固定分支调度，不启动独立 exact 分支；插件自身多轮 probe/AUTO 不被描述为“只执行一次底层搜索”。NULL/缺行回到原关系代价，并有至多单行来源保留的开销。

普通 EXPLAIN 展示 selector/source 和可能的两条结果路径，不执行 provider 或泄露其向量值。通过实际执行统计/Scope 启动和物理地址验证选中了哪条分支；不能仅看到索引节点或计划标题就宣称 ANN 真正执行或布局正确。

### 变更与验证地图

| 闭包 | 位置 | 风险与验收 |
|---|---|---|
| 单行证明/关系改写 | [scalar planner](../../pkg/sql/plan/apply_indices_scalar_vector.go) | R2；公共 SQL 正向控制、拒绝相关/多行/次序反例，插件拒绝保留原计划 |
| 列/生成协议消费者 | plan remap、类型刷新、result provenance、proto/deepcopy/EXPLAIN | R2/R3；结果元数据、roundtrip、旧操作码稳定，普通 EXPLAIN 不求值来源 |
| 物化/惰性状态与布局 | [compile](../../pkg/sql/compile/vector_query.go)、[operator](../../pkg/sql/colexec/vectorquery/vector_query.go) | R3；实际 Scope 启动计数、失败/取消/Reset/Free、两个读者、代际和 race；整查询协调 CN |
| 索引 provider 消费 | 既有 IVF/HNSW 插件及 table function | R2/R3；真实合法 DDL、NULL/空值/参数、过滤及分页，不改索引内核 |
| SQL 公共契约 | [SQL1 BVT](../../test/distributed/cases/vector/vector_scalar_query.sql) 和 [既有回归](../../test/distributed/cases/vector/vector_prepared_limit.sql) | R2；mo-tester 生成、独立审查、正常比较、teardown 后同实例重复 |

### 验收矩阵与证据边界

- 主键命中非 NULL：公共 planner 能达 ANN，实际结果及未选分支零启动均有 oracle。
- NULL/空来源：保留外表行；全 NULL 并列只检查确定性条件或行数，不固定 SQL 未承诺的 tie 次序。
- 无单行证明、相关来源、第二排序键、found-rows、force 模式：不启用新改写；多行保留原标量错误。
- PREPARE：不同来源、来源更新、NULL/空值切换、参数化 LIMIT、LIMIT 0 后复用及 BIGINT 结果类型。
- 生命周期：来源/结果/完成屏障错误、取消、未启动读者清理、部分初始化与重复执行。
- 真实双 CN：从 CN1/CN2 分别进入，SQL1 全部 Scope 地址只为各自协调 CN；literal IVF 对照仍部署两个 CN，且实际结果正确；跨 CN 更新的来源在下一语句可见。

最小低维数据即可区分结果；不依赖 issue 的外部 S3 大数据，不用 sleep、skip 或盲目接受生成结果代替 oracle。Go 工具链与 go.mod 一致，CGo 使用仓库 wrapper，变更覆盖率不低于 75%。

实现快照 `c642c7e46b` 的已完成证据：七个 owning packages、compile/vectorquery 整包 race、实际 lazy Scope race 100 次、增量 lint；变更行覆盖率 365/439（83.14%）。新 BVT 77/77、原回归 49/49，单 CN 分服务部署同实例两轮、双 CN proxy/CN1/CN2 三入口均通过，0 ignored，teardown 无残留。单 CN 部署为独立 Log/TN/CN/Proxy，并非新增的本机单进程验证；没有性能倍数、小物化开销基准或混合版本成功声明。

## 8. 决策与后续变更

已定决策：仅 SQL1；局部单次物化、输入状态决定的惰性分支；复用已有 CPU 索引插件；整查询协调 CN；不新增通用 InitPlan 或跨代际缓存。

无意通过此 PR 关闭整个 #23158。SQL2/SQL3、GPU 扩展、解除整查询单 CN 约束及旧分支移植由后续需求单独设计。任何影响上述机制、所有权、边界或兼容假设的变更，都必须更新版本并重新审阅，不能继续复用本版批准。
