# 本地 CTE 外层引用：关联域去关联（v1）

- 所属 issue：https://github.com/matrixorigin/matrixone/issues/29145
- 设计版本：v1，2026-09-24。
- 审批：用户在生产实现前 review 本地 `CLAUDE_DESIGN_29145.md` v1 并回复 `go ahead`，批准稳定行标识与参数列方案及以下范围。本文件保存该设计的可版本化契约；未新增执行协议。
- 实施范围：本文件所在 PR，目标分支 main。

## 问题与依据

MySQL 8.0.14 起允许嵌套查询块内 CTE 引用外层查询列（https://dev.mysql.com/doc/refman/8.0/en/with.html）。声明所在查询块的同层 FROM 名称仍不可见。

MatrixOne 能绑定这些列，但普通去关联只沿 Children 抽取相关谓词。非递归 CTE 内部的相关投影，以及被拆成独立 Steps 的递归 seed/member，可能保留 Corr；执行器没有“当前外层行”可供读取。仅转发 isCorrelated 或把 Corr 改成原外层 ColRef 不能建立跨 Step 的数据依赖。

基线 `95b1d2bfd9` 上，公开 SQL planner fixture 的三个 issue 等价查询分别在 PROJECT、PROJECT、TABLE_SCAN 中残留 Corr；普通相关标量及非相关递归 CTE 两个对照通过。第一责任边界是 planner 的生产者/消费者区域改写。

## 目标和范围

- issue 的 parent、递归 depth、祖先 NOT EXISTS 三例必须返回精确结果。
- NULL seed 是一行，过滤后的空 seed 是零行；重复外层参数不合并行身份。
- 保留标量多行错误、COUNT 空集补偿、递归深度和 DISTINCT 语义。
- 外层仅接受能证明保留隐藏 Row_ID 的单基表及透明 FILTER/SORT；不复制改变 multiplicity、快照、锁或权限边界的关系。
- 生产者支持普通表扫描、无 FROM 的隐式单行扫描、投影、过滤、内连接和递归 UNION ALL/DISTINCT。递归 member 可以引用域中的外层参数。
- 不实现通用 APPLY/LATERAL、任意多层相关、复杂外层 JOIN/AGG、producer 分页/排序/聚合/窗口或副作用表达式。
- 显式 VALUES 的表达式执行器在域 JOIN 下方，无法读取域列，v1 拒绝这种生产者；收集相关引用时仍检查 RowsetData，不能漏检后进入执行器。
- CTE 消费者的窗口按隐藏行标识分区，物理 PARTITION 输入排序也须包含标识，不能仅改窗口表达式。单个 COUNT 聚合的 HAVING 在 LEFT JOIN 补偿空组之后按外层行求值，避免把「聚合结果被 HAVING 删除」误作「空输入 COUNT=0」。UNION ALL 两条相关分支均须输出同位置、同类型、对应同一外层标识的隐藏列，UNION 输出同步扩展该列；单条分支不具备标识时在规划期拒绝。其他集合运算与未验证的聚合/HAVING 形状仍须明确拒绝，不能返回错误结果或内部 schema 错误。特别是 HAVING 经 LIMIT/OFFSET 或 UNION/JOIN 分支的移动不保语义，消费者的非 INNER JOIN 会丢失保留行，COUNT HAVING 的非 COUNT 投影不能被误写成 COUNT；这些形状在 v1 规划期拒绝，非语义支持。COUNT 的空组补偿仅适用于原始无显式 GROUP BY 且输出能沿 ORDER BY/DISTINCT 等透明投影追溯至该 COUNT 的标量子查询；COUNT 表达式在可证明的单行投影路径上应于 LEFT JOIN 后用恢复的 COUNT 值重新求值，不能把 COUNT+1 的空输入直接替换为 0；HAVING 必须在该恢复之后按 COUNT 原值（而非 COUNT+1 等最终投影）求值；LIMIT 1 的 per-key ROW_NUMBER 容器可透传无分组聚合行，LIMIT 0/正 OFFSET 删除结果行时任何聚合都不能补偿（包括 BIT_OR/BIT_AND）；确定删除结果行的 COUNT HAVING 无需在 JOIN 后再次求值，也不应报 NYI。多个聚合引用须逐个恢复并在 join 后重建最终表达式。普通显式 GROUP BY HAVING 保持原路径；参数化 HAVING 可保留 ParamRef 和 CAST 类型节点。该约束也适用于普通相关子查询，而非只约束本地 CTE。显式 GROUP BY 的空输入返回零行，标量语义为 NULL（包括 LIMIT/OFFSET），而不同外层行的非空同值分组仍须各自独立。无分组聚合在空输入仍生成单行：非标量 EXISTS/IN、UNION/JOIN 分支和嵌套聚合均不可沿用标量 COUNT 的空组补偿；无 HAVING 的 COUNT 跨分页及聚合上的窗口结果也不可套用该补偿。消费者内由外层行控制的 WHERE 须先于聚合和窗口执行，不能把该过滤上提到这两个节点之后。上述形状在 v1 继续安全拒绝。消费者 JOIN.OnList 中仍包含 Corr 的形状也必须在参数化前拒绝，不能留给 executor 处理。聚合结果上的窗口（即使显式分组）没有独立的身份窗口与空输入证明时不能套用 COUNT 补偿，仍需规划期拒绝。
- 域回放可能先于消费者的 CASE 条件执行生产者表达式：与外层参数关联的文本强制转换若不具备全域总性（例如 VARCHAR 转 SIGNED）在只读准入时拒绝，不能通过「非易变」准入提前触发本不应执行行的错误。已证明全域安全的转换仍可回放；其他可能抛错的函数应逐类验证执行域，不能把该保护当成通用按需 APPLY。
- 没有新相关区域的既有查询保持原路径。这是有边界的兼容性扩展，不是全部 MySQL 相关 CTE 能力。

## 备选方案与选择

| 方案 | 权衡 | 决策 |
|---|---|---|
| 只传播相关标志或提前报错 | 无法为独立 Step 提供外层值，不满足成功结果契约 | 不选 |
| 每个外层行启动依赖执行器 | 通用，但新增参数/调度/reset/cancel/远端协议和逐行执行成本 | 不选 v1 |
| 物化任意外层关系并分配 occurrence ID | 避免重放，但需要新的共享消费者 drain、spill 和提前退出证明 | 后续候选 |
| 有条件重放基表并携带稳定 Row_ID 和参数 | 多一次同快照扫描，但复用现有算子和递归协议 | 采用 |

## 需求域修订（2026-09-29）

用户确认继续修复消费者 WHERE 与第三个原始查询；NYI 不作为这两项验收成功。

- 外层 WHERE 拆分后，无子查询、无外层相关引用、具有总性证明的独立合取项先形成 FILTER，之后才对依赖子查询展开。该 FILTER 是屏障，进入原查询输入和回放输入；保留原谓词。未证明安全的合取项不提前求值，不消除其 guarded 限制。于是第三个查询的 `active=1` 在启动祖先分区之前生效，SQL 谓词书写顺序不影响需求域。
- 消费者 WHERE 中仅引用当前外层基表行且具有总性证明的谓词可投影到回放域；不穿越 HAVING、窗口、分支或其他 CTE 边界。域上的谓词使用克隆基表 tag，执行在 producer 参数注入之前，原消费者谓词仍保留。该行不需要消费时不创建对应 producer 分区，避免提前 ABS/CAST。
- 混合外层列与 CTE 结果的消费者 WHERE（例如 `p.id=2 OR n>0`）不能抽取到外层需求域。将其中当前外层行的列纳入隐藏 payload，经 PROJECT、DISTINCT、已按外层身份分区的 WINDOW 和已支持的按外层身份分页转发，并在原 FILTER 中改写为输入列；WHERE 不再含 Corr，通用去关联不能将其提到可能抛错的消费者投影之上。纯外层需求谓词按整个身份分区筛选，与该分区的 DISTINCT / LIMIT 可交换；依赖 CTE 结果的谓词则必须留在 DISTINCT / LIMIT 之后。DISTINCT 的隐藏身份/参数参与去重，若有 PAD SPACE 物理等值键也同步扩展，不能合并不同外层身份。纯外层谓词只移除整个窗口身份分区，不改变分区内排名；依赖 CTE 值或窗口值的谓词保留在窗口之后。该转发不穿越普通聚合/分支；HAVING 不作为 CTE 行级需求谓词。
- 这一优化不能将依赖 CTE 结果的谓词当成外层谓词，也不能从特定测试数据推断无环。没有需求域证明的 CASE/分页/递归组合保持既有界限，不宣称泛化 APPLY。显式/隐式分组、COUNT 零行和标量 NULL 继续分别遵循原有补偿规则。
- 纯外层需求谓词若不能通过回放证明，必须标记该域为 guarded，不能因未生成 demand FILTER 就退化为无条件递归回放。ABS 的绑定若明确为 INT8/16/32 扩为 INT64 后取绝对值，其整个值域可表示，可按类型证明总性；这不放行 ABS(BIGINT_MIN)，也不放行参数内部的潜在错误运算。INT 主键上的 `WHERE ABS(p.id)=2` 必须建立域过滤并返回正确 COUNT，空 seed/真正激活递归仍沿原语义。
- PREPARE 的无条件外层需求谓词允许直接 ParamRef 的 statement-constant CAST，不能要求未知参数对所有潜在值都总性成立；仅证明副本将该转换视为语句常量，执行表达式保持原参数、类型及转换，绑定时沿原有值/错误语义求值。CASE 等 guarded 消费者仍要求严格总性，不能借此提前执行未选择分支的转换。
- 不新增执行协议、缓存、并发对象或逐行调度。额外代价为绑定期间遍历谓词和克隆已有安全条件；执行期的域 FILTER 必须先于 producer，有利于减少不需要的递归分区。
- 验收：三条原始 SQL 的精确结果，ABS/CAST consumer WHERE 的 `(1,NULL),(2,2)`、真实激活错误、COUNT `(1,0),(2,1)`、外层过滤掉非终止分区成功，以及现有错误后恢复。

## 逻辑模型与不变量

设外层关系为 O，稳定行标识为 k，被引用列为 p：

```text
D = 同快照的 O 输入，提供 (k,p)
anchor' = 对各 D 行计算 anchor(p)，输出 (用户列,k,p)
member' = 从上一轮输入取得 k,p，计算 member(p)，保留相同 k,p
R' = recursive_union(anchor',member')
consumer' = 在 R' 的入口加入 R'.k = outer.k，执行原消费者
```

固定 k 后，每轮 R' 的该分区必须等于原查询以该外层行独立执行到该轮的结果，忽略隐藏列即可。seed 建立此关系，member 只能继承自己输入的 k,p；若多个相关分支均带域，必须按 k 等值连接，禁止跨 k 组合。

- k 非 NULL，结果按 k 关联，不以可 NULL 的参数等值代替身份。
- 参数相同但 k 不同的外层行保持独立。
- UNION DISTINCT 对完整 payload 去重；k 防止跨行去重，固定 k 的 p 不变，不改变用户列的去重关系。
- 隐藏列不进入星号展开、CTE 用户列名列表、用户列数或结果元数据。
- 无 seed 的 k 不制造占位递归行；COUNT 的 0 由现有标量聚合补偿处理。

## 改写与原子性

1. 在外层 nodeID 与子查询区域都可见、最终列重映射前收集本地 CTE，沿 SourceStep 收集生产者，用 visited 集合终止递归回边。
2. 对所有候选只读准入：检查外层身份、表达式、关系边界和全部相关列的所有者/深度；预先绑定身份等值函数。
3. 所有候选通过后才发布改写，禁止后一 CTE 拒绝时留下前一 CTE 的半成品。
4. 域重放复制已绑定、已授权的对象/快照/合法过滤和 pruning 信息，分配新 tag，不重新解析底表。外层行相关谓词必须满足安全重放检查；绑定参数/常量转换保留其原值或错误。
5. seed 获取域；member 优先使用 recursive scan 的域列。将 Corr 改为实际输入参数列，逐投影传播。
6. 同步扩展 sink、recursive scan、CTE scan 的 schema，并保留隐藏列的裁剪约束。更新多引用生产者的相关标志，阻止非相关共享物化优化误用。
7. 消费入口加入 k 的相关谓词，复用原 FILTER pull-up、分组、SINGLE JOIN、COUNT 补偿和存在性改写。

递归深度按有新行的轮次计数。各 k 同时从 seed 开始，任何 k 超限仍报原错误；耗尽的 k 不产生占位行延长递归。相关递归 LIMIT/OFFSET 需要按 k 独立计数，v1 拒绝，不能沿用全局窗口。

## 所有权、资源与兼容性

- 新描述符和根映射由 QueryBuilder 所有，生命周期仅为一次规划；不新增全局缓存、worker、goroutine、逐行 process 或重试。
- 执行 payload、hash distinct、内存 reservation 与清理由现有算子所有；成功、分配失败、超限、取消和 reset 仍走原查询生命周期。
- N 个外层行、K 个参数、T 条递归结果：域重放增加扫描，payload 增量随 `T*(Row_ID宽度+参数宽度)` 增长。没有独立无界缓存，不承诺更低峰值内存或性能提升。
- 隐藏 payload 必须受现有 recursive CTE memory quota 约束；配额失败后同 session 的正常查询和 prepared 重执行必须恢复。
- 不改变 protobuf、执行节点协议、catalog、持久化或配置格式，不需要数据迁移；回滚为移除 planner 新路径。
- 不跨视图、权限或租户边界重新解释对象。双 CN 验证不等于所有小查询都强制产生远端扫描；没有新增远端参数协议。

## 验证合同

- UT：公开 SQL 入口和独立 typed-plan oracle，检查全部执行 Steps 无残留 Corr；覆盖合法/非法作用域、多个参数/引用、嵌套生产者、只读准入原子性以及不安全边界拒绝。
- BVT：最小 5 行层级数据，精确 parent/depth/祖先结果，NULL/空 seed、重复值、member 参数、DISTINCT、标量多行错误。
- 生命周期消费者：递归深度边界、1 byte memory quota 失败后恢复、prepared 参数改变后的重新执行；复用现有 mergecte schema/reset/失败清理测试。
- owning-package、增量 gofmt/vet/lint、代码覆盖率至少 75%；使用 go.mod 指定工具链和 mo-cgo-test native provenance。
- mo-tester 生成 result 后逐项审查，再正常比较两轮，并在重跑前独立检查 teardown。

设计决策：用户批准 v1；不新增执行协议。若必须支持任意外层关系、相关递归分页或新增共享物化生命周期，先修订设计，不静默扩大实现。
