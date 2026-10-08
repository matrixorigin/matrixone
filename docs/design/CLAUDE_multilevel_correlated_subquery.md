# 完整多层关联子查询：调查与设计草案 r3

- 所属需求：[#7559](https://github.com/matrixorigin/matrixone/issues/7559)。
- 状态：**设计先行 draft PR 的调查/方案稿，未批准；不是 #7559 实现，不关闭 issue，不能进入生产/测试实现。**
- 当前源码基线：`mo/main`，`83d82b8ee0cd694e0c6a7146902d74ae4dfb415a`（2026-10-08 fetch/pull；2026-10-09 再次 fetch 核对未前进）；未改生产代码或测试，未运行功能 SQL/BVT。r0–r2 的源码/基准基线为 `dce60631f8c857ae730a4d2f328df5962e4f2a8d`，历史数据不能代替当前验收；第 19 节明确刷新范围。
- 调查日期：2026-10-08—2026-10-09；当前 `go.mod` 要求 Go **1.27.1**，已实际核对。第 16.3 节的 Go 1.27.0 数据是历史记录。
- 用户已授权继续直至 PR，包括本任务 push/draft PR；不等于批准尚未闭环的技术方案。覆盖、求值等价、资源/性能必须在实施前闭环；原始二列 IN 回归是实现交付硬门禁。设计稿可以先提交 review，批准前不混入生产/测试实现。
- 本稿用于记录已核对的源码事实、语义定义、方案比较及待关闭的设计问题，不将候选接口或既有测试断言称为新功能验证证据。
- 修订说明：第 1–11 节保留需求/候选模型；第 12–18 节保留 r2 方向并明确剩余阻塞；第 19 节补最新 CTE/prepare、Step owner 和特殊 JOIN 的具体源证据与设计修正。r3 作为独立可 review 的文档交付，不宣称技术设计 PASS 或功能完成。

## 1. 需求与非目标

完整支持合法多层 correlated subquery：引用可跨多个祖先作用域、跳过中间层并同时使用不同层的参数，不能按固定深度、子查询种类、连接两侧或某个 SQL 形状返回 NYI。

完成不是“放宽几个 guard”。必须有一般执行语义及完整 producer/consumer/终止闭包，所有优化只是该语义的可证明实现。不能把按形状拒绝改名为资源错误。

范围按子查询种类与出现位置展开：

| 维度 | 必须覆盖的合同 |
|---|---|
| 标量与行值 | scalar；合法行值比较，含二列及更多列；零/一/多行；左右操作数内的合法子查询组合 |
| 量化 | EXISTS/NOT EXISTS、IN/NOT IN（含行值 IN）、ANY/ALL；投影、过滤、否定和 NULL-observing consumer |
| 作用域 | 一层、两层、三层与更深；跨级跳跃、多祖先、多 binding、别名遮蔽、合法派生表引用 |
| 连接 | INNER ON；LEFT/RIGHT/FULL 等既有合法 ON 入口；同时引用两侧，及 ON 内跨祖先引用；保留匹配前后边界 |
| 聚合 | 聚合输入中的子查询、相关 GROUP BY scalar 入口、分组后投影/HAVING；全局/分组/嵌套聚合、空输入与 DISTINCT |
| 窗口 | 子查询关系中的窗口；既有合法窗口表达式入口中的依赖；分区、排序和 frame 在每个求值环境内保持原语义 |
| 关系组合 | CTE（含多次引用）、派生表、UNION/INTERSECT/MINUS 及其 ALL/DISTINCT 变体、排序/分页 |
| DML | 已支持的 INSERT SELECT、UPDATE WHERE/赋值、DELETE WHERE 等子查询入口；保留写入、锁、快照与原有合法性规则 |
| consumer/协议 | CASE/IF/COALESCE 的已定义需求；prepare/reprepare/多次 execute；单 CN、多 CN、错误/取消/重用 |

“既有合法”不能被用于排除只因为 correlation 而失败的情况。例如：

- `flatten_subquery.go:99–112` 的深层/双侧外连接 ON 拒绝在范围内。
- `group_binder.go:403–404` 在允许 scalar subquery 后额外拒绝 correlation，也须纳入相关语义闭包，不能拿非关联成功做证据却宣称关联是独立特性。
- 普遍非法的同层非 LATERAL FROM 引用、GROUP BY 中窗口函数、既有 RETURNING 不允许子查询等，不由本需求重新定义。归为独立限制须提供同形非关联对照及对应既有契约，不能只引用某个 NYI 文本。
- 递归 CTE、特殊连接、frame 等入口须逐项检查合法性和依赖 owner；尚未核对不能算覆盖或排除。

不扩展分区表、charset/collation、存储过程、UDF、warning 兼容性等默认排除领域。但通用路径必须保真传递当前合法参数的完整类型/元数据，不能因没有新增这些特性而把既有合法类型统一转为字符串或整数。

## 2. 已核对的源码事实与限制

以下原始调查表针对 r0–r2 的 `dce60631f8`，行号也对应该历史版本；不要将它们解释为全部重新核验于最新 main。r3 针对 `83d82b8ee0` 的增量核验见第 19 节。源码/已有测试只证明结构和意图，不冒充本轮执行结果。

| 位置 | 已核对事实 | 对设计的约束 |
|---|---|---|
| `base_binder.go:645–775`、`bind_context.go:28–80` | 名字按当前 binding 解析，向上查找会跳过 binder-less context；相关结果含 tag/列/Depth；上下文另有继承的 owner 信息 | Depth 不是任意 context 指针链的长度，更不是改写后语义 owner；先冻结解析 owner，再替换 |
| `base_binder.go:912–975`、`query_builder.go:5696` | 子查询绑定建立 context/block；`bindSelect` 设置 queryBlockOwner；返回带 NodeId/Child/RowSize 的 SubqueryRef | 需要显式记录每个 occurrence 与语义阶段，不以最终 root NodeId 代替 occurrence/owner |
| `flatten_subquery.go:292–380` | 按函数参数递归 flatten；结果变为 projected ColRef，并维护 memo/prepared metadata | 递归提到关系层不等于遵守运行时条件 mask；新路径不得丢失 prepared 类型或已有单次求值元数据 |
| `flatten_subquery.go:513`、`:3730–3799` | pull-up 会修改 child、过滤、projection/group key，之后才继续检查能否 flatten | 不可 catch NYI 后重用已修改的旧树作为 fallback |
| `utils.go:161–205` | 旧 decreaseDepth 就地修改；所读函数的递归分支是 Corr 和 F | 通用替换须完整覆盖表达式/子计划 DAG，不能靠重复减 Depth 来证明所有 consumer 的 owner 正确 |
| `flatten_subquery.go:58–123`、`query_builder.go:13322–13390` | INNER 的 subquery ON 提为 JOIN 上方 FILTER；LEFT/RIGHT 当前只能装饰一侧，拒绝深层与双侧 | 双侧 ON 必须在候选 pair 与匹配决定之间求值；不能在 NULL-extension 后过滤 |
| `flatten_subquery.go:612–625`、`:1389–1457` | scalar 深层仅有受限 aggregate 通路；行值比较有独立逻辑，易变行排序比较会拒绝重复求值 | 不仅需要 marker；需每个 occurrence 的基数断言、单次 operand 求值与行级三值比较 |
| `deep_existential.go:43–75` 及现有 RFC | pending existential 只处理受限两层、truth-only WHERE；最多八个 arm，有类型/关系限制 | 可保留为有证明的优化，不能当作本功能的通用执行语义或资源合同 |
| `query_builder.go:12142–12332` | 相关 FROM 只归一化透明 PROJECT/FILTER/scan、简单等值和邻近祖先 | 一般派生表组合必须处理 owner，而不是继续增加透明形状特例 |
| `bind_context.go:164–176`、`query_builder.go:4259–4371` | CTE 有 declaration context；集合运算有分支 contexts 并传播 correlation | CTE 定义的可见性与参数化求值环境不能取决于引用者偶然增加的 binding；集合去重不可跨环境 |
| `group_binder.go:384–406`、`flatten_subquery_cte_aggregate_test.go` | 有相关 GROUP BY 的专门拒绝；相关 CTE 聚合也有独立正/负向测试 | 必须区分聚合输入、输出分组、CTE 参数环境，不能一律使用基础表行号 |
| `colexec/evalExpression.go:1467–1545`、`:1731–1777` | CASE/COALESCE 使用 remaining/selected mask；IFF 也有专用 evaluator；一般函数递归求值参数，没有相同专用顺序短路约定 | 必须把子查询 demand 接到专用条件语义；不能推导出 WHERE/AND/OR 必须按文本顺序短路 |
| `colexec/evalExpression.go:293–529` | 普通 executor 不接受 Expr_Corr/Expr_Sub；prepared ParamRef 有 TEXT transport 适配 | 新相关参数不能借用 prepared TEXT ParamRef 通道；普通 executor 最终不能接残留 Corr/Sub |
| `colexec/apply/types.go:71–79`、`apply.go:42–210`、`compile.go:7476` | AppliedSource 是表函数/向量扫描的逐输入行生命周期；compileApply 直接读取右侧 TblFunc；不是任意关系子计划执行器 | “已有 APPLY 所以加 fallback 即可”不成立；必须设计子计划初始化、依赖、预算、终止与 generation |
| `compile.go:904–984`、`:9649–9711`、`internal/materialized/source.go` | 既有 CTE Source 属进程本地；Begin 在 scopes 启动前；runPipelineAttempt 在 scopes 静止后 Close；有 memory/spill/FD 账本 | 可借鉴物化/终止机制，但 reader/consumer 准入是 CTE 合同，不能无证明用于动态参数域或远端共享指针 |
| `query_builder.go:4011–4110`、`:11615–11628`、`:13646–13657` | 有规划阶段取消检查；appendNode 同时建立 context/tag 及 stats；本轮所读 appendNode 没有通用展开预算 | 新生成器须在每个生成/复制闭包中检查取消和预算，不能仅在阶段外层检查 |
| `bind_update.go:234–317`、`:595–620`、`build_delete.go:44–55`、`build_insert.go:159` | DML 复用 SELECT/flatten/createQuery；顺序 UPDATE 赋值使用上一投影的 row image | 同一机制须覆盖这些生产入口；不能把旧 row/新 row 或 statement 边界混用 |

既有设计证据：

- [物理前置修复](CLAUDE_correlated_subquery_physical_prerequisites.md)与 #29452 仅修 JOIN locality/WINDOW/CAST 问题，不是通用方案批准。
- [受限 existential 设计](../rfcs/20260907_deep_existential_decorrelation.md)及[验证报告](../rfcs/20260907_deep_existential_validation.md)显示 domain product、INNER witness fanout 与 repeated match-group 工作可能昂贵。作为算法反例复用，不能作为本新路径 exact-head 正确性或性能证据。
- 本轮可见本地/remote 分支名未找到另一条 #7559 通用实现；不据此宣称所有历史实验已搜尽。

## 3. 准确的求值等价合同

### 3.1 用语

- **BlockId/BindingId**：解析期冻结的 SQL 可见性与绑定身份；不是树的当前位置。
- **OccurrenceId**：一个语义求值位置。两个文本相同子查询不自动是同一 occurrence；既有 memo 的合法同次求值共享另有合同。
- **StageId**：关系阶段，如聚合输入、分组输出、连接候选 pair、窗口结果、DML 当前 row image。
- **EvaluationId**：某 occurrence 在该阶段一次需求求值的身份。建议键为 `(attempt generation, occurrence, stage, lineage token)`；token 由实际阶段产生，而非参数值或基础表 PK。
- **Environment**：按解析 owner 定位的 typed 参数 slot 集合，含 NULL、精度/scale、运行时合法表示及必要 session/statement 状态；中间 block 不读取某参数也须能传递给更深 block。
- **Demand**：consumer 当前要求求值的身份集合/row mask；它是运行时的语义需求，不是 WHERE 文本位置。

初始身份在协调 CN 的实际 stage/lane 内生成，不需要跨 CN identity 协议；slot 布局方向见第 18.3 节。精确 IR 字段、stage frame 的生命周期和资源 admission 仍属第 12 节阻塞项，此处不是已批准 protobuf。

### 3.2 允许与不允许的变换

对于确定性、充分排序且需求固定的语义片段，比较完整结果 bag、类型、NULL 与必要错误合同。对于非确定性片段，比较 SQL 允许的结果/求值轨迹集合及其必要性质，不能要求两个执行产生同一个随机值或同一个无完整排序 LIMIT 候选。

参数相同不能单独授权共享：还须证明 occurrence/需求/易变性/可失败表达式/快照与 statement 状态相容。初始一般路径不跨 EvaluationId 缓存结果。后续参数域去重只对证明稳定的区域启用，并始终保留 identity-to-domain 映射，重挂接时恢复原 bag multiplicity。

- 外连接的子查询 ON 输入是候选 `(left lineage, right lineage, ancestor environment)`；ON 全部不为 TRUE 后才决定一次 NULL-extension。NULL 参数值与“没有候选右行”是不同事实。
- 聚合前子查询按输入身份求值；聚合后按分组身份求值。全局空聚合仍有它的语义分组，不是虚构的基础表行。
- CASE/IF/COALESCE 按已经定义的 evaluator 语义建立需求；不执行未选分支的子查询，也不提前把其 scalar 多行错误暴露。
- WHERE conjunct/AND/OR 可按 SQL 和既有 optimizer 规则重排；不能写“左条件为 FALSE 则右子查询永远不执行”的固定 oracle。安全换序仍须保留必须惰性的控制流边界。
- 易变函数不要求跨两次执行相等，但同一次语义 operand 被行值比较展开时不能无授权重复求值。语句稳定与逐次易变必须区分；`CannotFold == false` 不足以证明 total、无状态或共享合法。
- EXISTS 的无用投影不要求求值；ANY/ALL 可按允许的决定条件提前结束。scalar 必须判断该需求身份最终是否超过一行，不把第二行归到别的参数，不能用全局 LIMIT 1 抹掉基数错误。
- 第一个匹配并不总能证明所有需要完成的 producer 已正常结束。短路、blocking aggregate、源副作用及晚到实质错误的合法可见性必须按执行边界定义，不把 StopSending 解释为整个查询成功。

### 3.3 子查询结果归约

每个 demanded EvaluationId 独立归约；行比较使用绑定后的比较 overload 和行值三值逻辑：

| 类别 | 语义摘要 |
|---|---|
| SCALAR/行标量 | 零行：各输出字段 NULL；一行：完整输出 tuple；多行：既有 cardinality 错误，发生在需求位置 |
| EXISTS/NOT EXISTS | 是否存在合法输入行，二值；取反；不能按内部 payload 重复次数复制外层行 |
| IN/ANY | 对每个完整比较结果做三值 OR：任一 TRUE 则 TRUE；否则存在 UNKNOWN 则 UNKNOWN；否则 FALSE；空集 FALSE |
| NOT IN | 对 IN 的三值结果取 NOT，不等于“没有 TRUE 就 TRUE” |
| ALL | 三值 AND：任一 FALSE 则 FALSE；否则存在 UNKNOWN 则 UNKNOWN；否则 TRUE；空集 TRUE |

行值 UNKNOWN 不是“任意字段 NULL”：例如 `(1,NULL) = (2,NULL)` 为 FALSE，`(1,NULL) = (1,NULL)` 为 UNKNOWN。行序比较先物化各 operand 的一次值，再按首个决定字段/NULL 规则比较。不能复用单个 key 的“右侧有 NULL”摘要证明多列 IN。

## 4. 方案比较与候选方向

| 方案 | 完整性/成本 | 生命周期与兼容性 | 结论 |
|---|---|---|---|
| A. 扩展既有 pull-up 特例 | 小步快，但深度、GROUP/窗口/外连接/条件需求互相组合，无法仅靠 guard 列表证明一般语义；重复树展开可能很大 | 少量改动也仍有就地修改和早 flatten 边界问题 | 不作为完整支持主体；保留已有证明的 fast path |
| B. 全量参数域 decorrelation | 依赖关系被转换为按域分区的关系代数，集合执行与批处理效率好；须覆盖聚合空组、WINDOW、集合运算、需求、身份和所有类型 | 域/source 多 consumer 与跨 CN 预算复杂；简单 `D × inner` 可达 O(D·I)；值去重不能处理任意易变位置 | 是重要优化方向，但单一无条件域产品不能满足全部合同 |
| C. 任意关系的 dependent/apply 执行 | 需求环境绑定到同一个已编译子程序，每个身份独立执行，易于保留分页、复杂关系与条件需求；一般最坏 O(E·W_inner) | 当前 APPLY 不具备它；需新增无前端递归、同 txn/snapshot 的子计划运行生命周期，避免 worker/receiver 互等 | 需要一般语义保障，不能“只是调用 Compile.Run” |
| D. 一个 dependency IR，证明式集合 lowering + 一般 dependent 执行 | 一般路径保证 B 不适用时仍能执行；优化路径避免普遍 N+1；两者共享作用域/需求/归约合同 | 比纯 planner rewrite 跨边界更多，但生产复杂性来自一个共同语义 owner，不是按 SQL 类别多个 fallback | **建议继续深化的方向，尚非批准决策** |

成熟先例：SQL 行子查询/三值逻辑与 MySQL 可见性是公开合同；[Unnesting Arbitrary Queries (2015)](https://15799.courses.cs.cmu.edu/spring2025/papers/11-unnesting/neumann-btw2015.pdf)、[Improving Unnesting of Complex Queries (2025)](https://15799.courses.cs.cmu.edu/spring2025/papers/11-unnesting/neumann-btw2025.pdf)可用于比较规则。文献不自动证明 MO 的 demand、参数类型、pipeline generation 或资源准入，需结合本仓库闭包落地。

D 的选择条件：C 的一般执行接口和终止闭包必须先设计清楚；不能先实现 B 的少数成功形状再将 C 无限延期。若调查证明 D 的总复杂性/开销不可接受，应在设计阶段重选，不以实现时继续 NYI 当作 fallback。

## 5. 候选端到端架构

本节定义拟议的共同语义边界；符号名不是已存在 API，编排细节须完成第 12 节才能批准。

```text
解析/名字绑定
  -> 冻结 owner、类型、occurrence、关系阶段和依赖图
  -> 保留未被一般机制消费的 typed region（不进入普通 optimizer）
  -> 需求/求值身份分析
  -> 无副作用的 lowering 选择
       -> 已证明等价的既有/域式集合计划
       -> 一般 dependent 子程序与 typed environment slots
  -> 一次性提交闭合计划 + 校验
  -> 常规优化（不得破坏 demand/身份屏障）
  -> compile：同事务/快照与独立 execution generation
  -> demand 环境执行/按身份归约/重挂接
  -> 正常 EOF / 实质错误 / cancel / reset
```

### 5.1 绑定和发布

1. 使用原始绑定身份定位祖先，完整收集 Node 表达式及 expression-owned subquery DAG 的依赖，包含 SubqueryRef.Child、tuple、窗口/frame、LIMIT/OFFSET、CTE/SourceStep 依赖。名解析错误先按现有合同返回；解析身份不在 rewrite 后重新猜测。
2. 仅在需要新机制的 region 建立惰性 registry/descriptor；普通无子查询和既有 simple fast path 不付出重复全图遍历。读分类不调用旧 mutating pull-up。
3. 不同 block 的通用 deferred state 可嵌套，不能沿用“一个 consumer 至多一个 pending child”限制。各 region 自己的 root、输入、occurrences 与 producer 依赖有显式 owner。
4. 发布前要求每个 slot 来源存在、每个子程序闭合、每个输出 consumer 对齐；普通 optimizer/remap/executor 不接 pending Expr_Sub/Corr。若采用正式 dependent IR，须合法地声明其字段为参数模板，而非让它偷偷绕过原校验。
5. 生成取消/失败丢弃 QueryBuilder-owned 候选；不得在部分发布后重走旧路径。证明式优化不适用应选择一般表示，不返回 NYI。

### 5.2 需求求值

- 输入已有批次 -> 根据 StageId 产生稳定 lineage token -> consumer 提交被需求的 identities 和 typed slots -> 执行/归约 -> 按 token 回填结果 vector；未需求位置不给子程序启动源。
- 需求必须能从 expression evaluator 的 row selection 传播至子查询执行。可以选择 demand-aware 关系节点或专门子查询 evaluator，但最终必须定义谁拥有上下文/子程序、运行权限及 Reset/Free；不能仅把 CASE 下子查询预投影成普通列。
- 参数 slots 用独立 typed 参数源，不复用 prepared ParamRef 的 TEXT 传输；每一列的 NULL/类型/必要 runtime provenance 与现有 vector/batch 表示对齐。
- 初始通用 dependent 执行每个 mutable owner 至多一个活动求值 generation，参数捕获串行窗口化；runner 不为每个 EvaluationId 新建 goroutine、parse/bind 或事务。普通 Scope/reader 内已有的短命 helper 也须纳入峰值和 churn 测量，不能把这条要求解释为现有调用链已经零 goroutine 创建。嵌套 generation 的父等待不得占用子任务需要的同池配额。
- 一般 dependent 模板编译一次（或在 reprepare 时重编译），但每次求值有独立 reader/operator state。原 relation 中语义上只求值一次的外层 stream 必须单次消费；重扫 inner 只能按合法语义执行，不能重扫 outer 以恢复结果。

### 5.3 外连接 ON 闭包

候选 pair 的需求发生在 outer join 判定匹配之前。推荐比较“join residual 内的 demand evaluator”和“候选 pair 流 + 按保存侧 lineage 重建 outer join”两种执行边界，不能先替换成 CROSS JOIN 再补 DISTINCT。

二者都必须满足：

- 左/右输入重复值用不同 lineage 保留；TRUE 候选原样输出；FALSE/UNKNOWN 不建立匹配。
- 没有候选或所有候选都失败时，LEFT/RIGHT 每个保存侧身份恰好 NULL-extension 一次；FULL 分别跟踪两侧。不能靠输出 payload 相等做去重。
- 单侧引用可以作为优化提前求值，但须证明 demand、错误和易变性不会改变。跨祖先参数由 environment 保持，不因 pair 中同时有两侧而丢失。
- 错误/cancel 直接终止整个查询；不能将失败 candidate 当作 FALSE 后继续 NULL-extension。
- 候选 pair 可用已证明安全的 key/residual 限制；无 key 的一般 O(L·R) 工作是合法算法代价，不构成拒绝条件。只窗口化候选和摘要，不一次性物化全部 pair。

**r2 已选 JOIN residual 边界，并将普通物理分支收敛到 HashJoin/LoopJoin（第 15 节）；专用连接、stage 接口、并行/spill 仍有阻塞，不能声称全部外连接闭环。**

### 5.4 聚合、窗口、CTE、集合运算和 DML

- 参数化关系的 filter/project 在每个 environment 内工作。集合 lowering 在需要的分组/窗口/去重 key 上加入 environment identity/domain，不把参数值误当 identity；operator 内部 grouping 相等与 SQL `=` 匹配是不同用途。
- 无 GROUP BY 空聚合需要为每个 demanded environment 产生其 aggregate empty-input 值，再运行 HAVING/投影；有 GROUP BY 的空输入没有组。一般 dependent 执行保持原算子顺序；集合 lowering 必须独立证明每一步空输入重建。
- WINDOW 的 PARTITION/ORDER/frame 在环境内计算；无完整排序 ties 的结果按合法集合验证。不是给所有窗口全局添一个基础表 PK。
- CTE 定义按照 declaration scope，依赖环境是其可见祖先的参数。稳定无依赖 CTE 可沿原共享规则；参数化 CTE 的多 consumer sharing 按环境与需求证明，不把整个语句所有环境混到一个现有 CTE Source。递归参数环境与 source generations 的细节待补。
- UNION ALL 保留环境内 bag；DISTINCT/INTERSECT/MINUS 只在该环境内按当前类型规则执行，不把跨环境相同行合并。最后 ordering/limit 仍是每环境语义。
- DML 同一 SELECT/赋值语义机制、同 statement txn/snapshot/权限，不开启内部 SQL session。顺序 UPDATE 的 rhs slots 指向该阶段当前 row image；写节点仍由原 DML pipeline 唯一拥有，子查询程序只计算其合法结果。

## 6. 所有权、状态、终止与上界

下面是新机制必须满足的模型，不是对现有全仓库作出无缺陷结论。

| 资源/状态 | 创建/持有 owner | 正常终止 | 失败/取消/部分初始化/重用 |
|---|---|---|---|
| 绑定 descriptor/slot schema | QueryBuilder | 提交不可变计划后结束 | 候选丢弃，无后台任务，无 query 外缓存 |
| 子程序模板 | 编译计划 owner | prepare 生命周期内只读 | reprepare 替换；不保留上次执行 reader/参数 |
| environment 批次/token/结果 | execution attempt 下的 region runner | 按窗口回填后释放 | 终止后不再发布，配套释放批次与 allocation reservation |
| 需求求值 generation | region runner | 输入 EOF、必要归约完成 | 独立 cancel child；失败首因保留；join 静止后回收，不以 reset 新代覆盖旧代 |
| reader/operator state | 子程序执行 owner | EOF 后 Reset/Free | 初始化到哪层，立即注册哪层清理；旧 callback/handle 带 generation，迟到不可写入新代 |
| 物化/spill 与 reader | attempt-owned producer/reader | producer Finish + reader release，最终 Close | 配额逐级 rollback；未启动 producer/reader 也由 attempt 安全网关闭；不重复不可逆释放 |
| remote receivers/streams | compile/dispatch registration owner | 既有 EOF 与 receiver completion | 查询 cancel/connection error/receiver-local stop 区分；子程序结束不能吞掉实质远端错误 |

拟议 generation 状态：`Created -> Admitted -> Ready -> Running -> Draining -> Quiesced -> Released`。成功、error、cancel、panic/初始化失败都必须达到 Quiesced/Released；它们是不同终态原因。新 generation 只有在旧代静止并释放后才发布。

Q1：每项 acquire 立即建立唯一有效清理责任，transfer 和共享 reader ref 明确计数。Q2：需求 evaluator 等子程序、子程序等输入/remote 接收、producer 等 credit 的每条等待边都有独立 cancel/terminal；控制路径不能等待被它控制的 worker 才取得锁。Q3：环境窗口、pair 窗口、结果、节点/表达式复制、reader 数、spill metadata/FD 均有可计量上界。

本轮 Q1–Q3 调查状态：materialized、pipeline terminal、lazy completion、Scope cleanup 与 sequence/insert-ID 锁 owner 已定位；第 13、15、18 节记录必须切断的等待边。新 runner 的 task admission/完整 operator 复用和专用 JOIN 图仍待关闭，不报告未经完整图验证的 leak/hang bug。

## 7. 规划预算与资源错误

### 7.1 规划阶段

规划预算属 QueryBuilder，本需求新增 region 的消耗须计量：

- 实際生成的 node 数、expression atom 数、dependency/slot descriptor 数与保留字节；计量包含复制和候选搜索，失败候选释放 retained-byte 账本但不抹掉已发生的累计工作。
- 进入生成/复制前检查当前已消费量与本次实际申请；每轮图遍历/生成循环及大块复制有取消检查；避免只在 createQuery 阶段边界检查。新增闭包采用有界迭代工作栈，不依赖固定相关深度来保护 Go 栈。
- 参数域优化预计展开过大时直接选择紧凑的一般 dependent 表示；预测 cost 高不是资源报错理由。若候选自己的搜索预算耗尽但一般表示可构造，也不终止查询。
- 只有查询本身构造/执行所需的真实预算申请触达批准的容量，才返回具备 resource/requested/used/cap 的通用资源错误；取消保留 query cancellation 原因。

默认节点/表达式/字节预算与检查粒度必须用基线成功 workload、表示宽度和 allocation 数据校准，**目前未确定数值，属于设计批准阻塞项**。不能将既有 `maxExistentialArms=8` 当作该预算；第九个 arm 也有一般正确路径。

### 7.2 执行阶段

- 每个 attempt 使用现有 query/CN ExecutionResourceBudget、allocation account 与 spill disk/FD ledger。资源错误须区分 memory、disk、FD；不能将磁盘不足重试成“缩 batch”无限循环。
- 窗口主动分批/回压，超出可保留内存采用合法 spill 或更小窗口；单行宽度/schema 与序列化也有实际 admission。即使估计 E·I 很大仍可执行到真实预算/timeout/cancel，不能静态把复杂 query 拒绝。
- 定义新增 allocation owner/site 而非伪装成 CTE allocation；记账、observer 标签和 spill reservation 要随消费者一起更新。不能只统计 vector 而忽略 token/map/Go heap/reader metadata。
- 现有 materialized per-source 64 MiB retention/64 MiB decoded record 是复用机制的事实，不是本功能整个查询的内存预算，更不证明任意参数域准入已通过。
- r2 初始每 owner 串行、每次捕获一个环境；但 `Eval` 必须返回与当前输入 N 行对齐的结果，不能据此宣称结果只保留 B 行。stage/memo 与 aligned result 的真实成本见第 16 节；task/window/default budget 与 boundary 注入方案仍待关闭。

## 8. 性能与容量模型、验收标准

### 8.1 成本变量

令 E 为实际 demanded 求值身份数，D 为**有共享证明**的参数域数（D <= E），I 为内层输入行数，W_inner 为一个环境内工作，M 为匹配 payload 行数，B 为环境/pair 窗口行数，K 为 typed slot 宽度，N 为 caller 当前 frame 的逻辑输入行数，P 为仍必须保留的输入/物化字节。N 不由 B 决定；全部活动 owner 的峰值须取和，见第 16.1 节。

| 路径 | 工作/中间基数 | 内存与传输合同 |
|---|---|---|
| 一般 dependent | 最坏 Σ W_inner(e)，非选择性约 O(E·I)；输出 scalar/marker 至多 E，关系子查询按其合同输出 | live 环境 O(B·K)，aligned 结果/memo O(N·(输出宽度+memo 状态宽度))，加活动 inner 算子/spill；不得同时保留 E 套 reader/operator |
| 证明式等值域/SEMI | 在有 hash 可用性的证明下，约 O(E + D + I + M_required)；existential 不枚举所有重复 witness | 域/映射 O(D·K + E·token width) 需窗口/spill；声明 map 累积成本，不能仅引用最终输出 E |
| 非等值域 | 最坏 O(D·I)，不能谎称所有 decorrelation 线性 | 分批域/pair，不完整物化 product；传输按实际批次计量 |
| ON 双侧 | 无 key 最坏 O(L·R + Σ W_sub(pair))，safe key 可降低候选 | pair 只保留 B；保存侧匹配摘要/身份也计入预算；FULL 两侧分别计量 |
| 按窗口远端执行 | 一般上界包含重复 inner scan/请求与窗口数 ceil(E/B) | `parameter bytes + result bytes + actual scanned/transferred inner bytes + protocol/credit overhead`；不能仅统计参数/最终结果 |

是否共享 P 必须有 demand/totality/快照证明；不能为了降成本把原本按需求执行的易变/fallible 源强制一次全量物化。初始多 CN 方案必须明确是否有远端反复扫描以及量级，不把“扫描仍 remote”当作网络效率结论。

### 8.2 提议的回归门禁（待纳入批准修订）

- 既有成功 SQL 选择：普通无子查询、浅层 scalar/IN/EXISTS、旧成功多层 aggregate/existential、行值、JOIN ON、CTE、多 CN 与 DML 控制；baseline/candidate 同工具链、拓扑、数据、session、索引/statistics。
- 无子查询路径不创建 dependency registry/runner；既有证明式 path 不引入 N+1、额外外层 scan 或全关系 domain product。计划比较使用稳定 operator/scan/runtime-filter 合同，不冻结 NodeId/文本排版。
- 对不需要改语义路径的控制，planner nodes/expr/allocation 不增加；确需共同验证成本须在批准设计中明确绝对值和成本来源。规划与执行 latency 分开测，不能用新查询原来 NYI 的报错耗时作 baseline。
- 候选 latency 门槛：成对轮转 warm benchmark 的相对回归 95% 区间上界不超过 5%；小查询批量测 per-statement，不用一次毫秒比例。若区间宽于门槛，应改进测量，不能当作通过。该数字目前是提议，不是已有性能 PASS。
- 对新增集合高效类，比较最优已证明等价的 SEMI/显式关系参考，检查扫描、CPU、重复 key fanout、内存峰值、spill 与多 CN bytes。对必须按身份求值的易变/demand 类，比较语义相同的逐环境参考与 B 窗口扩展模型，不强行与不等价的按值共享方案比较。
- 1x/4x 的 E、D、I，重复 key、NULL、高/低 NDV、outer 选择性与 pair 候选增长；使用 benchmark 层，不把大数据/时间阈值加入普通 BVT。
- 有界性 UT 用小预算的 N-1/N/N+1、cancel barrier、spill/FD 故障注入验证。性能测量记录 planner 分配、execution accounted peak/必要 RSS、scan、spill、网络实际计数；累计输出 bytes 不证明峰值内存。

窗口默认值、预算数值、既有控制 corpus 与允许的新增共同检查开销须补齐后批准；不能仅写“必要性能检查”。

## 9. 兼容性、安全与运维

- 逻辑新 IR/typed environment 的 wire/plan 持久化策略必须明确。禁止未经协议版本门禁把新 operator/表达式送旧 CN；最稳候选是新 demand owner 保持协调 CN 本地，只把旧 CN 能执行的闭合普通子计划和现有 batch 传输发往远端。该候选仍须验证必要 runtime slot 如何进入远端，不是已经成立的无 wire 变更方案。
- 若需要新 plan/pipeline field，修改 proto 源并通过工具生成，更新版本/capability gate 与所有编码/解码/复制/remap/explain 消费者；旧字段不能偷存字符串 SQL。保留混合版本的正确 local execution，不能新 query 仅因 worker 老版本变回 correlation NYI。
- 不新增 catalog/on-disk 格式是设计目标，但须审计持久 View、prepared plan、缓存和协议消费者后确认；reprepare 按现有版本/依赖失效规则创建新 generation。临时 spill 不作为恢复持久状态。
- 权限、tenant/account、snapshot、对象引用和 DML statement offset 沿原始 bound dependency 传递；禁止用内部 SQL 文本或新 session 绕过权限/事务。物化仅 query-owned，不做跨用户缓存，参数不写日志/metric 标签。
- explain/诊断应显示 occurrence/阶段、所选集合/dependent 路径、共享证明类别与估计/实际 E/D/work/资源；指标只有固定 owner/event 维度，不引入 query/参数高基数标签。
- 回滚旧 binary 后新能力自然不再可用，但旧 query/catalog 不应损坏；新 plan 不允许被旧 executor 误读。具体 rollout/default 与回退的协议方案仍为设计阻塞项，不能用 feature flag 默认关闭来宣称完整支持。

## 10. 验证矩阵与原始回归

### 10.1 最便宜的独立证据

| 合同 | focused typed UT | 公共 oracle/额外证据 |
|---|---|---|
| owner/slot/多级跳跃/遮蔽 | owner 图、完整 Expr visitor、提交原子性、取消 | 最小三个及更深层 SQL；计划不得残留不可执行参数；确定性结果 bag/类型 |
| 行值与量化 | 每个身份比较/归约 truth table；一次 operand 值 | 二列 IN/NOT IN，FALSE-vs-UNKNOWN 对照；scalar 零/一/多行；ANY/ALL 空集 |
| demand 与共享 | selection mask + 脚本化可失败/易变 evaluator，不睡眠 | CASE/IF/COALESCE 未需求多行错误不暴露，需求分支正确报错；不写文本 AND 短路断言 |
| 外连接 ON | pair/matched/NULL-extension 状态，双侧和祖先 slots | duplicates、无候选/全失败/部分成功、UNKNOWN、LEFT/RIGHT/FULL，输出完整 bag |
| 聚合阶段/窗口 | group identity、empty aggregate canonical values、每环境 partition | COUNT/COALESCE/HAVING/嵌套聚合、窗口、相关 GROUP BY；独立手算/参考 SQL |
| CTE/集合/DML | declaration owner、各分支参数/去重、row image/offset | CTE 多引用，UNION/INTERSECT/MINUS，INSERT/UPDATE/DELETE 结果与错误原子性 |
| generation/资源/remote | 部分初始化/迟到/释放/预算/receiver typed terminal | prepare/reuse、多 CN 实际 placement、cancel/error 后健康与资源余额；必要 race/fault |
| 规划/执行性能 | 紧凑表示展开上界与预算边界 | 第 8 节配对 benchmark 与真实传输/峰值，无无谓新 fixture |

使用已有 planner、row_scalar、outer_join、CTE、sqlintegration/multicn、dispatch/materialized fixtures；每个新案例必须贡献新的语义 cell。所有选择非空，模式/工具链/head/终态记录；本轮不实现或执行新功能测试；第 16.3 节保留历史既有 planner baseline，第 19.1 节是当前 Go 1.27.1 的既有 planner baseline。

### 10.2 原始二列 IN 硬验收

源：`test/distributed/cases/subquery/subquery-with-in.sql:1031–1045`。对应 result 在**同一 cases 目录**的 `subquery-with-in.result:1373–1385`，不是 `test/distributed/results/`。

现状：SQL 带 `@bvt:issue#7559` 跳过；result 保留 `unsupported expression executor ... corr ... depth:1`。这是旧 golden 文件事实，不是本轮复现的当前错误码或完整根因定位。

原 fixture：`b` 的 key 是 7，`bb` 的唯一 pk 是 10，因此 parent 的 `pk = outer key` 永远不命中；原 SQL 的正确结果是空集。`cc` 有两个自等字符行，`c` 包含 outer 的 `'f'`；关联是否正确不能仅用空集证明。

实现验收必须：

1. 移除这组 skip delimiters，保留原 SQL 和空结果合同；以 mo-tester 生成并审查 result，再正常比较实际执行。
2. 同形命中对照：在该 fixture 的隔离段加入 `bb (7,'2002-02-22')`，原 SQL 应得到一行 `col_int_key=7`。`cc` 的两个 witness 不能把 IN 的外层行复制两次。
3. 同形未命中对照：改变外层字符或 key，结果应恢复为空；需要 NULL/重复/不同字段的二列 IN 真值对照，不能只把 `(key,key)` 当作全部 row semantics 证据。
4. 必要时用独立小 fixture 验证外层 duplicate bag；该表当前 pk 不容纳重复 PK，但不同 PK 可有相同参数。
5. 恢复/清理本段对象；BVT 结果由 mo-tester 生成+独立 oracle 核对+普通模式两次比较，并断言 teardown。SQL 注释不新增 issue 号。

原 SQL 已有 ORDER BY，但无 LIMIT 且结果为空/单个外层行，此验收不涉及 ties 的跨执行相等问题。

## 11. 全系列变更/风险图

具体新增文件与接口待选型，以下是预计的完整消费闭包，不是已修改 diff。

| 闭包 | owner/消费者 | 风险 | 必需证据 |
|---|---|---|---|
| bind + dependency/typed slots | baseBinder/BindContext/各 clause 与 DML binder -> region | R2；复杂生成为 R3 | scope/type UT、合法/非法对照、取消/预算 |
| 通用 IR/提交 + 优化边界 | QueryBuilder -> optimizer/remap/copy/stats/explain/prepared | R3：公共行为、热路径 | graph 完整性、原子发布、plan/control benchmark、公共 SQL |
| demand/归约与 operand memo | expression consumer + region runner -> scalar/row/marker output | R3：求值/错误/生命周期 | masks/三值/基数 UT、公开分支/行值/类型 |
| ON candidate/matched owner | JOIN 生产者 -> demand -> outer match/extension | R3：基数/身份/控制 | full pair closure、小状态 UT、LEFT/RIGHT/FULL BVT、性能 |
| 编译/一般子程序执行 | Compile -> reader/operators/txn/process -> Reset/Free | R3：新执行 generation | init/cancel/error/reuse、Q1–Q3、owning packages/race |
| source/account/remote 协议 | producer -> materialized/receiver/credit -> terminal | R3：资源/多 CN/兼容性 | admission/spill/FD/迟到/stop/error、实际 multi-CN、版本 |
| schema/proto 与全部 reader | proto 源 -> 生成/编码/解码/feature gate | 若选择新增则 R3 | 各 consumer/build/roundtrip/混合版本；不手改生成文件 |
| UT/BVT/benchmark | 最便宜 fixture -> 独立 oracle | R1，分布式 fixture 自身含 R3 | 原始回归非空选择、结果生成复核与比较、生命周期/性能测量 |
| 本设计/TODO | 用户 review | R0 | 文档链接、差异检查；TODO 不提交 |

无需重复每个文件做全仓库扫描；按此图对关键 owner、反向消费者和终态追踪。现有测试代码不因本轮设计被改动。

## 12. r3 决策登记与剩余阻塞

这些是**开发侧的待决实现问题，不是要求用户重新限定需求**。本修订供设计 review，保留 r2 的 process-copy/B-only 修正并增加第 19 节的新证据；未关闭的问题不能由“请继续到 PR”自动变成已批准接口。

| ID | 当前建议方向/源证据（尚未批准） | 仍阻塞实施批准的具体闭包 |
|---|---|---|
| D1 | 借用同一个 statement Base，以显式 execution domain 隔离可变执行态；保留 sequence/insert-ID 锁与语句副作用；关系模板编译一次，见第 13、18 节 | task/CTE/lazy/cleanup 的完整可实施图；全部非 TP Prepare/Reset/Free、初始化/元数据锁和 runtime allocation 编排；必须隔离 program-owned Steps，不能启动所有 Query.Steps；不可用 Base 浅拷贝填缺项 |
| D2 | 空 selection 零启动；dependent/slot 结果显式 row-aligned；只在当前 stage frame memo；终态接 typed pipeline outcome，见第 14 节 | 所有 optimizer/fold/memo/cast 与 expression owner 闭包、stage frame 的具体传递/复用接口；不新建文本顺序短路合同 |
| D3 | 普通连接物理实现是 HashJoin/LoopJoin；candidate residual 在 matched update 前，LoopJoin 保留跨输出恢复的 condition window，见第 15 节 | 每个 join type 的 pair cursor、semi/anti/single/mark、bitmap mailbox 与 spill；ASOF/DEDUP 等专用匹配合同，不擅自排除 |
| D4 | native provenance 检查与当前 Go 1.27.1 的 7×3 既有 planner 基线已成功；每 owner 串行、capture window=1，结果/memo 按 N 计量，见第 16/19 节 | planner/default budgets、task/Go-stack 真实计量、stage 输入窗口与 old-success 门槛；当前 latency 波动大，不与旧 head/Go 直接比较；只有 baseline 不能证明 candidate 通过 |
| D5 | `DeepCopyQuery`、`VisitExprTree`、structural hash 与 `generatePipeline`/grouping transport 已定位；本地 placement 不能阻止 whole Query 发送，见第 17/19 节 | Step ownership/源码索引、全部 copy/remap/cache/prepare/View、所有附带 Plan 的 remote 边界与具体 capability；旧 CN 必须得到真正闭合的普通 fragment |
| D6 | interval grammar 实为 `INTERVAL expression unit`；frame binder 会 bind-time 求值/改写，不能因此排除合法依赖；DML 保留独立 route/assignment/row-default stage | frame 的公开合法性对照与 runtime owner；ODKU assignment version、multi-insert demand、REPLACE default dependency 的精确构造与验证矩阵 |

设计门禁记录：

```text
范围：#7559 完整功能及全部 PR 系列。
触发：跨 planner/compile/executor，公共求值合同、分布式 generation 与资源/热路径。
设计：本文件 r3，设计先行 draft，未批准。
决策：BLOCKED FOR IMPLEMENTATION（上述闭包未关闭）。允许发布纯设计稿 review，禁止将发布它称为实现批准或 #7559 完成。
已采纳：用户四项补充全部进入合同/批准条件/交付门禁。
实现偏离：无（未开始实现）。
证据：历史记录见第 16.3 节；当前源码/Go 1.27.1 与 7×3 baseline PASS 见第 19 节。无新功能 SQL/UT/BVT/candidate 性能证据。
```

## 13. r2：一般子程序与执行代

### 13.1 排除不能直接复用的接口

源码约束：

- `Compile.Reset` 只允许 prepared TP 拓扑，并重置 query/statement 状态；它不是每行子查询 API。`compileQuery` 对 prepared 非 TP 也有拒绝，不能设置 `isPrepare=true` 假装通用复用。
- `Scope.resetForReuse` 清 reader/remote filter，重置 pipeline edge terminal、snapshot 和 analyzer；单独调用它并不完成 operator Reset/Free、旧 worker join 或 producer Close。
- `Scope.Run` 的 pipeline cleanup 和 reader Close 是已有 owner。不能同时让 runner 再关闭同一 reader，或在 Run 返回前清空其字段。
- 并行动态 clones 由 `parallelGenerations` 持有至分析完成，`Scope.Reset` 再释放。若每环境保留一套 clone 至顶层语句结束，会随 E 累积；每环境分析/释放的边界必须在新 owner 内完成。
- pipeline child 通常共享 `BaseProcess`。`NewViewBindingProcess` 的独立 Base 仅适合 binding，不能直接作执行子进程：sequence gate、insert-ID mutex、user-lock identity 与会话副作用必须共享同一个 owner，复制 map/pointer 而复制或遗漏锁会破坏同步。r2 选择同一 statement Base + 显式 execution domain，完整字段责任见第 18 节。
- `MergeRun` 用 `ants.Submit`、结果 channel、completion 与 cancel 组织 PreScopes。不得在持有一个有界池 worker 的父任务内依赖同池排队子任务；源码存在 Submit 不等于当前池配置已证明会死锁。

### 13.2 接口草案与线性化点

以下是拟议 API 合同，不是已经存在或已批准的 Go 实现：

```text
ProgramTemplate = CompileDependent(bound region, slot schema, snapshot contract)
ProgramOwner    = OpenAttempt(template, statement execution services)
EvalGeneration  = Begin(owner, EvaluationId, immutable typed environment)
Result          = Evaluate(generation, reducer contract)
Terminal        = Finish(generation, EOF | decision-stop | error | cancel)
                  -> cancel/stop owned producers
                  -> wait all started scope tasks/receivers
                  -> collect real terminal causes
                  -> Reset reusable operators / Free nonreusable state
                  -> release environment and reservations
Close(owner)    = join any generation -> release borrowed template reference
                  -> free owned domain/task state (不 Free shared Base/prepare plan)
```

- 模板含闭合关系及所有 expression-owned 子程序 roots；compile 一次，reprepare 才重新 bind/compile。Begin 可重建 reader、edge、runtime filter、物化 generation 和不能复用的执行状态，**不得重新 parse/bind/compile 整个 SQL**。
- `Begin` 发布点之前完成参数借用/复制、资源 admission、清理登记和 receiver 初始化；此后参数不可被父 batch 重用覆盖。初始化第 k 步失败，只回滚已 acquire 的 1..k，不向 consumer 发布可执行 handle。
- `Finish -> Quiesced` 是静止点。只有所有该代 sender/receiver/task 停止并归还借用后，才能重置 edge、reader 或接收新参数。旧 generation 的 completion 不能设置新代 EOF。
- `Evaluate` 的归约结果在 child cleanup 前复制/转移到 caller stage 的 owned result；不能返回借用 child batch 然后在 Finish 中将它 Reset/Free。scalar 第一 tuple、比较 lhs/必要 metadata 的拥有责任即时登记；只有 Finish 收齐终态成功后才标记该 identity 的结果可发布。若后续出现第二行或 producer 实质错误，丢弃尚未发布的结果，不将部分值当成功。
- owner 与 generation 不拥有 txn commit/rollback、语句 offset、顶层 result writer、全局 found_rows 更新或顶层 MessageBoard Reset。锁/读服务沿原 statement contract 借用；程序只读其合法关系，不能经新 session 发 SQL。
- 模板存只读 schema/算子构造描述；执行状态只属于一个 active generation。初始含 general dependent 的需求 stage 和程序内决策算子采用本地单 lane，普通输入仍可并行 gather；不能借这个限制改变 SQL 结果。未来 DOP 必须各 lane 独立 owner/epoch，禁止 clone 裸共享 executor。模板内普通 Scope 的并行展开初始不启用，避免每环境累积 parallelGenerations；这不消除多 PreScope/CTE 的调度与预算责任。
- EOF、实质错误、未启动、decision-stop 为不同事实。先记录终态，再 cleanup；保留原 `moerr` 类型，不能用任意 `errors.Join` 包裹孤立错误导致 wire 错误类别改变。

### 13.3 调度候选与等待图

r1 优先调查 **attempt-owned、按物理 Scope 的可重用任务槽**：任务槽数量与活动物理拓扑有关，不与 E 或参数不同值数有关；父等待子程序不能占用子程序所需的任务槽。任务槽不属于新全局池，不做每 EvaluationId 一次 goroutine 创建。

```text
parent consumer
  -> demand evaluator -> child generation completion
child root task
  -> its PreScope receiver / child demand completion / CTE producer completion
producer tasks
  -> bounded data credit / storage read / cancellation
cleanup controller
  -> broadcast stop/cancel (不持 owner lock)
  -> join started tasks
  -> drain errors/borrowed batches
  -> release
```

必须满足的切割规则：

1. child 环境只携带已求值参数，不携带需要父 consumer 继续推进才能产生的 live pipeline 输入。
2. child 引用的 source producer 要么归该程序闭包所有，要么是已有共享证明允许的独立 producer；不能为了共享节省扫描引入 `parent waits child -> child waits parent output` 环。
3. parent/child scope 不复用同一个 receiver、MessageBoard generation 或 owner lock；completion 发布有单独终态槽，不能等满 data queue 才发布 EOF/error。
4. StopSending/credit 中断必须让 producer 退出并回收未交付批次；cleanup 自己有取消/远端 cleanup deadline，不等待数据 consumer 恢复读。
5. 递归 CTE 的循环是其专用 fixed-point/iteration 协议，不是允许任意跨程序循环等待的理由；其 iteration source 与子查询 environment 值必须分离。

**尚未关闭**：这个调度候选能否以最小 Scope 执行扩展覆盖 lazy UNION、递归 CTE、remote 通知及全部动态 clones；能否复用全部非 TP state；task/stack 元数据如何真实 admission。不能将它写成“已有 scheduler 可以直接用”。若任务槽复杂度不低于显式协作式 operator continuation，须在批准前重选，而非实现中临时加全局池。

## 14. r2：demand executor、typed slots 与归约

### 14.1 选型与身份

选用专用 dependent expression executor：保留子查询在表达式控制流中的位置，最终执行 `Eval(proc, batches, selectList)` 时才提交需求。普通 relational flatten 仅适用于有需求/错误/易变等价证明的 region，不能只因 flatten 成功就提前 CASE 分支。

拟议身份分层：

```text
TemplateId/OccurrenceId/StageId : 计划内编号
AttemptId                     : 顶层执行重试/prepare execute 的代
OwnerLaneId                   : 本 attempt 中一个 mutable runner 的身份
InvocationSeq                 : owner 单调调用序号
LineageSeq                    : 当前 stage 单调身份，不复用参数值/PK
EvaluationKey                 : (AttemptId, OwnerLaneId, OccurrenceId, StageId, LineageSeq)
GenerationHandle              : (AttemptId, OwnerLaneId, InvocationSeq)
```

初始通用 owner 在协调 CN 本地，不需要跨 CN 分配 EvaluationKey；普通远端输入合并至真实 stage 后再生成 lineage。投影/过滤传递 stage 身份，聚合、candidate pair、DML assignment version 生成对应新 stage 身份。计数器实际溢出返回明确资源/身份容量错误，绝不回绕；不设固定关联层数。

slot schema 为 `(SlotOrdinal, BindingOwner, ColumnOrdinal, full plan.Type, runtime representation contract)`；r2 选择每个 program 的编译期闭合 flat slot array，见第 18.3 节。保存 native vector/NULL 与必要 provenance，不复用 ParamRef position/TEXT；未读但更深 child 需要的祖先 slot 由显式 forwarding descriptor 传递，不在运行时递归减 Depth 或按值查 map。

### 14.2 求值伪代码与单次 operand

```text
Eval(input, mask):
  identities = stage.identities(input, mask)
  for each demanded identity in bounded window:
    evaluate each left/row operand once for this occurrence
    bind/capture typed ancestor slots
    begin program generation
    reduce complete row comparisons / scalar cardinality
    finish + join terminal protocol
    publish one typed result for this identity
  return result vector with ordinary evaluator selection contract
```

- `mask` 必须遵循 EvalCase/EvalIff/EvalCoalesce 的真实 selection 约定。CASE/COALESCE **仍可能调用 executor 并传全 FALSE mask**；dependent leaf 必须自己识别空需求，不能只依赖 caller 不调用。空需求不 Begin、不启动 reader/producer，也不制造 scalar cardinality 错误。
- 对 N 行输入返回 N 行逻辑长度，未选位置按现有 contract 不消费；dependent/slot executor 必须加入父 function 的 input-row-aligned 分类，不能将部分需求 compact result 当第 0 行常量，或沿用非 row-aligned executor 的选择补齐方式重复求值。
- expression 返回值只读，有效期截至下一次 `Eval`/Reset；capture、memo 与结果 transfer 在此之前完成。比较展开、prepared cast wrapper 和已有 single-evaluation metadata 引用同一次 operand 的已求值 native tuple。
- stage frame 由真实计算算子/lane 持有，同一保留输入的多次 mask 求值/输出恢复保持 epoch 与 row identity；新输入/assignment/group/pair 才推进对应 cursor。memo 只保留当前 frame 的结果与 demanded/evaluated 状态，代价计入 N；不按参数值、batch 指针或偶然相同 payload 共享，也不保留历史 E map。
- ordinary AND/OR 仍按既有 evaluator/optimizer 合同，不引入“文本左边决定永不执行右边”的保证。专用条件 demand 屏障则不能被 optimizer 提到分支外。
- JOIN residual 可能只有一个 probe 行和一个 build 行；slot binder 按显式 left/right 映射读取，不假定两批同长度、不误用 nil selection 意味着全 build 批需求。
- scalar cardinality 只对该 identity 计数；在实际产生第二行时锁定 cardinality 错误。行排序比较先取一次完整 tuple，再运行首个决定字段算法，不能重新执行 volatile scalar operand。

### 14.3 早停与错误可见性

归约器将“真值已经决定”与“程序完成”分开：

| consumer | 决定条件 | 不能省略的终态 |
|---|---|---|
| scalar/行 scalar | EOF 后零/一行；实际第二行必错 | 一行不等于 EOF；必须等待真实第二行/EOF，不能全局 LIMIT 1 |
| EXISTS | 产生第一行；空集须 EOF | 无用 projection 不求值；已启动必要 producer 的实质错误不能被本地 stop 覆盖 |
| IN/ANY | 一个完整比较 TRUE | 若无 TRUE，EOF 后根据 UNKNOWN 摘要判定；NULL 字段本身不等于 tuple UNKNOWN |
| ALL | 一个完整比较 FALSE | 若无 FALSE，EOF 后按 UNKNOWN/空集规则判定 |
| NOT IN/NOT EXISTS | 先运行对应归约，再按三值/二值取反 | 不能用“未找到 TRUE”在 EOF 前判定 NOT IN 为 TRUE |

终态接现有协议，不以 error channel 的到达次序推断因果：

| 状态/动作 | 对应 owner/线性化点 | r2 合同 |
|---|---|---|
| 未需求/未启动 lazy branch | 空 mask；`cleanScopeTreeWithStartFail` 的未提交责任 | 没有该分支的 SQL 错误要求；仍退还静态注册的 edge/reader 责任 |
| 实质错误记录 | `newScopeRunResultForContext` + `MarkPipelineFailure`；receiver typed Error/Abort | 在发布 completion 前冻结；budget/cardinality/读错误不可当作普通 cancel |
| 归约决定 | 请求本 generation pipeline 的 `ErrPipelineStopped` | 不取消仍活着的 statement/domain query context，不重置结果，也不覆盖已冻结错误 |
| 成功 stop 的取消回声 | `normalizeScopeRunError`，query context 仍 live | 仅可归因于该 pipeline stop 的 cancellation 被消去；独立失败叶不能一起消去 |
| 外部取消/超时 | query/parent context | deadline/cancel 保持原归属；先发生 local stop 也不能掩盖后来生效的 query deadline |
| 完成/join | lazy completion 的 done、MergeRun 的 wg 与 `collectMergeRunResults` | completion 证明 Run/cleanup 已结束；仅 `wait(ctx)` 返回不证明分支静止；所有已启动任务结果收齐后才发布最终 Result |
| 次生 cleanup 错误 | End delivery fallback、allocation lifecycle join | 保留实际 producer 首因/txn retry；孤立具体 moerr 不用任意 errors.Join 改 wire 类别 |

Finish 的顺序是 request stop/cancel -> join -> collect frozen outcomes -> Reset/Free -> release slots；query-context 的最终 cleanup cancel 放在收齐结果之后。Blocking aggregate 的必要输入已开始并须完成/仲裁；未启动的 lazy UNION arm 不被强行启动。代码源：`compile.go` 的 scope-result/normalization/arbitration，`scope.go` 的 sibling cancel/结果回收/start-fail，`lazy_branch_completion.go` 和 `process_spoolr.go`。现有 helper 提供协议，不等于新的 runner 已实现；所有 stage/remote 边界仍须按第 12 节完成接口接入。

## 15. r2：外连接 ON 选 JOIN residual 内求值

### 15.1 为什么不选完整 pair 流重建

`hashjoin/join.go` 的残余条件在候选 pair 检查时已经拿到两侧 batch；匹配判断尚未完成。把 dependent executor 放在这一边界，可复用已有匹配与 NULL-extension owner，而不是新增产品物化、保存侧去重、FULL matched bitmap 与 spill 重建框架。r0 第 5.3 节的两个选项在 r1 中优先选择前者。

这不是“HashJoin 已经支持一般子查询”：目前残余 Eval 常传 `selectList=nil`，ordinary executor 不接受 Sub/Corr，仍须更新精确 pair demand、process/runner owner 和各 join 物理分支。

### 15.2 匹配合同

```text
for each existing candidate pair (leftIdentity, rightIdentity):
  pairIdentity = new stage lineage
  evaluate ON residual with [left row, right row, frozen ancestor environment]
  error/cancel -> abort (不是 FALSE)
  TRUE         -> current join semantics marks matched/emits pair
  FALSE/NULL   -> not matched
after candidate set exhausted:
  LEFT/RIGHT/FULL use existing preserved-side matched state for NULL-extension
```

- demand 在 matched update 之前；没有 candidate 就没有虚构 ON 求值。保存侧 unmatched 行的 NULL-extension 不再次运行 ON 子查询。
- equal left/right payload 不是同一身份；FULL 对未匹配两侧分别处理，不能把“右侧字段全 NULL”的真实匹配行误认为 unmatched。
- 安全 equi key 可以限制候选；从 ON 提取 key/residual 的分析必须尊重 dependent/volatile/fallible 边界，不能把两侧引用错误归类为只依赖一侧并提前求值。
- INNER 不再无条件把带 demand 屏障的 ON 子查询提升到 post-join FILTER。semi/anti 在符合各自语义的决定条件结束；single 仍执行其本身的 cardinality 合同；mark 仍保留 UNKNOWN。
- 不新增全量 pair bitmap。一般无 key join 的工作仍可为 O(L·R)，预算只对实际 state/分配/传输准入，不因乘积估算拒绝。
- 含 dependent residual 的匹配决策及保存侧终态须位于同一个合法 owner 拓扑；初始必要时将该 join 决策固定在协调 CN，但输入扫描可用现有普通远端 gather。不能在不同 CN 独立 NULL-extension 后汇总。

### 15.3 必须关闭的反例

| 反例 | 被否定的不变量 |
|---|---|
| 左行有两个相同右 payload，仅一个 candidate 的 dependent ON 为 TRUE | 不能按值共享或去重 candidate 求值 |
| 两个右 candidate 都 UNKNOWN，LEFT 应一行 NULL-extension | post-join FILTER 不能替代匹配前 ON |
| TRUE 匹配行的右列本身全 NULL | 不能按右 payload NULL 判断未匹配 |
| FULL 两侧各有 duplicates、零/部分匹配 | 不能只保留左 matched 或按 PK 假设唯一 |
| ON 引用 left、right 和被跳过层级的祖先 | 参数来源不能以“decorated side”二选一 |
| 第一个 candidate 为 TRUE，必要 producer 已记录晚到读错误 | 不能把 local stop 当整个查询成功 |
| 并行 probe lane 复制 dependent executor | 不能跨 lane 共享可变 generation/operand memo |

上述是设计反例，不是已执行测试。r2 的实际物理路线如下，不再寻找已合并的 legacy right/full/semi 包：

| 实际分支/owner | 接入与保真要求 |
|---|---|
| `HashJoin` keyed candidate residual | 每个已产生 candidate 的两侧行明确映射；在 matched bitmap/update 前运行 demand；key extraction 只用已证明安全的普通条件 |
| `LoopJoin` condition window | condition 结果和 probe/build cursor 会跨输出 batch 恢复；同一 candidate 只求值一次，恢复不能重开 generation；空 build 不制造 ON demand |
| logical RIGHT -> LEFT | `rewriteRightJoinToLeftJoin` 交换 children；slot 的语义 binding owner 不交换，物理 row accessor 显式 remap，不能只看 left/right 指针位置 |
| FULL/右侧保存、bitmap mailbox | 两侧 matched 状态仍由现有 join owner 管；并行合并只能归约匹配事实，不能按值共享 dependent ON；取消/未启动 lane 的 completion 责任必须收齐 |
| SEMI/ANTI/SINGLE/MARK | 各自匹配、基数与 UNKNOWN 合同保持；general row-IN reducer 不复用单 key 的 NULL 摘要；key/hash-MARK 证明不足时使用现有合法 loop/general 比较路线 |
| hash spill / 分区重放 | stable candidate cursor/epoch 与真实匹配状态一起恢复；含 dependent 的 key 不送 build/storage 提前求值；不可 spill 的状态实际触达内存 cap 才资源失败 |
| ASOF/DEDUP 等专用匹配 | `compileJoin` 有 ASOF build-left/shuffle 路线，不能套普通任意 residual 或径直排除；须核对其公开 ON 合法性、predecessor/去重选择与 demand stage，这是 D3/D6 明确残项 |

`DeferredJoinDiagnostic` 还表明“statement constant”不等于可提前报告错误：HashBuild 的 constant-key 错误等待两侧输入出现后才发布。dependent occurrence 默认不参加 build key/constant folding；除非证明其原需求、totality、volatility 与错误边界完全等价。若没有 safe key，走普通 loop candidate 边界，不按 O(L·R) 估算拒绝。

初始含 dependent 的匹配 stage 固定本地单 lane，沿用现有 outer-join owner，不新增平行 matched 协议；普通输入仍可并行 gather。未来并行优化仍要独立 lane/bitmap 证据，不能把单 lane 控制当作该证据。

## 16. r2：具体资源 owner、峰值公式与实测基线

### 16.1 不新建另一套 execution budget

`process.ExecutionResourceGeneration` 已提供 MPool admission、`ReserveTransientMemory`、spill disk/FD reservation，错误含 component/requested/used/cap；`statementAllocationAttempt` 负责本地 generation 的 allocation owner。新 runner 的环境/结果/token/任务元数据加入这条账本，不能给每个子程序 OpenGeneration 后各自拿到完整 query cap。

动态 runtime operator 必须在启动前用既有 runtime allocation owner attachment 加入 attempt；owner 关闭后禁止再登记。参数 borrowed vector 与拥有的 copy 不双计 payload，但 retained borrower metadata 和真正复制的 bytes 都计。程序-local MessageBoard 不能逃离顶层 attempt 的终态 drain。

初始串行求值的峰值模型：

```text
新增 live memory <= sum_over_active_owners(
    B_capture * captured_slot_width
  + N_current_frame * (aligned_result_width + memo_state_width)
  + stage_cursor/frame_metadata
  + one_generation_operator_state
  + scope_tasks/edges/reader_metadata
) + 必须保留的 shared producer/input/spill-decoder state
```

- r2 初始 `B_capture=1`、每 owner 一代；不是整个 query 只一个 active owner。嵌套调用、多个现存 frame 与 producer 的 live state 均取和，不能只报告最内层。
- `N_current_frame` 是 caller 当前保留的输入行数，不能用 capture window=1 代替。projection/filter 通常是普通 batch，但 window/sort/group consumer 可能对较大保留 batch Eval；返回 vector、同 frame memo/evaluated bitmap 必须按真实 N/capacity 计量。要进一步缩小 N 须改变实际 consumer 窗口，不能仅在 dependent leaf 内循环分批后仍声称 O(B) 结果内存。
- 初始路径不保留历史 E identity-to-domain map；frame 消费完即释放其 memo/结果责任。scalar reducer 只需一 tuple 和 cardinality state，第二行锁定错误，但 parent aligned vector 的值字节另计；大单值按实际 bytes 准入。
- matched/CTE/window/group state 仍由实际算子持有并计量/spill；说“每次只一环境”不等于整个查询 O(B)。Go 栈/task 数、map 扩容、decoder capacity 和非 MPool 临时内存不能略去。
- 窗口增减是 admission/backpressure 策略，不是 SQL 拒绝条件；重试缩窗不能重复已产生的 volatile/有副作用求值或已发布结果。不可逆 acquire/publish 前才允许改变窗口。

### 16.2 规划预算仍需数值与计量方案

必须把实际 retained bytes、生成节点/expr/slot/task-descriptor 和累计生成工作分开。可选集合优化搜索耗尽允许选紧凑一般路径；必须构造的基础 IR 实际申请超预算才是资源错误。不能按 Depth、max arms、估计 cost 或 E·I 拒绝。

本轮未把任意猜测的 64 MiB/节点数写成“批准默认值”。节点预算若无所有 generator/DeepCopy/expr-owned roots 的真实计量覆盖，只是另一种形状 guard；Go heap/stack 若只有常数估算也未证明内存上界。D4 依然阻塞技术设计批准。

### 16.3 已选择的基线与实际运行记录

最便宜的既有 planner corpus：

- `BenchmarkExistentialPlanningControls`：shallow、depth_two_old、scalar，使用原 mock schema/SQL。
- `BenchmarkFlattenSubqueriesWithoutSubquery`：无子查询热路径。
- 后续执行 corpus 复用第 8 节的旧成功 scalar/row/ON/CTE/DML/multi-CN fixtures，不能把 NYI 耗时当执行 baseline。

历史实际命令（2026-10-08，精确源码 `dce60631f8`，不是 r3 当前基线）：

```text
GOTOOLCHAIN=go1.27.0 go version
=> go version go1.27.0 darwin/arm64，exit 0

GOTOOLCHAIN=go1.27.0 .agents/skills/mo-dev/scripts/mo-cgo-test
  -run '^$'
  -bench '^(BenchmarkExistentialPlanningControls|BenchmarkFlattenSubqueriesWithoutSubquery)$'
  -benchtime=200ms -count=3 ./pkg/sql/plan
=> exit 1，wrapper prerequisites：worktree 缺 libmo.dylib；primary artifact provenance 不匹配
```

日志在 worktree 的 `CLAUDE_artifacts_7559_stage1/CLAUDE_baseline_planner.log`。没有启动 benchmark/test binary，未产生 ns/op、allocs/op 或测试 PASS/FAIL。不能绕过 provenance 手工链接主目录 artifact，也未修改 `.go`、native source、Makefile、go.mod 或 stamp。

r2 实际完成前置恢复并得到基线（同 head，2026-10-08）：

1. 在 worktree 运行顶层 `make NATIVE_BUILD_JOBS=4 cgo`，Go 1.27.0、worktree-local TMPDIR；退出码 **2**。ONNX 下载的 curl timeout/HTTP2 framing error 是环境前置失败，不是 UT/产品失败。日志：`CLAUDE_artifacts_7559_stage1/CLAUDE_native_build.log`。
2. 只读核对主目录 `thirdparties/install/lib/onnxruntime_arm64.dylib`，SHA-256 为 `30afadcfc3c704f7671f8430d6252956651c1972373901d2be629da2e6a4d8ee`，与仓库固定 ONNX 1.26.0 checksum 相符。通过 Makefile 正式支持的 `ONNX_PREBUILT_DIR` 提供**仅这一项第三方预构建依赖**，再次执行相同顶层 make。全部 thirdparty/CGo generation 按 provenance 合同清理/重建/登记，worktree `libmo.dylib` 新建；退出码 **0**。未复用主目录 libmo，未手工链接/改 stamp，主目录 artifact 未修改。日志：`CLAUDE_artifacts_7559_stage1/CLAUDE_native_build_prebuilt.log`。
3. 不绕过 wrapper，以原 benchmark 命令重跑；退出码 **0**，非空选择为 7 个子基准、每个 3 次，`PASS`。日志：`CLAUDE_artifacts_7559_stage1/CLAUDE_baseline_planner_rebuilt.log`。`-run '^$'` 明确不执行 UT；不能把 benchmark PASS 称为功能 UT/BVT 通过。

环境：darwin/arm64，Apple M1 Pro，GOMAXPROCS=10，Go 1.27.0，benchtime=200ms，count=3。下面是三样本中位数，仅作描述性基线，不是统计置信区间：

| 既有子基准 | ns/op 中位数 | B/op 中位数 | allocs/op |
|---|---:|---:|---:|
| planning/shallow | 73,383 | 75,651 | 781 |
| planning/depth_two_old | 105,034 | 109,778 | 1,220 |
| planning/scalar | 91,091 | 84,285 | 915 |
| no-subquery/depth_32 | 344.0 | 0 | 0 |
| no-subquery/depth_128 | 1,996 | 0 | 0 |
| no-subquery/depth_512 | 10,637 | 0 | 0 |
| no-subquery/depth_1024 | 20,607 | 0 | 0 |

日志有重复 rpath/library linker warning；sonic/ast 在 Go 1.27 环境回退 encoding/json 的 warning。运行成功，但未来成对测试须保留同工具链/后端，不无声忽略环境差异。no-subquery 的 depth 是表达式遍历控制，不是已执行 1024 层 correlation 证据。

这些 baseline 不包含 candidate、SQL executor、native rebuild 耗时的性能结论、扫描/网络/峰值或新语义测试。三样本不足以批准第 8 节的 5% 门槛；规划默认预算、Go heap/stack/task admission 和真实执行 corpus 仍须关闭 D4。

## 17. r2：IR/placement 消费闭包与新增入口

### 17.1 IR 与 mixed-version 候选

拟选新逻辑 `dependent occurrence` / `correlation slot` 表达式和 Query-owned program/slot descriptors。所有关系节点仍留在 flat `Query.Nodes`，program root references 是显式边；不把内层表藏进 SQL string 或 opaque callback，不借 ParamRef/ExtraOptions 偷传。

| 消费者 | 必须采取的动作/原因 |
|---|---|
| binder/region classifier | 在原始可见性 owner 冻结后生成 slot；非法同层 FROM 仍拒绝 |
| visitor/prepared rules | `VisitPlan` 从 Steps 沿 Children；`VisitExprTree` 显式列举 Lit.Src/F/List/Sub.Child/window/frame；两者都须补 program/slot/operand 边。`VisitExpressionsInOwner` 的 reflect traversal 能发现新容器内的 Expr root，但不会自动递归 Expr 内新 oneof，不能把它当作完整替代 |
| copy/remap/tag/refcount/stats/prune | `DeepCopyQuery` 手列 Query 字段，`DeepCopyExpr` 手列 oneof；必须同时补 descriptor/root/捕获与 operand。tag/column remap 处理 slot 不同于 ColRef；prune 从 Steps + program roots + source edges 建完整 reachability，不能删 still-demanded 内层表 |
| authenticate/metadata/dependencies | 当前权限逻辑扫描 flat Query.Nodes；保持子程序表/视图可见，补只沿 root/children 的消费者；准备计划 catalog dependencies 仍统一失效 |
| explain/analysis/runtime allocation | 区分 template 与 generation；不随 E 保留整份 AnalyzeExecPlan；all runtime owners 在 start 前登记 |
| proto 源/生成/cache | proto 新字段只通过正式生成；缓存只保存 immutable template，不保存 reader/environment/result |
| remote encoder/protocol gate | `compileScope` 给 Scope.Plan 附整个 Plan，`attachGroupingTransportPlan` 再递归附 Plan，`generatePipeline(ctxId==1)` 写入 `p.Qry=s.Plan`。必须审计 `fillPipeline` 后整个 payload，不只 InstructionList；仅将执行计算放本地不阻止新 IR 到旧 CN |
| ViewData/rebind | 当前 View 保存 SQL/解析模式/required protocol contract；明确新表达式 capability 在旧节点绑定时如何拒绝/转本地，不改变 catalog schema |

初始物理策略：dependent evaluator 与一般子程序在新协调 CN 本地；普通外层输入可以用既有 remote fragment/gather，写节点按既有 DML 合同接收计算结果。inner 的本地 distengine reader 仍按相同 snapshot 读存储，但这不等于 inner 计算在多 CN 并行。

不向旧 CN 发新 IR/operator；普通 fragment 的已有类型/函数版本门禁继续生效，不能为了支持 correlation 绕过其他协议合同。若不能安全切出 remote fragment，则把必要计算闭包保留本地，而不是把新 SQL 退回 correlation NYI。代价可能是协调 CN 的 O(E·I) 扫描/网络，须按第 8/16 节测量；后续远端参数化优化不能冒充初始实现。

这是 placement/兼容性选型，不是“无需 wire 变更”。remote scope 编码前要构造或选择真正闭合的 transport plan；若依赖 grouping 等元数据不能安全切分，则把必要闭包保留本地，禁止向旧 CN 发整个带新 registry 的 Query，也不能直接删除 Plan 丢失旧 grouping/protocol 合同。新 descriptor 在本地 plan/cache 的 protobuf roundtrip 仍须正式 proto 生成与 capability，无法因初始不 remote 就省略。

prepared/cache 的 immutable template 与每次 execute 的 mutable owner 分开；root AP prepare 不能靠 TP-only Compile.Reset 承诺重用。既有 frontend/AP 的 plan-cache/recompile 路线、late-bound 参数类型与 definition fence 必须一起核对：重绑定/重特化可重编译，禁止每个 EvaluationId 编译。View 保留 SQL/parser mode/required protocol，旧 CN 不能忽略新 required contract 再绑定成旧特例计划。精确新增字段与完整消费者仍阻塞 D5。当前 latest MORPC 为 106；真正实现时重新核对，不预占版本。

### 17.2 入口 owner/consumer 表（扩充，不是穷尽完成声明）

| 入口 | 绑定 owner / 实际求值阶段 | r2 处理约束 |
|---|---|---|
| WHERE/SELECT/HAVING/ORDER | 当前 block；输入行/分组输出/排序输入 | 标量/量化 occurrence 的 demand 与 stage 分开；ORDER/LIMIT 不跨环境 |
| GROUP BY scalar | 聚合输入 block / 输入行形成 group key 前 | correlation-specific guard 在范围内；分组后的结果不能反向当输入 slot |
| JOIN ON | 当前 join + 可见祖先 / candidate pair | 第 15 节；不能绕成 post-outer FILTER |
| 派生表 | SQL 可见祖先；非 lateral 当前 FROM binding 不可见 | 替代透明形状 guard 的是 owner 合法性+一般程序，不是删掉全部可见性检查 |
| 窗口函数参数/PARTITION/ORDER | window 所属 block / 对应 window 输入 stage | 子查询在需求输入上求值，partition/order/frame 不跨 environment |
| frame bound | 普通 offset 是 literal/参数/interval；`interval_expr` 实为 `INTERVAL expression time_unit` | interval 内 scalar subquery 可达 public grammar。`makeWindowFrameConstValue` bind-time Eval、`resetWindowIntervalExpr` 对 Literal 的假设不能排除合法依赖；须保留表达式/权限边，将环境内稳定 bound 接到 window runtime frame owner，并以同形非关联/当前行变量对照确定独立 constant-frame 合法性 |
| 普通 CTE 多引用 | declaration context / 对应实例与引用 occurrence | `reusableCTEProducer` 排除 correlated、recursive，并要求 deterministic/drain/totality；初始一般程序内按既有 inline 语义，未证明不得跨 EvaluationId 共享 |
| 递归 CTE | seed/recursive member owner / 当前递归行、独立 iteration source | 现有正例允许 `exists(select 1 where r.n < 3)`；不能因此将整个递归 CTE 标为不支持 correlation |
| 递归子查询重复读取 r | recursive source 合法性 | 已有测试要求递归表只出现一次且不在子查询中读取；传 `r.n` 值与再次 `FROM r nested` 不同；保留非法源规则，不排除合法深层值引用 |
| UNION/INTERSECT/MINUS | 各 arm own block + 外部 owner / 环境内关系 | arm 的局部 binding 不串联；ALL bag/去重/排序逐环境保持 |
| 单表 UPDATE 赋值 | `newSequentialUpdateProjectionBinder` / assignment i 的当前 row image | 每个 rhs 接上一投影，保留原行 sidecar 做索引/约束；不能全部绑定 OLD，也不能把所有赋值共用一个 stage |
| 多目标 UPDATE、DELETE、INSERT SELECT | 现有 DML SELECT owner / 写前计算 stage | 子程序不写、不新建 txn，不重置 statement offset；目标表限制/锁沿原契约 |
| INSERT ON DUPLICATE UPDATE | `OndupUpdateBinder` / conflict action 的 existing row + incoming VALUES row | binder 的 scalar subquery 是实际入口；`scanTag` 与 `selectTag` 两套 row image 不能混；见 `bind_insert.go:2745–2796` 的 flatten 接入 |
| multi-table INSERT WHEN | statement CTE declaration + source-output alias / source row 与路由阶段 | 不可见 source-private CTE。INSERT FIRST 的后续 WHEN 子查询目前因 eager flatten 不能 masked 而拒绝；一般 demand 机制须处理，不标成独立 SQL 排除 |
| conditional INTO VALUES/ELSE | branch context / 已选 route row | 现有 guard 同样是 eager PreScope 无法跳过导致；新路径在 branch demand 中执行，不能无行仍启动 source |
| REPLACE VALUES scalar | 行 ordinal / 一行 VALUES input | 现有 32 个 subquery-row branch cap 与 row-local default dependency guard 源于旧表示；须用紧凑 occurrence/row identity 与真实预算评估，不套成新的关联深度/形状排除 |
| RETURNING | 既有 returning AST 合法性 | `returning_test.go` 有非关联 `(select 1)` 拒绝及专门合同；不自动开放全 RETURNING 子查询，但若只因 correlation 才失败的既有入口仍在范围内 |

新增 DML guard 的发现意味着第 1 节“既有 DML 入口”不能只检查 UPDATE/DELETE/INSERT SELECT。需要在同一个 demand 接入闭包中验证 WHEN/VALUES、ODKU 和 row-local defaults 的可达性与求值阶段；不要求用户重新批准这些范围。

### 17.3 下一轮调查的最短闭环

1. 第 18 节字段责任已收敛，继续 D1 的非 TP operator Prepare/Reset/Free、datasource/meta-lock 初始化、CTE/lazy/cleanup 与 task admission 实施图；如果 task/stack 模型无法真实计量，在批准前与 continuation 方案重新比较。
2. 固定 D2 的 stage frame 传递/memo/reset 和全部优化消费者；按第 15 节完成 join type、ASOF/DEDUP、bitmap/spill 的具体 cursor/owner，不继续泛称“所有 join 都一样”。
3. 完成 D5 的 plan/copy/prepare/View/remote payload 清单和 D6 的 public frame/ODKU/multi-insert/REPLACE 构造。独立 N/A 仍必须有公开合同与同形非关联对照。
4. 使用已有真实 planner baseline，补 execution corpus/实际 retained-byte/task/stack 计量，再确定预算默认值与允许开销；没有 candidate 不称性能门槛通过。
5. 先将本纯设计修订提交 draft PR，公开 review 开放决策；不请求把有阻塞稿批准为实现。关闭阻塞并批准精确修订后才改生产/测试。用户已授权本任务 push/提 PR，仍不擅自评论 issue/PR。

## 18. r2：statement Base、execution domain 与 slot 布局

### 18.1 为什么不复制 BaseProcess

`BaseProcess` 不只是服务集合：sequence gate 明确只同步共享 Base 的 child；`statementInsertIDMu` 与 LastInsertID/StatementLastInsertID 配套；user-lock identity 也有 Base 内 mutex。`SessionInfo` 含可变 map、slice 和 session side effects。复制 map/pointer 后配不同锁、浅拷贝活 mutex、按值复制 SeqLastValue/LastInsertID，均不是合法执行隔离。

r2 选择 **同一个 statement Base + `Process` 上显式 execution-domain overlay**，不新建会话或假造 binding process。domain 为空时走现有 root 行为；普通 child 继承当前 domain，新的 dependent owner 建子 domain。各代新建/更新它自己的 query/pipeline context、message board、rows/iterator counters 与 slot source；Base 仍是实际同步/服务/预算 owner。

这需要修改相关 accessor 和直接 Base 访问消费者，不能仅在 constructor 放一个新 pointer 就声称隔离完成。下表列出本基线字段责任，括号中均是候选域设计，不是现有 API：

| Base/Process 字段族 | owner / 子程序规则 |
|---|---|
| `sqlContext`、`atRuntime` | root 保持原值；child 经 domain-aware context/runtime accessor 使用本代状态；不得在 Begin 调 `ResetQueryContext` 清 root context/counters |
| `messageBoard`、`CloneTxnOperator` | domain-local board/clone 生命周期；root txn operator 借用且同 snapshot/definition fence；仅关闭自己的 clone，不能 Reset root board/txn |
| `LoadTag`、LoadLocalReader、Aicm、PostDmlSqlList | 顶层 DML 独占；child 只读程序不继承 load/output/post-DML 驱动能力；不能清空/执行 parent PostDmlSqlList |
| `StmtProfile`、DivByZeroErrorMode | statement 错误/IGNORE/profile 合同借用，不用 child SELECT 覆写 parent INSERT/UPDATE profile；如需 function runtime scratch，放 domain，不能触发 SetStmtProfile 的 query-budget 重置 |
| Id、UnixTime、Lim、mp | 同 statement Id/时间/限制/MPool；本代 context/attempt token 独立；参数/results 的 allocation owner 接同 attempt，不新开完整 query cap |
| TxnClient/TXNOperator、services/engine、FileService、Lock/Task/Partition/Incr/Query/Hakeeper/Udf services、WaitPolicy | 借用现有服务与 transaction；UDF 不扩展范围；读/锁遵循原 snapshot、权限与 cancellation；没有 commit/rollback/statement-offset 权限 |
| execution/warning/CTE memory budget 及其 mutex | 同 statement 所有权和 cap；domain 只拿 reservation，不新建另一套上限；部分初始化/结束恰好归还一次 |
| `sequenceGate` 与 SessionInfo.SeqCurValues/SeqAddValues/SeqDeleteKeys/SeqLastValue | 同一个 Base/gate/会话 map；已有 nextval/setval 等完整操作与发布继续受原 gate 保护；每环境不能 ResetSequence 或偷偷丢副作用 |
| LastInsertID/StatementLastInsertID、statementInsertIDMu、AffectedRows | session/statement 原 owner；合法 LAST_INSERT_ID(expr) 副作用沿原 setter；child 不开新 OK packet，不更新顶层 affected rows 或 generated-key boundary |
| userLevelLockIdentityMu/owner/conn/generation | 原 session lock owner，借用、不复制、不在 child Finish 重置 |
| groupConcatInputRowCounters/provenance flag | 当前 execution domain 的逻辑 Group cursor；每代独立重置，不用 root map 的 node-id collision；并行 child 同域共享。来源可信度保留当前 process/transport 的真值 |
| incrStatementDisabled | child 无论 root 模式都不推进 workspace statement boundary；不能借 internal SQL executor 重新打开 statement |
| StageCache/logger、resolveVariable callbacks、prepareParams+kind metadata | 借用，同 session/prepare snapshot；prepare 参数不是 correlation slot；child Free 不清理 root 所有的 prepare vector 或 callbacks |
| SessionInfo immutable settings/identity | account/user/host/role/DB/version/timezone/sql mode/week/lc_time_names/timeout/autoincrement/max_error_count/session ID/native mode/restore/frontend/session capability 沿 root；不构造新 tenant/session |
| SessionInfo FoundRows/ResultRows/FoundRowsRecorded/SqlCalcFoundRows | 用户 builtin 的 previous/session 观察走原 statement owner；child sink 的累计结果/临时计数放域，不能调用顶层 BeginFoundRowsStatement/SetFoundRows；显式内部 LIMIT 保留，客户端 SQLSelectLimit 不再套到 inner relation |
| SessionInfo QueryId/ResultColTypes/Buf/CompilerContext/SqlHelper、Process.Session | 不改 root frontend result buffer/metadata/query list；借用 compile/权限/会话 capabilities；collector 是本代 typed sink，不调用新的 SQL session |
| Process.Reg/Ctx/Cancel、snapshot/generation/shuffle、WarningSink | edges/context 当前域所有；plan snapshot 与已冻结 shuffle/类型协议沿原 contract；WarningSink 沿 statement attempt（不扩展 warning 兼容范围），不能 reset parent destination |

statement side-effect 的隔离不是“副作用全部扔掉”：序列、LAST_INSERT_ID(expr)、session-level lock/外部会话 capability 等合法函数不能因位于 subquery 就变成另一会话。只有顶层结果写入、DML 边界/统计和 child iterator scratch 分离。volatile 行为不因参数相等而共享。

`Process.Free` 当前会 reset Base MessageBoard、prepare params 等，`ResetQueryContext` 还重置 GROUP_CONCAT counters；child owner 不直接调用它们释放 root。需要 domain Close 的有限责任与 nil-domain 原路径，完整消费者清单纳入 D1/D5。

### 18.2 producer/等待与执行入口切割

不调用 top `prePipelineInitializer/runPipelineAttempt` 原封不动：前者会 metadata/table lock、datasource 初始化和全部 materialized Begin；后者还有顶层 post-DML/check/retry 责任。新 owner 借用 statement 已建立的 metadata/lock/definition fence；必须补充子程序关系依赖，不能略过新表权限或锁。

| 等待边 | 防环/退出合同 |
|---|---|
| parent Eval -> child completion | parent slot 与 child 必需 worker 分开；不持 owner/session gate/receiver lock 等待；参数先成为 immutable native 值 |
| child -> PreScopes/build/hash producer | 本 program 任务图先 admission/注册，再运行；start-fail 也补 terminal 责任；不存在 producer 时禁止继续 receive 等待 |
| child -> shared CTE Source | 仅既有共享证明允许；producer 必须独立 runnable，不能依赖已暂停 parent 的 live output；相关/recursive source 按 program/iteration owner，不跨 E 混环境 |
| producer -> data/credit | cancellation/typed terminal 有独立控制槽；stop 前请求所有 sibling 退出，再 join，不等待 consumer 恢复 drain 才能通知失败 |
| lazy UNION -> branch completion | 未启动 arm 不启动、不暴露错误；已启动 arm 的 done 证明 cleanup 结束；取消 wait 本身不代替 join |
| recursive CTE -> iteration source | 传当前递归行的值与 reread recursive FROM 不同；专用 iteration completion 收齐才能换代，不把合法值引用整体拒绝 |
| reader/remote -> context/registration/StopSending | 保留原 cancellation/connection/receiver-stop 区别；子 stop 不解释成 statement 成功；registration/credits/FD 归本代或原 source owner |
| cleanup -> task/receiver join | 先 cancel siblings、release owned credits，再等待；root/domain query context cleanup cancel 在 frozen outcome 收集之后；结束后才重用 operator/edge/slot |

初始 static single-lane 减少动态 clone，但不减少所有 PreScope/producer；task 数与实际启动拓扑有关。statement-owned reusable task slots 仍是候选：需证明 admission、重用、panic/部分启动、递归/lazy 分支和 legacy cleanup helper 的实际峰值。Go goroutine/stack 不能因不在 MPool 就免计；尚没有实测执行 evidence，不能关闭 D1/D4。

### 18.3 flat typed slots 与 stage frame

选每 program 一份编译期闭合的 flat slot array：

```text
SlotBinding = (semantic BindingOwner, ColumnOrdinal, full Type/provenance)
CaptureOrigin = current-stage row accessor | parent-program slot ordinal
ProgramInputs = direct reads + descendant-required forwarded inputs
Environment = immutable native single-value vectors aligned to ProgramInputs
```

- 编译期冻结 owner 后求 transitive closure；同一 binding slot 仅在该 program schema 中去重，不共享 EvaluationId 的运行结果。跳过层级的依赖由 intermediate descriptor 显式 forward；CTE declaration owner 与 invocation owner 分开，不能按偶然 binder context 链重新解析。
- CaptureOrigin 是已 remap 的实际输入 accessor，JOIN 包含双侧映射，UPDATE/ODKU 包含 row-image/version，分组后是 group output；普通 `CorrColRef.Depth` 不再作为运行时寻址依据。构造/遍历用显式 work stack、seen set、取消与真实预算，不加 depth cap。
- 当前 stage 值在 Begin 前按原生类型捕获；可变 evaluator output 必须在 next Eval 前转为拥有的值。immutable ancestor slot 可以合法借用，但父代必须活到 child Finish/join；若寿命证明不足则复制并入同 attempt 账本。不经 TEXT 参数/重解析，不丢 NULL/decimal scale/temporal/string runtime metadata。
- 编译一次并不意味着每环境重建 slot schema、tag map 或整个 plan；只更新本代 native values/reader edges。layout 元数据是实际生成成本，forwarding 多层的总 descriptor 数/字节必须计量，不用估计复杂度返回 NYI。
- stage frame 明确当前输入 epoch、row cursor、occurrence memo 与 evaluated bitmap。projection/filter/group/window/sort、JOIN candidate、DML route/assignment 各自拥有真实阶段；重复输出恢复沿同 frame，不按 batch pointer/PK/value 生成 identity。当前 N 行 aligned result/memo 入第 16 节账本。

IR 字段、stage frame 的具体 owner API 与 copy/remap/fold/codegen 消费者仍须关闭 D2/D5；flat layout 是设计方向，不是已落地或批准接口。

## 19. r3：最新 main 刷新与可公开 review 的具体问题

### 19.1 当前 head 与旧证据的界限

当前源码基线 `83d82b8ee0` 的 main 历史包含 `5b7453a924`（#29372 本地 CTE 外层引用）及 `83d82b8ee0`（#29694 prepared metadata/privilege snapshot overhead）。`go.mod` 从 1.27.0 升至 1.27.1。当前 native/Go/planner 的证据必须逐项重新判定，不把“只是 pull”当成无影响。

- [已合入的本地 CTE 设计](CLAUDE_local_cte_outer_reference.md)明确其批准范围：可证明重放的单基表身份与有限关系/表达式；不是通用 APPLY、多层任意 owner、任意窗口/聚合/集合/guarded 需求。
- `parameterizeLocalCTEs` 在旧 flatten 的入口被调用；先只读准入再 lowering。`localCTEDomain` 的 totality/demand guard、透明外层、producer/consumer/window/分页限制仍存在。新机制应在破坏性 lowering 之前选择，不可捕获这些 NYI 后重用已改变的 Query。
- 当前 scalar COUNT/aggregate empty-input/HAVING/projection 恢复代码有新的修正；它们是旧优化的合同与 nearest controls，不能在新通用路径中删掉，或用跨环境全局 COUNT/LIMIT 替代。
- 当前 prepared binding key 只编码 binding 所观察的类型/种类/域元数据，不按参数值 hash 共享。dependent template 的缓存也只包含 immutable type/binding/plan，任何结果、env、reader、task completion 都不进入 prepare/cache。
- 当前 `exprStructuralHash` 的 uncommon oneof 使用 proto marshal fallback；新 IR 正式生成后可保留结构身份，但相等判定仍要包括 occurrence/stage/slot 身份，不得 dedup 两个独立易变/可失败 occurrence。这个事实不是“现有 hash 已支持新 IR”的证明。

精确源文件：`cte_outer_reference.go`、`cte_scalar_aggregate_projection.go`、`flatten_subquery.go`、`expr_hash.go`、`prepared_execution.go` 和 `computation_wrapper.go`。这里只重新核验这些相关边界，不将全量 upstream 230-file diff 作为本 PR 自己的改动。

当前 baseline 实跑：源码 `83d82b8ee0`，Go 1.27.1，darwin/arm64、Apple M1 Pro、GOMAXPROCS=10；相同 repository wrapper、`-run '^$'`、两个既有 benchmark family、`-benchtime=200ms -count=3`。退出码 **0**，7 个子基准各 3 样本，`PASS`；`-run '^$'` 没有运行 UT。native 输入路径相对历史 head 未变化，wrapper 实际通过 provenance；没有再 rebuild 或绕过 stamp。

| 当前子基准 | ns/op 中位数 | B/op 中位数 | allocs/op |
|---|---:|---:|---:|
| planning/shallow | 413,984 | 75,645 | 781 |
| planning/depth_two_old | 455,670 | 109,775 | 1,220 |
| planning/scalar | 381,054 | 85,681 | 933 |
| no-subquery/depth_32 | 1,652 | 0 | 0 |
| no-subquery/depth_128 | 8,363 | 0 | 0 |
| no-subquery/depth_512 | 25,926 | 0 | 0 |
| no-subquery/depth_1024 | 59,115 | 0 | 0 |

latency 样本波动明显，且旧样本的源码/工具链不同；不能据这两张表归因 upstream/Go 回归，不能作为 5% 门槛的统计判定。scalar allocation 与旧数据有变化，须在未来同输入成对实验中解释，不隐去差异。日志的 linker duplicate warning 和 sonic encoding/json fallback 同样保留；日志在本地 `CLAUDE_artifacts_7559_stage1/CLAUDE_baseline_planner_83d82b8_go1271.log`，不提交本地 artifact。

此运行只刷新既有 planner baseline，不验证 proposal 的任何新 IR/operator/demand/资源/多 CN 行为；功能验收仍全部待实现后执行。

### 19.2 Step owner 是不可遗漏的执行/协议边

仅保留 flat `Query.Nodes` 并给 Expr 一个 program root **不够**。当前 `compileQuery` 会倒序遍历 `Query.Steps`，为这些根建立 sink/recursive receiver，然后编译所有 product regions；`SourceStep` 存的是 Steps 数组索引。已有 final literal LIMIT 0 优化只跳过其特定无需求 producer，不提供一般 CASE/WHEN 的惰性 program ownership。

反例：未选 CASE 分支的相关程序含递归 CTE或可失败 producer。即使 dependent leaf 没有 Begin，只要它的 Steps 留在顶层默认启动列表，producer 仍可能启动、报错或等待不存在的 consumer。`mask=FALSE` 因而不是完整闭包。

需要遵守的共同 IR 合同：

```text
Query.Nodes : 全部可授权、可解释、可复制的关系节点
Query.Steps : SourceStep 使用的稳定 step catalog
StepOwner   : 每个 catalog index 唯一归 main region 或某个 program region
ProgramSpec : root + owned step indices + slots + dependent occurrences
编译某 region : 仅建立该 region 的 owned tasks/edges/source generations
授权/元数据/缓存依赖 : 看整个 bound Query，不只当前启动 region
```

这里的名称是拟议字段语义，不是已占用的 proto field number。没有新 program 的旧 Query 默认全部为 main owner，原路径不创建额外 registry。SourceStep 索引、recursive iteration 回边、Root/Program 的 reachability、cache/deepcopy/remap 必须一起更新，不靠物理 Children 推断 owner。

Program 引用外部 CTE 时只有两条合法边：已证明可共享且独立 runnable 的 statement producer，或 program 自己闭合的 producer。前者有显式借用/reader 责任，后者随本代 Begin/Finish/Close；不接受 live parent stream，不按相同参数偷共享。相关 recursive CTE 的 seed/member/consumer 属该环境的闭包，递归的值引用与 illegal reread 仍按原规则区分。

**开放决策 D1/D5**：确定 descriptor/StepOwner 的确切布局、源索引 remap、根编译/程序编译接口和全部 SourceStep consumer。原 `prePipelineInitializer`/`runPipelineAttempt` 不能整体当成 Begin：metadata/statement action、all-source initialization 与 top-level post-DML/retry 必须先分离。这个决策要在实施前批准，不留给编码时临时补。

### 19.3 特殊 JOIN 与 window frame 不按名字排除

当前 `buildJoinTable`/grammar 明确有公开 `DEDUP JOIN`，不能因内部 DML 也生成该 node 就声称它仅是内部结构。普通 FULL/DEDUP 使用 TableBinder；TableBinder 的旧 Sub 拒绝与“没有对应 lowering”有关，单凭该 NYI 不足以证明是独立语言排除。

当前 ASOF binder 允许 equality-key expressions，但 temporal operands 必须是同类型列，并要求恰好一个向后 temporal inequality、至少一个 equality key；TOLERANCE 必须是 constant interval。已有 public-parser UT 对非列 temporal operand、反向查找、无 equality key、非 constant tolerance 等提供最近非关联控制。

因此须分别决定：

- 普通 JOIN 的 ON 是 candidate-pair demand，已有第 15 节合同。
- ASOF 在 equality/key 表达式包含合法 dependent occurrence 时，必须保留其实际需求及 predecessor/tolerance/NULL/tie 选择；不能无证明提前到 hash build，也不能简单回退普通 INNER/LEFT。若采用 candidate enumeration 的一般路径，winner 归约与错误/早停位置须显式定义。
- DEDUP 的 public key/conflict action 与内部写入阶段分别建 owner；不能把重写后 ColRef 当成证明“用户表达式在此从不求值”。泛化需要同形非关联合同、key/candidate 需求与 duplicate action 的独立 oracle。
- window interval grammar 可容纳任意 expression；现有 prepared-marker helper 将 Subquery 视为需要拦截的输入，旧拒绝并未证明祖先环境的合法常量 bound 是非法 SQL。普通同 window 输入行依赖与更外层 program env 的 invariant bound 必须区分。权限 gate 前禁止执行 subquery/control function；新表达式应保持可见并交给实际 runtime frame owner。

**开放决策 D3/D6**：上述路径的具体算法/stage/合法性对照与接受/拒绝矩阵。不能把这四项改成产品范围外，也不宣称现有测试已覆盖新的关联行为。

### 19.4 资源/调度的批准前决策

当前 `ReserveTransientMemory` 明确用于非 MPool scratch，与 vector allocation account 共用 query/CN ceiling；它不自动替 Go goroutine、stack、map 扩容或未经过它的 Go heap 记账。r2 的 reusable static Scope tasks 也不是当前代码已有的 scheduler。

必须在设计批准前选定、测量和定界：

1. Program/task metadata、native capture/result/memo、reader/edge 与 CTE/spill decoder 的实际容量/owner；没有给每 child 独立 query cap。
2. Planning IR 的 actual retained bytes 与累计生成工作；optional optimization search 用尽时选紧凑一般表示，不按 depth/shape/cost 报资源错。
3. 单 lane 的多个 PreScope/CTE/lazy/cleanup 所需 task 数、task admission/终止发布和 Go stack 峰值。父等待不能占子容量；若不能闭合，重选 continuation，而非加 per-row goroutine 或全局无界池。
4. 当前输入 N 的 aligned result/memo 与 capture window=1 的不同上界；wide value、空 mask、部分错误、取消、frame 切换与重复 Eval 的 reservation rollback。
5. 既有成功路径性能门槛与 execution corpus；5% 是未经批准的提议。不能用旧 head/Go 的三样本 planner 中位数或 original-NYI 耗时替代。

**开放决策 D4**：具体默认值/计量/边界注入与批准性能条件。此处刻意不猜一个 64 MiB、node count、arm/depth 或超时作为合法性 guard。

### 19.5 设计 PR 与实现交付分开

本设计稿的交付是 R0 文档：完整目标、历史/当前证据分层、候选接口、具体开放决策和完整实现验收矩阵可公开 review。它不修改 production/test/proto/native source、配置或 SQL result；没有 changed-code coverage/功能 BVT 的通过声明。

设计 PR 必须明确：Related #7559，**不使用 Fixes/Closes**；draft；方案未批准；上述 D1–D6 未闭合；待精确修订批准后才实施。最终实现仍须第 10 节原始跳过移除/非空二列 IN、全部合法多层入口、UT/BVT/多 CN、资源/性能、75% changed-code coverage 与零 blocker self-review。设计稿发布既不是这些门禁的豁免，也不是最终功能交付。
