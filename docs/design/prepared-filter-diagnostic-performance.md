# Prepared 参数诊断与选择性：根因及系统性修复方案

日期：2026-09-28。状态：设计及扫描 block 谓词补全、二进制整数快速证明修订已通过审查；实施与验收证据见第 8 节。

实施对象：issue #29429 的 prepared 查询修复 PR。设计触发条件：新增有界的 prepared 优化变体，改变原 temporal compatibility 的缓存契约；跨 frontend、planner、compiler；影响 TPCC 热路径与可用性。本方案不改变 SQL 协议、持久格式或跨 CN 证明传输；多 CN 的执行副本仍需验证同一绑定语义。

## 1. 范围与结论

- 问题：[issue #29429](https://github.com/matrixorigin/matrixone/issues/29429)，尤其是 [root-cause update](https://github.com/matrixorigin/matrixone/issues/29429#issuecomment-5872195452)。
- 引入提交：`a99db843c7b4630a86883addc9424433d93fc2d2`，PR #28851。
- 相邻正常版本：`1575e70c3db676d3ed00abc080aadc029b0e1435`。
- 初期实验基于 main `1ded9fc914803cb2ffc1215be353703d640b3f02`；当前修复已 rebase 到 `a543410f76`，工作树 `/home/xupeng/mo-worktrees/issue-29429-main-fix`。
- 本文以单 CN、server-side prepare、TPCC 10 仓 10 并发作性能验收；物理 remote scope 的过滤传播也属于正确性闭包。问题本身不要求多 CN 才能出现。

核心缺陷是**把 prepare 时“这个转换可能产生诊断”的静态属性，当作 execute 时“本次绑定不能下推”的最终结论**。当前绑定的安全证明没有统一覆盖关系优化、reader filter、block filter 和物理编译。恢复路径又依赖全量重规划，引入了新的执行开销。

这同时是语义阶段划分和成本契约的问题：参数类型/表达式结构的事实应随 prepared plan 保存；当前参数值是否安全应每次绑定后证明；执行值和证明不能跨 EXECUTE 复用。

## 2. 原提交为什么导致退化

### 2.1 修改的目的与误伤范围

#28851 为转换诊断建立执行所有权：不能因常量折叠、下推或 JOIN 改写，提前产生告警、重复产生告警，或让原本会激活的告警消失。空输入和未激活 CASE 分支应保持安静。

`MayDiagnoseStatementParameter` 不只覆盖日期函数，也覆盖 implicit string-to-numeric CAST。Prepared 参数的物理表示包含字符串向量，语义来源类型另有元数据。因此整数键点查也可能包含 `id = CAST(? AS INT)`，不能从 PR 标题推断只影响日期 SQL。

### 2.2 扫描路径：在使用绑定值前丢掉谓词

原提交的执行链如下：

```text
WHERE integer_key = ?
  -> prepared 表达式包含隐式 CAST
  -> MayDiagnoseStatementParameter = true
  -> compileTableScanDataSource 在参数折叠前筛选存储谓词
  -> filterScanStorageExprs 排除含此表达式的谓词
  -> reader FilterList 和 BlockFilterList 失去选择性
  -> 读取/物化更多行和块，由上层 Filter 继续执行 WHERE
  -> 持锁事务中的 SQL 变慢，后续事务排队、1205、超时
```

相邻 parent 的 block filter 没有经过这套新增诊断筛选；reader 的诊断排除规则也更少。原提交把保守规则加入这两个位置，却没有在这里消费当前绑定的安全证明。

**单表逻辑下推与存储谓词丢失必须区分。** `pushdownFilters` 的 TABLE_SCAN 分支仍会把属于本表的条件放进 `node.FilterList`。EXPLAIN 显示 Filter Cond / Block Filter Cond，并不能证明编译后存储 reader 收到了这些条件。物理 Filter 的存在也可能来自编译器的诊断所有权要求。

本地复现记录了 stock 点查扫描约 125 个块，持锁事务执行缓慢；issue 的独立调查还有同一 holder 的扫描、长执行和 waiter 超时链。这支持“持锁期间工作被放大”，不构成锁释放泄漏或 commit 死锁的证据。

### 2.3 关系优化路径：保守边界扩大中间结果

`filterPushdownBarrier` 以及 JOIN 诊断保护限制跨 JOIN/聚合等节点的过滤移动、JOIN 重排和部分物理选择。原有候选检测偏向 JOIN ON；位于 FilterList/BlockFilterList 的参数转换未完整触发恢复路径。

因此只补 storage block filter 不能恢复整个查询的成本。扩大当前绑定的安全证明范围后，允许恢复关系优化；这与实验中 LoopJoin、向量复制热点消退一致。

证据边界：目前是规划代码与聚合 CPU 栈相互印证。尚未把全部 JOIN 开销逐条映射到具体 TPCC SQL 的前后计划和实际中间行数，不能把某一个 JOIN 算法选择声称为所有事务的唯一原因。

## 3. 为什么补丁后的性能还在变化

本节首先保留早期非 NVMe 探索，说明为何从只恢复 storage filter 走向绑定证明和复用；这些数据不用于最终性能验收。以下是各自 60 秒 CPU profile 的累计 CPU 秒；累计项互相包含，不能相加，也不能除以整场两分钟的事务数当作每事务成本。

| 路径 | parent2 | fix2：恢复存储过滤 | fix3：扩大证明并逐次重规划 | fix7：加入复用 |
| --- | ---: | ---: | ---: | ---: |
| 总 CPU 样本 | 18.95 | 22.50 | 21.22 | 19.10 |
| Filter.Call，累计 | 0.20 | 10.95 | 1.46 | 3.24 |
| TableScan.Call，累计 | 3.13 | 4.56 | 2.17 | 3.22 |
| initExecute，累计 | 1.78 | 0.91 | 8.48 | 3.28 |
| specialize，累计 | 0.75 | 0.38 | 5.22 | 0.74 |
| protobuf expression walk，累计 | 1.46 | 1.01 | 5.78 | 2.53 |
| PreparedPlanHasPercentileParams，累计 | 0.61 | 0.36 | 0.22 | 1.43 |

三层成本不能混在一起：

1. 原提交破坏选择性，放大扫描/物化，以及部分关系计划的中间结果。
2. fix3 恢复更多优化，但每次 execute 重新 BuildPlan / specialize，把成本转移到 frontend。
3. fix7 的缓存减少重规划，仍反复遍历计划判断静态属性。仅 `PreparedPlanHasPercentileParams` 就有 1.43 CPU 秒，占该 profile 总 CPU 的 7.49%；这是稳态中可以避免的工作。

`Filter.Call` 先调用子算子，不能把其累计时间全部当作过滤计算。反过来，也不能把 fix2 的累计时间全部解释为子扫描：排除 TableScan 栈后，Filter 下仍有 7.33 CPU 秒，其中 LoopJoin 3.68 秒、`appendJoinedRange` 3.27 秒、`Vector.UnionMulti` 2.71 秒（这些仍是嵌套项）。后续需要用实际输入/输出行数确认放大倍数。

fix7 为 216.32 tpmC，后跑 parent4 为 224.60 tpmC，但两者不是严格配对快照；主机还出现显著 I/O 等待和 slow WAL。比值约 96.3% **不能作为已经恢复性能的验收结论**。parent3 使用新数据目录时遇到 MORPC100/99 不兼容，也不能把启动失败计作性能结果。

### 3.1 NVMe 上的相邻版本对照

后续在 `/mnt/nvme` 使用各版本兼容、不可变的 10 仓初始数据快照。每轮复制到新的数据目录，启动新的服务进程和 BenchmarkSQL 进程；全部使用 10 仓、10 终端、`runMins=2`、server-side prepare。相邻 parent `1575e70c3d` 的四轮为 **4915.80、5782.55、5715.16、4610.70 tpmC**，全部 0 个事务错误。它们给出本机的实测波动范围，不能把单轮最高值当成稳定容量。

对 parent、fix11 与 fix13 的独立 60 秒 CPU profile，累计栈如下；各段采样的完成事务数不同，只比较热点及 CPU 占比，不把绝对秒数当作每事务成本。这些 profile 在同步落盘的配对吞吐实验之前采集，仅用于定位热点，不充当吞吐验收。

| 路径 | parent（CPU 秒 / 占比） | fix11（CPU 秒 / 占比） | fix13（CPU 秒 / 占比） |
| --- | ---: | ---: | ---: |
| 总样本 | 392.66 / 100% | 321.58 / 100% | 268.55 / 100% |
| `initExecuteStmtParamInSession` | 44.87 / 11.43% | 27.86 / 8.66% | 22.01 / 8.20% |
| plan expression walk | 36.14 / 9.20% | 13.51 / 4.20% | 8.77 / 3.27% |
| `specializePreparedExecutionPlan` | 17.88 / 4.55% | 13.19 / 4.10% | 9.87 / 3.68% |
| `ProbePreparedDiagnosticCandidatesWithProof` | 不存在 | 8.83 / 2.75% | 7.02 / 2.61% |
| `TableScan.Call` | 113.32 / 28.86% | 96.17 / 29.91% | 95.05 / 35.39% |

证明开销可见，但旧的重复表达式遍历和特化占比下降；扫描仍是最大的查询执行栈之一。fix13 的 `Filter.Call` 累计为 53.48 CPU 秒，其中大部分栈包含子节点 `TableScan`；不能把 53.48 秒与 95.05 秒相加，也不能由此直接断言多读了块。该 profile 只确认热点分布，吞吐验收使用无 profile 的独立运行。

## 4. 修复必须满足的契约

同一 SQL、绑定值、schema 和会话语义下：

- 结果、错误、告警次数与激活时机，不因存储裁剪、过滤位置、JOIN 选择或缓存命中而改变。
- 合法参数点查必须保留等价的 reader/block 选择性。
- 未能证明安全时保留原有诊断 owner；推测执行不向 statement warning sink 发布诊断。
- inactive CASE、空输入保持原语义；显式数字 CAST/写入转换仍保留原来的行级语义。
- 取消、资源不足、内部错误立即传播，不能伪装成“参数不安全，继续执行”。
- 稳态执行的证明成本与相关参数表达式数量相关；不能每次全计划反射扫描、重新规划。
- 证明和折叠值只属于一次绑定 generation；缓存只持有被证明可复用的参数化结构。

反例：safe → warning → NULL → safe 复用同一 handle 时，旧 safe 证明让 warning 被存储裁剪吞掉，或旧常量让 safe 第二次返回第一次的值，均是不可接受的修复。

## 5. 系统性方案

### 5.1 选择：同一 PREPARE generation 的两个参数化逻辑模板

保守模板沿用现有 PREPARE 结果。另有一个**假设诊断安全的参数化模板**，在没有绑定值的 PREPARE 环境里由同一原始 SQL/AST 生成；它只放宽由语句常量参数诊断引起的关系优化屏障。模板生成不执行 SQL、不探测参数、不发布告警；该模板在任何 execute 中都必须先通过本次证明才能使用。

首个候选执行可触发延迟生成：生成时暂时分离 Process 的绑定参数并在所有返回路径恢复，使用现有 `rebuildPreparePlan`/`WithPreparedJoinDiagnosticFree`，以 `IsPrepare`、`ParamTypes`、参数位置和计划属性作为准入。计划优化不能读取当前绑定值；任何调用仍需绑定值，或生成的模板丢失所需 ParamRef，立即放弃缓存并保守执行。生成和准入是本设计的首个定向验证门槛，未通过则不能用“先绑定后恢复 ParamRef”替代。

PREPARE generation 还记录当时的全局及语句级 `optimizer_hints`。懒构造时若任一来源发生变化，拒绝该 generation 的安全模板并记住拒绝结论，继续使用保守计划；不让新的 JOIN/order/runtime-filter hint 混入旧 prepared generation。即使 hint 匹配，候选与保守计划的 scan `ExtraOptions`、参数引用覆盖及值敏感缓存 traits 仍需一次性准入核对。

一个 PrepareStmt 最多持有一个安全逻辑模板及现有单条 runtime compile 缓存；模板属于该 generation，reprepare/schema/mode/protocol 失效时同时清除。安全模板本身保留参数引用，不持有当前证明、数据值、statement warning 或执行 Process。

在 prepare/reprepare、计划变换完成的明确边界上，一次性收集既有 prepared 元数据：

- 哪些 predicate owner 有待绑定证明的语句常量子表达式；对应参数位置和转换语义。
- 哪些表达式静态安全，哪些依赖行/volatile，哪些需要绑定后证明。
- percentile、pagination、numeric overload、generate_series、外部快照等缓存限制。

优先扩充现有 `PrepareStmt` 元数据，局部使用随 plan generation 失效的表达式位置/引用；不新增全局缓存、通用表达式框架或 wire schema。**初版选双模板候选列表**：保守模板和安全模板各在构造时收集 `OnList`、`FilterList`、`BlockFilterList`、vector index prefilter 中可能诊断的参数表达式；含未列入这些既有消费者字段的新诊断 owner 时拒绝安全模板。计划复制/改写后重新收集，不能沿用旧指针。两份模板各自执行证明，不能凭保守模板的结果授权安全模板。合法、warning、NULL、再合法的同 handle SQL 结果与告警需保持不变。结构等价候选去重曾作为额外优化实验；由于同步落盘的 TPCC A/B 无稳定收益，未纳入最终实现。

静态 traits 随各计划 generation 固定；当前 `shouldCachePreparedRuntimeSpecialization` 对 template/execution plan 的重复百分位扫描应移出执行热路径。

### 5.2 最终绑定边界负责一次执行的证明

在二进制参数、long data、来源类型和 runtime specialization 所需参数归一化完成之后，统一执行证明：

1. 从预先收集的候选中，只探测相关的最大语句常量子表达式。
2. 使用现有隔离求值机制，依次探测保守与安全模板的预计算候选列表；全部安全才产生本次执行的 plan-wide 结论。允许形成执行局部的 typed value，所有内存由本次执行拥有。无论优化模板是新建还是缓存命中，均证明两份候选列表；不凭“旧模板安全”推断“新模板安全”。
3. 有 SQL 诊断：丢弃推测诊断，保留原 owner 和保守计划，由实际激活位置发布。
4. 无诊断：允许相关谓词进入存储过滤，并允许其作用范围内的关系优化。
5. 取消/内部/资源错误：失败退出并释放探测结果。

证明至少受 plan generation、binding generation 和所依赖的会话语义约束。retry 若重绑定、重规划或改变这些依赖就重新证明；reset/free 不保留旧证明。一个布尔 proof 只授权本次选择的整份计划中已登记的诊断候选；它不表示任何未登记表达式安全。若安全模板在本次证明后才生成，先完成第二列表的探测再执行；不能用首轮保守证明提前授权它。

不能把“packet 来源为整数”直接当作所有 CAST 安全；有符号性、范围、窄化、scale 等仍须匹配。对于协议已成功解码并规范化的 COM_STMT_EXECUTE 整数参数，可加严格的类型快速证明：候选必须恰好为隐式 `CAST(ParamRef AS integer)`，协议来源是已解码的有符号 SHORT/LONG/LONGLONG 或无符号对应类型，目标整数与来源同符号且宽度不小于来源；NULL 无转换诊断。该集合内的转换保持整数值不变，也不会产生截断/溢出/格式告警，故只对这类候选跳过表达式求值。协议类型和 unsigned 位必须由 frontend 从本次成功解码的 `PrepareStmt.ParamTypes` 按参数位置显式传入探测；`Process.GetPrepareParamKind` 不含宽度/符号，普通整数参数的 `GetPrepareParamType` 仍为 `T_any`，两者均不能替代该来源。还须对本次参数原始字节做无分配的十进制解析与规范格式往返校验：以协议来源的固定位宽解析，要求完整消耗、范围内且字节恰好等于对应的规范十进制格式；这使内部手工构造的参数即使错误地搭配整数 `ParamTypes` 也无法取得证明。按本次 `getFromSendLongData` 集合逐位置排除 long data。仅使用整数 OID 的固定位宽，不把 `Type.Width` 显示元数据当作值域。解析失败或元数据缺失一律回退到现有隔离求值；跨 execute 不保存任何通过状态。TINY 的布尔兼容启发式、BIT/YEAR、SEND_LONG_DATA、SQL PREPARE 变量、未知来源类型、不同符号、缩窄、浮点/小数/日期及下述白名单之外的复合表达式仍使用现有隔离转换探测。快速证明每次绑定读取本次协议类型，不缓存 proof；需要类型边界、NULL、非法文本、宽度/符号反例及实际 SQL 对照测试。若 CPU 降低但 TPCC 吞吐无改善，移除此优化，不扩大准入。

后续固定 JDBC 参数序列定位到复合主键候选：`serial(CAST(? AS INT), CAST(? AS INT))` 在同一绑定中分别出现在保守与安全模板，根节点不是裸 CAST，现有快速证明会对整个 `serial` 做两次隔离求值。仅对 `serial` / `serial_full` 且**每个参数都恰为上述直接隐式整数 CAST** 的表达式，允许逐个 CAST 使用同一次绑定的字节/来源类型证明，并省略对外层编码函数的推测求值；任何其他参数形状、显式 CAST、未知类型、NULL 之外的非法转换均回退原探测。两个模板仍各自按本次绑定验证，证明不跨 EXECUTE 缓存。`serial`/`serial_full` 对已类型化整数的编码本身不产生 SQL 转换诊断；其类型支持由计划绑定保证，实际执行仍会构造键并传播内部/资源错误。该扩展只消除诊断探测重复编码，不改变实际查询谓词、结果或存储准入。需要覆盖两种编码函数、多参数中单个 warning/NULL/long data、显式 CAST 和非整数参数反例；用固定输入 JDBC 点查/JOIN 与同步落盘的 10W10T 配对重新验收。若无清晰收益或吞吐仍低于 parent，撤回此扩展或继续定位其他路径。

### 5.3 消费者使用同一结论，避免阶段错位

关系优化、reader filter、block filter 和物理编译消费同一次执行、同一语义范围的证明。编译器不再为同一执行的每个 reader/block 副本分别探测；它根据本次 proof，只对所选模板已登记的语句常量参数诊断放行，继续独立排除 literal、row-scoped、volatile 诊断。未登记表达式仍走保守排除。不要在参数尚不可用的拓扑构造/worker 阶段自行重新求值。

先闭合简单 scan：合法绑定的存储过滤可在执行初始化时折叠，不需要为了单表点查全量重规划。保留完整 SQL 谓词的最终语义检查。

扫描专用路径须区分“上层 Filter 作为诊断 owner 持有同一谓词副本”和“必须重新规划才能移动谓词”。若上层参数诊断谓词与 TABLE_SCAN.FilterList 的谓词结构等价，且计划无 JOIN，则该单表扫描可以保留原参数化模板；它只需本次绑定证明，仍保留原来的上层语义 owner。若谓词仅在上层、存在 JOIN，或无法证明副本等价，使用安全模板。

PREPARE 的统计选择仍可能因为未知参数而未在 BlockFilterList 保留对应块谓词。优化器明确设置 `blockFilter=2` 时则是禁用决策，不能把空列表当成统计遗漏。规划结束时，若生效的 hint 为 2，对普通 TABLE_SCAN 在已有 `ExtraOptions` 为空时写入专用的禁用标记；已有非空 `ExtraOptions` 的特殊 scan 不做补全。该标记属于参数化计划，经过已有深拷贝和计划序列化保留，不依赖执行时会话变量。物理编译只在普通 scan 的标记不存在且 `ExtraOptions` 为空、已验证本次绑定、谓词已获准进入 reader 时，对其中可用 `ExprIsZonemappable` 证明可用于 zone map 的参数诊断谓词补充原始 block 副本；按现有 block 谓词等价规则去重，只改本次 Scope 的节点副本。明确禁用、无证明、字面量/行级诊断、volatile 或不能映射 zone map 的谓词不得经此路径补充。补充的原始表达式在远端独立折叠，不传 coordinator Fold ID。对全局及语句级 hint 分别增加 `blockFilter=2` 对照测试，并检验同一 prepared handle 在执行前后改变会话 hint 时仍遵循已规划的决策。这个补全会改变 block 评估成本，需由实际 NVMe profile 和 TPCC 验收；若收益不足或出现语义差异，应收窄准入，而非放宽证明。

对确实需要改变关系结构的 JOIN，使用上述无绑定安全模板。初版只在整份保守模板的相关候选全部安全、且安全模板全部诊断转换验证通过时选择它；部分安全仍选择保守模板。这样布尔选择有明确的整份计划范围，不会把某个 predicate 的证明授权给无关节点。

之前把安全判定直接塞进 Filter 嵌入 TableScan 的编译路径，出现大量 `get prepare params error`。该实验已撤销。其具体参数生命周期错误尚未取得完整栈证据；它说明不能把“同一表达式能在某处求值”当作“所有编译阶段都能求值”。

### 5.4 缓存参数化结构，每次更新证明和值

现有 `temporal-compatibility.md` 明确约定 proof 和 execution-local specialized plan 不进入 reusable prepared cache。本设计保持 **proof 和按当前值特化的执行计划不缓存**，但允许一个无绑定生成的参数化假设模板进入 PrepareStmt。该区别需同步写入原文并随设计审查；目前的 fix7 直接缓存绑定后生成的 localQuery，不符合此准入。

复用现有有界、单条 runtime specialization cache。缓存准入需证明：

- 安全模板在 Process 没有绑定参数时生成；其表达式与计划结构不得含当前值派生常量、删除的值相关分支、基于当前值的 cardinality 或物理配置。模板与保守计划使用相同 normalized ParamTypes。`RestorePreparedRuntimeParamRefs` 不能证明这些条件，也不用于这个模板的生成。
- 相同缓存类别下，计划变换对允许的所有绑定成立。无法证明时不缓存该变体；必要的值相关配置沿用现有不缓存/显式值键策略。
- 缓存 key/失效规则覆盖结构依赖的参数语义域、schema、SQL mode、timezone、collation 等；首先复用已有规则，审核缺失项，而不是盲目加入所有 session 值。
- 每次命中前重新建立当前绑定的 proof；执行值不从旧 proof 或旧 fold 结果继承。
- 对模板/最终计划中新产生的转换执行独立证明，或拒绝优化；只证明保守模板不足以授权缓存命中。相同缓存类别的结果/告警一致性需由两组不同合法绑定值以及非法绑定值的真实 SQL 结果验证。

状态流：

```text
prepare/reprepare -> 静态 traits + 保守模板；安全参数化模板初始为空
execute bind -> 当前 proof（本次绑定所有相关 owner）
  不安全 -> 保守模板 + 原诊断 owner
  安全   -> 简单 scan 执行局部折叠；若需关系优化：
          -> 无模板时在无绑定环境构造并审核参数化模板
          -> 验证本次模板诊断转换 -> 编译执行 -> 成功后才发布模板/compile
execute cleanup -> 丢弃当前 proof / values -> 释放被替换的旧 compile
generation change / close -> 清除关联 metadata、模板和 compile cache
```

编译失败不发布候选；旧 compile 必须在当前 statement 清理后退休，避免释放共享 Process 时清掉新参数。候选构造和安装由 PrepareStmt 所在 session 串行化；如果此假设不能由现有 session 执行模型证明，必须加同步或取消缓存。沿用现有 AP 不缓存执行特定物理拓扑的边界。

多 CN 不传递 proof bool。协调 CN 在本次绑定证明后编译物理 Scope，确定 reader/block 的**已选择原始谓词列表**。`copyBlockFiltersForRemoteRun` 当前会优先从 `DataSource.node.BlockFilterList` 复制原逻辑列表以避开 coordinator Fold ID；因此必须让每次物理 Scope 的 node 副本携带**本次筛选后的原始 block 谓词**，远端再用自己的 Process 重新 Fold。不能把保守执行中排除的原始谓词通过该路径送回远端，也不能发送 coordinator 的 Fold ID。reader 的物理 FilterExpr 沿既有 Scope/wire 传递。远端使用同一次 EXECUTE 的参数快照；最小 2-CN 测试须确认 safe/invalid 切换、远端 block 谓词和告警一致。若此路径不能验证，优化准入必须限制在确定没有 remote Scope 的执行，不能靠文档宣称“只测单 CN”。

| 事件 | 证明 | 模板 / compile |
| --- | --- | --- |
| safe 绑定 | 本次新建，执行后销毁 | 通过准入才可命中或发布安全变体 |
| SQL warning/invalid | 本次判为不安全 | 不使用安全变体；原 owner 在真实激活时发诊断 |
| NULL | 本次无转换诊断，但无稳定缓存类别 | 可用当次已证明的安全结构；结果及告警仍按 SQL NULL 语义执行，不缓存该绑定的物理变体 |
| cancel/内部/资源错误 | 立即失败，不降级为 SQL warning | 未完成候选不发布，释放执行局部资源 |
| 编译/执行失败 | 执行局部证明丢弃 | 编译失败无发布；已发布模板仍属于 generation，失败原因若涉及模板失效则清除 |
| retry/reprepare/schema/mode/protocol 变化 | 在新上下文重新证明 | 新 generation 重建 traits，旧模板及 compile 失效 |
| cache replacement/close | 当前执行完成后丢弃 | 旧 compile 在当前 statement 清理后退休，Close 全部释放 |

### 5.5 优化优先级

1. 恢复选择性：共用绑定证明，闭合 reader/block/关系优化消费者。
2. 消除逐次重规划：简单 scan 不重建，关系变体只缓存经验证可复用的参数化结构。
3. 消除重复静态分析：plan traits 每 generation 一次，探测只访问候选表达式。
4. 根据新 profile 再决定是否优化 CAST parser、typed binding、Filter/Project 融合。当前证据不足以优先进行这些更大改动。

## 6. 不采用的方案

| 方案 | 原因 |
| --- | --- |
| 全局移除诊断 guard | 会破坏告警、空输入、短路语义 |
| 只放行 BlockFilterList | reader 与关系优化仍可能失去选择性 |
| 所有参数一律改成数字类型 | 改变协议来源/转换语义；无法覆盖范围与非法输入 |
| 每次 EXECUTE 重建完整计划 | fix3 已暴露明显 frontend 成本 |
| 缓存绑定后再恢复 ParamRef 的优化计划 | 不能证明未残留值相关的计划形状及配置；fix7 仅为探索 |
| PREPARE 时无条件执行安全计划 | 非法绑定会丢诊断；必须由当前绑定 proof 决定选择 |
| 缓存本次安全结论/折叠常量 | 下一次绑定可能非法或值不同 |
| 只优化反射遍历实现 | 先删除本来不该发生的重复遍历，收益与契约更直接 |
| 增大超时、锁配额或事务并发 | 无法恢复点查选择性 |

## 7. 实施与验收顺序

### A. 建立最小公共入口证据

通过真实 SQL prepare/execute，覆盖简单主键、复合键、JOIN ON、JOIN 上方过滤。分别保存逻辑计划、实际存储谓词、扫描块/行、中间行数和告警。JOIN 案例用来补齐当前聚合 profile 的 SQL 归属缺口。

### B. 统一执行证明，先修 scan

实现一次绑定的证明和消费者闭合，保留保守 fallback。通过公开执行结果和 reader/block 选择性的独立 oracle 验证，不能只测 helper 返回 true。

### C. 受控复用和生命周期闭合

已用无绑定 `rebuildPreparePlan` 的定向测试验证真实 PREPARE SQL 在 Process 参数分离期间能重建，ParamTypes 一致且谓词 ParamRef 保留；另用现有 `select_test.bind_select` JOIN 案例比较两种无绑定计划，要求安全模板比保守模板多出实际 scan filter，且两参数均留在计划中。此项不是所有 SQL 的通用证明；每个候选仍需准入。实现参数化关系变体后，更新原 temporal cache 契约。覆盖 safe → warning → NULL → safe、参数来源类型变化、reprepare、schema/session 变化、retry、取消、编译失败与 cache replacement。验证实际返回值和 warning 次数，而不只是计划指针相等。

### D. 静态 traits 与热点回归

删除逐次百分位/诊断候选全计划扫描。稳态简单点查不重规划，不因缓存准入全计划反射遍历；用计数或分配基准确认，再查看整体 CPU。

语义案例还需覆盖：合法/非法数值、溢出、有符号/无符号、NULL、空表、过滤后空输入、inactive CASE、JOIN 两种空输入方向、nonempty no-match、显式 CAST 与写入转换控制组。预期沿用已声明的诊断激活契约，不能机械地把“最后零行”当作“永远无告警”。

性能验收仍采用用户指定的 **10 仓 / 10 并发 / 每轮两分钟**，数据与数据库的物理目录经 `findmnt`、`df` 和启动日志确认在 `/mnt/nvme`：

- 相邻 parent 和修复版使用独立目录、来自兼容且不可变的同一初始数据快照；若格式不兼容，确定性重新装载等价数据，记录差异。
- 固定 workload 配置与随机输入分布，等价预热，交替顺序至少两组；每轮前恢复快照，避免复用已跑过 TPCC 的数据。复制后对目标 NVMe 文件系统执行 `sync -f`，记录同步前后的 Dirty/Writeback，仅在同步完成后启动服务和计时，避免初始数据复制的异步写回混入压测。
- 记录代码 SHA、补丁 hash、二进制 build ID、配置、数据 manifest、开始时间与主机 I/O。
- 同时记录结果/事务错误、吞吐、尾延迟、scan blocks/rows、JOIN intermediate rows、CPU/完成语句；CPU 归一化使用 profile 同一窗口内的完成量。
- 验收门槛：语义测试全过，合法点查选择性与 parent 一致，稳态不全量重规划/静态反射扫描。固定版 10W10T 吞吐至少达到退化前 parent，采用三组以上交替、相同 workload 配置与独立初始数据的配对结果判断；样本波动若盖过差异，就继续排查/补测，不把单轮比值视为恢复。事务错误不能增加，statement 级扫描和 CPU 成本不得留下新的确定性退化。

## 8. 当前交付与证据边界

已实现按绑定 generation 重新证明的诊断安全谓词、reader/block 共同准入、普通扫描的 block 谓词补全、无绑定构造并审核的关系安全模板、严格限定的二进制整数快速证明，以及执行局部 Scope 的远端 block 谓词选择。单表 scan 保留原参数化计划；需要关系结构变化时才构建无绑定模板。模板仅在物理编译成功后发布，拒绝准入的 generation 不再反复重建。全局及语句级 optimizer hint 在 PREPARE 时记录；延迟构建时若改变，则保守执行。

单元测试覆盖合法/非法/NULL 绑定切换、真实 PREPARE/EXECUTE 值与 warning、无绑定模板 ParamRef 和静态准入、整数类型边界、block hint、远端 Scope 的空/非空筛选，以及缓存生命周期。fix13 代码的 `pkg/frontend`、`pkg/sql/plan` 与 `pkg/sql/compile` 全包测试、`go vet` 及用 Go 1.26.4 构建的 `golangci-lint` 增量检查均通过；同步配对压测使用的 fix13 二进制 SHA256 为 `8e724bb3ba4604b69c7bd7af061eca975409cf44391c341f3ba393a8e3576bed`。新增 BVT 的 safe → warning → NULL → safe、scan/JOIN 与清理结果，在该二进制的全新单 CN 中连续两次、全新同进程双 CN 的每个 CN 中连续两次均为 34/34 成功；双 CN 结果只证明该拓扑和用例下的实际 SQL 行为，不代表独立进程的所有远端调度。补丁之后无冲突 rebase 到 `a543410f76`，两个上游提交只改 fulltext/vector；相关三包测试和新二进制的单 CN BVT 34/34 再次通过。

fix11 阶段的干净 NVMe 10 仓 10 并发两分钟运行分别为 5929.05、5028.30、4572.19 tpmC，均 0 事务错误；相邻 parent 四轮为 4915.80、5782.55、5715.16、4610.70 tpmC，均 0 错误。两组波动重叠，第三轮 fix11 比 parent 最低值低约 0.8%，因此这些非配对数据尚不足以宣称达到吞吐门槛。

fix13 首次六轮交替配对的 parent 为 5156.49、6019.53、5603.66 tpmC，fix 为 4488.30、4407.80、5549.58 tpmC，全部 0 错，均值比 86.09%。随后一次独立相邻监控轮次 fix 为 5746.14、parent 为 5644.13 tpmC，同样 0 错，方向反转。监控轮次的前 20 秒 NVMe 写等待 fix 约 60 ms、parent 约 13 ms，后段降至约 2 ms；复制后的 Dirty 曾达 988,400 kB，`sync -f` 后为 740 kB、Writeback 为 0。首次六轮没有同步屏障，数据复制的异步写回污染了计时前段，不能用其均值断言补丁稳定退化。

按相同顺序重跑六轮，每轮从版本兼容的不可变 10 仓快照复制到新目录，先 `sync -f` 再启动服务和两分钟压测；各轮同步后的 Dirty 为 144–1160 kB，Writeback 为 0–160 kB。三组 parent 为 5890.13、5823.96、6302.09 tpmC，fix13 为 5641.21、5813.17、6591.57 tpmC，全部 0 事务错误。均值分别为 6005.39 和 6015.32 tpmC，比值 100.17%；逐组比值为 95.77%、99.81%、104.59%。前 20 秒 NVMe `w_await` 的 parent/fix 分别为 1.35/1.40、1.29/1.36、1.44/1.33 ms，消除了早期明显不均衡的写回。该结果支持吞吐恢复到同一水平，0.17% 小于轮间波动，不能解释为确定性提速。CPU profile 独立于性能轮次，仅作热点定位；`Filter.Call` 的累计样本包含其子算子的扫描成本，不能当作额外 CPU 加总。

实验性结构等价候选去重（fix14）另以同一主线快照、每轮 `sync -f` 做了三组相邻 A/B：fix13 为 5232.85、5588.52、6316.49 tpmC，fix14 为 6188.37、5169.12、5275.61 tpmC，全部 0 错。均值 fix13 5712.62、fix14 5544.37，fix14/fix13 为 97.05%；逐组方向相反，无法证明该额外优化有收益。代码撤回了结构去重。

rebase 后的交付二进制 SHA256 为 `d3fd1d269ad7e6d20cde859811814ad7ffb56d49cfe6fb26b9db0c83d1561931`。再做三组同步落盘、干净快照的 parent/fix 交替对照，parent 6557.35/5682.15/6203.31，fix 5856.60/5779.74/5869.87 tpmC，均 0 错；均值 6147.60/5835.40，fix/parent 为 94.92%，**未达到吞吐验收门槛**。单组在 `sync -f` 后额外用 `POSIX_FADV_DONTNEED` 将两版复制数据的驻留页从约 685 MB 清到 0，parent 6533.72、fix 5763.26 tpmC，0 错；整盘读量约 33.1/47.7 GB，写等待接近。该单组确认复制预热不能单独解释差距，但整盘读量不是 SQL 级扫描计数。固定 JDBC server-side prepare 的相同整数参数序列在两组独立新快照上测得 stock 点查 parent/fix 分别 1.496/1.623 与 1.463/1.468 ms/次，customer/warehouse JOIN 为 0.361/0.467 与 0.358/0.443 ms/次；JOIN 有重复的额外成本。`EXPLAIN VERBOSE FORCE EXECUTE` 的两版逻辑计划相同；调试日志确认复合主键的 `serial(CAST(?), CAST(?))` 在每次绑定无法命中裸 CAST 快证，保守及安全模板分别做一次隔离求值。

严格限定的 `serial`/`serial_full` 整数参数快速证明消除了该重复隔离求值。真实 JDBC 二进制 prepared handle 的 safe→invalid→NULL→safe 序列在旧补丁和新优化上的行、告警次数完全一致，新增 BVT 的 SQL PREPARE 控制组在旧、新二进制分别为 51/51 与连续两次 51/51。旧补丁与新优化在同一 main 快照、每轮 `sync -f` 并将初始数据文件缓存清零后，按旧/新/新/旧/旧/新交叉跑三组两分钟 10 仓 10 并发：旧为 6852.14/5689.54/5828.81，新为 5878.39/6333.64/6582.75 tpmC，全部 0 错；均值 6123.50/6264.93，新/旧为 102.31%，但逐组方向不一致。对应六轮整盘 NVMe 读取量为旧 23.38/47.88/48.42 GiB、新 48.22/31.30/17.92 GiB；随机输入与 I/O 波动盖过小幅吞吐差异，不能把 2.31% 当作确定性提速。

另用同样的全新快照各跑一轮两分钟 TPCC，在中间采集相同 60 秒 CPU profile：旧补丁与新优化分别完成 14,202/15,324 个事务，CPU 总样本 394.63/408.21 秒；`ProbePreparedDiagnosticCandidatesWithProof` 累计 10.67/1.60 秒（占总样本 2.70%/0.39%），`initExecuteStmtParamWithResolverInSession` 累计 34.27/26.83 秒，`TableScan.Call` 累计 118.77/121.36 秒。每完成事务的证明 CPU 样本约 0.75/0.10 ms，直接验证重复隔离求值的稳态热点已消除；采样轮次的 NVMe 读量仍为 34.05/20.19 GiB，不将总吞吐差额完全归因于证明优化。

对退化前直接 parent 版本 `1575e70c3d` 与复合键优化版再做三组交叉，沿用每轮全新数据副本、`sync -f`、`POSIX_FADV_DONTNEED`、10 仓 10 并发两分钟。parent 为 5769.84/5780.03/7096.99，优化版为 6920.98/5116.77/5878.75 tpmC，全部 0 错；均值 6215.62/5972.17，优化版为 parent 的 **96.08%**，目前**不能认定达到性能验收门槛**。对应 parent 的 NVMe 总读量为 45.88/45.95/7.36 GiB，优化版为 20.75/55.48/48.62 GiB；第三轮 parent 也出现低读量高吞吐，说明这类波动不只发生在补丁版，但总盘读量尚未归因到具体 SQL 或计划。

为减少 workload 随机输入变化，另造仅供本地测量的 BenchmarkSQL JAR，将 `io.mo.jTPCCRandom` 的 `System.nanoTime()` 种子入口替换为固定起点的原子递增种子，事务逻辑及比例保持原版；原始 JAR SHA256 为 `6d3b9549b4cfe435085aa0db817faa6068d93c5ea5576d4cdf8f91c105232b2e`，测量 JAR 为 `167a124cd7fc8ecf8f975915bcd3b3b7944642395fe0672c4ca4e63a015b3ab2`。这固定 PRNG 初始序列，不固定线程调度。最终修复二进制 SHA256 `938d5bb3890240af1bfe779d4ded50b62d8ac732e5ef05837494097b29d52b82` 的两轮同版本、干净快照校准为 5922.86/5751.51 tpmC，NVMe 读量 47.68/45.10 GiB，均 0 错；比此前随机种子轮次稳定，但仍存在约 3% 吞吐差。

随后用该 JAR 和最终二进制，对退化前直接 parent 做三组交叉顺序 `parent→fix / fix→parent / parent→fix`。每轮复制兼容版本的不可变 10 仓初始快照到全新 NVMe 目录，`sync -f` 并清除数据文件页缓存后启动全新服务及 BenchmarkSQL，10 终端、2 分钟。结果如下；NVMe 读量为整盘监控值，不能直接等同 SQL 扫描量。

| 组 | parent tpmC | fix tpmC | parent NVMe 读 GiB | fix NVMe 读 GiB | 事务错误 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 5786.94 | 5764.79 | 45.75 | 45.99 | 0 / 0 |
| 2 | 5596.39 | 5701.52 | 43.98 | 44.78 | 0 / 0 |
| 3 | 5772.13 | 5764.99 | 45.34 | 46.44 | 0 / 0 |

均值 parent **5718.49**、fix **5743.77 tpmC**，fix/parent **100.44%**。相邻组方向有正有负，0.44% 小于轮间波动；结合低于 1.1 GiB 的逐组 NVMe 读量差与 0 错误，该对照支持“恢复到退化前水平”，不支持“稳定提速”。此前随机种子的三组 96.08% 结果保留为波动/方法边界，不能隐藏或用事后调整替代。

增量复审指出 NULL 形状在检查来源和目标类型前获准的问题；现已先验证协议 SHORT/LONG/LONGLONG、目标整数 OID、符号、位宽和 long data，再对 NULL 快证。新增负向 UT、真实 COM_STMT_EXECUTE 解码→`initExecuteStmtParam`→候选探测的同 handle safe→invalid→NULL→safe 测试均通过，`pkg/frontend` 全包测试、该包 `go vet` 和 Go 1.26.4 `golangci-lint` 增量检查通过。测试中复合 `serial` 候选为手工构造；自动收集和实际行/告警由独立真实 JDBC 复合主键 SQL 验证，BVT 的 SQL PREPARE 控制组不触发二进制快证。代码正确性复审 PASS。

最终二进制还在新建同进程双 CN 集群上执行新增 BVT，CN1/CN2 各连续两次均为 51/51，通过且无忽略。真实 JDBC `useServerPrepStmts=true` 的同一二进制 prepared handle 在两个 CN 上分别执行 safe→invalid→NULL→safe，行集合依次为 `[two]`、`[three,two]`、`[]`、`[three]`，warning 数依次为 0、4、2、0；两端一致。该结果覆盖实际参数解码与复合键表达式路径；仍只代表本地同进程双 CN 拓扑。

本地证据目录：`/mnt/nvme/issue-29429-perf`，包括两版快照、二进制、BVT 日志、单独的 60 秒 profile、`paired-final` 未同步轮次和 `paired-synced` 同步轮次的脚本及输出；早期非 NVMe 探索保存在 `/home/xupeng/issue-29429-exact`、`/home/xupeng/issue-29429-main-bench`。
