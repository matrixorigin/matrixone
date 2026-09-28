# Prepared 参数诊断与选择性：根因及系统性修复方案

日期：2026-09-28。状态：设计审查中；本文不代表方案已实现或性能验收通过。

实施对象：issue #29429 的 prepared 查询修复 PR。设计触发条件：新增有界的 prepared 优化变体，改变原 temporal compatibility 的缓存契约；跨 frontend、planner、compiler；影响 TPCC 热路径与可用性。本方案不改变 SQL 协议、持久格式或跨 CN 证明传输；多 CN 的执行副本仍需验证同一绑定语义。

## 1. 范围与结论

- 问题：[issue #29429](https://github.com/matrixorigin/matrixone/issues/29429)，尤其是 [root-cause update](https://github.com/matrixorigin/matrixone/issues/29429#issuecomment-5872195452)。
- 引入提交：`a99db843c7b4630a86883addc9424433d93fc2d2`，PR #28851。
- 相邻正常版本：`1575e70c3db676d3ed00abc080aadc029b0e1435`。
- 实验补丁基于 main `1ded9fc914803cb2ffc1215be353703d640b3f02`，工作树 `/home/xupeng/mo-worktrees/issue-29429-main-fix`。
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

以下是各自 60 秒 CPU profile 的累计 CPU 秒；累计项互相包含，不能相加，也不能除以整场两分钟的事务数当作每事务成本。

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

一个 PrepareStmt 最多持有一个安全逻辑模板及现有单条 runtime compile 缓存；模板属于该 generation，reprepare/schema/mode/protocol 失效时同时清除。安全模板本身保留参数引用，不持有当前证明、数据值、statement warning 或执行 Process。

在 prepare/reprepare、计划变换完成的明确边界上，一次性收集既有 prepared 元数据：

- 哪些 predicate owner 有待绑定证明的语句常量子表达式；对应参数位置和转换语义。
- 哪些表达式静态安全，哪些依赖行/volatile，哪些需要绑定后证明。
- percentile、pagination、numeric overload、generate_series、外部快照等缓存限制。

优先扩充现有 `PrepareStmt` 元数据，局部使用随 plan generation 失效的表达式位置/引用；不新增全局缓存、通用表达式框架或 wire schema。**初版选双模板候选列表**：保守模板和安全模板各在构造时收集 `OnList`、`FilterList`、`BlockFilterList`、vector index prefilter 中可能诊断的参数表达式；含未列入这些既有消费者字段的新诊断 owner 时拒绝安全模板。计划复制/改写后重新收集，不能沿用旧指针。相同表达式指针只探测一次；同一表达式的独立副本允许有界重复，成本只与候选数相关。

静态 traits 随各计划 generation 固定；当前 `shouldCachePreparedRuntimeSpecialization` 对 template/execution plan 的重复百分位扫描应移出执行热路径。

### 5.2 最终绑定边界负责一次执行的证明

在二进制参数、long data、来源类型和 runtime specialization 所需参数归一化完成之后，统一执行证明：

1. 从预先收集的候选中，按相关的最大常量子表达式去重。
2. 使用现有隔离求值机制，依次探测保守与安全模板的预计算候选列表；全部安全才产生本次执行的 plan-wide 结论。允许形成执行局部的 typed value，所有内存由本次执行拥有。无论优化模板是新建还是缓存命中，均证明两份候选列表；不凭“旧模板安全”推断“新模板安全”。
3. 有 SQL 诊断：丢弃推测诊断，保留原 owner 和保守计划，由实际激活位置发布。
4. 无诊断：允许相关谓词进入存储过滤，并允许其作用范围内的关系优化。
5. 取消/内部/资源错误：失败退出并释放探测结果。

证明至少受 plan generation、binding generation 和所依赖的会话语义约束。retry 若重绑定、重规划或改变这些依赖就重新证明；reset/free 不保留旧证明。一个布尔 proof 只授权本次选择的整份计划中已登记的诊断候选；它不表示任何未登记表达式安全。若安全模板在本次证明后才生成，先完成第二列表的探测再执行；不能用首轮保守证明提前授权它。

不能把“packet 来源为整数”直接当作所有 CAST 安全；有符号性、范围、窄化、scale 等仍须匹配。类型快速证明仅作为后续可证明等价的优化，初版复用现有转换实现。

### 5.3 消费者使用同一结论，避免阶段错位

关系优化、reader filter、block filter 和物理编译消费同一次执行、同一语义范围的证明。编译器不再为同一执行的每个 reader/block 副本分别探测；它根据本次 proof，只对所选模板已登记的语句常量参数诊断放行，继续独立排除 literal、row-scoped、volatile 诊断。未登记表达式仍走保守排除。不要在参数尚不可用的拓扑构造/worker 阶段自行重新求值。

先闭合简单 scan：合法绑定的存储过滤可在执行初始化时折叠，不需要为了单表点查全量重规划。保留完整 SQL 谓词的最终语义检查。

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
| SQL warning/invalid/NULL | 本次判为不安全 | 不使用安全变体；原 owner 在真实激活时发诊断 |
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

删除逐次百分位/诊断候选全计划扫描并去重 probe。稳态简单点查不重规划，不因缓存准入全计划反射遍历；用计数或分配基准确认，再查看整体 CPU。

语义案例还需覆盖：合法/非法数值、溢出、有符号/无符号、NULL、空表、过滤后空输入、inactive CASE、JOIN 两种空输入方向、nonempty no-match、显式 CAST 与写入转换控制组。预期沿用已声明的诊断激活契约，不能机械地把“最后零行”当作“永远无告警”。

性能验收仍采用用户指定的 **10 仓 / 10 并发 / 每轮两分钟**，数据与数据库的物理目录经 `findmnt`、`df` 和启动日志确认在 `/mnt/nvme`：

- 相邻 parent 和修复版使用独立目录、来自兼容且不可变的同一初始数据快照；若格式不兼容，确定性重新装载等价数据，记录差异。
- 固定 workload 配置/随机输入，等价预热，交替顺序至少两组；每轮前恢复快照，避免复用已跑过 TPCC 的数据。
- 记录代码 SHA、补丁 hash、二进制 build ID、配置、数据 manifest、开始时间与主机 I/O。
- 同时记录结果/事务错误、吞吐、尾延迟、scan blocks/rows、JOIN intermediate rows、CPU/完成语句；CPU 归一化使用 profile 同一窗口内的完成量。
- 验收门槛：语义测试全过，合法点查选择性与 parent 一致，稳态不全量重规划/静态反射扫描。固定版 10W10T 吞吐至少达到退化前 parent，采用三组以上交替、相同输入及独立初始数据的配对结果判断；样本波动若盖过差异，就继续排查/补测，不把单轮比值视为恢复。事务错误不能增加，statement 级扫描和 CPU 成本不得留下新的确定性退化。

## 8. 当前交付与证据边界

已有补丁证明了恢复存储选择性、扩大安全证明和减少逐次规划的方向。它仍是实验实现：尚未完成参数化缓存的全套语义/生命周期验收；新增缓存测试主要检查计划复用，不能替代结果与告警断言。

已有 focused tests 和构建证据可复用；较大 frontend 测试选择曾出现 `no such table .nation`，涉及重规划进入真实 session compiler context。没有相邻基线证据，不把它称为既存失败，也不声称全 frontend suite 已通过。

本次只新增分析设计文档，未继续改生产代码或启动性能测试。

本地证据目录：`/home/xupeng/issue-29429-exact`、`/home/xupeng/issue-29429-main-bench`。主要 profile：`parent2-cpu-60s.pprof`、`fix2-cpu-60s.pprof`、`fix3-cpu-60s.pprof`、`fix7/fix7-cpu-60s.pprof`。
