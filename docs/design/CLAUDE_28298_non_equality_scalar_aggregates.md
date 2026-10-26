# #28298 非等值相关标量聚合修复设计

## 状态与范围

- 版本：r1；状态：2026-10-26 用户以 “go ahead” 批准；已实现。
- Issue：https://github.com/matrixorigin/matrixone/issues/28298
- 初始设计基线：`a1562a07e4abae50e6c22d25beb392a2bb4f5139`；实现期间同步上游至 `83c1e23526fc0ed31188abd7fc2ca87cd3a6d4b3`。
- 当前 `go.mod` 要求 Go **1.27.1**，不使用 CLAUDE.md 示例中的旧版本。
- 本文可作为后续交付的版本化设计依据；根目录 TODO 不提交。尚未创建 PR。

## 1. 问题与当前证据

Issue 原始复现覆盖：聚合结果表达式、HAVING、DISTINCT、GROUP BY + ORDER BY + LIMIT、派生外层输入、多外层绑定、UNION ALL 内层输入。后续评论补充 DISTINCT 日期 CTE 的滚动去重计数、不同 SQL 消费入口，以及递归 CTE / 普通表上的分组 HAVING。

设计评审时的基线存在以下边界：

- `pkg/sql/plan/flatten_subquery.go:flattenSubqueryWithConsumer` 在上提关联谓词后拒绝非等值分组聚合及部分 HAVING。
- `flattenScalarSubqueryWithNonEqAgg` 要求可证明的聚合投影结构、单一外层绑定、可保留外层身份，以及可访问内层行标记；不能直接删除这些条件，否则会合并重复行、错误计算空集或丢失过滤与分页。
- `pkg/sql/plan/correlation_binding.go:subqueryNeedsDependent` 目前主要识别深层关联、dependent 子关系和关联分组键，普通单层形态仍进入旧改写。
- 当前主线已有 `bindDependentSubquery`、typed slots、完整关系程序、标量归约器，以及聚合、分页、集合和 CTE 执行支持。其既有设计见 [CLAUDE_7559_from_scratch.md](CLAUDE_7559_from_scratch.md)。
- 最近的 inline CTE 行身份修复已支持部分简单 CTE 情况；不能把 issue 的旧构建结果直接当成当前基线结果。

r1 送审时以上仅为源码与既有测试的调查结论；实施从独立基线复现开始，再验证修复后的支持矩阵，不以旧 issue 构建或自生成 result 作为正确性证据。

## 2. 合同、目标与非目标

不变量：每个实际外层行的标量结果，等于在该行词法环境下执行完整内层关系后进行标量归约；值相同的外层行不能被合并。

| 合同 | 最小反例 |
| --- | --- |
| 保留 bag 行数和全部外层引用 | 派生表或 UNION ALL 产生重复外层行；内层同时引用 JOIN 两侧 |
| 聚合、表达式、HAVING、去重和分页按原关系语义求值 | `COALESCE(MAX(v),-1)` 空集；HAVING 删除聚合输出；每行不同的 Top-1 |
| 空结果标量为 NULL；全局 COUNT 空输入为 0 | 显式分组 COUNT 无组与无分组 COUNT 无输入混淆 |
| 标量第二行必须报基数错误 | GROUP BY + HAVING 留下两个组而没有 LIMIT |
| 按实际需求求值并保留类型 | CASE 未选分支中的多行错误；PREPARE 重用时结果类型变化 |
| 只在未破坏原关系时选择通用路径 | 捕获 pull-up 后的 NYI 再绑定已经被改写的子树 |
| 已支持的快速路径不无故退化 | 简单基表非等值 MAX 或已支持 inline CTE 改成逐外层行重扫 |

目标：关闭 issue 的原始形态及同合同的评论复现，覆盖 SELECT、WHERE、ORDER BY、CTAS、VIEW、PREPARE。评论中的非聚合相关 Top-1 作为共享分页边界检查，不扩大为所有非聚合子查询优化。

递归 CTE 补充示例在外层 n >= 2 时返回多个组，正确结果应是标量多行错误，而不是强行返回一个 COUNT。另加限定一个组 / LIMIT 1 的正例区分执行支持与 SQL 基数约束。

非目标：新增 SQL 语法、LATERAL 可见性、通用去关联优化器、运行时缓存 / worker / 协议或存储格式；分区、存储过程、UDF、warning 兼容性不纳入。

外部合同参考：[MySQL 8.0 scalar subqueries](https://dev.mysql.com/doc/refman/8.0/en/scalar-subqueries.html)、[subquery errors](https://dev.mysql.com/doc/refman/8.0/en/subquery-errors.html)。预期值独立手算小数据；MySQL 可用时做差分，不把 MO 自生成 result 当独立正确性证据。

## 3. 方案比较与决定

| 方案 | 评价 | 决定 |
| --- | --- | --- |
| 仅移除 NYI 条件 | 旧 LEFT JOIN + 重聚合不能保持所有行身份、HAVING 和分页语义 | 拒绝 |
| 为各类形态分别扩展去关联、生成新行身份并重建聚合后关系 | 可行，但重复实现完整关系语义，且增加大量 tag 重映射和资源风险 | 不作为本次主方案 |
| 在旧改写前选择现有 dependent 完整关系执行，保留可证明的快速路径 | 复用已批准架构；主要修改 planner 路由，代价是新支持形态可能逐行执行 | 推荐 |

### 3.1 入口与所有权

1. 在 `flattenSubqueryWithConsumer` 的破坏性规范化 / 上提之前完成只读资格判定，结合内层原始结构、关联表达式、实际外层 `nodeID` 和绑定上下文。
2. 仅对本合同涉及的非等值相关标量聚合及关联分页边界判断旧路径是否能保持语义。识别表达式树、CTE source dependencies 和作用域，不匹配 SQL 文本或 issue 特定名称。
3. 复用 / 提取旧路径的只读结构与身份检查，保持现有成功形态的快速路径；不复制一套会漂移的聚合重写逻辑。没有安全证明的相关候选选择完整关系执行。
4. 原始子树交给 `bindDependentSubquery`，由现有 binder 捕获 typed slots；保持所有 PROJECT / AGG / FILTER / DISTINCT / SORT / LIMIT / SET / CTE 层次，不依赖物理 Row_ID。
5. 不在通用错误捕获器里吞掉 NYI；保留非法 SQL、标量基数和 CTE 求值身份等独立错误。禁止先尝试修改原树再回退。
6. 不改变 shared / volatile CTE 的既有求值身份合同，不以重复执行 CTE 的方式规避 `validateCTEInvocation`。

首个责任边界是 planner 的路径选择；消费者是现有 correlation program 编译器与标量归约器。预计生产修改在 `pkg/sql/plan` 内；若发现必须改变 runtime / 协议 / CTE 生命周期，先补充设计并重新 review，不静默扩大范围。

### 3.2 成本、兼容性和失败闭包

- 已有普通 SQL、等值关联、简单非等值聚合及透明 CTE 快速路径保持原路由，以 typed-plan 控制用例证明；普通路径不得新增整计划复制。
- 只读判定限定候选子树及其依赖，按访问过的节点去重；不对无关查询反复全图扫描。
- 新支持的通用执行沿用最坏 `E × I` 成本（外层需求数 × 内层执行成本），不跨相同值外层行缓存。进入 dependent 路径可能影响整个语句的计划，不能假定只改变一个表达式的物理成本。
- 沿用现有 statement 事务、snapshot、授权、预算、continuation 与清理 owner，不增加持久状态或后台任务。用现有执行 fixture 检查错误 / 取消后释放及下一次执行不携带旧结果。
- 沿用既有 dependent capability（MORPC 112）及协调 CN 执行边界；不新增协议字段。无数据迁移或升级格式变化；旧版本不支持这些新增可执行形态的事实不变。
- 主要回滚手段是撤回 planner 路由修改，无存储回滚步骤。无新增配置 / feature flag。

## 4. 改动 / 风险与验证地图

| 闭包 | 预计文件 / 责任层 | 风险与证据 |
| --- | --- | --- |
| 原子路径选择、关联捕获 | `flatten_subquery.go`、`correlation_binding.go` 及必要同包 helper | R2；公共建计划测试、只读判定、合法 slots / tags、旧快速路径对照 |
| 执行成本和已有消费者接通 | `pkg/sql/compile/dependent_test.go`，现有 dependent 执行 / reducer 测试 | R3 热路径维度；精确结果、错误、有限 benchmark、扫描 / 编译次数，不默认改 runtime |
| SQL 公开行为 | `test/distributed/cases/subquery` 的相关 `.sql/.result` | R2；独立预期、真实服务与 mo-tester、消费入口、重复运行与清理 |
| 设计 / TODO | 本文 / 根目录 TODO | R0；检查状态与交付，TODO 不提交 |

设计门禁：修复扩展 SQL 可执行形态且可能改变执行热路径，采用显式设计先行。r1 未批准前不修改生产代码或测试；如无新增运行时状态，不做无关持久化、restart 或全仓 race 检查。

### 验证矩阵

1. 原始十条查询及最近邻控制：MAX、MAX+1、COALESCE、CASE、HAVING、DISTINCT、分组 Top-1、派生外层、多外层引用、UNION ALL COUNT。
2. 0 / 1 / 2 个候选组、全 NULL、无匹配、重复内外行、NULL 关联键；HAVING true / false / UNKNOWN；COUNT(*) / COUNT(col) / COUNT(DISTINCT col)。
3. LIMIT 0 / 1、OFFSET 越过结果；无 LIMIT 的两组必须报多行错误。CASE 未选分支不能触发该错误。
4. DISTINCT 日期 CTE 滚动去重计数、稳定递归 CTE；手算和等价 LEFT JOIN + GROUP BY 查询作独立 oracle。
5. 同一共享 fixture 验证 SELECT / WHERE / ORDER BY、CTAS / VIEW 和 PREPARE 多次执行，核对类型 / NULL 与模板不被修改。
6. planner 断言 `ValidateCorrelation`、捕获的两个外层 binding、未改写输入，以及旧快速路径没有 correlation program；不冻结 node ID 或 EXPLAIN 全文本。
7. 复用 `TestDependentNativeStages` 与标量 reducer fixture；仅补充新路由带来的执行组合，避免为每条 SQL 起服务。
8. BVT 优先扩展 `scalar_cte_non_equality_aggregate`、`scalar_cte_row_identity` 以及相邻 projection / limit 用例；已阅读前两者 sql/result。改变旧拒绝用例时保留其反例合同，替换为正确结果或正确运行时错误，不删断言。修改前继续审查其余 fixture，避免扩大 catch-all 脚本。
9. 使用 Go 1.27.1 和仓库 `mo-cgo-test`；先非空 focused tests、再实际修改包。按改动闭包跑 gofmt、vet、增量 lint；修改行覆盖率至少 75%。
10. 自有服务及 native artifacts 上以 mo-tester 生成 result 草稿，独立核对后 comparison 模式验证；检查 teardown 后同实例再次执行。保留真实退出码和日志。新增执行路径补充最小多 CN 消费验证，复用既有 capability 合同，不重跑无关迁移矩阵。
11. 用小规模 benchmark 比较基线 / 修复的旧路由建计划成本，以及新形态的 E / I 增长、单次编译与资源释放；不把性能负载塞入 BVT。现有路径不得无故变成每行内层扫描。
12. 最终运行 `mo-self-review`，无未解决 blocker 后再考虑交付；未运行 / 被阻塞的证据明确列出。

## 5. 评审记录

- 调查完成：已读取 issue 及评论、最新主线关键路由、dependent binder、既有设计与相关测试。
- 当前决定：**r1 已批准**，2026-10-26 用户原文：“go ahead”；批准先于代码修改。验证结果另记 TODO，不提前声明通过。
- 已知取舍：新支持复杂形态先保证语义，接受通用路径的逐外层需求执行成本；已有快速路径必须保留。
- 实施触发重新评审的条件：需要新执行机制、无法保留已支持快速路径、改变 shared CTE 求值身份、或引入协议 / 生命周期变化。

## 6. 实现落点

- `correlation_range.go` 在原始关系上识别穿越聚合或分页的非等值关联，复用旧重聚合的外层身份、内层 marker 和聚合输入准入检查；`flatten_subquery.go` 在 pull-up 前选择执行路径，并共享这些检查。
- 既有 `subqueryNeedsDependent / legacyDeepScalarAggregateSafe` 优先拥有深层及已有 dependent 关系的分类；新的 range 准入只补充其余单层关系。单层分页资格中的 depth=1 限制不能否定已证明安全的深层等值集合路径，无 FROM 或多个中间绑定也不能改变这个优先级。
- 行比较的左操作数通过现有递归 dependent 表达式绑定保留在比较的 demand stage；不能预先展开为外层 SINGLE JOIN。外层 CASE / COALESCE 或左操作数内部 CASE 跳过它时，不执行其基数检查；实际选中且返回多行时仍报错。
- 已有投影拥有的关联具有独立的 MARK join / per-outer-row LIMIT 规范化与准入边界，不由新分页选择器接管。独立聚合之上的关联过滤也保留原标量 join 路由。
- 不新增 runtime 状态、协议或资源 owner。现有 native fixture 覆盖两次执行、读源关闭与预算归零，并检查词法源仅编译一次、内层按实际外层需求开启。
- SQL 回归复用原聚合与行身份 fixture；同步更新行构造器与 hint CTE 中不再适用的拒绝预期，保留标量基数错误及原始安全快速路径的对照。
