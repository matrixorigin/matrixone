# PR #27483 第二轮 review finding 整改

## 已确认不变量

1. **dependency 只属于 numeric-prefix common-type consumer**：numeric-prefix 协议能力仅表示参数具备该能力，不能把所有参数表达式自动标记为 dependency。普通算术、聚合和其它非 consumer 必须保留 prepare-time/runtime rebinding 的既有隐式 cast。
2. **物理 cast 不能被语义 dependency 展开误删**：外层 consumer 重绑只能移除因 prepare-time TEXT 域产生的 provisional cast；YEAR 等执行器或索引比较所需的物理 cast 必须保留。
3. **最近邻控制组**：`coalesce(?, decimal)` 参与外层精确比较时仍需传播 dependency；普通 `(? + ?)` 不传播；YEAR covering-index 与非索引查询结果必须一致。

## 根因与最小设计

- 当前 `Expr_P` 分支只要参数位置启用 numeric-prefix capability 就无条件标记 dependency，混淆了“capability”与“本表达式实际选择了 numeric-prefix domain”。改为仅当 `preparedNumericPrefixArgs` 确认 consumer 命中并完成 contextual cast/rebind 后，才从该 consumer 向父表达式传播 dependency；参数替换本身不传播。
- dependency 子树外层重绑时，不再无条件剥离任意 binder cast。复用/收窄现有 provisional-cast 判定，只移除 numeric source 被 prepare-time 临时提升到 TEXT/FLOAT 的 cast；保留 YEAR 及其它物理表示所需 cast。
- 不新增状态、缓存或 plan 字段；修改限制在 execute-time visitor 及其回归测试，热路径复杂度和分配阶数不变。

## 反例矩阵

| 不变量 | 见证 | Oracle | 未修复结果 |
|---|---|---|---|
| 非 consumer 不传播 | `cast((? + ?) + 1 as decimal(30,0))`，两个字符串参数 | 返回 `9007199254740994`，无 bind error | `[TEXT TEXT]` 加法错误 |
| common-type 仍传播 | `coalesce(?, d) = decimal(38,10)` | suffix/native decimal 不误等，NULL 精确命中 | 若过度收窄则退回 FLOAT 误等 |
| YEAR cast 保留 | covering index `where y=?` | 与 table scan 相同行集，命中目标 id | 索引路径漏行 |
| 普通数值生命周期不回归 | `TestIssue25753PreparedNumericProtocolLifecycle` | merge-base 与修复头均通过 | 当前 head 失败 |

## 实施与验证步骤

1. 在当前 exact head 运行两个同名现有测试并记录失败；必要时增加 typed white-box 断言定位 dependency/cast 边界。
2. 实施最小 visitor 修复，逐项运行两个 P1、nested COALESCE、FLOAT/NULL、UUID 与 retry focused tests。
3. 正常 merge 最新 `mo/main`，重新执行所有语义证据；从最终 diff 推导 owning packages，运行 list/build/vet/full CGo tests、changed-code coverage 与 `git diff --check`。
4. 使用 `mo-self-review` 和 unhappy-path Q1-Q3 审查完整 PR diff；零 blocker 后直接 commit、正常 push，不评论 PR/issue。

本节属于 review comment / CI failure 修复，按仓库流程记录后直接实施，无需再次等待 review checkpoint。

## 实施记录（merge 最新 main 前）

- 在 `26a76a623e` 逐项复现：`TestIssue25753PreparedNumericProtocolLifecycle` 报
  `invalid argument operator +, bad value [TEXT TEXT]`；
  `TestIssue26873BinaryPreparedEnumAndYearCoveringIndex/year` 报 `sql: no rows in result set`。
- 删除 `Expr_P` 上仅由 numeric-prefix capability 触发的 dependency 标记；dependency 现在只由
  `preparedNumericPrefixArgs` 实际命中 numeric-prefix common-type consumer 后产生。
- `unwrapNumericPrefixDependentImplicitCast` 仅展开 numeric source 到 FLOAT/TEXT 的 binder
  provisional cast；YEAR、DECIMAL、整数等物理 cast保持不变。父表达式 sibling 重绑复用同一窄判定。
- typed test 验证 YEAR cast 不展开、DECIMAL→FLOAT provisional cast仍展开；不可达的直接
  `[TEXT TEXT] +` synthetic binder case 已删除，真实 SQL prepare/COM_STMT 作为算术语义 oracle。
- 两个用户点名测试已由失败转为通过；`TestIssue27088PreparedDecimalCommonType` 及 frontend 的
  retry、SQL eligibility、NULL provenance focused tests继续通过，证明未回退上一轮修复。
- merge 最新 `mo/main` 时发现 v25 已由 #27442 用于 UPDATE changed-row counting。保留 main 的
  v25 契约，将 prepared numeric-prefix 的首次可用版本顺延为 v26；v25 worker 必须拒绝携带
  numeric-prefix sentinel 的远程计划，v26 同时兼容 v25 changed-row 能力。

## 最终验证与自审

- 最终 merge 头 `5a7d3930be` 上，三个核心黑盒测试共同通过：
  `TestIssue25753PreparedNumericProtocolLifecycle`、
  `TestIssue26873BinaryPreparedEnumAndYearCoveringIndex`、
  `TestIssue27088PreparedDecimalCommonType`。
- `pkg/sql/plan`、`pkg/frontend`、`pkg/pb/plan`、`pkg/sql/compile`、
  `pkg/sql/plan/function`、`pkg/sql/plan/rule`、`pkg/tests/issues`、`pkg/defines`
  全包测试通过；`pkg/frontend` 与 `pkg/sql/plan` 全包 race 通过。
- v25/v26 边界测试覆盖 numeric-prefix upgrade/rollback、远程发送端和接收端拒绝，以及普通 cast
  在 v25 下仍可执行；UPDATE changed-row 的 v24/v25 边界保持不变。
- 完整 diff 的功能闭包、retry 快照所有权、远程版本门禁及失败路径已复核；本轮改动不新增 goroutine、
  wait-for 边或资源所有权，未发现新的 correctness/lifecycle blocker。`git diff --check` 通过。

---

# PR #27483 review comments 修复计划

## 背景与不变量

- SQL `EXECUTE ... USING` 的 eligibility 预扫描必须与实际 prepared-plan specialization 对运行时参数类型的判定一致。
- 精确 `DECIMAL` 运行时参数应允许 `INT`/`DECIMAL` 公共类型重写；近似 `FLOAT` 域仍必须排除，避免扩大语义边界。
- issue 回归测试的每个 `database/sql.Rows` 消费点必须在 linter 可证明的调用作用域检查 `Rows.Err()`。
- PR 协议说明必须与当前 MORPC 版本一致（v25/v26）。

## 实施步骤

1. 拉取远端并 merge 最新 `mo/main`，确认 PR 分支基线及协议常量。
2. 追踪 frontend SQL EXECUTE eligibility、plan 预扫描和实际重写的完整调用链，定位运行时 `PrepareParamDecimal` 丢失点。
3. 以最小通用修改让预扫描接收并识别运行时参数种类，同时显式保留 `PrepareParamFloat` 排除边界。
4. 补充/调整 plan 白盒测试：覆盖 INT + 运行时 DECIMAL 可 specialized，以及运行时 FLOAT 不可 specialized 的控制组。
5. 修复 issue 黑盒测试的三处 `rowserrcheck`，在查询消费调用作用域显式检查 `rows.Err()`，不关闭 linter。
6. 运行受影响 package 的 focused/full test、build、vet 和相关 lint；按 diff 派生验证范围并检查覆盖率。
7. 对完整 diff 执行 pre-push 自审，确认 correctness、边界、性能及 unhappy path 无新增问题。
8. commit、push 到现有 PR 分支，更新 PR 正文 MORPC v25/v26 描述；处理可解决的 inline conversation 并请求 re-review。

## 测试矩阵

| 场景 | 参数类别 | 预期 |
|---|---|---|
| SQL EXECUTE: INT column = decimal variable | `PrepareParamDecimal` | eligibility 通过并完成公共类型重写，返回正确行 |
| SQL EXECUTE: INT column = float variable | `PrepareParamFloat` | eligibility 保持排除，不跨入精确 DECIMAL 域 |
| 静态 DECIMAL 同伴 | plan static DECIMAL | 既有行为保持 |
| 整数运行时参数 | integer | 既有行为保持 |
| issue 查询 rows 消费 | success / iteration error | 调用作用域显式检查 `Rows.Err()`，SCA 通过 |

## 成本与设计约束

- eligibility 仅在每次 SQL EXECUTE 的计划预扫描中增加对已有参数元数据的常数时间分类，不引入逐行成本、缓存或新状态。
- 不修改缓存 prepared plan；仍只 specialized 隔离的执行计划副本。
- 不把 FLOAT 近似值误分类为 DECIMAL，也不针对单个 SQL/issue 写特判。

## 执行结果

- eligibility 现携带每个运行时参数的 `StringConversionKind`，INT + runtime DECIMAL 可进入 specialization，FLOAT peer 继续排除。
- secondary-index `prefix_*` 路径会识别运行时 DECIMAL；对值等价于整数（如 `9.0`）的序列化索引参数按原计划整数目标类型物化，避免字符串直接 cast 报错并保持索引 key 类型。
- issue 黑盒新增带二级索引的 SQL EXECUTE 验收格，`9.0` 正确返回 `id=1`。
- 所有 `Rows` 查询调用作用域均显式检查 `rows.Err()`，targeted `rowserrcheck` 为 0 issue。
- 验证：`pkg/sql/plan` full test、issue #27088 embedded test、build、vet 均通过。

---

## PR #27483 第二轮阻塞评论修复计划

### 不变量与根因

1. 科学计数法是否可由 DECIMAL256 精确表示，必须依据 `exponent - fractionalDigits + trailingZeros` 的净指数，而不是原始指数；超长指数扫描仍须 O(n)、无大整数解析和无输入长度级分配。
2. DDL 内嵌 Query（CTAS）的 `replaceParamVals` specialization 结果必须向 frontend 传播；SQL EXECUTE 命中 eligibility 后必须采用 isolated runtime plan。
3. Prepared SET 的表达式计划必须进入与 SELECT/CTAS 相同的 runtime common-type rebinding，且不得修改 cached prepared plan。

### 步骤

1. 重构 bounded exponent：先以饱和/O(n)方式解析原始指数，再与小数位和 coefficient 尾零做有界净化，最后判断 DECIMAL256 width/scale。
2. 增加补偿指数白盒边界测试及 COM_STMT 黑盒反例，保留真正越界和超长指数控制组。
3. 让 DDL Query 返回 `replaceParamVals` 的 specialized 标志，并验证 frontend 对 SQL CTAS 采用副本。
4. 调研 SET 的实际 Plan variant/表达式 owner，扩展 eligibility、DeepCopy 与 replace 入口，增加 SQL EXECUTE SET 黑盒测试。
5. 运行 plan/frontend/issues 的 focused/full test、build、vet、SCA；完整 diff 自审后 commit、push。

### 测试矩阵

| 场景 | 预期 |
|---|---|
| `0.000000000000000000000000000000000001e100` | DECIMAL(65,0)，区分 1e64 与 1e64+1 |
| 未补偿的净指数 > DECIMAL256 | FLOAT64 |
| 攻击者长度指数文本 | O(n) 有界处理，FLOAT64 |
| SQL EXECUTE CTAS + DECIMAL prefix tail | isolated DDL query specialization 被采用，结果精确 |
| SQL EXECUTE SET + COALESCE DECIMAL | `@out = 12.5000000000` |
| FLOAT peer controls | 不进入精确 DECIMAL specialization |

### 第二轮执行结果

- 指数解析改为先结合 fractional digits 与 coefficient trailing zeros 得到净指数，再做 Decimal256 边界判断；超长指数仍仅线性扫描并拒绝进入大整数解析。
- CTAS DDL Query 现在传播 specialization 标志；内部 follow-up INSERT 仅对 runtime plan 中 numeric-prefix cast 对应的参数位置传递已截取的数值前缀。
- DeepCopyPlan 支持 DCL；Prepared SET 的 specialized `SetVariablesItem.Value` 由表达式执行器直接求值，不再回退到未特化 AST synthetic SELECT。
- 新增补偿指数 COM_STMT、SQL EXECUTE CTAS、SQL EXECUTE SET 黑盒回归及净指数白盒测试。
- 验证通过：plan/frontend/compile 全包测试，issue #27088 embedded 黑盒，受影响包 list/build/vet，targeted rowserrcheck。

---

## PR #27483 第三轮阻塞评论修复计划

### 不变量

1. Prepared SET 仅能把标量执行器支持的表达式交给 `NewExpressionExecutor`；含 `Expr_Sub` 的 assignment 必须保留 synthetic SELECT/query 执行闭包及其原子多赋值语义。
2. CTAS numeric-prefix 参数位置收集必须覆盖 `Expr_F/List/W/Sub/Lit provenance` 等完整表达式树，且每个节点只由一个统一 traversal 处理。
3. 参数位置只能来自实际 numeric-prefix 语义域，不能因同一 CTAS 中存在一个 v26 cast 就误改无关字符串参数。

### 步骤与测试

1. 在 `pkg/pb/plan` 增加完整 Expr tree visitor，并为 Window、Subquery、Literal source 等形态增加单测。
2. Prepared SET 使用统一 visitor 检测 `Expr_Sub`：含 subquery 回退既有 synthetic SELECT；纯标量 specialized item 继续直接执行。
3. CTAS 使用同一 visitor 收集 numeric-prefix cast 与其对应参数来源，删除 F/List 手写递归。
4. 在 issue #27088 增加 Window CTAS tail 黑盒；复跑既有 issue #26685 text/binary multi-SET 与 scalar subquery 回归。
5. 运行 pb/plan、plan、compile、frontend、issues 的 focused/full test、build、vet、SCA，自审后推送。

### 第三轮执行结果

- 新增 `plan.VisitExprTree` 作为 Expr_F/List/W/Sub/Lit provenance/frame/order 等完整变体的统一前序遍历。
- Prepared SET 检测到 `Expr_Sub` 时保留 synthetic SELECT/query 执行闭包；纯标量 specialized item 才使用 expression executor。
- runtime numeric-prefix literal 记录 ParamRef provenance；CTAS 仅从实际 Charset=255 numeric-prefix cast 子树收集参数位置，Window 路径不再漏参，也不改无关字符串参数。
- 新增 scalar-subquery SET 与 Window CTAS tail 黑盒；issue #26685 text/binary multi-SET 回归通过。

---

## PR #27483 第四轮阻塞评论修复计划

### 基线更新

- 已 merge 最新 `mo/main`。main 占用 MORPC v26（remote statement LAST_INSERT_ID），因此 numeric-prefix capability 顺延至 v27；相关边界测试与 PR 文档同步改为 v26/v27。

### 不变量与步骤

1. Prepared SET 含 subquery 时必须保留 query-aware synthetic SELECT，但其中的参数仍使用 runtime numeric-prefix specialization 后的值域；不能丢弃 specialized outer/inner consumer。
2. 设计 query-aware 参数桥接：从 specialized DCL expression 的 numeric-prefix literal provenance 精确收集参数位置，在 synthetic SELECT 生命周期内为这些位置提供截取后的参数副本，执行后恢复原 process 参数所有权与 metadata。
3. 增加 inner-subquery decimal comparison 与 outer COALESCE + scalar subquery 两个公开黑盒，并保留 `set @out=(select ?)` 控制组。
4. 重构 issue 测试 rows helper：调用点显式 `defer/Close`，helper 只消费和断言，确保 sqlclosecheck/rowserrcheck 均可静态证明。
5. 运行 issue #26685/#27088、frontend/plan/compile/pb 全包测试、build、vet、SCA，自审后推送。

### 第四轮执行结果

- SET scalar subquery 继续由 synthetic SELECT/query pipeline 执行；该 transient query 在 build 后使用当前 process 参数构造 runtime `ParamValue` 并执行 numeric-prefix specialization，因此 inner subquery consumer 获得同一契约。
- 含 subquery 的 outer DECIMAL consumer 在 query 执行后按 specialized DCL result type 物化，保留 DECIMAL width/scale，而不是返回 suffix 文本域。
- issue rows helper 不再拥有 Close；每个 QueryContext 调用作用域显式 defer `rows.Close()` 并检查 `rows.Err()`，sqlclosecheck/rowserrcheck 均为 0 issue。
- merge main 后 numeric-prefix MORPC capability 顺延至 v27，v26 保留 statement LAST_INSERT_ID；发送/接收边界与文档需使用 v26/v27。

## 第五轮 Review P1 修复方案

1. 为 COM_STMT prepared SET 增加 direct/subquery 对照回归，覆盖 JSON 数值类型与 `SET @x = ?` metadata，证明 scalar subquery 不改变 binary 参数 RuntimeType、protocol/type metadata 或值域。
2. 删除 outer DECIMAL 的字符串 prefix 后处理；让含 subquery 的完整 specialized expression 在 query-aware 执行路径中完成 common-type coercion，覆盖无 prefix、prefix、NULL、overflow/error/warning 语义。
3. 将外层执行已构造的完整 `ParamValue` 传入 synthetic prepared-expression build/retry generation，不再从 process text vector 重建；初始 build 与 definition-change retry 共用同一填值和 specialization helper。
4. 增加 definition-change retry 白盒测试，断言每一代 runtime plan 都无 ParamRef 且维持 DECIMAL consumer/type。
5. 运行 gofmt、目标 UT/issue 黑盒测试、frontend/plan/compile/pb 全包、sqlclosecheck/rowserrcheck、build/vet，并按完整 diff 做 self-review 后提交推送。

### 第五轮执行结果

- synthetic prepared-expression 不再从 process text vector 重建参数；`UserInput` 直接携带 outer execution 的完整不可变 `ParamValue` snapshot（含 binary protocol 与 RuntimeType metadata）。
- scalar subquery 由 query pipeline 独立求值并取得 typed result；结果替换到 deep-copied runtime-specialized DCL expression 后，再对完整 outer consumer 求值，删除字符串 prefix 条件后处理。
- 无数字前缀、NULL、多个 scalar subquery 顺序均由 specialized outer DECIMAL expression 的 cast/coalesce 语义处理。
- definition-change retry 对 forced prepared-expression generation 无条件重放完整参数填值，不再以 `specialized` 标志决定是否采用 runtime plan。
- binary prepared SET 即使 overload 未变化也采用 literal-materialized DCL copy，保留 direct/subquery 的 int64 runtime type 与 JSON number domain。
- 新增 COM_STMT JSON/type oracle、无 prefix、NULL、多 subquery 与 binary retry typed white-box 回归。

## 第六轮 Review P1 与 main 冲突修复方案

1. merge 最新 `mo/main`，按 v27 numeric-prefix、prepared retry 和 main 新增行为的功能闭环逐块解决冲突，不丢失任一侧测试。
2. 移除 scalar-subquery result 对 numeric-only `PreparedRuntimeParamExpr` 的泛用依赖；建立 result-vector typed materialization，确保 literal oneof/显式 cast 与 JSON、DATE、TIME、DATETIME、TIMESTAMP、UUID、ENUM、ARRAY 等实际 OID 一致。
3. 增加 COM_STMT direct/subquery 类型与 nested JSON consumer 对照，以及 DATE `date_add` consumer 对照；补齐 NULL、错误和 retry generation 回归。
4. 重新运行 issue #26685/#27088、frontend/plan/compile/pb 全包、SCA、build/vet；对 merge 后完整 diff 执行 self-review 后推送。

### 第六轮执行结果

- 已 merge `mo/main` @ `dc403fbac6`；冲突源仅为 main 的 prepared-type revert，保留本分支已验证实现，同时纳入其余 main 变更。
- scalar-subquery replacement 现在直接从 result vector 生成匹配 oneof 的 literal；JSON/UUID/Decimal256 等无直接 literal 路径的类型使用显式 CAST，不再以 `Sval + 非字符串 Expr.Typ` 冒充 typed vector。
- 新增 nested JSON consumer、JSON metadata 和 DATE `date_add` direct/subquery 对照，均通过。
- merge 后 frontend/plan/compile/pb 全包、issue #26685/#27088、build、vet 通过；sqlclosecheck/rowserrcheck 为 0 issue。

## 第七轮 Review P1 性能修复方案

1. 以 main 的 #27485 回滚为性能基线，恢复普通 COM_STMT Query cached plan/compile 快路径：不做 runtime inference、eligibility traversal、DeepCopyPlan 或 recompile。
2. 在 PREPARE generation 上一次性缓存“实际 numeric-prefix consumer”标志；仅该标志为真时允许 binary Query execute-time specialization。binary SET/DDL typed-literal 路径独立保留。
3. 增加白盒测试/benchmark：普通 TPCC 风格 Query 不调用 specialization 且复用 cached compile；numeric-prefix Query、SET、DDL 仍进入各自限定路径。
4. 运行 issue #27477 可复现的 TPCC 10.10 对照（若仓库脚本/环境可用），并执行 frontend/plan/compile/pb、issue #26685/#27088、SCA、build/vet 与 self-review。

### 第七轮执行结果

- 普通 COM_STMT Query 的 execute path 现在只绑定 protocol vector，`cwft.paramVals` 保持 nil；不调用 runtime type inference、pagination/numeric-prefix traversal、DeepCopyPlan 或 specialization，并复用 cached plan/compile。
- numeric-prefix consumer 与 pagination 标志在 PREPARE/rebuild generation 各计算一次；仅静态 DECIMAL peer 被缓存为 binary Query consumer，普通 INT TPCC predicate 不进入 specialization。
- binary SET/DDL、prepared EXPLAIN、pagination 与 SQL EXECUTE 使用各自限定路径，不扩大普通 Query 热路径。
- 新增 fast-path identity UT 和 microbenchmark；Apple M1 Pro 三轮结果为 583.7/586.4/602.1 ns/op，200 B/op，3 allocs/op，且每轮断言 cached plan/compile identity 与 nil paramVals。
- issue #27477 的 TKE TPCC 10.10 harness 不在本地仓库；未伪造 TPCC 数字，使用结构性 fast-path oracle 与 microbenchmark 作为本地性能门禁。
