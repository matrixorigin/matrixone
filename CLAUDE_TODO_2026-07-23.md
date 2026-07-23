# CLAUDE TODO 2026-07-23

## PR #25335 最新 review 修复

### 有效问题

最新 `CHANGES_REQUESTED` 指出 DISTINCT grouping-set 的 `ORDER BY` 匹配仍存在两个 P1：

1. 匹配发生在真实名称绑定前。可见表达式使用限定列、`ORDER BY` 使用等价的非限定列时，
   文本 key 会错误拒绝合法查询。
2. 旧规范化逻辑会删除 `GROUPING()` 参数内的表限定符。Self-join 中
   `GROUPING(t1.a)` 与 `GROUPING(t2.a)` 会被视为同一表达式，可能绑定到错误 grouping bit。

这两个问题均有效。修复必须停止用未绑定 AST 文本判断表达式身份。

### 修复方案

1. `prepareGroupingSetOrderByProjects` 对 DISTINCT grouping-related `ORDER BY` 只记录延迟绑定标记，
   不再构造文本 key，也不提前计算 star 展开位置。
2. 第一个 grouping-set 分支完成 FROM、GROUP BY、projection 和 star 展开绑定后：
   - `GROUPING()` 参数按 `NoAlias` 绑定为真实源列，保留关系身份；
   - 外层 `ORDER BY` 表达式按 `AliasBeforeColumn` 绑定；
   - 通过绑定后的 `projectExprKey` 在 DISTINCT 可见 projection 中查找；
   - 未命中时返回普通 DISTINCT ORDER BY 的既有错误。
3. 命中后使用真实 projection `ColPos` 映射到 UNION 输出，因此天然支持任意 star 布局。
4. 删除旧 `groupingOrderExprKey`、文本规范化和 qualifier stripping 代码及对应测试。
5. 保留非 DISTINCT hidden sort-key 路径，不扩大改动范围。
6. grouping-set 分支输出的向量仍携带 grouping 位，普通 DISTINCT 哈希会把 grouping NULL 与
   普通 SQL NULL 当成不同键。仅在 grouping-set DISTINCT 的最终 PROJECT 中，将每个可见列改写为
   `IF(GROUPING(col), typed NULL, col)`，先把 grouping NULL 物化为普通 SQL NULL，再执行全局 DISTINCT；
   非 DISTINCT 路径和普通 UNION 不受影响。

### 测试

- 合法的限定/非限定外层列等价表达式能够绑定到同一可见 projection。
- Alias shadowing 仍按 ORDER BY 语义处理。
- Self-join 的歧义列被拒绝。
- 直接测试证明 `GROUPING(t1.a)` 与 `GROUPING(t2.a)` 保留不同关系身份。
- 回归 star、`t.*`、多 star 夹置、全局 DISTINCT 去重和非 DISTINCT hidden key。
- 用真实 `rollup.sql` 验证 grouping NULL 与普通 NULL 的可见值能够全局去重。
- 运行完整 planner UT、build、vet、覆盖率、静态检查及 `rollup.sql` BVT。

### Git 与 GitHub

- 合并最新 `mo/main` 并解决冲突。
- 不 force push。
- 不擅自回复或 resolve GitHub review thread。
- 验证与 self-review 通过后提交并正常推送现有 Draft PR。

## PR #25335 第四轮 review 修复

### 有效问题

1. grouping-set DISTINCT 当前对整个 `ctx.projects` 去重，普通 `ORDER BY` 追加的隐藏键会污染
   DISTINCT tuple；`ORDER BY rand()` 还会因隐藏列越界触发优化器 panic。
2. grouping NULL 规范化通过 `CAST(ANY NULL AS target)` 构造空值，UUID 等不支持该 cast 的类型
   无法建计划。
3. grouping-related DISTINCT `ORDER BY` 只接受完整表达式命中，未支持由多个可见输出派生的表达式。

### 修复方案

1. grouping-set DISTINCT 与普通 DISTINCT 保持相同节点顺序：
   - 最终分支 PROJECT 只包含 `resultLen` 个可见列；
   - 先对可见列执行全局 DISTINCT；
   - DISTINCT 之后用 `appendDistinctOrderProjectionNode` 计算派生或隐藏排序键；
   - SORT 后再投影回可见列。
2. 直接构造 `Typ` 为目标类型且 `Isnull=true` 的 literal，不经过 ANY 到目标类型的 CAST。
3. grouping-related 排序表达式先在首个真实分支中绑定，再递归映射到 DISTINCT 可见输出：
   - 完整可见子表达式直接替换为 UNION 输出列；
   - 普通函数允许由已选择子表达式继续派生；
   - 未被选择的 `GROUPING()` 子表达式不可从已清除 provenance 的普通列重算，必须拒绝。
4. 非 grouping 排序表达式复用现有 `distinctOrderBinder`，保持 `RAND()` 和由可见列派生表达式的
   既有行为。

### 新增测试矩阵

- `ORDER BY rand()` 不 panic，且随机隐藏键不进入 DISTINCT tuple。
- `GROUPING(a) + b` 在 `GROUPING(a)` 与 `b` 都可见时通过；缺少任一依赖时拒绝。
- ROLLUP 与 CUBE 均覆盖派生排序表达式。
- UUID、普通标量、NULL、GROUPING 输出的 typed NULL 规范化均可建计划。
- 真实 BVT 验证可见去重、隐藏排序和最终输出列数。

## PR #25335 第五轮 review 修复

### 有效问题

`remapGroupingSetDistinctOrderExpr` 未递归处理 `plan.Expr_List`，导致合法的
`GROUPING(a) + (b IN (...))` 被 DISTINCT projection 规则错误拒绝。若仅复制整个列表，
列表中的未选择列又可能绕过可见 projection 限制。

### 修复方案

1. 对 `Expr_List.List` 的每个元素递归调用同一重映射函数，保留完整的可见列约束；允许绑定阶段
   将纯常量列表折叠成的 `Expr_Vec`。
2. 对 nil list 保持保守拒绝，避免构造无效 plan。
3. 增加 ROLLUP `IN`、ROLLUP `NOT IN`、CUBE 和显式 `GROUPING SETS` 的合法矩阵。
4. 增加列表内引用未选择源列的拒绝用例，证明列表没有直接复制逃逸。

### 验证

- 运行新增定向 UT 和完整 `pkg/sql/plan` CGo 测试。
- 运行 build、vet、覆盖率、静态检查和 `git diff --check`。
- 完成全差异 self-review 后提交并正常推送，不 force push，不修改 GitHub thread。

## PR #25335 第六轮 review 修复

### 有效问题

1. grouping-set DISTINCT 使用 `IF(GROUPING(project), NULL, project)` 清除 grouping provenance，
   但 IF 缺少 VECF32/VECF64 结果类型，导致合法的向量 GROUP BY 在 ROLLUP/CUBE 下无法建计划。
2. grouping-set DISTINCT 的 ORDER BY 重映射在每层递归入口复用任意完整 SELECT 子表达式，
   比普通 `distinctOrderBinder` 更宽松，并会把嵌套的 volatile `RAND()` 错误替换为 SELECT 输出。

### 修复方案

1. 为 IF 的结果类型检查和执行路径补齐 VECF32/VECF64；继续使用 grouping-aware IF 将 rollup
   marker 转成普通 SQL NULL，避免同类型 CAST 把 grouping-only NULL 错当成向量零值。
2. 重映射只在顶层允许完整 SELECT 表达式命中；递归层仅允许：
   - 已选择的直接列；
   - 已选择的 `GROUPING()` 输出；
   - ORDER BY 明确引用的 SELECT alias。
3. 未通过以上规则的函数继续递归绑定，因此嵌套 `RAND()` 保持为新的调用；未选择输入和任意
   SELECT 子表达式不再绕过普通 DISTINCT 规则。

### 测试矩阵

- VECF32/VECF64 的 ROLLUP 与 CUBE planner 用例及真实 BVT。
- 完整 ORDER BY 精确命中仍通过，嵌套已选择复合子表达式加常量被拒绝。
- `GROUPING(a) + RAND()` 保留新的 volatile 调用，不引用 RAND SELECT 输出列。
- 复杂 SELECT alias 的派生排序仍通过；直接写同一复杂子表达式则按普通 DISTINCT 规则拒绝。
- 运行完整 planner/function UT、build、vet、覆盖率、静态检查和 rollup BVT。

## PR #25335 第七轮 review 修复

### 有效问题

1. grouping-set DISTINCT 通过未绑定 AST 文本和出现次数猜测 alias ordinal；重复投影、NULLIF
   参数复制和 tuple-IN 重写会使 AST 与 plan 树失配，合法 alias 派生排序被拒绝。
2. IFF 复用通用隐式转换成本选择向量结果，混合 VECF32/VECF64 会降级到 VECF32；同类型
   不同维度也未拒绝，导致声明类型与运行值维度不一致。

### 修复方案

1. 移除 alias 文本/次数配对。绑定 ORDER BY 时把 alias 引用直接编码为对应可见输出 ordinal，
   再绑定其余表达式；重复表达式不再经过 `projectByExpr` 的首个 ordinal 猜测。
2. 为 IFF 增加向量专用 common type：混合精度提升为 VECF64；相同及混合类型都要求维度一致，
   不一致时绑定失败；结果保留统一维度。
3. 增加重复 alias、NULLIF、tuple-IN、双向混合精度、超 float32 精度值及维度不一致回归。

### 验证

- 运行 planner/function 定向与全量 CGo UT，真实 mo-service BVT，build、覆盖率、静态检查。
- 完成全差异 self-review 后提交并正常推送，不 force push，不修改 GitHub thread。
