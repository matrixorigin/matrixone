# PR #27483 第二轮 review finding 整改

## 已确认不变量

1. **dependency 只属于 numeric-prefix common-type consumer**：v25 仅表示参数具备 numeric-prefix 能力，不能把所有参数表达式自动标记为 dependency。普通算术、聚合和其它非 consumer 必须保留 prepare-time/runtime rebinding 的既有隐式 cast。
2. **物理 cast 不能被语义 dependency 展开误删**：外层 consumer 重绑只能移除因 prepare-time TEXT 域产生的 provisional cast；YEAR 等执行器或索引比较所需的物理 cast 必须保留。
3. **最近邻控制组**：`coalesce(?, decimal)` 参与外层精确比较时仍需传播 dependency；普通 `(? + ?)` 不传播；YEAR covering-index 与非索引查询结果必须一致。

## 根因与最小设计

- 当前 `Expr_P` 分支只要参数位置启用 v25 就无条件标记 dependency，混淆了“capability”与“本表达式实际选择了 numeric-prefix domain”。改为仅当 `preparedNumericPrefixArgs` 确认 consumer 命中并完成 contextual cast/rebind 后，才从该 consumer 向父表达式传播 dependency；参数替换本身不传播。
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
- 删除 `Expr_P` 上仅由 v25 capability 触发的 dependency 标记；dependency 现在只由
  `preparedNumericPrefixArgs` 实际命中 numeric-prefix common-type consumer 后产生。
- `unwrapNumericPrefixDependentImplicitCast` 仅展开 numeric source 到 FLOAT/TEXT 的 binder
  provisional cast；YEAR、DECIMAL、整数等物理 cast保持不变。父表达式 sibling 重绑复用同一窄判定。
- typed test 验证 YEAR cast 不展开、DECIMAL→FLOAT provisional cast仍展开；不可达的直接
  `[TEXT TEXT] +` synthetic binder case 已删除，真实 SQL prepare/COM_STMT 作为算术语义 oracle。
- 两个用户点名测试已由失败转为通过；`TestIssue27088PreparedDecimalCommonType` 及 frontend 的
  retry、SQL eligibility、NULL provenance focused tests继续通过，证明未回退上一轮修复。
