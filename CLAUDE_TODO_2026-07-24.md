# PR #25335 第九轮 review 修复

## 有效问题

grouping-set DISTINCT 分支用同一 projection 表达式同时构造
`GROUPING(project)` 和 `IF` 的 else 分支，导致 `nextval()` 等易变或有副作用的
表达式被重复执行，改变序列状态乃至 DISTINCT 结果行数。

## 修复方案

1. 在 grouping-set 分支内先用独立 PROJECT 计算一次原始可见 projection。
2. 后续 NULL 物化只引用该 PROJECT 的输出列及其 grouping provenance，不再复制原始
   表达式树；然后维持 UNION ALL 上方的一次全局 DISTINCT。
3. 增加 planner 计划形状断言，确认每个分支只有一个 `nextval()` 求值点；增加
   ROLLUP、CUBE、GROUPING SETS 的真实执行回归，校验输出和 `currval()`。

## 验证

- 运行 planner 定向及全量 CGo UT、真实 sequence BVT、build、覆盖率、静态检查。
- 完成全差异 self-review 后提交并正常推送，不 force push，不修改 GitHub thread。
