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
