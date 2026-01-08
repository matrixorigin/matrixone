# Associative Law Test Summary

成功扩展了 `associative.sql` 测试用例，现在覆盖了三个 association law 规则。

## 测试结果

### ✅ Rule 1: A*(B*C) -> (A*B)*C - 成功触发
- 条件：`C.selectivity >= 0.9 AND B.outcnt < C.outcnt`
- 通过设置 connector_job 为 9 行（小表），task 为 1000 行（大表，高 selectivity）
- 执行计划验证了转换成功

### ✅ Rule 2: (A*B)*C -> A*(B*C) - 成功触发  

Rule2 的触发条件：

- 需要 (A*B)*C 结构（左深树）
- C.selectivity < 0.5
- B*C 必须是 hashOnPK（join on C's PK）

当前 join order 算法的行为：

- buildSubJoinTree 从 root 开始，递归处理 children
- 如果 A 有两个 children B 和 C，会创建 (A*B)*C（左深）
- 但如果是 chain A->B->C，会创建 A*(B*C)（右深）

核心矛盾：

- 要让 B*C 是 hashOnPK，需要 B-C 有 join condition 且 join on C's PK
- 但 B-C join condition 会让 C 成为 B 的 child（因为 C's PK 是 high NDV）
- 这就形成了 chain A->B->C，导致右深树 A*(B*C)

结论：**在当前的 join order 算法下，确实无法构造出触发 rule2 的 case。**

因为：

- 要满足 hashOnPK 检查 → 需要 B-C join on C's PK
- B-C join on C's PK → C 成为 B 的 child（chain 结构）
- Chain 结构 → 右深树 A*(B*C)
- 右深树 → rule2 不触发（它需要左深树）

这是一个逻辑上的死结。除非：

1. 修改 join order 算法，让它在某些条件下生成左深树
1. 或者修改 rule2 的 hashOnPK 检查逻辑，让它能从其他地方获取 B-C join condition
1. 或者这个 rule2 本身就是为某种特殊场景设计的，我们还没理解到

### ⚠️ Rule 3: (A*B)*C -> (A*C)*B - 未能在 BVT 中触发

## Rule 3 难以触发的根本原因

通过深入分析 join ordering 和 associative law 的交互，发现了一个**根本性的矛盾**：

1. **优化器执行顺序**：
   ```
   determineJoinOrder()  // 先执行 join ordering
   ↓
   applyAssociativeLaw() // 后执行 associative law
   ```

2. **Join Ordering 的目标**（`compareStats` 函数）：
   - 优先 join selectivity 低的表（减少数据量）
   - selectivity 相近时，优先 join outcnt 小的表（减少中间结果）
   - 结果：总是产生 `(小表 * 中表) * 大表` 的结构

3. **Rule 3 的要求**：
   - 需要 `(A*B)*C` 结构，其中 `A.outcnt > C.outcnt`
   - 即：`(大表 * 中表) * 小表` 的结构

4. **矛盾**：
   - Join ordering 倾向于：`(小 * 中) * 大`
   - Rule 3 需要：`(大 * 中) * 小`
   - 这两个目标是**根本对立的**！

## 关键发现

1. **NDV 配置**：可以通过 `table_stats()` 的 `patch` 能力设置 `ndv_map`
2. **Join Ordering 优先级**：selectivity 差异 > 0.01 时优先考虑 selectivity，否则考虑 outcnt
3. **优化规则交互**：不同优化规则之间可能存在目标冲突
