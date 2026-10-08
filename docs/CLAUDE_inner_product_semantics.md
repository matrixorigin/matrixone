# INNER_PRODUCT：数学点积与迁移说明

`INNER_PRODUCT(a, b)` 返回数学点积 `Σ aᵢbᵢ`，不是负点积距离：

```sql
SELECT INNER_PRODUCT('[1,2,3]', '[4,5,6]'); -- 32
SELECT INNER_PRODUCT('[1,2,3]', '[1,2,3]'); -- 14
SELECT INNER_PRODUCT('[1,2,3]', '[-4,-5,-6]'); -- -32
```

该约定适用于常量和列，以及 vecf32、vecf64、vecbf16、vecf16、vecint8、vecuint8。NULL 传播、维度错误、非有限结果拒绝及既有精度约定不变。自点积为向量范数平方；L2 距离、cosine_similarity、cosine_distance 不变。

## 排序与向量索引

- `ORDER BY INNER_PRODUCT(v, q) DESC LIMIT k`：取内积最高的 k 行。`vector_ip_ops` 近邻索引可以服务这一方向，返回数学点积分数（可为负数）。
- `ORDER BY INNER_PRODUCT(v, q) ASC LIMIT k`：取内积最低的 k 行。仅提供近邻候选的索引不能通过反排候选集回答这一查询，应保留精确路径。
- 内部距离内核、索引候选堆和 IVF 质心路由仍使用越小越近的负点积；usearch 的 `1-dot` 只在对外分数边界转换为 `dot`。无需重建既有索引，没有索引存储格式变更。
- 内积受向量长度影响；未归一化向量的最高内积并不等于最小 L2 距离。

## 从历史负点积行为迁移

这是 SQL 结果的兼容性修复：历史版本将 `INNER_PRODUCT` 返回为负点积。

| 历史用法 | 修复后的等价用法 |
|---|---|
| 将返回值当作负距离 | `-INNER_PRODUCT(a, b)` |
| `ORDER BY INNER_PRODUCT(v, q) ASC` 获取最高内积 | 改为 `DESC` |
| `ORDER BY INNER_PRODUCT(v, q) DESC` 获取最低内积 | 改为 `ASC` |
| 应用通过 `-INNER_PRODUCT(a, b)` 绕过符号错误 | 移除额外负号 |

涉及分数阈值、聚合、持久化派生分数的应用也需要检查符号和不等式方向。滚动升级期间不要混用旧、新语义的节点对同一内积表达式求值；回滚时同步回退应用的符号与排序迁移。

外部 MOI 文档及版本 Known Issues 位于独立仓库，不在此引擎修复中修改。
