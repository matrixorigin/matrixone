# **比较函数和运算符概述**

| 名称 | 描述|
|---|-----|
| [>](greater-than.md) | 大于 |
| [>=](greater-than-or-equal.md) | 大于等于 |
| [<](less-than.md) | 小于 |
| [<>,!=](not-equal.md) | 不等于 |
| [<=](less-than-or-equal.md) | 小于等于 |
| [=](assign-equal.md) | 等于 |
| [BETWEEN ... AND ...](between.md) | 在两值之间 |
| [IN()](in.md) | 在集合中 |
| [IS](is.md) | 测试值是否是布尔值，若是布尔值，则返回“true” |
| [IS NOT](is-not.md) | 测试值是否是布尔值，IS 的否定用法 |
| [IS NOT NULL](is-not-null.md) | 不为空 |
| [IS NULL](is-null.md) | 为空 |
| [LIKE](like.md) | 模糊匹配 |
| [NOT BETWEEN ... AND ...](not-between.md) | 不在两值之间 |
| [NOT LIKE](not-like.md) | N模糊匹配，Like的否定用法 |

比较运算的结果为 `TRUE`、`FALSE` 或 `NULL`。这些运算对数字和字符串均有效。字符串可以自动转换为数字，数字根据需要自动转换为字符串。

以下比较运算符不仅可以用于比较标量运算数，也可以用于比较行运算数:

```
=  >  <  >=  <=  <>  !=
```
