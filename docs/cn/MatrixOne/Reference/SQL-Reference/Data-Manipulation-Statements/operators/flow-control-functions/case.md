# **CASE**

## **语法说明**

`CASE WHEN` 语句用于计算条件列表并返回多个可能结果表达式之一，`CASE WHEN` 可以比较等于、范围的条件。遇到第一个满足条件的即返回，不再往下比较，如果没有满足的条件则返回 `else` 里的结果，如果没有 `else` 则返回 `NULL`。

CASE有两种格式，两种格式都支持可选的 `ELSE` 参数。：

- 一个简单的 `CASE` 函数将一个表达式与一组简单表达式进行比较以确定结果。
- `CASE` 搜索函数计算一组布尔表达式来确定结果。

## **语法结构**

- **语法结构 1**:

```
CASE value WHEN compare_value THEN result [WHEN compare_value THEN result ...] [ELSE result] END
```

这里的 `CASE` 语法返回的是第一个 `value=compare_value` 为 `true` 的分支的结果。

- **语法结构 2**:

```
CASE WHEN condition THEN result [WHEN condition THEN result ...] [ELSE result] END
```

这里的 `CASE` 语法返回的是第一个 `condition` 为 `true` 的分支的结果。

如果没有一个 `value=compare_value` 或者 `condition` 为 `true`，那么就会返回 `ELSE` 对应的结果，如果没有 `ELSE` 分支，那么返回 `NULL`。

!!! note  "<font size=4>note</font>"
    <font size=3> `CASE` 语句不能有 `ELSE NULL` 从句, 并且 `CASE` 语句必须以 `END CASE` 结尾。
</font>

## **示例**

```sql
> SELECT CASE WHEN 1>0 THEN 'true' ELSE 'false' END;
+------------------------------------------+
| case when 1 > 0 then true else false end |
+------------------------------------------+
| true                                     |
+------------------------------------------+
1 row in set (0.00 sec)

> SELECT CASE 1 WHEN 1 THEN 'one'
    -> WHEN 2 THEN 'two' ELSE 'more' END;
+------------------------------------------------------+
| case 1 when 1 then one when 2 then two else more end |
+------------------------------------------------------+
| one                                                  |
+------------------------------------------------------+
1 row in set (0.00 sec)
```
