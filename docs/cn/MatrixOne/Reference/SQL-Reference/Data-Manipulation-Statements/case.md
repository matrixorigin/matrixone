# **CASE**

## **语法说明**

`CASE` 语句用于存储过程的复合语句中。

如果没有 `when_value` 或 `search_condition` 匹配的值，并且 `CASE` 语句不包含 `ELSE` 从句，那么 `CASE` 语句报错。

每个 `statement_list` 可以由一个或多个语句组成，但不允许使用空。

还有一个 `CASE` 操作符，它与本章 `CASE` 语句不同。参见[流控制函数](operators/flow-control-functions/case-when.md)。`CASE` 语句不能有 `ELSE NULL` 从句, 并且 `CASE` 语句必须以 `END CASE` 结尾。

## **语法结构**

### **语法结构 1**

```
CASE case_value
    WHEN when_value THEN statement_list
    [WHEN when_value THEN statement_list] ...
    [ELSE statement_list]
END CASE
```

在这个语法结构中，`case_value` 是一个表达式。这个表达式会从左到右依次查找 `when_value`，直到找到和表达式相等的 `when_value`，并返回对应的结果，病执行相应的 `THEN` 从句 `statement_list`；如果没有找到相等的 `when_value`，则返回 `ELSE` 语句后的 `statement_list` 结果。

但是这个语法不能用于测试是否与 `NULL` 相等，因为 `NULL = NULL` 为 `false`。

### **语法结构 2**

```
CASE
    WHEN search_condition THEN statement_list
    [WHEN search_condition THEN statement_list] ...
    [ELSE statement_list]
END CASE

```

在这个语法中，`CASE` 表达式会从左到右依次计算 `search_condition`，直到第一个为 `true`，然后执行 `THEN` 从句 `statement_list` 并返回对应结果。如果没有为 `true` 的`search_condition`，则执行 `ELSE` 从句 `statement_list` 并返回结果。

## **示例**

```sql
> CREATE TABLE t1(c0 INTEGER, c1 INTEGER, c2 INTEGER);
> INSERT INTO t1 VALUES(1, 1, 1), (1, 1, 1);
> SELECT CASE AVG (c0) WHEN any_value(c1) * any_value(c2) THEN 1 END FROM t1;
+------------------------------------------------------------+
| case avg(c0) when any_value(c1) * any_value(c2) then 1 end |
+------------------------------------------------------------+
|                                                          1 |
+------------------------------------------------------------+
1 row in set (0.01 sec)

> SELECT CASE any_value(c1) * any_value(c2) WHEN SUM(c0) THEN 1 WHEN AVG(c0) THEN 2 END FROM t1;
+--------------------------------------------------------------------------------+
| case any_value(c1) * any_value(c2) when sum(c0) then 1 when avg(c0) then 2 end |
+--------------------------------------------------------------------------------+
|                                                                              2 |
+--------------------------------------------------------------------------------+
1 row in set (0.01 sec)

> SELECT CASE any_value(c1) WHEN any_value(c1) + 1 THEN 1 END, ABS(AVG(c0)) FROM t1;
+------------------------------------------------------+--------------+
| case any_value(c1) when any_value(c1) + 1 then 1 end | abs(avg(c0)) |
+------------------------------------------------------+--------------+
|                                                 NULL |            1 |
+------------------------------------------------------+--------------+
1 row in set (0.00 sec)
```
