# **UNION**

## **语法说明**

`UNION` 运算符允许您将两个或多个查询结果集合并到一个结果集中。

## **语法结构**

```
SELECT column_list
UNION [DISTINCT | ALL]
SELECT column_list
UNION [DISTINCT | ALL]
SELECT column_list ...
```

### 语法说明

#### `UNION` 和 `UNION ALL`

使用 `UNION` 运算符组合两个或多个查询的结果集，需要满足以下条件：

- 所有 `SELECT` 语句中出现的列的数量和顺序必须相同。
- 列的数据类型必须相同或可转换。

使用 `UNION ALL`，则重复行（如果可用）将保留在结果中。因为 `UNION ALL` 不需要处理重复项。

#### `UNION` 与`ORDER BY`，`LIMIT`

使用 `ORDER BY` 或 `LIMIT` 子句来对全部 UNION 结果进行分类或限制，则应对单个地 `SELECT` 语句加圆括号，并把 `ORDER BY` 或 `LIMIT` 放到最后一个的后面。

例如：

```
(SELECT a FROM t1 WHERE a=10 AND B=1 ORDER BY a LIMIT 10)
UNION
(SELECT a FROM t2 WHERE a=11 AND B=2 ORDER BY a LIMIT 10);
```

或：

```
(SELECT a FROM t1 WHERE a=10 AND B=1)
UNION
(SELECT a FROM t2 WHERE a=11 AND B=2)
ORDER BY a LIMIT 10;
```

<!--第二个例子需要确认，暂时不能生效-->

## **示例**

```sql
> CREATE TABLE t1 (id INT PRIMARY KEY);
> CREATE TABLE t2 (id INT PRIMARY KEY);
> INSERT INTO t1 VALUES (1),(2),(3);
> INSERT INTO t2 VALUES (2),(3),(4);
> SELECT id FROM t1 UNION SELECT id FROM t2;
+------+
| id   |
+------+
|    4 |
|    1 |
|    2 |
|    3 |
+------+

> SELECT id FROM t1 UNION ALL SELECT id FROM t2;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    2 |
|    3 |
|    4 |
+------+
```

```sql
> drop table t1;
> CREATE TABLE t1 (a INT, b INT);
> INSERT INTO t1 VALUES ROW(4,-2),ROW(5,9),ROW(10,1),ROW(11,2),ROW(13,5);
> drop table t2;
> CREATE TABLE t2 (a INT, b INT);
> INSERT INTO t2 VALUES ROW(1,2),ROW(3,4),ROW(11,2),ROW(10,3),ROW(15,8);
> (SELECT a FROM t1 WHERE a=10 AND B=1 ORDER BY a LIMIT 10) UNION (SELECT a FROM t2 WHERE a=11 AND B=2 ORDER BY a LIMIT 10);
+------+
| a    |
+------+
|   10 |
|   11 |
+------+
```
