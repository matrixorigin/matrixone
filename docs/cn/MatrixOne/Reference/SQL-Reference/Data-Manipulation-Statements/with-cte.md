# **WITH (Common Table Expressions)**

## **语法说明**

通用表表达式（CTE，common table expression），它是在单个语句的执行范围内定义的临时结果集，只在查询期间有效。它可以自引用，也可在同一查询中多次引用。

定义 `CTE` 后，可以像 `SELECT`，`INSERT`，`UPDATE`，`DELETE`或 `CREATE VIEW` 语句一样引用它。

使用 `WITH` 从句指定通用表表达式，`WITH` 从句可以使用一个或多个逗号分隔。每个从句提供一个子查询，该子查询生成一个结果集，并将名称与子查询关联起来。

## **语法结构**

```
WITH cte_name ( column_name [,...n] )
AS
(
    CTE_query_definition –- Anchor member is defined.
)
```

#### 参数释义

在包含 `WITH` 从句的语句中，可以引用每个 `CTE` 名称来查询相应的 `CTE` 结果集。

## **示例**

```sql
> CREATE TABLE t1
    -> (a INTEGER,
    -> b INTEGER,
    -> c INTEGER
    -> );
> INSERT INTO t1 VALUES
    -> (1, 1, 10), (1, 2, 20), (1, 3, 30), (2, 1, 40), (2, 2, 50), (2, 3, 60);
> CREATE TABLE t2
    -> (a INTEGER,
    -> d INTEGER,
    -> e INTEGER
    -> );
> INSERT INTO t2 VALUES
    -> (1, 6, 60), (2, 6, 60), (3, 6, 60);
> WITH
    -> cte AS
    -> (SELECT SUM(c) AS c, SUM(b) AS b, a
    -> FROM t1
    -> GROUP BY a)
    -> SELECT t2.a, (SELECT MIN(c) FROM cte AS cte2 WHERE t2.d = cte2.b)
    -> FROM t2 LEFT JOIN cte AS cte1 ON t2.a = cte1.a
    -> LEFT JOIN t2 AS tx ON tx.e = cte1.c;
+------+------------------------------------------------------+
| a    | (select min(c) from cte as cte2 where t2.d = cte2.b) |
+------+------------------------------------------------------+
|    1 |                                                   60 |
|    1 |                                                   60 |
|    1 |                                                   60 |
|    2 |                                                   60 |
|    3 |                                                   60 |
+------+------------------------------------------------------+
5 rows in set (0.01 sec)
```

## **限制**

MatrixOne 暂不支持递归 CTE。
