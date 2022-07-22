# **WITH (Common Table Expressions)**

## **Description**

A common table expression (CTE) is a named temporary result set that exists within the scope of a single statement and that can be referred to later within that statement, possibly multiple times.

To specify common table expressions, use a `WITH` clause that has one or more comma-separated subclauses. Each sub-clause provides a subquery that produces a result set, and associates a name with the subquery.

## **Syntax**

```
WITH cte_name ( column_name [,...n] )
AS
(
    CTE_query_definition –- Anchor member is defined.
)
```

#### Explanations

In the statement containing the WITH clause, each CTE name can be referenced to access the corresponding CTE result set.

## **Examples**

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

## **Constraints**

MatrixOne doesn't support recursive CTE yet.