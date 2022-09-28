# **UNION**

## **Description**

`UNION` combines the result from multiple `SELECT` statements into a single result set.

## **Syntax**

```
SELECT ... UNION [ALL | DISTINCT] SELECT ... [UNION [ALL | DISTINCT] SELECT ...]
```

### **Arguments**

#### `UNION [DISTINCT]` and `UNION ALL`

By default, duplicate rows are removed from the `UNION` results.  `UNION` is used as the same as `UNION [DISTINCT]`.

Combining the result sets of two or more queries using the UNION operator requires the following conditions:

- All `SELECT` statements must have the same number and order of columns.

- Data types must be same or convertible.

With `UNION ALL`, repeated lines (if available) are retained in the result.

#### `ORDER BY` and `LIMIT` In `UNION`

To apply an`ORDER BY` or `LIMIT` clause to an individual `SELECT`, parenthesize the `SELECT` and place the clause inside the parentheses:

Use of `ORDER BY` for individual `SELECT` statements implies nothing about the order in which the rows appear in the final result because UNION by default produces an unordered set of rows. Therefore, `ORDER BY` in this context typically is used in conjunction with LIMIT, to determine the subset of the selected rows to retrieve for the `SELECT`, even though it does not necessarily affect the order of those rows in the final UNION result.

For example:

```
(SELECT a FROM t1 WHERE a=10 AND B=1 ORDER BY a LIMIT 10)
UNION
(SELECT a FROM t2 WHERE a=11 AND B=2 ORDER BY a LIMIT 10);
```

Or:

```
(SELECT a FROM t1 WHERE a=10 AND B=1)
UNION
(SELECT a FROM t2 WHERE a=11 AND B=2)
ORDER BY a LIMIT 10;
```

<!--第二个例子需要确认，暂时不能生效-->

## **Examples**

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
