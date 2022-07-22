# **CASE**

## **Description**

`CASE WHEN` statement is used in that evaluates a list of conditions and returns one of multiple possible result expressions.

Cases come in two formats: a simple `CASE` function compares an expression to a set of simple expressions to determine the result. The `CASE` search function evaluates a set of Boolean expressions to determine the result. Both formats support the optional `ELSE` argument.

## **Syntax**

- **Syntax 1**:

```
CASE value WHEN compare_value THEN result [WHEN compare_value THEN result ...] [ELSE result] END
```

This `CASE` syntax returns the result for the first value=compare_value comparison that is true.

- **Syntax 2**:

```
CASE WHEN condition THEN result [WHEN condition THEN result ...] [ELSE result] END
```

This `CASE` syntax returns the result for the first condition that is true. If no comparison or condition is true, the result after ELSE is returned, or NULL if there is no ELSE part.

!!! note  "<font size=4>note</font>"
    <font size=3>The `CASE` statement cannot have an `ELSE NULL` clause, and it is terminated with `END CASE` instead of `END`.
</font>

## **Examples**

```sql
> SELECT CASE WHEN 1>0 THEN 'true' ELSE 'false' END;
+------------------------------------------+
| case when 1 > 0 then true else false end |
+------------------------------------------+
| true                                     |
+------------------------------------------+
1 row in set (0.00 sec)
```

```sql
> CREATE TABLE t1 (a INT, b INT);
Query OK, 0 rows affected (0.01 sec)

> INSERT INTO t1 VALUES (1,1),(2,1),(3,2),(4,2),(5,3),(6,3);
Query OK, 6 rows affected (0.01 sec)

> SELECT CASE WHEN AVG(a)>=0 THEN 'Positive' ELSE 'Negative' END FROM t1 GROUP BY b;
+-------------------------------------------------------+
| case when avg(a) >= 0 then Positive else Negative end |
+-------------------------------------------------------+
| Positive                                              |
| Positive                                              |
| Positive                                              |
+-------------------------------------------------------+
3 rows in set (0.00 sec)
```
