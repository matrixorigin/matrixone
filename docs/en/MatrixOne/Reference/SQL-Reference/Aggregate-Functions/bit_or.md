# **BIT_OR**

## **Description**

Aggregate function.

The BIT_OR(expr) function returns the bitwise OR of all bits in expr.

## **Syntax**

```
> BIT_OR(expr)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| expr  | UINT data types |

## **Examples**

```sql
> drop table if exists t1;
> CREATE TABLE t1 (id CHAR(1), number INT);
> INSERT INTO t1 VALUES
      ('a',111),('a',110),('a',100),
      ('a',000),('b',001),('b',011);

> select id, BIT_OR(number) FROM t1 GROUP BY id;
+------+----------------+
| id   | bit_or(number) |
+------+----------------+
| a    |            111 |
| b    |             11 |
+------+----------------+
```
