# **BIT_XOR**

## **Description**

Aggregate function.

The BIT_XOR(expr) function returns the bitwise XOR of all bits in expr.

## **Syntax**

```
> BIT_XOR(expr)
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

> select id, BIT_XOR(number) FROM t1 GROUP BY id;
+------+-----------------+
| id   | bit_xor(number) |
+------+-----------------+
| a    |             101 |
| b    |              10 |
+------+-----------------+
```
