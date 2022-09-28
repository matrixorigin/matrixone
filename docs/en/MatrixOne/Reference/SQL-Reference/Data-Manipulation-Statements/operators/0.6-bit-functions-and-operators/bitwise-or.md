# **\|**

## **Description**

Bitwise OR.

The result type depends on whether the arguments are evaluated as binary strings or numbers:

- Binary-string evaluation occurs when the arguments have a binary string type, and at least one of them is not a hexadecimal literal, bit literal, or `NULL` literal. Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit integers as necessary.

- Binary-string evaluation produces a binary string of the same length as the arguments. If the arguments have unequal lengths, an `ER_INVALID_BITWISE_OPERANDS_SIZE` error occurs. Numeric evaluation produces an unsigned 64-bit integer.

## **Syntax**

```
> SELECT value1 | value2;
```

## **Examples**

```sql
> SELECT 29 | 15;
+---------+
| 29 | 15 |
+---------+
|      31 |
+---------+
1 row in set (0.01 sec)

> > select null | 2;
+----------+
| null | 2 |
+----------+
|     NULL |
+----------+
1 row in set (0.01 sec)

> select null | 2;
+----------+
| null | 2 |
+----------+
|     NULL |
+----------+
1 row in set (0.01 sec)

> create table t1(a int, b int unsigned);
> insert into t1 values (-1, 1), (-5, 5);
> select a | 2, b | 2 from t1;
+-------+-------+
| a | 2 | b | 2 |
+-------+-------+
|    -1 |     3 |
|    -5 |     7 |
+-------+-------+
```
