# **>>**

## **Description**

Shifts a longlong (BIGINT) number or binary string to the right.

The result type depends on whether the bit argument is evaluated as a binary string or number:

- Binary-string evaluation occurs when the bit argument has a binary string type, and is not a hexadecimal literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument conversion to an unsigned 64-bit integer as necessary.

- Binary-string evaluation produces a binary string of the same length as the bit argument. Numeric evaluation produces an unsigned 64-bit integer.

Bits shifted off the end of the value are lost without warning, regardless of the argument type. In particular, if the shift count is greater or equal to the number of bits in the bit argument, all bits in the result are 0.

## **Syntax**

```
> SELECT value1 >> value2;
```

## **Examples**

```sql
> select 1024 >> 2;
+-----------+
| 1024 >> 2 |
+-----------+
|       256 |
+-----------+
1 row in set (0.01 sec)

> select -5 >> 2;
+---------+
| -5 >> 2 |
+---------+
|      -2 |
+---------+
1 row in set (0.01 sec)

> select null >> 2;
+-----------+
| null >> 2 |
+-----------+
|      NULL |
+-----------+
1 row in set (0.00 sec)

> create table t1(a int, b int unsigned);
> insert into t1 values (-1, 1), (-5, 5);
> select a >> 2, b >> 2 from t1;
+--------+--------+
| a >> 2 | b >> 2 |
+--------+--------+
|     -1 |      0 |
|     -2 |      1 |
+--------+--------+
2 rows in set (0.01 sec)
```
