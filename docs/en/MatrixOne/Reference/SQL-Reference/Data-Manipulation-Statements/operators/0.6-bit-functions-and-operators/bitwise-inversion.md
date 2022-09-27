# **~**

## **Description**

Invert all bits.

The result type depends on whether the bit argument is evaluated as a binary string or number:

- Binary-string evaluation occurs when the bit argument has a binary string type, and is not a hexadecimal literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument conversion to an unsigned 64-bit integer as necessary.

- Binary-string evaluation produces a binary string of the same length as the bit argument. Numeric evaluation produces an unsigned 64-bit integer.

## **Syntax**

```
> SELECT value1 ~ value2;
```

## **Examples**

```sql
> select ~-5;
+--------+
| ~ (-5) |
+--------+
|      4 |
+--------+
1 row in set (0.00 sec)

> select ~null;
+-------+
| ~null |
+-------+
|  NULL |
+-------+
1 row in set (0.00 sec)

> select ~a, ~b from t1;
+------+----------------------+
| ~a   | ~b                   |
+------+----------------------+
|    0 | 18446744073709551614 |
|    4 | 18446744073709551610 |
+------+----------------------+
2 rows in set (0.00 sec)
```
