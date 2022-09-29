# **&**

## **Description**

`Bitwise AND` operator returns an unsigned 64-bit integer.

The result type depends on whether the arguments are evaluated as binary strings or numbers:

- Binary-string evaluation occurs when the arguments have a binary string type, and at least one of them is not a hexadecimal literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit integers as necessary.

- Binary-string evaluation produces a binary string of the same length as the arguments. If the arguments have unequal lengths, an ER_INVALID_BITWISE_OPERANDS_SIZE error occurs. Numeric evaluation produces an unsigned 64-bit integer.

## **Syntax**

```
> SELECT value1 & value2;
```

## **Examples**

```sql
> SELECT 29 & 15;
+---------+
| 29 & 15 |
+---------+
|      13 |
+---------+
1 row in set (0.06 sec)

> CREATE TABLE bitwise (a_int_value INT NOT NULL,b_int_value INT NOT NULL);
> INSERT bitwise VALUES (170, 75);  
> SELECT a_int_value & b_int_value FROM bitwise;  
+---------------------------+
| a_int_value & b_int_value |
+---------------------------+
|                        10 |
+---------------------------+
1 row in set (0.02 sec)
```
