# **DIV**

## **Description**

The `DIV` operator is used for integer division. Discards from the division result any fractional part to the right of the decimal point.

If either operand has a non-integer type, the operands are converted to `DECIMAL` and divided using `DECIMAL` arithmetic before converting the result to `BIGINT`. If the result exceeds `BIGINT` range, an error occurs.

## **Syntax**

```
> SELECT value1 DIV value2;
```

```
> SELECT column1 DIV column2... FROM table_name;
```

## **Examples**

```sql
> SELECT 5 DIV 2, -5 DIV 2, 5 DIV -2, -5 DIV -2;
+---------+----------+----------+-----------+
| 5 div 2 | -5 div 2 | 5 div -2 | -5 div -2 |
+---------+----------+----------+-----------+
|       2 |       -2 |       -2 |         2 |
+---------+----------+----------+-----------+
1 row in set (0.00 sec)
```

```sql
> create table t2(c1 int, c2 int);
> insert into t2 values (-3, 2);
> insert into t2 values (1, 2);
> select c1 DIV 3 from t2;
+----------+
| c1 div 3 |
+----------+
|       -1 |
|        0 |
+----------+
2 rows in set (0.00 sec)
> select c1 DIV c2 from t2;
+-----------+
| c1 div c2 |
+-----------+
|        -1 |
|         0 |
+-----------+
2 rows in set (0.00 sec)
```
