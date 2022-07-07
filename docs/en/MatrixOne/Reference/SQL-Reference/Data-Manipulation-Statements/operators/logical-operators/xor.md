# **XOR**

## **Description**

Logical `XOR`. Returns `NULL` if either operand is NULL. For non-NULL operands, evaluates to `true` if an odd number of operands is nonzero, otherwise `false` is returned.

`a XOR b` is mathematically equal to `(a AND (NOT b)) OR ((NOT a) and b)`.

## **Syntax**

```
> SELECT column_1 XOR column_2 FROM table_name;
```

## **Examples**

```sql
> select 1 xor 1;
+---------+
| 1 xor 1 |
+---------+
| false   |
+---------+
1 row in set (0.01 sec)

> select 1 xor 0;
+---------+
| 1 xor 0 |
+---------+
| true    |
+---------+
1 row in set (0.00 sec)

> select 1 xor null;
+------------+
| 1 xor null |
+------------+
| NULL       |
+------------+
1 row in set (0.01 sec)

> select 1 xor 1 xor 1;
+---------------+
| 1 xor 1 xor 1 |
+---------------+
| true          |
+---------------+
1 row in set (0.00 sec)
```

```sql
> create table t1 (a boolean,b bool);
> insert into t1 values (0,1),(true,false),(true,1),(0,false),(NULL,NULL);
> select * from t1;
> select a xor b from t1;
+---------+
| a xor b |
+---------+
| true    |
| true    |
| false   |
| false   |
| NULL    |
+---------+
5 rows in set (0.00 sec)
```
