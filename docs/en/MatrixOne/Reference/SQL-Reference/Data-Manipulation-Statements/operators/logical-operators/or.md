# **OR**

## **Description**

Logical `OR,||`. When both operands are non-NULL, the result is `true` if any operand is nonzero, and `false` otherwise. With a NULL operand, the result is `true` if the other operand is nonzero, and NULL otherwise. If both operands are `NULL`, the result is `NULL`.

## **Syntax**

```
> SELECT column_1 OR column_2 FROM table_name;
```

## **Examples**

```sql
> select 1 or 1;
+--------+
| 1 or 1 |
+--------+
| true   |
+--------+
1 row in set (0.01 sec)

> select 1 or 0;
+--------+
| 1 or 0 |
+--------+
| true   |
+--------+
1 row in set (0.00 sec)

> select 0 or 0;
+--------+
| 0 or 0 |
+--------+
| false  |
+--------+
1 row in set (0.01 sec)

> select 0 or null;
+-----------+
| 0 or null |
+-----------+
| NULL      |
+-----------+
1 row in set (0.00 sec)

> select 1 or null;
+-----------+
| 1 or null |
+-----------+
| true      |
+-----------+
1 row in set (0.00 sec)
```

```sql
> create table t1 (a boolean,b bool);
> insert into t1 values (0,1),(true,false),(true,1),(0,false),(NULL,NULL);
> select * from t1;
> select a or b from t1;
+--------+
| a or b |
+--------+
| true   |
| true   |
| true   |
| false  |
| NULL   |
+--------+
5 rows in set (0.00 sec)
```
