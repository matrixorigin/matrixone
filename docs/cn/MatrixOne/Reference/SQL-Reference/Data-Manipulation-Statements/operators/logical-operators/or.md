# **OR**

## **运算符说明**

`OR,||` 逻辑运算符用作*逻辑或*运算。当两个操作数都非 `null` 时，如果操作数同时也非零，则返回结果为 `true`，否则为 `false`；对于 `NULL` 操作数，如果另一个操作数非零，则返回结果为 `true`，否则为 `NULL`；如果两个操作数都为 `NULL`，则返回结果为 `NULL`。

## **语法结构**

```
> SELECT column_1 OR column_2 FROM table_name;
```

## **示例**

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
