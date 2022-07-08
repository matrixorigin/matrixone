# **XOR**

## **运算符说明**

`XOR` 逻辑运算符用作于*逻辑异或*运算。如果任意一个操作数为 `NULL` 则返回结果为 `NULL`；对于非 `NULL` 操作数，如果有奇数个操作数是非零，则返回结果为 `true`，否则返回结果为 `false`。

`a XOR b` 在数学运算上等于 `(a AND (NOT b)) OR ((NOT a) and b)`。

## **语法结构**

```
> SELECT column_1 XOR column_2 FROM table_name;
```

## **示例**

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
