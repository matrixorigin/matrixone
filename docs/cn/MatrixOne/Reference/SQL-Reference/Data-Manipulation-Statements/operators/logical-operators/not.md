# **NOT,!**

## **运算符说明**

`NOT,!` 逻辑运算符用作于*逻辑非*运算。如果操作数为零，则返回结果为 `true`；如果操作数非零，则返回结果为 `false`；若果操作数为 `NOT NUL` 则返回 `NULL`。

## **语法结构**

```
> SELECT not column_name FROM table_name;
```

## **示例**

```sql
> select not 0;
+-------+
| not 0 |
+-------+
| true  |
+-------+
1 row in set (0.02 sec)
> select not null;
+----------+
| not null |
+----------+
| NULL     |
+----------+
1 row in set (0.00 sec)
> select not 1;
+-------+
| not 1 |
+-------+
| false |
+-------+
1 row in set (0.01 sec)
```

```sql
> create table t1 (a boolean,b bool);
> insert into t1 values (0,1),(true,false),(true,1),(0,false),(NULL,NULL);
> select * from t1;
> select not a and not b from t1;
+-----------------+
| not a and not b |
+-----------------+
| false           |
| false           |
| false           |
| true            |
| NULL            |
+-----------------+
5 rows in set (0.00 sec)
```

## **限制**

MatrixOne 暂时还不支持 `!` 运算符。
