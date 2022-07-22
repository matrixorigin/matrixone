# **Subqueries with ANY or SOME**

## **语法描述**

由于列子查询返回的结果集是多行一列，因此不能直接使用（=，>，<，>=，<=，<>）这些比较操作符。在列子查询中可以使用 `ANY`、`SOME`操作符与比较操作符联合使用：

- `ANY`：与比较操作符联合使用，表示与子查询返回的任何值比较为 `TRUE`，则返回结果为 `true`。
- `SOME`：`ANY` 的别名，与 `ANY` 意义相同，但较少使用。

## **语法结构**

```
> SELECT column_name(s) FROM table_name WHERE column_name ANY (subquery);
```

## **示例**

```sql
> create table t1 (a int);
> create table t2 (a int, b int);
> create table t3 (a int);
> create table t4 (a int not null, b int not null);
> insert into t1 values (2);
> insert into t2 values (1,7),(2,7);
> insert into t4 values (4,8),(3,8),(5,9);
> insert into t3 values (6),(7),(3);
> select * from t3 where a <> any (select b from t2);
+------+
| a    |
+------+
|    6 |
|    3 |
+------+
2 rows in set (0.00 sec)

> select * from t3 where a <> some (select b from t2);
+------+
| a    |
+------+
|    6 |
|    3 |
+------+
2 rows in set (0.00 sec)

> select * from t3 where a = some (select b from t2);
+------+
| a    |
+------+
|    7 |
+------+
1 row in set (0.00 sec)

> select * from t3 where a = any (select b from t2);
+------+
| a    |
+------+
|    7 |
+------+
1 row in set (0.00 sec)
```
