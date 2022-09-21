# **Subqueries with EXISTS or NOT EXISTS**

## **语法描述**

`EXISTS` 用于检查子查询是否至少会返回一行数据。即将主查询的数据，放到子查询中做条件验证，根据验证结果（TRUE 或 FALSE）来决定主查询的数据结果是否得以保留。

如果子查询返回任何行，`EXISTS` 子查询条件为 `TRUE`，`NOT EXISTS` 子查询条件为 `FALSE`。

## **语法结构**

```
> SELECT column_name(s)
FROM table_name
WHERE EXISTS
(SELECT column_name FROM table_name WHERE condition);
```

## **示例**

```sql
> create table t1 (a int);
> create table t2 (a int, b int);
> create table t3 (a int);
> create table t4 (a int not null, b int not null);
> create table t5 (a int);
> create table t6 (a int, b int);
> insert into t1 values (2);
> insert into t2 values (1,7),(2,7);
> insert into t4 values (4,8),(3,8),(5,9);
> insert into t5 values (null);
> insert into t3 values (6),(7),(3);
> insert into t6 values (10,7),(null,7);
> select * from t3 where exists (select * from t2 where t2.b=t3.a);
+------+
| a    |
+------+
|    7 |
+------+
1 row in set (0.00 sec)
> select * from t3 where not exists (select * from t2 where t2.b=t3.a);
+------+
| a    |
+------+
|    6 |
|    3 |
+------+
2 rows in set (0.00 sec)
```

## **限制**

MatrixOne 暂不支持选择多列进行子查询。
