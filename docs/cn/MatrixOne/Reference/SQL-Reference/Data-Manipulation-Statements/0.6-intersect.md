# **INTERSECT**

## **语法说明**

`INTERSECT` 运算符是一个集合运算符仅返回两个查询或多个查询的不同行。

## **语法结构**

```
SELECT column_list FROM table_1
INTERSECT
SELECT column_list FROM table_2;
```

## **示例**

```sql
> drop table if exists t1;
> drop table if exists t2;
> create table t1 (a smallint, b bigint, c int);
> insert into t1 values (1,2,3);
> insert into t1 values (1,2,3);
> insert into t1 values (3,4,5);
> insert into t1 values (4,5,6);
> insert into t1 values (4,5,6);
> insert into t1 values (1,1,2);
> create table t2 (a smallint, b bigint, c int);
> insert into t2 values (1,2,3);
> insert into t2 values (3,4,5);
> insert into t2 values (1,2,1);
> select * from t1 intersect select * from t2;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 |    2 |    3 |
|    3 |    4 |    5 |
+------+------+------+
2 rows in set (0.01 sec)

mysql> select a, b from t1 intersect select b, c from t2;
+------+------+
| a    | b    |
+------+------+
|    4 |    5 |
+------+------+
1 row in set (0.01 sec)
```
