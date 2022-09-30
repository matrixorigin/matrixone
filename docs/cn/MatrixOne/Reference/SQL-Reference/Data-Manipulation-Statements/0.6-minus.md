# **MINUS**

## **语法说明**

`MINUS` 比较两个查询的结果，并返回第一个查询中不是由第二个查询输出的不同行。

## **语法结构**

```
SELECT column_list_1 FROM table_1
MINUS
SELECT columns_list_2 FROM table_2;
```

## **示例**

- 示例 1

```sql
> CREATE TABLE t1 (id INT PRIMARY KEY);
> CREATE TABLE t2 (id INT PRIMARY KEY);
> INSERT INTO t1 VALUES (1),(2),(3);
> INSERT INTO t2 VALUES (2),(3),(4);
> SELECT id FROM t1 MINUS SELECT id FROM t2;
+------+
| id   |
+------+
|    1 |
+------+
```

- 示例 2

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
> select * from t1 minus select * from t2;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 |    1 |    2 |
|    4 |    5 |    6 |
+------+------+------+

> select a, b from t1 minus select b, c from t2;
+------+------+
| a    | b    |
+------+------+
|    3 |    4 |
|    1 |    1 |
|    1 |    2 |
+------+------+
```
