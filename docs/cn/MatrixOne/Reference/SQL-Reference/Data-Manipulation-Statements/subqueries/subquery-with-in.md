# **Subqueries with IN**

## **语法描述**

子查询可以与 `IN` 操作符一起使用，作为“表达式 IN (子查询)”，查询某个范围内的数据。子查询应该返回带有一行或多行的单个列，以形成 `IN` 操作使用的值列表。

对多记录、单列子查询使用 `IN` 子句。子查询返回 `IN` 或 `NOT IN` 引入的结果后，外部查询使用它们返回最终结果。

* 如果子查询结果中有匹配的行，则结果为 `TRUE`。

* 如果子查询结果为 `NULL`，则返回结果为 `false`。

* 如果子查询结果中没有匹配的行，结果也是 `FALSE`。

* 如果子查询结果中所有的值都为 `NULL`，则返回结果为 `false`。

## **语法结构**

```
> SELECT ... FROM table_name WHERE column_name IN (subquery)
```

## **示例**

- 示例 1：

```sql
> create table t1(val varchar(10));
> insert into t1 values ('aaa'), ('bbb'),('eee'),('mmm'),('ppp');
> select count(*) from t1 as w1 where w1.val in (select w2.val from t1 as w2 where w2.val like 'm%') and w1.val in (select w3.val from t1 as w3 where w3.val like 'e%');
+----------+
| count(*) |
+----------+
|        0 |
+----------+
1 row in set (0.01 sec)
```

- 示例 2：

```sql
> create table t1 (id int not null, text varchar(20) not null default '', primary key (id));
> insert into t1 (id, text) values (1, 'text1'), (2, 'text2'), (3, 'text3'), (4, 'text4'), (5, 'text5'), (6, 'text6'), (7, 'text7'), (8, 'text8'), (9, 'text9'), (10, 'text10'), (11, 'text11'), (12, 'text12');
> select * from t1 where id not in (select id from t1 where id < 8);
+------+--------+
| id   | text   |
+------+--------+
|    8 | text8  |
|    9 | text9  |
|   10 | text10 |
|   11 | text11 |
|   12 | text12 |
+------+--------+
5 rows in set (0.00 sec)
```

- 示例 3：

```sql
> CREATE TABLE t1 (a int);
> CREATE TABLE t2 (a int, b int);
> CREATE TABLE t3 (b int NOT NULL);
> INSERT INTO t1 VALUES (1), (2), (3), (4);
> INSERT INTO t2 VALUES (1,10), (3,30);
> select * from t1 where t1.a in (SELECT t1.a FROM t1 LEFT JOIN t2 ON t2.a=t1.a);
+------+
| a    |
+------+
|    1 |
|    2 |
|    3 |
|    4 |
+------+
4 rows in set (0.01 sec)
> SELECT * FROM t2 LEFT JOIN t3 ON t2.b=t3.b WHERE t3.b IS NOT NULL OR t2.a > 10;
Empty set (0.01 sec)
> SELECT * FROM t1 WHERE t1.a NOT IN (SELECT a FROM t2 LEFT JOIN t3 ON t2.b=t3.b WHERE t3.b IS NOT NULL OR t2.a > 10);
+------+
| a    |
+------+
|    1 |
|    2 |
|    3 |
|    4 |
+------+
4 rows in set (0.00 sec)
```

- 示例 4：

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
> select a,b from t6 where (a,b) in ( select a,b from t4 where a>3);
Empty set (0.02 sec)
```

## **限制**

MatrixOne 暂不支持选择多列进行子查询。
