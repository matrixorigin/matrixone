# **Subqueries with IN**

## **Description**

A subquery can be used with the `IN` operator as "expression IN (subquery)". The subquery should return a single column with one or more rows to form a list of values to be used by the `IN` operation.

Use the `IN` clause for multiple-record, single-column subqueries. After the subquery returns results introduced by `IN` or `NOT IN`, the outer query uses them to return the final result.

- If any row in the sub-query result matches, the answer is true.
- If the subquery result is empty, the answer is false.
- If no row in the sub-query result matches, the answer is also false.
- If all of the values in the sub-query result are null, the answer is false.

## **Syntax**

```
> SELECT ... FROM table_name WHERE column_name IN (subquery)
```

## **Examples**

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

## **Constraints**

MatrixOne does not support selecting multiple columns for the subquery.
