# **COALESCE()**

## **Description**

The `COALESCE()` function returns the first non-null value in a list.

## **Syntax**

```
> COALESCE(val1, val2, ...., val_n)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| val1, val2, val_n | Required. The values to test |

## **Examples**

- Example: Calculate

```sql
> SELECT COALESCE(1)+COALESCE(1);
+---------------------------+
| coalesce(1) + coalesce(1) |
+---------------------------+
|                         2 |
+---------------------------+
```

- Example: Comparison

```SQL
> drop table if exists t2;
> create table t2(a float, b datetime);
> insert into t2 values (12.345, '2022-02-20 10:10:10.999999');
> insert into t2 values (3.45646, NULL);
> insert into t2 values(NULL, '2023-04-03 22:10:29.999999');
> insert into t2 values (NULL, NULL);
> select * from t2;
+---------+---------------------+
| a       | b                   |
+---------+---------------------+
|  12.345 | 2022-02-20 10:10:11 |
| 3.45646 | NULL                |
|    NULL | 2023-04-03 22:10:30 |
|    NULL | NULL                |
+---------+---------------------+
> select coalesce(a, 1.0) from t2;
+--------------------+
| coalesce(a, 1.0)   |
+--------------------+
| 12.345000267028809 |
| 3.4564599990844727 |
|                  1 |
|                  1 |
+--------------------+
> select coalesce(a, 1) from t2;
+--------------------+
| coalesce(a, 1)     |
+--------------------+
| 12.345000267028809 |
| 3.4564599990844727 |
|                  1 |
|                  1 |
+--------------------+
> select coalesce(b, 2022-01-01) from t2;
+---------------------------+
| coalesce(b, 2022 - 1 - 1) |
+---------------------------+
| 2022-02-20 10:10:11       |
|                           |
| 2023-04-03 22:10:30       |
|                           |
+---------------------------+
```

- Example: `ORDER BY` Clause, `DATE TYPE`

```sql
> CREATE TABLE t1 ( a INTEGER, b varchar(255) );
> INSERT INTO t1 VALUES (1,'z');
> INSERT INTO t1 VALUES (2,'y');
> INSERT INTO t1 VALUES (3,'x');
> SELECT MIN(b) AS min_b FROM t1 GROUP BY a ORDER BY COALESCE(MIN(b), 'a');
+-------+
| min_b |
+-------+
| x     |
| y     |
| z     |
+-------+

> SELECT MIN(b) AS min_b FROM t1 GROUP BY a ORDER BY COALESCE(min_b, 'a');
+-------+
| min_b |
+-------+
| x     |
| y     |
| z     |
+-------+

> SELECT MIN(b) AS min_b FROM t1 GROUP BY a ORDER BY COALESCE(MIN(b), 'a') DESC;
+-------+
| min_b |
+-------+
| z     |
| y     |
| x     |
+-------+
```

- Example: `Case When` Clause

```sql
> select if(1, cast(1111111111111111111 as unsigned), 1) i,case when 1 then cast(1111111111111111111 as unsigned) else 1 end c, coalesce(cast(1111111111111111111 as unsigned), 1) co;
+---------------------+---------------------+---------------------+
| i                   | c                   | co                  |
+---------------------+---------------------+---------------------+
| 1111111111111111111 | 1111111111111111111 | 1111111111111111111 |
+---------------------+---------------------+---------------------+
```

- Example: `IN Subquery`

```sql
> CREATE TABLE ot (col_int_nokey int(11), col_varchar_nokey varchar(1));
> INSERT INTO ot VALUES (1,'x');
> CREATE TABLE it (col_int_key int(11), col_varchar_key varchar(1));
> INSERT INTO it VALUES (NULL,'x'), (NULL,'f');
> SELECT col_int_nokey FROM ot WHERE col_varchar_nokey IN(SELECT col_varchar_key FROM it WHERE coalesce(col_int_nokey, 1) );
+---------------+
| col_int_nokey |
+---------------+
|             1 |
+---------------+
```

- Example: `WHERE`

```sql
> CREATE TABLE ot1(a INT);
> CREATE TABLE ot2(a INT);
> CREATE TABLE ot3(a INT);
> CREATE TABLE it1(a INT);
> CREATE TABLE it2(a INT);
> CREATE TABLE it3(a INT);
> INSERT INTO ot1 VALUES(0),(1),(2),(3),(4),(5),(6),(7);
> INSERT INTO ot2 VALUES(0),(2),(4),(6);
> INSERT INTO ot3 VALUES(0),(3),(6);
> INSERT INTO it1 VALUES(0),(1),(2),(3),(4),(5),(6),(7);
> INSERT INTO it2 VALUES(0),(2),(4),(6);
> INSERT INTO it3 VALUES(0),(3),(6);
> SELECT * FROM ot1 LEFT JOIN ot2 ON ot1.a=ot2.a WHERE COALESCE(ot2.a,0) IN (SELECT a FROM it3);
+------+------+
| a    | a    |
+------+------+
|    0 |    0 |
|    1 | NULL |
|    3 | NULL |
|    5 | NULL |
|    6 |    6 |
|    7 | NULL |
+------+------+
```

- Example: `HAVING`

```sql
> drop table if exists t1;
> create table t1(a datetime);
> INSERT INTO t1 VALUES (NULL), ('2001-01-01 00:00:00.12'), ('2002-01-01 00:00:00.567');
> select a from t1 group by a having COALESCE(a)<"2002-01-01";
+---------------------+
| a                   |
+---------------------+
| 2001-01-01 00:00:00 |
+---------------------+
```

- Example: `ON CONDITION`

```sql
> drop table if exists t1;
> drop table if exists t2;
> create table t1(a INT,  b varchar(255));
> create table t2(a INT,  b varchar(255));
> insert into t1 values(1, "你好"), (3, "再见");
> insert into t2 values(2, "日期时间"), (4, "明天");
> SELECT t1.a, t2.a FROM t1 JOIN t2 ON (length(COALESCE(t1.b)) = length(COALESCE(t2.b)));
+------+------+
| a    | a    |
+------+------+
|    1 |    4 |
|    3 |    4 |
+------+------+
```
