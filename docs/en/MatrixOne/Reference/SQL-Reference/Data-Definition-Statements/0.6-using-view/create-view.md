# **CREATE VIEW**

## **Description**

The view is a virtual table based on the result-set of an SQL statement.

A view contains rows and columns, just like a real table. The fields in a view are fields from one or more real tables in the database.

You can add SQL statements and functions to a view and present the data as if the data were coming from one single table.

A view is created with the `CREATE VIEW` statement.

## **Syntax**

```
> CREATE VIEW view_name AS
  SELECT column1, column2, ...
  FROM table_name
  WHERE condition;
```

!!! note
    A view always shows up-to-date data! The database engine recreates the view, every time a user queries it.

## **Examples**

- Example 1:

```sql
> CREATE TABLE t00(a INTEGER);
> INSERT INTO t00 VALUES (1),(2);
> CREATE TABLE t01(a INTEGER);
> INSERT INTO t01 VALUES (1);
> CREATE VIEW v0 AS SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a);
l> SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a);
+------+------+
| a    | b    |
+------+------+
|    1 |    1 |
|    2 | NULL |
+------+------+
2 rows in set (0.01 sec)

> SELECT * FROM v0 WHERE b >= 0;
+------+------+
| a    | b    |
+------+------+
|    1 |    1 |
+------+------+
1 row in set (0.01 sec)

> SHOW CREATE VIEW v0;
+------+----------------------------------------------------------------------------+
| View | Create View                                                                |
+------+----------------------------------------------------------------------------+
| v0   | CREATE VIEW v0 AS SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a) |
+------+----------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

- Example 2:

```sql
> drop table if exists t1;
> create table t1 (id int,ti tinyint unsigned,si smallint,bi bigint unsigned,fl float,dl double,de decimal,ch char(20),vch varchar(20),dd date,dt datetime);
> insert into t1 values(1,1,4,3,1113.32,111332,1113.32,'hello','subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t1 values(2,2,5,2,2252.05,225205,2252.05,'bye','sub query','2022-04-28','2022-04-28 22:40:11');
> insert into t1 values(3,6,6,3,3663.21,366321,3663.21,'hi','subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t1 values(4,7,1,5,4715.22,471522,4715.22,'good morning','my subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t1 values(5,1,2,6,51.26,5126,51.26,'byebye',' is subquery?','2022-04-28','2022-04-28 22:40:11');
> insert into t1 values(6,3,2,1,632.1,6321,632.11,'good night','maybe subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t1 values(7,4,4,3,7443.11,744311,7443.11,'yes','subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t1 values(8,7,5,8,8758.00,875800,8758.11,'nice to meet','just subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t1 values(9,8,4,9,9849.312,9849312,9849.312,'see you','subquery','2022-04-28','2022-04-28 22:40:11');
> drop table if exists t2;
> create table t2 (id int,ti tinyint unsigned,si smallint,bi bigint unsigned,fl float,dl double,de decimal,ch char(20),vch varchar(20),dd date,dt datetime);
> insert into t2 values(1,1,4,3,1113.32,111332,1113.32,'hello','subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t2 values(2,2,5,2,2252.05,225205,2252.05,'bye','sub query','2022-04-28','2022-04-28 22:40:11');
> insert into t2 values(3,6,6,3,3663.21,366321,3663.21,'hi','subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t2 values(4,7,1,5,4715.22,471522,4715.22,'good morning','my subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t2 values(5,1,2,6,51.26,5126,51.26,'byebye',' is subquery?','2022-04-28','2022-04-28 22:40:11');
> insert into t2 values(6,3,2,1,632.1,6321,632.11,'good night','maybe subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t2 values(7,4,4,3,7443.11,744311,7443.11,'yes','subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t2 values(8,7,5,8,8758.00,875800,8758.11,'nice to meet','just subquery','2022-04-28','2022-04-28 22:40:11');
> insert into t2 values(9,8,4,9,9849.312,9849312,9849.312,'see you','subquery','2022-04-28','2022-04-28 22:40:11');
> select * from (select * from t1) sub where id > 4;
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
| id   | ti   | si   | bi   | fl       | dl      | de   | ch           | vch            | dd         | dt                  |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
|    5 |    1 |    2 |    6 |    51.26 |    5126 |   51 | byebye       |  is subquery?  | 2022-04-28 | 2022-04-28 22:40:11 |
|    6 |    3 |    2 |    1 |    632.1 |    6321 |  632 | good night   | maybe subquery | 2022-04-28 | 2022-04-28 22:40:11 |
|    7 |    4 |    4 |    3 |  7443.11 |  744311 | 7443 | yes          | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
|    8 |    7 |    5 |    8 |     8758 |  875800 | 8758 | nice to meet | just subquery  | 2022-04-28 | 2022-04-28 22:40:11 |
|    9 |    8 |    4 |    9 | 9849.312 | 9849312 | 9849 | see you      | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
5 rows in set (0.01 sec)

> create view v1 as select * from (select * from t1) sub where id > 4;
> create view v2 as select ti as t,fl as f from (select * from t1) sub where dl <> 4;
> create view v3 as select * from (select ti as t,fl as f from t1 where dl <> 4) sub;
> create view v4 as select id,min(ti) from (select * from t1) sub group by id;
> create view v5 as select * from (select id,min(ti) from (select * from t1) t1 group by id) sub;
> select * from v1;
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
| id   | ti   | si   | bi   | fl       | dl      | de   | ch           | vch            | dd         | dt                  |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
|    5 |    1 |    2 |    6 |    51.26 |    5126 |   51 | byebye       |  is subquery?  | 2022-04-28 | 2022-04-28 22:40:11 |
|    6 |    3 |    2 |    1 |    632.1 |    6321 |  632 | good night   | maybe subquery | 2022-04-28 | 2022-04-28 22:40:11 |
|    7 |    4 |    4 |    3 |  7443.11 |  744311 | 7443 | yes          | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
|    8 |    7 |    5 |    8 |     8758 |  875800 | 8758 | nice to meet | just subquery  | 2022-04-28 | 2022-04-28 22:40:11 |
|    9 |    8 |    4 |    9 | 9849.312 | 9849312 | 9849 | see you      | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
5 rows in set (0.00 sec)

> select * from v2;
+------+----------+
| t    | f        |
+------+----------+
|    1 |  1113.32 |
|    2 |  2252.05 |
|    6 |  3663.21 |
|    7 |  4715.22 |
|    1 |    51.26 |
|    3 |    632.1 |
|    4 |  7443.11 |
|    7 |     8758 |
|    8 | 9849.312 |
+------+----------+
9 rows in set (0.00 sec)

> select * from v3;
+------+----------+
| t    | f        |
+------+----------+
|    1 |  1113.32 |
|    2 |  2252.05 |
|    6 |  3663.21 |
|    7 |  4715.22 |
|    1 |    51.26 |
|    3 |    632.1 |
|    4 |  7443.11 |
|    7 |     8758 |
|    8 | 9849.312 |
+------+----------+
9 rows in set (0.00 sec)

> select * from v4;
+------+---------+
| id   | min(ti) |
+------+---------+
|    1 |       1 |
|    2 |       2 |
|    3 |       6 |
|    4 |       7 |
|    5 |       1 |
|    6 |       3 |
|    7 |       4 |
|    8 |       7 |
|    9 |       8 |
+------+---------+
9 rows in set (0.00 sec)

> select * from v5;
+------+---------+
| id   | min(ti) |
+------+---------+
|    1 |       1 |
|    2 |       2 |
|    3 |       6 |
|    4 |       7 |
|    5 |       1 |
|    6 |       3 |
|    7 |       4 |
|    8 |       7 |
|    9 |       8 |
+------+---------+
9 rows in set (0.01 sec)
```
