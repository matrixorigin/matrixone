# Explain Statements Using Subqueries

MatrixOne performs several optimizations to improve the performance of subqueries. This document describes some of these optimizations for common subqueries and how to interpret the output of `EXPLAIN`.

From the execution of SQL statements, subquery generally has the following two types:

- **Self-contained Subquery**: In a database nested query, the inner query is entirely independent of the outer query.

     For example: ``select * from t1 where t1.id in (select t2.id from t2 where t2.id>=3);``, the execution sequence is as follows:

     + Execute the inner query first: `(select t2.id from t2 where t2.id>=3)`。

     + The result of the inner query is carried into the outer layer, and then the outer query is executed.

- **Correlated Subquery**: In Correlated Subquery nested in databases, the inner and outer queries would not be independent, and the inner queries would depend on the outer queries.

     For example: ``SELECT * FROM t1 WHERE id in (SELECT id FROM t2 WHERE t1.ti = t2.ti and t2.id>=4);``, generally, the execution sequence is as follows:

     + Queries a record from the outer query: `SELECT * FROM t1 WHERE id`。

     + Put the queried records into the inner query, then put the records that meet the conditions into the outer query.

     + Repeat the above steps.

     However, MatrixOne will rewrite the SQL statement as an equivalent `JOIN` statement: `select t1.* from t1 join t2 on t1.id=t2.id where t2.id>=4;`

## Example

We have prepared a simple example to help you understand the execution plan for interpreting the `SUBQUERY` using `EXPLAIN`.

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
```

### Self-contained Subquery

```sql
> select * from t1 where t1.id in (select t2.id from t2 where t2.id>=3);
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
| id   | ti   | si   | bi   | fl       | dl      | de   | ch           | vch            | dd         | dt                  |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
|    3 |    6 |    6 |    3 |  3663.21 |  366321 | 3663 | hi           | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
|    4 |    7 |    1 |    5 |  4715.22 |  471522 | 4715 | good morning | my subquery    | 2022-04-28 | 2022-04-28 22:40:11 |
|    5 |    1 |    2 |    6 |    51.26 |    5126 |   51 | byebye       |  is subquery?  | 2022-04-28 | 2022-04-28 22:40:11 |
|    6 |    3 |    2 |    1 |    632.1 |    6321 |  632 | good night   | maybe subquery | 2022-04-28 | 2022-04-28 22:40:11 |
|    7 |    4 |    4 |    3 |  7443.11 |  744311 | 7443 | yes          | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
|    8 |    7 |    5 |    8 |     8758 |  875800 | 8758 | nice to meet | just subquery  | 2022-04-28 | 2022-04-28 22:40:11 |
|    9 |    8 |    4 |    9 | 9849.312 | 9849312 | 9849 | see you      | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
7 rows in set (0.02 sec)

> explain select * from t1 where t1.id in (select t2.id from t2 where t2.id>=3);
+---------------------------------------------------------------+
| QUERY PLAN                                                    |
+---------------------------------------------------------------+
| Project                                                       |
|   ->  Join                                                    |
|         Join Type: SEMI                                       |
|         Join Cond: (t1.id = t2.id)                            |
|         ->  Table Scan on db1.t1                              |
|         ->  Project                                           |
|               ->  Table Scan on db1.t2                        |
|                     Filter Cond: (CAST(t2.id AS BIGINT) >= 3) |
+---------------------------------------------------------------+
8 rows in set (0.00 sec)
```

The execution sequence is as follows:

1. Execute the inner query first: `(select t2.id from t2 where t2.id>=3)`。

2. The result of the inner query is carried into the outer layer, and then the outer query is executed.

### Correlated subquery

```sql
> SELECT * FROM t1 WHERE id in (SELECT id FROM t2 WHERE t1.ti = t2.ti and t2.id>=4);
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
| id   | ti   | si   | bi   | fl       | dl      | de   | ch           | vch            | dd         | dt                  |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
|    4 |    7 |    1 |    5 |  4715.22 |  471522 | 4715 | good morning | my subquery    | 2022-04-28 | 2022-04-28 22:40:11 |
|    5 |    1 |    2 |    6 |    51.26 |    5126 |   51 | byebye       |  is subquery?  | 2022-04-28 | 2022-04-28 22:40:11 |
|    6 |    3 |    2 |    1 |    632.1 |    6321 |  632 | good night   | maybe subquery | 2022-04-28 | 2022-04-28 22:40:11 |
|    7 |    4 |    4 |    3 |  7443.11 |  744311 | 7443 | yes          | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
|    8 |    7 |    5 |    8 |     8758 |  875800 | 8758 | nice to meet | just subquery  | 2022-04-28 | 2022-04-28 22:40:11 |
|    9 |    8 |    4 |    9 | 9849.312 | 9849312 | 9849 | see you      | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
6 rows in set (0.01 sec)

mysql> explain SELECT * FROM t1 WHERE id in (SELECT id FROM t2 WHERE t1.ti = t2.ti and t2.id>=4);
+---------------------------------------------------------------+
| QUERY PLAN                                                    |
+---------------------------------------------------------------+
| Project                                                       |
|   ->  Join                                                    |
|         Join Type: SEMI                                       |
|         Join Cond: (t1.ti = t2.ti), (t1.id = t2.id)           |
|         ->  Table Scan on db1.t1                              |
|         ->  Project                                           |
|               ->  Table Scan on db1.t2                        |
|                     Filter Cond: (CAST(t2.id AS BIGINT) >= 4) |
+---------------------------------------------------------------+
8 rows in set (0.01 sec)
```

MatrixOne will rewrite the SQL statement as an equivalent `JOIN` statement: `select t1.* from t1 join t2 on t1.id=t2.id where t2.id>=4;`, the execution sequence is as follows:

1. Execute the filter query: `where t2.id>=4;`。

2. Scan Table: `Table Scan on db1.t2`, and then the result "flow into" into the parent node.

3. Scan `table table scan on db1.t1`.

4. Execute `JOIN`.
