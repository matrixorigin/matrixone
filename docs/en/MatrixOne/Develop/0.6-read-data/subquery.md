# Subquery

This document describes how to use subquery statements in MatrixOne.

## 概述

An subquery is a query within another SQL query. With subquery, the query result can be used in another query.

In most cases, there are five types of subqueries:

- Scalar Subquery, such as `SELECT (SELECT s1 FROM t2) FROM t1`.
- Derived Tables, such as `SELECT t1.s1 FROM (SELECT s1 FROM t2) t1`.
- Existential Test, such as `WHERE NOT EXISTS(SELECT ... FROM t2)`, `WHERE t1.a IN (SELECT ... FROM t2)`.
- Quantified Comparison, such as `WHERE t1.a = ANY(SELECT ... FROM t2)`, `WHERE t1.a = ANY(SELECT ... FROM t2)`.
- Subquery as a comparison operator operand, such as `WHERE t1.a > (SELECT ... FROM t2)`.

For more information on SQL statement, see [SUBQUERY](../../Reference/SQL-Reference/Data-Manipulation-Statements/subquery.md).

In addition, from the execution of SQL statements, subquery generally has the following two types:

- Correlated Subquery: In Correlated Subquery nested in databases, the inner and outer queries would not be independent, and the inner queries would depend on the outer queries.

   The execution sequence is as follows:

    + Queries a record from the outer query.

    + Put the queried records into the inner query, then put the records that meet the conditions into the outer query.

    + Repeat the above steps

    For example: ``select * from tableA where tableA.cloumn &lt; (select column from tableB where tableA.id = tableB.id))``

- Self-contained Subquery: In a database nested query, the inner query is entirely independent of the outer query.

   The execution sequence is as follows:

    + Execute the inner query first.

    + The result of the inner query is carried into the outer layer, and then the outer query is executed.

    For example: ``select * from tableA where tableA.column = (select tableB.column from tableB)``

**Key Feature**：

- Subqueries allow structured queries so that each part of a query statement can be separated.

- Subqueries provides another way to perform operations that require complex `JOIN` and `UNION`.

## Example

### Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).
- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/).

### Preparation

```sql
drop table if exists t1;
create table t1 (id int,ti tinyint unsigned,si smallint,bi bigint unsigned,fl float,dl double,de decimal,ch char(20),vch varchar(20),dd date,dt datetime);
insert into t1 values(1,1,4,3,1113.32,111332,1113.32,'hello','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(2,2,5,2,2252.05,225205,2252.05,'bye','sub query','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(3,6,6,3,3663.21,366321,3663.21,'hi','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(4,7,1,5,4715.22,471522,4715.22,'good morning','my subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(5,1,2,6,51.26,5126,51.26,'byebye',' is subquery?','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(6,3,2,1,632.1,6321,632.11,'good night','maybe subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(7,4,4,3,7443.11,744311,7443.11,'yes','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(8,7,5,8,8758.00,875800,8758.11,'nice to meet','just subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(9,8,4,9,9849.312,9849312,9849.312,'see you','subquery','2022-04-28','2022-04-28 22:40:11');

drop table if exists t2;
create table t2 (id int,ti tinyint unsigned,si smallint,bi bigint unsigned,fl float,dl double,de decimal,ch char(20),vch varchar(20),dd date,dt datetime);
insert into t2 values(1,1,4,3,1113.32,111332,1113.32,'hello','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(2,2,5,2,2252.05,225205,2252.05,'bye','sub query','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(3,6,6,3,3663.21,366321,3663.21,'hi','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(4,7,1,5,4715.22,471522,4715.22,'good morning','my subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(5,1,2,6,51.26,5126,51.26,'byebye',' is subquery?','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(6,3,2,1,632.1,6321,632.11,'good night','maybe subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(7,4,4,3,7443.11,744311,7443.11,'yes','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(8,7,5,8,8758.00,875800,8758.11,'nice to meet','just subquery','2022-04-28','2022-04-28 22:40:11');
insert into t2 values(9,8,4,9,9849.312,9849312,9849.312,'see you','subquery','2022-04-28','2022-04-28 22:40:11');
```

#### Self-contained subquery

For a self-contained subquery that uses subquery as operand of comparison operators (`>`, `>=`, `<`, `<=`, `=` , or `! =`), the inner subquery queries only once, and MatrixOne rewrites it as a constant during the execution plan phase.

```sql
select * from t1 where t1.id in (select t2.id from t2 where t2.id>=3);
```

The inner subquery is executed before MatrixOne executes the above query:

```sql
select t2.id from t2 where t2.id>=3;
```

Result is as below:

```sql
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
```

For self-contained subqueries such as Existential Test and Quantified Comparison, MatrixOne rewrites and replaces them with equivalent queries for better performance.

#### Correlated subquery

For correlated subquery, because the inner subquery references the columns from the outer query, each subquery is executed once for each row of the outer query. That is, assuming that the outer query gets 10 million results, the subquery will also be executed 10 million times, which will consume more time and resources.

Therefore, in the process of processing, MatrixOne will try to Decorrelate of Correlated Subquery to improve the query efficiency at the execution plan level.

```sql
SELECT *
FROM t1
WHERE id in (
       SELECT id
       FROM t2
       WHERE t1.ti = t2.ti and t2.id>=4
       );
```

Rewrites it to an equivalent join query:

```sql
select t1.* from t1 join t2 on t1.id=t2.id where t2.id>=4;
```

As a best practice, in actual development, it is recommended to avoid querying through a correlated subquery if you can write another equivalent query with better performance.
