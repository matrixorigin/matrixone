# Explain Statements Using JOIN

The SQL Optimizer needs to decide in which order tables should be joined and what is the most efficient join algorithm for a particular SQL statement.

## Example

We have prepared a simple example to help you understand the execution plan for interpreting the `JOIN` using `EXPLAIN`.

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

### Hash Join

In the `Hash Join` operation, MatrixOne first reads the smaller tables in *t1* and *t2* and uses a hash function for each value to be joined to obtain a hash table. Then each row of the other table is scanned, and the hash value is calculated and compared with the hash table generated in the previous step. A new join table is generated based on the joinif any values meet the join criteria.

The hash join operator is multi-threaded in MatrixOne and executes in parallel.

An example of hash join is as below:

```sql
> SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t1, t2 WHERE t1.id = t2.id;
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
| id   | ti   | si   | bi   | fl       | dl      | de   | ch           | vch            | dd         | dt                  | id   | ti   | si   | bi   | fl       | dl      | de   | ch           | vch            | dd         | dt                  |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
|    1 |    1 |    4 |    3 |  1113.32 |  111332 | 1113 | hello        | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |    1 |    1 |    4 |    3 |  1113.32 |  111332 | 1113 | hello        | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
|    2 |    2 |    5 |    2 |  2252.05 |  225205 | 2252 | bye          | sub query      | 2022-04-28 | 2022-04-28 22:40:11 |    2 |    2 |    5 |    2 |  2252.05 |  225205 | 2252 | bye          | sub query      | 2022-04-28 | 2022-04-28 22:40:11 |
|    3 |    6 |    6 |    3 |  3663.21 |  366321 | 3663 | hi           | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |    3 |    6 |    6 |    3 |  3663.21 |  366321 | 3663 | hi           | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
|    4 |    7 |    1 |    5 |  4715.22 |  471522 | 4715 | good morning | my subquery    | 2022-04-28 | 2022-04-28 22:40:11 |    4 |    7 |    1 |    5 |  4715.22 |  471522 | 4715 | good morning | my subquery    | 2022-04-28 | 2022-04-28 22:40:11 |
|    5 |    1 |    2 |    6 |    51.26 |    5126 |   51 | byebye       |  is subquery?  | 2022-04-28 | 2022-04-28 22:40:11 |    5 |    1 |    2 |    6 |    51.26 |    5126 |   51 | byebye       |  is subquery?  | 2022-04-28 | 2022-04-28 22:40:11 |
|    6 |    3 |    2 |    1 |    632.1 |    6321 |  632 | good night   | maybe subquery | 2022-04-28 | 2022-04-28 22:40:11 |    6 |    3 |    2 |    1 |    632.1 |    6321 |  632 | good night   | maybe subquery | 2022-04-28 | 2022-04-28 22:40:11 |
|    7 |    4 |    4 |    3 |  7443.11 |  744311 | 7443 | yes          | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |    7 |    4 |    4 |    3 |  7443.11 |  744311 | 7443 | yes          | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
|    8 |    7 |    5 |    8 |     8758 |  875800 | 8758 | nice to meet | just subquery  | 2022-04-28 | 2022-04-28 22:40:11 |    8 |    7 |    5 |    8 |     8758 |  875800 | 8758 | nice to meet | just subquery  | 2022-04-28 | 2022-04-28 22:40:11 |
|    9 |    8 |    4 |    9 | 9849.312 | 9849312 | 9849 | see you      | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |    9 |    8 |    4 |    9 | 9849.312 | 9849312 | 9849 | see you      | subquery       | 2022-04-28 | 2022-04-28 22:40:11 |
+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+------+------+------+------+----------+---------+------+--------------+----------------+------------+---------------------+
9 rows in set (0.00 sec)

mysql> EXPLAIN SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t1, t2 WHERE t1.id = t2.id;
+------------------------------------+
| QUERY PLAN                         |
+------------------------------------+
| Project                            |
|   ->  Join                         |
|         Join Type: INNER           |
|         Join Cond: (t1.id = t2.id) |
|         ->  Table Scan on db1.t1   |
|         ->  Table Scan on db1.t2   |
+------------------------------------+
6 rows in set (0.01 sec)
```

MatrixOne executes the `Hash Join` operator in the following order:

1. Scan table *t2* and *t1* in parallel.

2. Execute the `JOIN` filter query: `(t1.id = t2.id)`.

3. Execute `INNER JOIN`.
