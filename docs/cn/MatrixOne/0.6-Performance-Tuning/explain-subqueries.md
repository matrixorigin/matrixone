# 用 EXPLAIN 查看子查询的执行计划

MatrixOne 会执行多种子查询相关的优化，以提升子查询的执行性能。本文档介绍一些常见子查询的优化方式，以及如何解读 EXPLAIN 语句返回的执行计划信息。

从 SQL 语句执行情况上，子查询语句一般有以下两种形式：

- **无关联子查询 (Self-contained Subquery)** ：数据库嵌套查询中内层查询是完全独立于外层查询的。

     例如：``select * from t1 where t1.id in (select t2.id from t2 where t2.id>=3);`` 执行顺序为：

     + 先执行内层查询：`(select t2.id from t2 where t2.id>=3)`。

     + 得到内层查询的结果后带入外层，再执行外层查询。

- **关联子查询（Correlated Subquery）**：数据库嵌套查询中内层查询和外层查询不相互独立，内层查询也依赖于外层查询。

     例如：``SELECT * FROM t1 WHERE id in (SELECT id FROM t2 WHERE t1.ti = t2.ti and t2.id>=4);``一般情况下，执行顺序为：

     + 先从外层查询中查询中一条记录：`SELECT * FROM t1 WHERE id`。

     + 再将查询到的记录放到内层查询中符合条件的记录，再放到外层中查询。

     + 重复以上步骤

     但是 MatrixOne 在处理该 SQL 语句是会将其改写为等价的 `JOIN` 查询：`select t1.* from t1 join t2 on t1.id=t2.id where t2.id>=4;`

## 示例

我们这里准备一个简单的示例，帮助你理解使用 EXPLAIN 解读子查询的执行计划。

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

### 无关联子查询

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

可以看到这个执行计划的执行顺序是：

1. 先执行内层查询：`(select t2.id from t2 where t2.id>=3)`。

2. 得到内层查询的结果后带入外层，再执行外层查询。

### 关联子查询

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

MatrixOne 在处理该 SQL 语句是会将其改写为等价的 `JOIN` 查询：`select t1.* from t1 join t2 on t1.id=t2.id where t2.id>=4;`，可以看到这个执行计划的执行顺序是：

1. 先执行过滤查询 `where t2.id>=4;`。

2. 再扫描表 `Table Scan on db1.t2`，将结果“流入”父节点后，

3. 扫描表 `Table Scan on db1.t1`。

4. 最后执行 `JOIN` 查询。
