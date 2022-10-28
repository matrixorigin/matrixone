# 子查询

本篇文档向你介绍 MatrixOne 的子查询功能。

## 概述

子查询是嵌套在另一个查询中的 SQL 表达式，借助子查询，可以在一个查询当中使用另外一个查询的查询结果。

通常情况下，从 SQL 语句结构上，子查询语句一般有以下几种形式：

- 标量子查询（Scalar Subquery），如 `SELECT (SELECT s1 FROM t2) FROM t1`。
- 派生表（Derived Tables），如 `SELECT t1.s1 FROM (SELECT s1 FROM t2) t1`。
- 存在性测试（Existential Test），如 `WHERE NOT EXISTS(SELECT ... FROM t2)`，`WHERE t1.a IN (SELECT ... FROM t2)`。
- 集合比较（Quantified Comparison），如 `WHERE t1.a = ANY(SELECT ... FROM t2)`。
- 作为比较运算符操作数的子查询, 如 `WHERE t1.a > (SELECT ... FROM t2)`。

关于子查询 SQL 语句，参见 [SUBQUERY](../../Reference/SQL-Reference/Data-Manipulation-Statements/subquery.md)。

另外，从 SQL 语句执行情况上，子查询语句一般有以下两种形式：

- 关联子查询（Correlated Subquery）：数据库嵌套查询中内层查询和外层查询不相互独立，内层查询也依赖于外层查询。

   执行顺序为：
   
     + 先从外层查询中查询中一条记录。
     
     + 再将查询到的记录放到内层查询中符合条件的记录，再放到外层中查询。
     
     + 重复以上步骤

    例如：``select * from tableA where tableA.cloumn < (select column from tableB where tableA.id = tableB.id))``

- 无关联子查询 (Self-contained Subquery) ：数据库嵌套查询中内层查询是完全独立于外层查询的。

   执行顺序为：
    
    + 先执行内层查询。
    
    + 得到内层查询的结果后带入外层，再执行外层查询。

    例如：``select * from tableA where tableA.column  = (select tableB.column from tableB )``
    
**子查询的作用**：

- 子查询允许结构化的查询，这样就可以把一个查询语句的每个部分隔开。
- 子查询提供了另一种方法来执行有些需要复杂的 `JOIN` 和 `UNION` 来实现的操作。

我们将举一个简单的例子帮助你理解 **关联子查询** 和 **无关联子查询**。

## 示例

### 开始前准备

你需要确认在开始之前，已经完成了以下任务：

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

### 数据准备

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

#### 无关联子查询

对于将子查询作为比较运算符 (`>`/ `>=`/ `<` / `<=` / `=` / `!=`) 操作数的这类无关联子查询而言，内层子查询只需要进行一次查询，MatrixOne 在生成执行计划阶段会将内层子查询改写为常量。

```sql
select * from t1 where t1.id in (select t2.id from t2 where t2.id>=3);
```

在 MatrixOne 执行上述查询的时候会先执行一次内层子查询：

```sql
select t2.id from t2 where t2.id>=3;
```

运行结果为：

```
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

对于存在性测试和集合比较两种情况下的无关联列子查询，MatrixOne 会将其进行改写和等价替换以获得更好的执行性能。

#### 关联子查询

对于关联子查询而言，由于内层的子查询引用外层查询的列，子查询需要对外层查询得到的每一行都执行一遍，也就是说假设外层查询得到一千万的结果，那么子查询也会被执行一千万次，这会导致查询需要消耗更多的时间和资源。

因此在处理过程中，MatrixOne 会尝试对关联子查询去关联，以从执行计划层面上提高查询效率。

```sql
SELECT *
FROM t1
WHERE id in (
       SELECT id
       FROM t2
       WHERE t1.ti = t2.ti and t2.id>=4
       );
```

MatrixOne 在处理该 SQL 语句是会将其改写为等价的 `JOIN` 查询：

```sql
select t1.* from t1 join t2 on t1.id=t2.id where t2.id>=4;
```

作为最佳实践，在实际开发当中，为提高计算效率，尽量选择等价计算方法进行查询，避免使用关联子查询的方式进行查询。
