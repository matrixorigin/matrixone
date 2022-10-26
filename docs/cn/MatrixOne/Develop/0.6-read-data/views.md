# 视图

本篇文档向你介绍 MatrixOne 的视图功能。

## 概述

视图作为一个虚拟表，进行存储查询，在调用时产生结果集。

**视图的作用**：

- 简化用户操作：视图机制使用户可以将注意力集中在所关心地数据上。如果这些数据不是直接来自基本表，则可以通过定义视图，使数据库看起来结构简单、清晰，并且可以简化用户的的数据查询操作。

- 以多种角度看待同一数据：视图机制能使不同的用户以不同的方式看待同一数据，当许多不同种类的用户共享同一个数据库时，这种灵活性是非常必要的。

- 对重构数据库提供了一定程度的逻辑独立性：数据的物理独立性是指用户的应用程序不依赖于数据库的物理结构。数据的逻辑独立性是指当数据库重构造时，如增加新的关系或对原有的关系增加新的字段，用户的应用程序不会受影响。层次数据库和网状数据库一般能较好地支持数据的物理独立性，而对于逻辑独立性则不能完全的支持。

## 开始前准备

你需要确认在开始之前，已经完成了以下任务：

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

### 数据准备

新建两张表，方便后续为使用视图做准备：

```sql
> CREATE TABLE t00(a INTEGER);
> INSERT INTO t00 VALUES (1),(2);
> CREATE TABLE t01(a INTEGER);
> INSERT INTO t01 VALUES (1);
```

可以查看一下表 *t00* 的结构：

```sql
> select * from t00;
+------+
| a    |
+------+
|    1 |
|    2 |
+------+
```

可以查看一下表 *t01* 的结构：

```sql
> select * from t01;
+------+
| a    |
+------+
|    1 |
+------+
```

## 创建视图

你可以通过 `CREATE VIEW` 语句来将某个较为复杂的查询定义为视图，其语法如下：

```sql
CREATE VIEW view_name AS query;
```

创建的视图名称不能与已有的视图或表重名。

示例如下：

```sql
> CREATE VIEW v0 AS SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a);
Query OK, 0 rows affected (0.02 sec)
```

## 查询视图

视图创建完成后，便可以使用 `SELECT` 语句像查询一般数据表一样查询视图。

```sql
> SELECT * FROM v0;
+------+------+
| a    | b    |
+------+------+
|    1 |    1 |
|    2 | NULL |
+------+------+
```

## 获取视图相关信息

使用 `SHOW CREATE TABLE|VIEW view_name` 语句：

```sql
> SHOW CREATE VIEW v0;
+------+----------------------------------------------------------------------------+
| View | Create View                                                                |
+------+----------------------------------------------------------------------------+
| v0   | CREATE VIEW v0 AS SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a) |
+------+----------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

## 删除视图

通过 `DROP VIEW view_name;` 语句可以删除已经创建的视图。

```sql
DROP VIEW v0;
```
