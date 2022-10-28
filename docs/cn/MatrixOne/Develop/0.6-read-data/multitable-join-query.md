# 多表连接查询

一些使用数据库的场景中，需要一个查询当中使用到多张表的数据，你可以通过 `JOIN` 语句将两张或多张表的数据组合在一起。

## 开始前准备

你需要确认在开始之前，已经完成了以下任务：

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

### 数据准备

新建三张表，方便后续为使用 `JOIN` 语句做准备：

```sql
> drop table if exists t1;
> drop table if exists t2;
> drop table if exists t3;
> create table t1 (libname1 varchar(21) not null primary key, city varchar(20));
> create table t2 (isbn2 varchar(21) not null primary key, author varchar(20), title varchar(60));
> create table t3 (isbn3 varchar(21) not null, libname3 varchar(21) not null, quantity int);
> insert into t2 values ('001','Daffy','Aducklife');
> insert into t2 values ('002','Bugs','Arabbitlife');
> insert into t2 values ('003','Cowboy','Lifeontherange');
> insert into t2 values ('000','Anonymous','Wannabuythisbook?');
> insert into t2 values ('004','BestSeller','OneHeckuvabook');
> insert into t2 values ('005','EveryoneBuys','Thisverybook');
> insert into t2 values ('006','SanFran','Itisasanfranlifestyle');
> insert into t2 values ('007','BerkAuthor','Cool.Berkley.the.book');
> insert into t3 values ('000','NewYorkPublicLibra',1);
> insert into t3 values ('001','NewYorkPublicLibra',2);
> insert into t3 values ('002','NewYorkPublicLibra',3);
> insert into t3 values ('003','NewYorkPublicLibra',4);
> insert into t3 values ('004','NewYorkPublicLibra',5);
> insert into t3 values ('005','NewYorkPublicLibra',6);
> insert into t3 values ('006','SanFransiscoPublic',5);
> insert into t3 values ('007','BerkeleyPublic1',3);
> insert into t3 values ('007','BerkeleyPublic2',3);
> insert into t3 values ('001','NYC Lib',8);
> insert into t1 values ('NewYorkPublicLibra','NewYork');
> insert into t1 values ('SanFransiscoPublic','SanFran');
> insert into t1 values ('BerkeleyPublic1','Berkeley');
> insert into t1 values ('BerkeleyPublic2','Berkeley');
> insert into t1 values ('NYCLib','NewYork');

> select * from t1;
+--------------------+----------+
| libname1           | city     |
+--------------------+----------+
| NewYorkPublicLibra | NewYork  |
| SanFransiscoPublic | SanFran  |
| BerkeleyPublic1    | Berkeley |
| BerkeleyPublic2    | Berkeley |
| NYCLib             | NewYork  |
+--------------------+----------+
5 rows in set (0.01 sec)

> select * from t2;
+-------+--------------+-----------------------+
| isbn2 | author       | title                 |
+-------+--------------+-----------------------+
| 000   | Anonymous    | Wannabuythisbook?     |
| 001   | Daffy        | Aducklife             |
| 002   | Bugs         | Arabbitlife           |
| 003   | Cowboy       | Lifeontherange        |
| 004   | BestSeller   | OneHeckuvabook        |
| 005   | EveryoneBuys | Thisverybook          |
| 006   | SanFran      | Itisasanfranlifestyle |
| 007   | BerkAuthor   | Cool.Berkley.the.book |
+-------+--------------+-----------------------+
8 rows in set (0.00 sec)

> select * from t3;
+-------+--------------------+----------+
| isbn3 | libname3           | quantity |
+-------+--------------------+----------+
| 000   | NewYorkPublicLibra |        1 |
| 001   | NewYorkPublicLibra |        2 |
| 002   | NewYorkPublicLibra |        3 |
| 003   | NewYorkPublicLibra |        4 |
| 004   | NewYorkPublicLibra |        5 |
| 005   | NewYorkPublicLibra |        6 |
| 006   | SanFransiscoPublic |        5 |
| 007   | BerkeleyPublic1    |        3 |
| 007   | BerkeleyPublic2    |        3 |
| 001   | NYC Lib            |        8 |
+-------+--------------------+----------+
10 rows in set (0.01 sec)
```

## Join 类型

### ``LEFT JOIN``

左外连接会返回左表中的所有数据行，以及右表当中能够匹配连接条件的值，如果在右表当中没有找到能够匹配的行，则使用 NULL 填充。

|语法| 图示 |
|---|---|
|SELECT /<select_list> FROM TableA A LEFT JOIN TableB B ON A.Key=B.Key|![leftjoin](https://github.com/matrixorigin/artwork/blob/main/docs/reference/left_join.png?raw=true)|
|SELECT /<select_list> FROM TableA A LEFT JOIN TableB B ON A.Key=B.Key WHERE B.Key IS NULL|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/left_join_where.png?raw=true)|

**示例**：

根据前文所述准备的数据，这里给出示例，解释左连接语句。

查询 `city` 列、`libname1` 列，以及查询 `libname1` 的检索行，（即，以 `a` 命名的列进行查询），查询步骤是：

1. 将表 t3 左连接表 t1，约束条件是表 t1 的列与表 t3 的列一致，即 libname1=libname3 ；
2. 步骤1中左连接的结果与表 t2 内连接，约束条件是表 t3 的列与表 t2 的列一致，即 isbn3=isbn2；
3. 按照表 t1 列 `city`，`libname1` 进行聚合分组。

```sql
> select city,libname1,count(libname1) as a from t3 left join t1 on libname1=libname3 join t2 on isbn3=isbn2 group by city,libname1;
+----------+--------------------+------+
| city     | libname1           | a    |
+----------+--------------------+------+
| NewYork  | NewYorkPublicLibra |    6 |
| SanFran  | SanFransiscoPublic |    1 |
| Berkeley | BerkeleyPublic1    |    1 |
| Berkeley | BerkeleyPublic2    |    1 |
| NULL     | NULL               |    0 |
+----------+--------------------+------+
5 rows in set (0.01 sec)
```

有关 SQL 执行详情可以参考如下代码，后面的 **右连接** 和 **内连接** 示例对执行详情将不做赘述：

```sql
> explain select city,libname1,count(libname1) as a from t3 left join t1 on libname1=libname3 join t2 on isbn3=isbn2 group by city,libname1;
+------------------------------------------------------------+
| QUERY PLAN                                                 |
+------------------------------------------------------------+
| Project                                                    |
|   ->  Aggregate                                            |
|         Group Key: t1.city, t1.libname1                    |
|         Aggregate Functions: count(t1.libname1)            |
|         ->  Join                                           |
|               Join Type: INNER                             |
|               Join Cond: (t3.isbn3 = t2.isbn2)             |
|               ->  Join                                     |
|                     Join Type: LEFT                        |
|                     Join Cond: (t1.libname1 = t3.libname3) |
|                     ->  Table Scan on db1.t3               |
|                     ->  Table Scan on db1.t1               |
|               ->  Table Scan on db1.t2                     |
+------------------------------------------------------------+
13 rows in set (0.00 sec)
```

### ``RIGHT JOIN``

右外连接返回右表中的所有记录，以及左表当中能够匹配连接条件的值，没有匹配的值则使用 NULL 填充。

|语法| 图示 |
|---|---|
|SELECT /<select_list> FROM TableA A RIGHT JOIN TableB B ON A.Key=B.Key|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/right_join.png?raw=true)|
|SELECT /<select_list> FROM TableA A RIGHT JOIN TableB B ON A.Key=B.Key WHERE A.Key IS NULL|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/right_join_where.png?raw=true)|

**示例**：

根据前文所述准备的数据，这里给出示例，解释右连接语句。

查询 `city` 列、`libname1` 列，以及查询 `libname1` 的检索行，（即，以 `a` 命名的列进行查询），查询规则是：

1. 将表 t3 右连接表 t1，约束条件是表 t1 的列与表 t3 的列一致，即 libname1=libname3 ；
2. 步骤1中右连接的结果与表 t2 内连接，约束条件是表 t3 的列与表 t2 的列一致，即 isbn3=isbn2；
3. 按照表 t1 列 `city`，`libname1` 进行聚合分组。

```sql
> select city,libname1,count(libname1) as a from t3 right join t1 on libname1=libname3 join t2 on isbn3=isbn2 group by city,libname1;
+----------+--------------------+------+
| city     | libname1           | a    |
+----------+--------------------+------+
| Berkeley | BerkeleyPublic1    |    1 |
| Berkeley | BerkeleyPublic2    |    1 |
| NewYork  | NewYorkPublicLibra |    6 |
| SanFran  | SanFransiscoPublic |    1 |
+----------+--------------------+------+
4 rows in set (0.03 sec)
```

### ``INNER JOIN``

内连接的连接结果只返回匹配连接条件的行。

|语法| 图示 |
|---|---|
|SELECT /<select_list> FROM TableA A INNER JOIN TableB B ON A.Key=B.Key|![innerjoin](https://github.com/matrixorigin/artwork/blob/main/docs/reference/inner_join.png?raw=true)|

**示例**：

根据前文所述准备的数据，这里给出示例，解释内连接语句。

查询 `city` 列、`libname1` 列，以及查询 `libname1` 的检索行，（即，以 `a` 命名的列进行查询），查询规则是：

1. 将表 t3 内连接表 t1，约束条件是表 t1 的列与表 t3 的列一致，即 libname1=libname3 ；
2. 步骤1中内连接的结果表 t2 内连接，约束条件是表 t3 的列与表 t2 的列一致，即 isbn3=isbn2；
3. 按照表 t1 列 `city`，`libname1` 进行聚合分组。

```sql
> select city,libname1,count(libname1) as a from t3 join t1 on libname1=libname3 join t2 on isbn3=isbn2 group by city,libname1;
+----------+--------------------+------+
| city     | libname1           | a    |
+----------+--------------------+------+
| NewYork  | NewYorkPublicLibra |    6 |
| SanFran  | SanFransiscoPublic |    1 |
| Berkeley | BerkeleyPublic1    |    1 |
| Berkeley | BerkeleyPublic2    |    1 |
+----------+--------------------+------+
4 rows in set (0.01 sec)
```

## 隐式连接

在 SQL 语句当中，除了使用 `JOIN`，也可以通过 `FROM t1, t2` 子句来连接两张或多张表，通过 `WHERE t1.id = t2.id` 子句来指定连接的条件。
