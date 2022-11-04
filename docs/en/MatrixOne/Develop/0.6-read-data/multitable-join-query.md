# Multi-table Join Queries

In many scenarios, you need to use one query to get data from multiple tables. You can use the `JOIN` statement to combine the data from two or more tables.

## Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).
- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/).

### Preparation

Create two tables to prepare for using the `JOIN` statement:

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

## Join Types

### ``LEFT JOIN``

The `LEFT JOIN` returns all the rows in the left table and the values ​​in the right table that match the join condition. If no rows are matched in the right table, it will be filled with NULL.

|Statement| Image |
|---|---|
|SELECT /<select_list> FROM TableA A LEFT JOIN TableB B ON A.Key=B.Key|![leftjoin](https://github.com/matrixorigin/artwork/blob/main/docs/reference/left_join.png?raw=true)|
|SELECT /<select_list> FROM TableA A LEFT JOIN TableB B ON A.Key=B.Key WHERE B.Key IS NULL|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/left_join_where.png?raw=true)|

**Example**：

Based on the data prepared above, here is an example to explain the left join statement.

Query the `city` column, `libname1` column, and the  aggregate functions `count(t1.libname1)`.

The query procedure is as below:

1. Left join from table t3 to table t1. The constraint is that the columns in table t1 are the same as those in table t3, that is, `libname1=libname3;`

2. Inner join from the result of *Step 1* to table t2. The constraint condition is that the columns in table t3 are the same as those in table t2, that is, `isbn3=isbn2;`.

3. Aggregate groups according to columns`city` and `libname1` in table t1.

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

For details about SQL execution, refer to the following code. The following examples of **RIGHT JOIN** and **INNER JOIN** will not describe the execution details:

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

A `RIGHT JOIN` returns all the records in the right table and the values ​​in the left table that match the join condition. If there is no matching value, it is filled with NULL.

|Statement| Image |
|---|---|
|SELECT /<select_list> FROM TableA A RIGHT JOIN TableB B ON A.Key=B.Key|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/right_join.png?raw=true)|
|SELECT /<select_list> FROM TableA A RIGHT JOIN TableB B ON A.Key=B.Key WHERE A.Key IS NULL|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/right_join_where.png?raw=true)|

**Example**：

Based on the data prepared above, here is an example to explain the right join statement.

Query the `city` column, `libname1` column, and the  aggregate functions `count(t1.libname1)`.

The query procedure is as below:

1. Right join from table t3 to table t1. The constraint is that the columns in table t1 are the same as those in table t3, that is, `libname1=libname3;`

2. Inner join from the result of *Step 1* to table t2. The constraint condition is that the columns in table t3 are the same as those in table t2, that is, `isbn3=isbn2;`.

3. Aggregate groups according to columns`city` and `libname1` in table t1.

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

The join result of an inner join returns only rows that match the join condition.

|Statement| Image |
|---|---|
|SELECT /<select_list> FROM TableA A INNER JOIN TableB B ON A.Key=B.Key|![innerjoin](https://github.com/matrixorigin/artwork/blob/main/docs/reference/inner_join.png?raw=true)|

**Example**：

Based on the data prepared above, here is an example to explain the inner join statement.

Query the `city` column, `libname1` column, and the  aggregate functions `count(t1.libname1)`.

The query procedure is as below:

1. Inner join from table t3 to table t1. The constraint is that the columns in table t1 are the same as those in table t3, that is, `libname1=libname3;`

2. Inner join from the result of *Step 1* to table t2. The constraint condition is that the columns in table t3 are the same as those in table t2, that is, `isbn3=isbn2;`.

3. Aggregate groups according to columns`city` and `libname1` in table t1.

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

## Implicit join

Before the `JOIN` statement that explicitly declared a join was added to the SQL standard, it was possible to join two or more tables in a SQL statement using the `FROM t1, t2` clause, and specify the conditions for the join using the `WHERE t1.id = t2.id` clause. You can understand it as an implicit join, which uses the inner join to join tables.
