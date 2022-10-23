# **CREATE TABLE**

## **语法说明**

`CREATE TABLE` 语句用于在当前所选数据库创建一张新表。

## **语法结构**

```
> CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]table_name [comment = "comment of table"];
(
    name1 type1 [comment 'comment of column'] [AUTO_INCREMENT] [[PRIMARY] KEY],
    name2 type2 [comment 'comment of column'],
    ...
)
    [partition_options]
```

### 语法说明

#### TEMPORARY

在创建表时，可以使用 `TEMPORARY` 创建一个临时表。`TEMPORARY` 表只在当前会话中可见，在会话关闭时自动删除。有关更多信息，请参见[CREATE TEMPORARY TABLE](create-temporary-tables.md)。

#### COMMENT

可以使用 `comment` 选项指定列或整张表的注释：

- `CREATE TABLE [IF NOT EXISTS] [db.]table_name [comment = "comment of table"];` 中的 `comment` 为整张表的注释，最长2049个字符。
- `(name1 type1 [comment 'comment of column'],...)` 中的 `comment` 为指定列的注释：最长1024个字符。

使用 `SHOW CREATE TABLE` 和 `SHOW FULL COLUMNS` 语句显示注释内容。注释内容也显示在 `INFORMATION_SCHEMA.COLUMN_COMMENT` 列中。

#### AUTO_INCREMENT

`AUTO_INCREMENT`：表的初始值，初始值从 1 开始，且数据列的值必须唯一。

- 设置 `AUTO_INCREMENT` 的列，需为整数或者浮点数据类型。
- 自增列需要设置为 `NOT NULL`，否则会直接存储 `NULL`。当你将 NULL（推荐）或 0 值插入索引的 `AUTO_INCREMENT` 列时，该列将设置为下一个序列值。通常这是 *value+1*，其中 *value* 是表中当前列的最大值。

- 每个表只能有一个 `AUTO_INCREMENT` 列，它必须可以被索引，且不能设置默认值。 `AUTO_INCREMENT` 列需要含有正数值，如果插入一个负数被判断为插入一个非常大的正数，这样做是为了避免数字出现精度问题，并确保不会意外出现包含 0 的 `AUTO_INCREMENT` 列。

#### Table PARTITION 和 PARTITIONS

```
partition_options:
 PARTITION BY
     { [LINEAR] HASH(expr)
     | [LINEAR] KEY [ALGORITHM={1 | 2}] (column_list)
     | RANGE{(expr) | COLUMNS(column_list)}
     | LIST{(expr) | COLUMNS(column_list)} }
 [PARTITIONS num]
 [SUBPARTITION BY
     { [LINEAR] HASH(expr)
     | [LINEAR] KEY [ALGORITHM={1 | 2}] (column_list) }
 ]
 [(partition_definition [, partition_definition] ...)]

partition_definition:
 PARTITION partition_name
     [VALUES
         {LESS THAN {(expr | value_list) | MAXVALUE}
         |
         IN (value_list)}]
     [[STORAGE] ENGINE [=] engine_name]
     [COMMENT [=] 'string' ]
     [DATA DIRECTORY [=] 'data_dir']
     [INDEX DIRECTORY [=] 'index_dir']
     [MAX_ROWS [=] max_number_of_rows]
     [MIN_ROWS [=] min_number_of_rows]
     [TABLESPACE [=] tablespace_name]
```

分区可以被修改、合并、添加到表中，也可以从表中删除。

· 和单个磁盘或文件系统分区相比，可以存储更多的数据。

· 优化查询。

   + where 子句中包含分区条件时，可以只扫描必要的分区。
   + 涉及聚合函数的查询时，可以容易的在每个分区上并行处理，最终只需汇总得到结果。

· 对于已经过期或者不需要保存的数据，可以通过删除与这些数据有关的分区来快速删除数据。

· 跨多个磁盘来分散数据查询，以获得更大的查询吞吐量。

- **PARTITION BY**

分区语法以 `PARTITION BY` 开头。该子句包含用于确定分区的函数，这个函数返回一个从 1 到 num 的整数值，其中 num 是分区的数目。

- **HASH(expr)**

在实际工作中经常遇到像会员表的没有明显可以分区的特征字段的大表。为了把这类的数据进行分区打散，MatrixOne 提供了 `HASH` 分区。基于给定的分区个数，将数据分配到不同的分区，`HASH` 分区只能针对整数进行 `HASH`，对于非整形的字段则通过表达式将其转换成整数。

· HASH 分区，基于给定的分区个数，把数据分配到不同的分区。

· Expr 是使用一个或多个表列的表达式。

示例如下：

```
CREATE TABLE t1 (col1 INT, col2 CHAR(5))
    PARTITION BY HASH(col1);

CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATETIME)
    PARTITION BY HASH ( YEAR(col3) );
```

- **KEY(column_list)**

·  KEY 分区，按照某个字段取余。分区对象必须为列，不能是基于列的表达式，且允许多列，KEY 分区列可以不指定，默认为逐渐或者唯一键，不指定的情况下，则必须显性指定列。

类似于 `HASH`。 `column_list` 参数只是一个包含 1 个或多个表列的列表（最大值：16）。下面的示例为一个按 `KEY` 分区的简单表，有 4 个分区：

```
CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE)
    PARTITION BY KEY(col3)
    PARTITIONS 4;
```

对于按 `KEY` 分区的表，可以使用 `LINEAR KEY` 来进行线性分区。这与使用 `HASH` 分区的表具有相同的效果。下面的示例为使用 `LINEAR KEY` 线性分区在5个分区之间分配数据：

```
CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE)
    PARTITION BY LINEAR KEY(col3)
    PARTITIONS 5;
```

- **RANGE(expr)**

`RANGE` 分区：基于一个给定连续区间范围，把数据（或者可以说是多行）分配到不同的分区。最常见的是基于时间字段. 基于分区的列最好是整型，如果日期型的可以使用函数转换为整型。

在这种情况下， expr 使用一组 `VALUES LESS THAN` 运算符显示一系列值。使用范围分区时，你必须使用 `VALUES LESS THAN` 定义至少一个分区，且不能将 `VALUES IN` 与范围分区一起使用。

`VALUES LESS THAN MAXVALUE` 用于指定小于指定最大值的“剩余”值。

子句排列方式为：每个连续的 `VALUES LESS THAN` 中指定的上限大于前一个的上限，引用  `MAXVALUE` 的那个在列表中排在最后。

- **RANGE COLUMNS(column_list)**

`RANGE COLUMNS(column_list)` 为 `RANGE` 的另一种形式，常用作为使用多个列上的范围条件（即，诸如 `WHERE a = 1 AND b < 10 或 WHERE a = 1 AND b = 10 AND c < 10）`之类的条件对查询进行分区修剪。它使你能够通过使用 `COLUMNS` 子句中的列列表和每个 `PARTITION ... VALUES LESS THAN (value_list)` 分区定义子句中的一组列值来指定多个列中的值范围。（在最简单的情况下，该集合由单个列组成。）`column_list` 和 `value_list` 中可以引用的最大列数为 16。

`column_list` 使用：

1. `column_list` 可以只包含列名。
2. 列表中的每一列必须是整数类型、字符串类型、时间或日期列类型。
3. 不允许使用 `BLOB`、`TEXT`、`SET`、`ENUM`、`BIT` 或空间数据类型的列；也不允许使用浮点数类型的列。也不可以在 `COLUMNS` 子句中使用函数或算术表达式。

分区定义说明：

1. 分区定义中，用于每个 `VALUES LESS THAN` 子句的值列表必须包含与 `COLUMNS` 子句中列出的列相同数量的值。
2. `NULL` 不能出现在 `VALUES LESS THAN` 中的任何值。可以对除第一列以外的给定列多次使用 `MAXVALUE`，如下例所示：

```
CREATE TABLE rc (
    a INT NOT NULL,
    b INT NOT NULL
)
PARTITION BY RANGE COLUMNS(a,b) (
    PARTITION p0 VALUES LESS THAN (10,5),
    PARTITION p1 VALUES LESS THAN (20,10),
    PARTITION p2 VALUES LESS THAN (50,MAXVALUE),
    PARTITION p3 VALUES LESS THAN (65,MAXVALUE),
    PARTITION p4 VALUES LESS THAN (MAXVALUE,MAXVALUE)
);
```

- **LIST(expr)**

`LIST` 分区和 `RANGE` 分区类似，区别在于 `LIST` 是枚举值列表的集合，`RANGE` 是连续的区间值的集合。二者在语法方面非常的相似。

分区使用：

1. `LIST` 分区列是非 null 列，否则插入 null 值如果枚举列表里面不存在 null 值会插入失败，这点和其它的分区不一样，`RANGE` 分区会将其作为最小分区值存储，`HASH` 或 `KEY` 分为会将其转换成 0 存储，因为 `LIST` 分区只支持整型，非整型字段需要通过函数转换成整形。
2. 使用 `LIST` 分区时，你必须使用 `VALUES IN` 定义至少一个分区，且不能将 `VALUES LESS THAN` 与 `PARTITION BY LIST`一起使用。

示例如下：

```
CREATE TABLE client_firms (
    id   INT,
    name VARCHAR(35)
)
PARTITION BY LIST (id) (
    PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
    PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
    PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
    PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
);
```

- **LIST COLUMNS(column_list)**

`LIST COLUMNS(column_list)` 是 `LIST` 的另一种书写形式，用于多列上的比较条件（即，具有诸如 `WHERE a = 5 AND b = 5 或 WHERE a = 1 AND b = 10 AND c = 5`之类的条件）对查询进行分区修剪。通过使用 `COLUMNS` 子句中的列列表和每个 `PARTITION ... VALUES IN` (value_list) 分区定义子句中的一组列值来指定多个列中的值。

`LIST COLUMNS(column_list)` 中使用的列列表和 `VALUES IN(value_list)` 中使用的值列表的数据类型规则与 `RANGE COLUMNS(column_list)` 中使用的列列表的规则 `VALUES LESS THAN(value_list)` 中使用的值列表规则相同，但在 `VALUES IN` 子句中，不允许使用 `MAXVALUE`，可以使用 `NULL`。

与 `PARTITION BY LIST COLUMNS` 一起使用的 `VALUES IN` 值列表与与 `PARTITION BY LIST` 一起使用时的值列表有一个重要区别。当与 `PARTITION BY LIST COLUMNS` 一起使用时，`VALUES IN` 子句中的每个元素都必须是一组列值；每个集合中的值的数量必须与 `COLUMNS` 子句中使用的列数相同，并且这些值的数据类型必须与列的数据类型匹配（并且以相同的顺序出现）。在最简单的情况下，该集合由一列组成。在 `column_list` 和组成 `value_list` 的元素中可以使用的最大列数是 16。

示例如下：

```
CREATE TABLE lc (
    a INT NULL,
    b INT NULL
)
PARTITION BY LIST COLUMNS(a,b) (
    PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
    PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
    PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
    PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
);
```

- **PARTITIONS num**

可以选择使用 `PARTITIONS num` 子句指定分区数，其中 `num` 是分区数。如果使用此子句的同时，也使用了其他 `PARTITION` 子句，那么 `num` 必须等于使用 `PARTITION` 子句声明的分区的总数。

#### 语法图

![Create Table Diagram](https://github.com/matrixorigin/artwork/blob/main/docs/reference/create_table_statement.png?raw=true)

## **示例**

- 示例 1

```sql
> CREATE TABLE test(a int, b varchar(10));

> INSERT INTO test values(123, 'abc');

> SELECT * FROM test;
+------+---------+
|   a  |    b    |
+------+---------+
|  123 |   abc   |
+------+---------+
```

- 示例 2

```sql
> create table t2 (a int, b int) comment = "事实表";
> show create table t2;
+-------+---------------------------------------------------------------------------------------+
| Table | Create Table                                                                          |
+-------+---------------------------------------------------------------------------------------+
| t2    | CREATE TABLE `t2` (
`a` INT DEFAULT NULL,
`b` INT DEFAULT NULL
) COMMENT='事实表',    |
+-------+---------------------------------------------------------------------------------------+
```

- 示例 3

```sql
> create table t3 (a int comment '列的注释', b int) comment = "table";
> show create table t3;
+-------+----------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                             |
+-------+----------------------------------------------------------------------------------------------------------+
| t3    | CREATE TABLE `t3` (
`a` INT DEFAULT NULL COMMENT '列的注释',
`b` INT DEFAULT NULL
) COMMENT='table',     |
+-------+----------------------------------------------------------------------------------------------------------+
```

- 示例 4

```sql
> CREATE TABLE tp1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col3) PARTITIONS 4;
> show create table tp1;
+-------+-------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                          |
+-------+-------------------------------------------------------------------------------------------------------+
| tp1   | CREATE TABLE `tp1` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL,
`col3` DATE DEFAULT NULL
) |
+-------+-------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

> CREATE TABLE tp2 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col3);
> show create table tp2;
+-------+-------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                          |
+-------+-------------------------------------------------------------------------------------------------------+
| tp2   | CREATE TABLE `tp2` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL,
`col3` DATE DEFAULT NULL
) |
+-------+-------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

> CREATE TABLE tp4 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM = 1 (col3);
> show create table tp4;
+-------+-------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                          |
+-------+-------------------------------------------------------------------------------------------------------+
| tp4   | CREATE TABLE `tp4` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL,
`col3` DATE DEFAULT NULL
) |
+-------+-------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp5 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR KEY ALGORITHM = 1 (col3) PARTITIONS 5;
> show create table tp5;
+-------+-------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                          |
+-------+-------------------------------------------------------------------------------------------------------+
| tp5   | CREATE TABLE `tp5` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL,
`col3` DATE DEFAULT NULL
) |
+-------+-------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp6 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col1, col2) PARTITIONS 4;
> show create table tp6;
+-------+-------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                          |
+-------+-------------------------------------------------------------------------------------------------------+
| tp6   | CREATE TABLE `tp6` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL,
`col3` DATE DEFAULT NULL
) |
+-------+-------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp7 (col1 INT NOT NULL PRIMARY KEY, col2 DATE NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL) PARTITION BY KEY(col1) PARTITIONS 4;
> show create table tp7;
+-------+----------------------------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                                                     |
+-------+----------------------------------------------------------------------------------------------------------------------------------+
| tp7   | CREATE TABLE `tp7` (
`col1` INT NOT NULL,
`col2` DATE NOT NULL,
`col3` INT NOT NULL,
`col4` INT NOT NULL,
PRIMARY KEY (`col1`)
) |
+-------+----------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp8 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1);
> show create table tp8;
+-------+-----------------------------------------------------------------------------+
| Table | Create Table                                                                |
+-------+-----------------------------------------------------------------------------+
| tp8   | CREATE TABLE `tp8` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL
) |
+-------+-----------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp9 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1) PARTITIONS 4;
> show create table tp9;
+-------+-----------------------------------------------------------------------------+
| Table | Create Table                                                                |
+-------+-----------------------------------------------------------------------------+
| tp9   | CREATE TABLE `tp9` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL
) |
+-------+-----------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp10 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3));
> show create table tp10;
+-------+------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                               |
+-------+------------------------------------------------------------------------------------------------------------+
| tp10  | CREATE TABLE `tp10` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL,
`col3` DATETIME DEFAULT NULL
) |
+-------+------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

> CREATE TABLE tp11 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR HASH( YEAR(col3)) PARTITIONS 6;
> show create table tp11;
+-------+--------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                           |
+-------+--------------------------------------------------------------------------------------------------------+
| tp11  | CREATE TABLE `tp11` (
`col1` INT DEFAULT NULL,
`col2` CHAR(5) DEFAULT NULL,
`col3` DATE DEFAULT NULL
) |
+-------+--------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp12 (col1 INT NOT NULL PRIMARY KEY, col2 DATE NOT NULL, col3 INT NOT NULL, col4 INT NOT NULL) PARTITION BY HASH(col1) PARTITIONS 4;
> show create table tp12;
+-------+-----------------------------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                                                      |
+-------+-----------------------------------------------------------------------------------------------------------------------------------+
| tp12  | CREATE TABLE `tp12` (
`col1` INT NOT NULL,
`col2` DATE NOT NULL,
`col3` INT NOT NULL,
`col4` INT NOT NULL,
PRIMARY KEY (`col1`)
) |
+-------+-----------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

> CREATE TABLE tp13 (id INT NOT NULL PRIMARY KEY, fname VARCHAR(30), lname VARCHAR(30), hired DATE NOT NULL DEFAULT '1970-01-01', separated DATE NOT NULL DEFAULT '9999-12-31', job_code INT NOT NULL, store_id INT NOT NULL) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), PARTITION p3 VALUES LESS THAN (21));
> show create table tp13;
+-------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                                                                                                                                                          |
+-------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| tp13  | CREATE TABLE `tp13` (
`id` INT NOT NULL,
`fname` VARCHAR(30) DEFAULT NULL,
`lname` VARCHAR(30) DEFAULT NULL,
`hired` DATE NOT NULL,
`separated` DATE NOT NULL,
`job_code` INT NOT NULL,
`store_id` INT NOT NULL,
PRIMARY KEY (`id`)
) |
+-------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp14 (id INT NOT NULL, fname VARCHAR(30), lname VARCHAR(30), hired DATE NOT NULL DEFAULT '1970-01-01', separated DATE NOT NULL DEFAULT '9999-12-31', job_code INT, store_id INT) PARTITION BY RANGE ( YEAR(separated) ) ( PARTITION p0 VALUES LESS THAN (1991), PARTITION p1 VALUES LESS THAN (1996), PARTITION p2 VALUES LESS THAN (2001), PARTITION p3 VALUES LESS THAN MAXVALUE);
> show create table tp14;
+-------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                                                                                                                                              |
+-------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| tp14  | CREATE TABLE `tp14` (
`id` INT NOT NULL,
`fname` VARCHAR(30) DEFAULT NULL,
`lname` VARCHAR(30) DEFAULT NULL,
`hired` DATE NOT NULL,
`separated` DATE NOT NULL,
`job_code` INT DEFAULT NULL,
`store_id` INT DEFAULT NULL
) |
+-------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

> CREATE TABLE tp15 (a INT NOT NULL, b INT NOT NULL) PARTITION BY RANGE COLUMNS(a,b) PARTITIONS 4 (PARTITION p0 VALUES LESS THAN (10,5), PARTITION p1 VALUES LESS THAN (20,10), PARTITION p2 VALUES LESS THAN (50,20), PARTITION p3 VALUES LESS THAN (65,30));
> show create table tp15;
+-------+------------------------------------------------------------+
| Table | Create Table                                               |
+-------+------------------------------------------------------------+
| tp15  | CREATE TABLE `tp15` (
`a` INT NOT NULL,
`b` INT NOT NULL
) |
+-------+------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp16 (id   INT PRIMARY KEY, name VARCHAR(35), age INT unsigned) PARTITION BY LIST (id) (PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21), PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22), PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23), PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));
show create table tp16;
> show create table tp16;
+-------+-------------------------------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                                                        |
+-------+-------------------------------------------------------------------------------------------------------------------------------------+
| tp16  | CREATE TABLE `tp16` (
`id` INT DEFAULT NULL,
`name` VARCHAR(35) DEFAULT NULL,
`age` INT UNSIGNED DEFAULT NULL,
PRIMARY KEY (`id`)
) |
+-------+-------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp17 (id   INT, name VARCHAR(35), age INT unsigned) PARTITION BY LIST (id) (PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21), PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22), PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23), PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));
> show create table tp17;
+-------+-----------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                                    |
+-------+-----------------------------------------------------------------------------------------------------------------+
| tp17  | CREATE TABLE `tp17` (
`id` INT DEFAULT NULL,
`name` VARCHAR(35) DEFAULT NULL,
`age` INT UNSIGNED DEFAULT NULL
) |
+-------+-----------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

> CREATE TABLE tp18 (a INT NULL,b INT NULL) PARTITION BY LIST COLUMNS(a,b) (PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ), PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ), PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ), PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) ));
> show create table tp18;
+-------+--------------------------------------------------------------------+
| Table | Create Table                                                       |
+-------+--------------------------------------------------------------------+
| tp18  | CREATE TABLE `tp18` (
`a` INT DEFAULT NULL,
`b` INT DEFAULT NULL
) |
+-------+--------------------------------------------------------------------+
1 row in set (0.00 sec)
```

- Example 5

```sql
> drop table if exists t1;
> create table t1(a bigint primary key auto_increment,
    b varchar(10));
> insert into t1(b) values ('bbb');
> insert into t1 values (3, 'ccc');
> insert into t1(b) values ('bbb1111');
> select * from t1 order by a;
+------+---------+
| a    | b       |
+------+---------+
|    1 | bbb     |
|    3 | ccc     |
|    4 | bbb1111 |
+------+---------+
3 rows in set (0.01 sec)

> insert into t1 values (2, 'aaaa1111');
> select * from t1 order by a;
+------+----------+
| a    | b        |
+------+----------+
|    1 | bbb      |
|    2 | aaaa1111 |
|    3 | ccc      |
|    4 | bbb1111  |
+------+----------+
4 rows in set (0.00 sec)

> insert into t1(b) values ('aaaa1111');
> select * from t1 order by a;
+------+----------+
| a    | b        |
+------+----------+
|    1 | bbb      |
|    2 | aaaa1111 |
|    3 | ccc      |
|    4 | bbb1111  |
|    5 | aaaa1111 |
+------+----------+
5 rows in set (0.01 sec)

> insert into t1 values (100, 'xxxx');
> insert into t1(b) values ('xxxx');
> select * from t1 order by a;
+------+----------+
| a    | b        |
+------+----------+
|    1 | bbb      |
|    2 | aaaa1111 |
|    3 | ccc      |
|    4 | bbb1111  |
|    5 | aaaa1111 |
|  100 | xxxx     |
|  101 | xxxx     |
+------+----------+
7 rows in set (0.00 sec)
```
