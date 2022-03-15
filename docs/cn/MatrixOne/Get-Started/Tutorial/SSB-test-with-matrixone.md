# **完成SSB测试**

SSB星型模式基准测试是OLAP数据库性能测试的常用场景，通过本篇教程，您可以了解到如何在MatrixOne中实现SSB测试。

## **准备工作**

确保你已经安装了[单机版MatrixOne](../install-standalone-matrixone.zh.md)并[连接到MatrixOne服务器](../connect-to-matrixone-server.zh.md).
  

## **1. 编译dbgen**

```
$ git clone git@github.com:vadimtk/ssb-dbgen.git
$ cd ssb-dbgen
$ make
```

## **2. 生成数据**

当使用`-s 1`时`dbgen`命令会生产近600万行数据(670MB)，当使用`-s 10`时会生产近6000万行数据，会耗费大量时间。

```
$ ./dbgen -s 1 -T c
$ ./dbgen -s 1 -T l`
$ ./dbgen -s 1 -T p
$ ./dbgen -s 1 -T s
$ ./dbgen -s 1 -T d
```


## **3. 在MatrixOne中建表**

```
create database if not exists ssb;
use ssb
drop table if exists lineorder;
drop table if exists part;
drop table if exists supplier;
drop table if exists customer;
drop table if exists dim_date;
drop table if exists lineorder_flat;

create table lineorder (
        lo_orderkey bigint,
        lo_linenumber int,
        lo_custkey int,
        lo_partkey int,
        lo_suppkey int,
        lo_orderdate char(10),
        lo_orderpriority char (15),
        lo_shippriority tinyint,
        lo_quantity double,
        lo_extendedprice double,
        lo_ordtotalprice double,
        lo_discount double,
        lo_revenue double,
        lo_supplycost double,
        lo_tax double,
        lo_commitdate char(10),
        lo_shipmode char (10)
) ;

create table part (
        p_partkey int,
        p_name varchar (22),
        p_mfgr char (6),
        p_category char (7),
        p_brand char (9),
        p_color varchar (11),
        p_type varchar (25),
        p_size int,
        p_container char (10)
) ;

create table supplier (
        s_suppkey int,
        s_name char (25),
        s_address varchar (25),
        s_city char (10),
        s_nation char (15),
        s_region char (12),
        s_phone char (15)
) ;

create table customer (
        c_custkey int,
        c_name varchar (25),
        c_address varchar (25),
        c_city char (10),
        c_nation char (15),
        c_region char (12),
        c_phone char (15),
        c_mktsegment char (10)
) ;

create table dim_date (
        d_datekey int,
        d_date char (18),
        d_dayofweek char (9),
        d_month char (9),
        d_year int,
        d_yearmonthnum int,
        d_yearmonth char (7),
        d_daynuminweek int,
        d_daynuminmonth int,
        d_daynuminyear int,
        d_monthnuminyear int,
        d_weeknuminyear int,
        d_sellingseason varchar (12),
        d_lastdayinweekfl varchar (1),
        d_lastdayinmonthfl varchar (1),
        d_holidayfl varchar (1),
        d_weekdayfl varchar (1)
) ;

CREATE TABLE lineorder_flat (
  LO_ORDERKEY bigint,
  LO_LINENUMBER int,
  LO_CUSTKEY int,
  LO_PARTKEY int,
  LO_SUPPKEY int,
  LO_ORDERDATE int,
  LO_ORDERPRIORITY char(15),
  LO_SHIPPRIORITY tinyint,
  LO_QUANTITY double,
  LO_EXTENDEDPRICE double,
  LO_ORDTOTALPRICE double,
  LO_DISCOUNT double,
  LO_REVENUE double,
  LO_SUPPLYCOST double,
  LO_TAX double,
  LO_COMMITDATE int,
  LO_SHIPMODE char(10),
  C_NAME varchar(25),
  C_ADDRESS varchar(25),
  C_CITY char(10),
  C_NATION char(15),
  C_REGION char(12),
  C_PHONE char(15),
  C_MKTSEGMENT char(10),
  S_NAME char(25),
  S_ADDRESS varchar(25),
  S_CITY char(10),
  S_NATION char(15),
  S_REGION char(12),
  S_PHONE char(15),
  P_NAME varchar(22),
  P_MFGR char(6),
  P_CATEGORY char(7),
  P_BRAND char(9),
  P_COLOR varchar(11),
  P_TYPE varchar(25),
  P_SIZE int,
  P_CONTAINER char(10)
);
```

## **4. 导入数据**
如果你使用`dbgen`命令带生成数据，那么还需要将每一行末尾的`,`替换掉。在 ssb-dbgen目录下，使用如下命令：
```
$ sed -i 's/.$//' customer.tbl
$ sed -i 's/.$//' date.tbl
$ sed -i 's/.$//' supplier.tbl
$ sed -i 's/.$//' lineorder.tbl
$ sed -i 's/.$//' part.tbl
```

将`system_vars_config.toml`参数增大，例如10GB，然后重新启动MatrixOne服务。
```
max-entry-bytes = "10GB"
```

使用如下命令将数据导入相关表：

```
load data infile '/ssb-dbgen-path/supplier.tbl ' into table supplier FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/customer.tbl ' into table customer FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/date.tbl ' into table dim_date FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/supplier.tbl ' into table supplier FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/part.tbl ' into table part FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/lineorder.tbl ' into table lineorder FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

接着你可以在MatrixOne中进行查询操作：  
如果你想要运行SSB宽表测试，你还需要`lineorder_flat`表数据，你可以从以下链接获取数据：
> <https://pan.baidu.com/s/1dCpcKsygdVuHzd-H-RWHFA>  code: k1rs

运行以下命令将数据导入`lineorder_flat`：

```
load data infile '/ssb-dbgen-path/lineorder_flat.tbl ' into table lineorder_flat FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

## **5. 运行SSB测试命令**

```
# Q1.1
SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE LO_ORDERDATE<=19930131 AND LO_ORDERDATE>=19930101 AND LO_DISCOUNT BETWEEN 1 AND 3 AND LO_QUANTITY < 25;

# Q1.2

SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE LO_ORDERDATE<=19920131 AND LO_ORDERDATE>=19920101 AND LO_DISCOUNT BETWEEN 4 AND 6 AND LO_QUANTITY BETWEEN 26 AND 35;

# Q1.3

SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE LO_ORDERDATE = 19920101 AND LO_DISCOUNT BETWEEN 5 AND 7 AND LO_QUANTITY BETWEEN 26 AND 35;

# Q2.1

SELECT sum(LO_REVENUE), LO_ORDERDATE, P_BRAND FROM lineorder_flat WHERE P_CATEGORY = 'MFGR#11' AND S_REGION = 'AMERICA' GROUP BY LO_ORDERDATE, P_BRAND ORDER BY LO_ORDERDATE, P_BRAND;

# Q2.2

SELECT sum(LO_REVENUE), LO_ORDERDATE, P_BRAND FROM lineorder_flat WHERE P_BRAND BETWEEN 'MFGR#2221' AND 'MFGR#2228' AND S_REGION = 'ASIA' GROUP BY LO_ORDERDATE, P_BRAND ORDER BY LO_ORDERDATE, P_BRAND;

# Q2.3

SELECT sum(LO_REVENUE), LO_ORDERDATE, P_BRAND FROM lineorder_flat WHERE P_BRAND = 'MFGR#2239' AND S_REGION = 'EUROPE' GROUP BY LO_ORDERDATE, P_BRAND ORDER BY LO_ORDERDATE, P_BRAND;

# Q3.1

SELECT C_NATION, S_NATION, LO_ORDERDATE, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE C_REGION = 'ASIA' AND S_REGION = 'ASIA' AND LO_ORDERDATE >= 19920101 AND LO_ORDERDATE <= 19970101 GROUP BY C_NATION, S_NATION, LO_ORDERDATE ORDER BY LO_ORDERDATE asc, revenue desc;

# Q3.2

SELECT C_CITY, S_CITY, LO_ORDERDATE, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE C_NATION = 'CHINA' AND S_NATION = 'CHINA' AND LO_ORDERDATE >= 19920101 AND LO_ORDERDATE <= 19971231 GROUP BY C_CITY, S_CITY, LO_ORDERDATE ORDER BY LO_ORDERDATE asc, revenue desc;

# Q3.3

SELECT C_CITY, S_CITY, LO_ORDERDATE, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE (C_CITY = 'UNITED KI0' OR C_CITY = 'UNITED KI7') AND (S_CITY = 'UNITED KI0' OR S_CITY = 'UNITED KI7') AND LO_ORDERDATE >= 19920101 AND LO_ORDERDATE <= 19971231 GROUP BY C_CITY, S_CITY, LO_ORDERDATE ORDER BY LO_ORDERDATE asc, revenue desc;

# Q3.4

SELECT C_CITY, S_CITY, LO_ORDERDATE, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE (C_CITY = 'UNITED KI0' OR C_CITY = 'UNITED KI7') AND (S_CITY = 'MOZAMBIQU1' OR S_CITY = 'KENYA    4') AND LO_ORDERDATE >= 19971201 GROUP BY C_CITY, S_CITY, LO_ORDERDATE ORDER BY LO_ORDERDATE asc, revenue desc;

# Q4.1

SELECT LO_ORDERDATE, C_NATION, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2') GROUP BY LO_ORDERDATE, C_NATION ORDER BY LO_ORDERDATE, C_NATION;

# Q4.2

SELECT LO_ORDERDATE, S_NATION, P_CATEGORY, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (LO_ORDERDATE>= 19970101 OR LO_ORDERDATE <= 19981231) AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2') GROUP BY LO_ORDERDATE, S_NATION, P_CATEGORY ORDER BY LO_ORDERDATE, S_NATION, P_CATEGORY;

# Q4.3

SELECT LO_ORDERDATE, S_CITY, P_BRAND, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE S_NATION = 'UNITED STATES' AND (LO_ORDERDATE>= 19970101 OR LO_ORDERDATE <= 19981231) AND P_CATEGORY = 'MFGR#14' GROUP BY LO_ORDERDATE, S_CITY, P_BRAND ORDER BY LO_ORDERDATE, S_CITY, P_BRAND;
```
