# **完成SSB测试**

SSB 星型模式基准测试是 OLAP 数据库性能测试的常用场景，通过本篇教程，您可以了解到如何在 MatrixOne 中实现 SSB 测试。

## **准备工作**

确保你已经安装了[单机版MatrixOne](../install-standalone-matrixone.md)并[连接到MatrixOne服务](../connect-to-matrixone-server.md).

## **1. 编译dbgen**

```
git clone git@github.com:vadimtk/ssb-dbgen.git
cd ssb-dbgen
make
```

## **2. 生成数据**

当使用`-s 1`时`dbgen`命令会生产近600万行数据(670MB)，当使用`-s 10`时会生产近6000万行数据，会耗费大量时间。

```
./dbgen -s 1 -T c
./dbgen -s 1 -T l
./dbgen -s 1 -T p
./dbgen -s 1 -T s
./dbgen -s 1 -T d
```

我们还准备了 1GB 的数据集供你下载。你可以在下面链接中直接获取数据文件：

```
https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/lineorder_flat.tar.bz2
```

## **3. 在MatrixOne中建表**

```
create database if not exists ssb;
use ssb;
drop table if exists lineorder;
drop table if exists part;
drop table if exists supplier;
drop table if exists customer;
drop table if exists dates;
drop table if exists lineorder_flat;

create table lineorder (
        lo_orderkey bigint,
        lo_linenumber int,
        lo_custkey int,
        lo_partkey int,
        lo_suppkey int,
        lo_orderdate date,
        lo_orderpriority char (15),
        lo_shippriority tinyint,
        lo_quantity double,
        lo_extendedprice double,
        lo_ordtotalprice double,
        lo_discount double,
        lo_revenue double,
        lo_supplycost double,
        lo_tax double,
        lo_commitdate date,
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

create table dates (
        d_datekey date,
        d_date char (18),
        d_dayofweek char (9),
        d_month char (9),
        d_yearmonthnum int,
        d_yearmonth char (7),
        d_daynuminweek varchar(12),
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

CREATE TABLE lineorder_flat(
  LO_ORDERKEY bigint primary key,
  LO_LINENUMBER int,
  LO_CUSTKEY int,
  LO_PARTKEY int,
  LO_SUPPKEY int,
  LO_ORDERDATE date,
  LO_ORDERPRIORITY char(15),
  LO_SHIPPRIORITY tinyint,
  LO_QUANTITY double,
  LO_EXTENDEDPRICE double,
  LO_ORDTOTALPRICE double,
  LO_DISCOUNT double,
  LO_REVENUE int unsigned,
  LO_SUPPLYCOST int unsigned,
  LO_TAX double,
  LO_COMMITDATE date,
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

使用如下命令将数据导入相关表：

```
load data infile '/ssb-dbgen-path/supplier.tbl' into table supplier FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/customer.tbl' into table customer FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/date.tbl' into table dates FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/part.tbl' into table part FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/lineorder.tbl' into table lineorder FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

接着你可以在MatrixOne中进行查询操作：  
如果你想要运行SSB宽表测试，你还需要`lineorder_flat`表数据，你可以从以下链接获取数据：
> <https://pan.baidu.com/s/1dCpcKsygdVuHzd-H-RWHFA>  
> code: k1rs

运行以下命令将数据导入`lineorder_flat`：

```
load data infile '/ssb-dbgen-path/lineorder_flat.tbl ' into table lineorder_flat FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

## **5. 运行SSB测试命令**

### **单表查询**

```sql
--Q1.1
SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE year(LO_ORDERDATE)=1993 AND LO_DISCOUNT BETWEEN 1 AND 3 AND LO_QUANTITY < 25;

--Q1.2
SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE year(LO_ORDERDATE)=1994 AND LO_DISCOUNT BETWEEN 4 AND 6 AND LO_QUANTITY BETWEEN 26 AND 35;

--Q1.3
SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE d_yearmonthnum=1994 AND LO_DISCOUNT BETWEEN 5 AND 7 AND LO_QUANTITY BETWEEN 26 AND 35;

--Q2.1
SELECT sum(LO_REVENUE),year(LO_ORDERDATE) AS year,P_BRAND FROM lineorder_flat WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA' GROUP BY year,P_BRAND ORDER BY year,P_BRAND;

--Q2.2
SELECT sum(LO_REVENUE), year(LO_ORDERDATE) AS year, P_BRAND FROM lineorder_flat WHERE P_BRAND BETWEEN 'MFGR#2221' AND 'MFGR#2228' AND S_REGION = 'ASIA' GROUP BY year, P_BRAND ORDER BY year, P_BRAND;

--Q2.3
SELECT sum(LO_REVENUE), year(LO_ORDERDATE) AS year, P_BRAND FROM lineorder_flat WHERE P_BRAND = 'MFGR#2239' AND S_REGION = 'EUROPE' GROUP BY year, P_BRAND ORDER BY year, P_BRAND;

--Q3.1
SELECT C_NATION, S_NATION, year(LO_ORDERDATE) AS year, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE C_REGION = 'ASIA' AND S_REGION = 'ASIA' AND year(LO_ORDERDATE)  between 1992 AND 1997 GROUP BY C_NATION, S_NATION, year(LO_ORDERDATE) ORDER BY year asc, revenue desc;

--Q3.2
SELECT C_CITY, S_CITY, year(LO_ORDERDATE) AS year, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE C_NATION = 'CHINA' AND S_NATION = 'CHINA' AND year(LO_ORDERDATE)  between 1992 AND 1997 GROUP BY C_CITY, S_CITY, year(LO_ORDERDATE)  ORDER BY year asc, revenue desc;

--Q3.3
SELECT C_CITY, S_CITY, year(LO_ORDERDATE) AS year, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE (C_CITY = 'UNITED KI0' OR C_CITY = 'UNITED KI7') AND (S_CITY = 'UNITED KI0' OR S_CITY = 'UNITED KI7') AND year(LO_ORDERDATE)  between 1992 AND 1997 GROUP BY C_CITY, S_CITY, year(LO_ORDERDATE) ORDER BY year asc, revenue desc;

--Q3.4
SELECT C_CITY, S_CITY, year(LO_ORDERDATE) AS year, sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE (C_CITY = 'UNITED KI0' OR C_CITY = 'UNITED KI7') AND (S_CITY = 'MOZAMBIQU1' OR S_CITY = 'KENYA    4') AND year(LO_ORDERDATE)= 1997 GROUP BY C_CITY, S_CITY, year(LO_ORDERDATE) ORDER BY year asc, revenue desc;

--Q4.1
SELECT year(LO_ORDERDATE) AS year, C_NATION, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2') GROUP BY year(LO_ORDERDATE), C_NATION ORDER BY year, C_NATION;

--Q4.2
SELECT year(LO_ORDERDATE) AS year, S_NATION, P_CATEGORY, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (year(LO_ORDERDATE) = 1997 OR year(LO_ORDERDATE) = 1998) AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2') GROUP BY  year(LO_ORDERDATE), S_NATION, P_CATEGORY ORDER BY year, S_NATION, P_CATEGORY;

--Q4.3
SELECT year(LO_ORDERDATE) AS year, S_CITY, P_BRAND, sum(LO_REVENUE - LO_SUPPLYCOST) AS profit FROM lineorder_flat WHERE S_NATION = 'UNITED STATES' AND (year(LO_ORDERDATE) = 1997 OR year(LO_ORDERDATE) = 1998) AND P_CATEGORY = 'MFGR#14' GROUP BY  year(LO_ORDERDATE), S_CITY, P_BRAND ORDER BY year, S_CITY, P_BRAND;
```

### **多表查询**

```sql
--Q1.1
select sum(lo_revenue) as revenue
from lineorder join dates on lo_orderdate = d_datekey
where year(d_datekey)  = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;

--Q1.2
select sum(lo_revenue) as revenue
from lineorder
join dates on lo_orderdate = d_datekey
where d_yearmonthnum = 199401
and lo_discount between 4 and 6
and lo_quantity between 26 and 35;

--Q1.3
select sum(lo_revenue) as revenue
from lineorder
join dates on lo_orderdate = d_datekey
where d_weeknuminyear = 6 and year(d_datekey)  = 1994
and lo_discount between 5 and 7
and lo_quantity between 26 and 35;

--Q2.1
select sum(lo_revenue) as lo_revenue, year(d_datekey) as year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
where p_category = 'MFGR#12' and s_region = 'AMERICA'
group by year, p_brand
order by year, p_brand;

--Q2.2
select sum(lo_revenue) as lo_revenue, year(d_datekey) as year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA'
group by year, p_brand
order by year, p_brand;

--Q2.3
select sum(lo_revenue) as lo_revenue, year(d_datekey) as year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
where p_brand = 'MFGR#2239' and s_region = 'EUROPE'
group by year, p_brand
order by year, p_brand;

--Q3.1
select c_nation, s_nation, year(d_datekey) as year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where c_region = 'ASIA' and s_region = 'ASIA' and year(d_datekey) between 1992 and 1997
group by c_nation, s_nation, year
order by year asc, lo_revenue desc;

--Q3.2
select c_city, s_city, year(d_datekey) as year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES'
and year(d_datekey) between 1992 and 1997
group by c_city, s_city, year
order by year asc, lo_revenue desc;

--Q3.3
select c_city, s_city, year(d_datekey) as year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where (c_city='UNITED KI1' or c_city='UNITED KI5')
and (s_city='UNITED KI1' or s_city='UNITED KI5')
and year(d_datekey) between 1992 and 1997
group by c_city, s_city, year
order by year asc, lo_revenue desc;

--Q3.4
select c_city, s_city, year(d_datekey) as year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = 'Dec1997'
group by c_city, s_city, year
order by year asc, lo_revenue desc;

--Q4.1
select year(d_datekey) as year, c_nation, sum(lo_revenue) - sum(lo_supplycost) as profit
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
join part on lo_partkey = p_partkey
where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by year, c_nation
order by year, c_nation;

--Q4.2
select year(d_datekey) as year, s_nation, p_category, sum(lo_revenue) - sum(lo_supplycost) as profit
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
join part on lo_partkey = p_partkey
where c_region = 'AMERICA'and s_region = 'AMERICA'
and (year(d_datekey) = 1997 or year(d_datekey) = 1998)
and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by year, s_nation, p_category
order by year, s_nation, p_category;

--Q4.3
select year(d_datekey) as year, s_city, p_brand, sum(lo_revenue) - sum(lo_supplycost) as profit,c_region , s_nation, p_category
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
join part on lo_partkey = p_partkey
where
(year(d_datekey) = 1997 or year(d_datekey) = 1998)
and s_nation='ALGERIA'
group by year, s_city, p_brand
order by year, s_city, p_brand;

```
