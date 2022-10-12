# **完成SSB测试**

SSB 星型模式基准测试是 OLAP 数据库性能测试的常用场景，通过本篇教程，您可以了解到如何在 MatrixOne 中实现 SSB 测试。

## **准备工作**

确保你已经安装了[单机版MatrixOne](../Get-Started/install-standalone-matrixone.md)并[连接到MatrixOne服务](../Get-Started/connect-to-matrixone-server.md).

## **1. 编译dbgen**

```
git clone https://github.com/vadimtk/ssb-dbgen.git
cd ssb-dbgen
make
```

!!! note
    如果你的硬件配置为 M1 芯片，编译 dbgen 仍需进行其他配置，参见[部署常见问题](../FAQs/deployment-faqs.md)。

## **2. 生成数据**

### 选项一：生成单表数据集

当使用 `-s 1` 时 `dbgen` 命令会生产近600万行数据(670MB)，当使用`-s 10`时会生产近6000万行数据，会耗费大量时间。

```
./dbgen -s 1 -T c
./dbgen -s 1 -T l
./dbgen -s 1 -T p
./dbgen -s 1 -T s
./dbgen -s 1 -T d
```

### 选项二：下载大宽表数据集

我们准备了 1GB 的大宽表数据集供你下载。

1. 在下面链接中直接获取大宽表数据集文件：

```
https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/lineorder_flat.tar.bz2
```

2. 下载完成后将数据文件解压。

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
  LO_ORDERKEY bigint key,
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

### 选项一：使用如下命令导入单表数据集

```
load data infile '/ssb-dbgen-path/supplier.tbl' into table supplier FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/customer.tbl' into table customer FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/date.tbl' into table dates FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/part.tbl' into table part FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/ssb-dbgen-path/lineorder.tbl' into table lineorder FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

接着你可以在 MatrixOne 中进行查询操作。

### 选项二：使用如下命令导入大宽表数据集

运行以下命令将数据导入`lineorder_flat`：

```
load data infile '/ssb-dbgen-path/lineorder_flat.tbl' into table lineorder_flat FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

## **5. 运行SSB测试命令**  

!!! note
    `GROUP BY` 暂不支持使用别名。

### **单表查询**

```sql
--Q1.1
SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE year(LO_ORDERDATE)=1993 AND LO_DISCOUNT BETWEEN 1 AND 3 AND LO_QUANTITY < 25;

--Q1.2
SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE year(LO_ORDERDATE)=1994 AND LO_DISCOUNT BETWEEN 4 AND 6 AND LO_QUANTITY BETWEEN 26 AND 35;

--Q1.3
SELECT sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue FROM lineorder_flat WHERE year(LO_ORDERDATE)=1994 AND LO_DISCOUNT BETWEEN 5 AND 7 AND LO_QUANTITY BETWEEN 26 AND 35;

--Q2.1
SELECT sum(LO_REVENUE),year(LO_ORDERDATE) AS year,P_BRAND FROM lineorder_flat WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA' GROUP BY year(LO_ORDERDATE), P_BRAND ORDER BY year,P_BRAND;

--Q2.2
SELECT sum(LO_REVENUE), year(LO_ORDERDATE) AS year, P_BRAND FROM lineorder_flat WHERE P_BRAND BETWEEN 'MFGR#2221' AND 'MFGR#2228' AND S_REGION = 'ASIA' GROUP BY year(LO_ORDERDATE), P_BRAND ORDER BY year, P_BRAND;

--Q2.3
SELECT sum(LO_REVENUE), year(LO_ORDERDATE) AS year, P_BRAND FROM lineorder_flat WHERE P_BRAND = 'MFGR#2239' AND S_REGION = 'EUROPE' GROUP BY year(LO_ORDERDATE), P_BRAND ORDER BY year, P_BRAND;

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
group by year(d_datekey), p_brand
order by year, p_brand;

--Q2.2
select sum(lo_revenue) as lo_revenue, year(d_datekey) as year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA'
group by year(d_datekey), p_brand
order by year, p_brand;

--Q2.3
select sum(lo_revenue) as lo_revenue, year(d_datekey) as year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
where p_brand = 'MFGR#2239' and s_region = 'EUROPE'
group by year(d_datekey), p_brand
order by year, p_brand;

--Q3.1
select c_nation, s_nation, year(d_datekey) as year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where c_region = 'ASIA' and s_region = 'ASIA' and year(d_datekey) between 1992 and 1997
group by c_nation, s_nation, year(d_datekey)
order by year asc, lo_revenue desc;

--Q3.2
select c_city, s_city, year(d_datekey) as year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES'
and year(d_datekey) between 1992 and 1997
group by c_city, s_city, year(d_datekey)
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
group by c_city, s_city, year(d_datekey)
order by year asc, lo_revenue desc;

--Q3.4
select c_city, s_city, year(d_datekey) as year, sum(lo_revenue) as lo_revenue
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = '199712'
group by c_city, s_city, year(d_datekey)
order by year(d_datekey) asc, lo_revenue desc;

--Q4.1
select year(d_datekey) as year, c_nation, sum(lo_revenue) - sum(lo_supplycost) as profit
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
join part on lo_partkey = p_partkey
where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by year(d_datekey), c_nation
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
group by year(d_datekey), s_nation, p_category
order by year, s_nation, p_category;

--Q4.3
select year(d_datekey) as year, s_city, p_brand, sum(lo_revenue) - sum(lo_supplycost) as profit, c_region, s_nation, p_category
from lineorder
join dates on lo_orderdate = d_datekey
join customer on lo_custkey = c_custkey
join supplier on lo_suppkey = s_suppkey
join part on lo_partkey = p_partkey
where
(year(d_datekey) = 1997 or year(d_datekey) = 1998)
and s_nation='ALGERIA'
group by year(d_datekey), s_city, p_brand, c_region, s_nation, p_category
order by year, s_city, p_brand;
```

## **6. 运行预期结果**

### 单表查询运行预期结果

```
--Q1.1
+--------------+
| revenue      |
+--------------+
| 702223464659 |
+--------------+

--Q1.2
+---------------+
| revenue       |
+---------------+
| 1842875090496 |
+---------------+

--Q1.3
+---------------+
| revenue       |
+---------------+
| 2208738861324 |
+---------------+

--Q2.1
+-----------------+------+-----------+
| sum(lo_revenue) | year | p_brand   |
+-----------------+------+-----------+
|       283684289 | 1992 | MFGR#121  |
|      1344702529 | 1992 | MFGR#1210 |
|       757158682 | 1992 | MFGR#1211 |
|      1156022815 | 1992 | MFGR#1212 |
|       676164367 | 1992 | MFGR#1213 |
|       522779256 | 1992 | MFGR#1214 |
|       233507213 | 1992 | MFGR#1215 |
|       726755819 | 1992 | MFGR#1216 |
|      1865504710 | 1992 | MFGR#1217 |
|       685600451 | 1992 | MFGR#1218 |
|       814671470 | 1992 | MFGR#1219 |
|       655405800 | 1992 | MFGR#122  |
|       962120553 | 1992 | MFGR#1220 |
|       961393626 | 1992 | MFGR#1221 |
|       922110830 | 1992 | MFGR#1222 |
|       814842712 | 1992 | MFGR#1223 |
|      1402773165 | 1992 | MFGR#1224 |
|       977517439 | 1992 | MFGR#1225 |
|      1392114944 | 1992 | MFGR#1226 |
|       658922951 | 1992 | MFGR#1227 |
|       892495927 | 1992 | MFGR#1228 |
|       806609100 | 1992 | MFGR#1229 |
|       578875657 | 1992 | MFGR#123  |
|       691236902 | 1992 | MFGR#1230 |
|       482715249 | 1992 | MFGR#1231 |
|       618556590 | 1992 | MFGR#1232 |
|       981657009 | 1992 | MFGR#1233 |
|      1050794669 | 1992 | MFGR#1234 |
|      1335217991 | 1992 | MFGR#1235 |
|       686009527 | 1992 | MFGR#1236 |
|       605242584 | 1992 | MFGR#1237 |
|       430455641 | 1992 | MFGR#1238 |
|       359654993 | 1992 | MFGR#1239 |
|       818818457 | 1992 | MFGR#124  |
|      1388502581 | 1992 | MFGR#1240 |
|       711566198 | 1992 | MFGR#125  |
|       893045647 | 1992 | MFGR#126  |
|      1240534333 | 1992 | MFGR#127  |
|       871966127 | 1992 | MFGR#128  |
|       719176622 | 1992 | MFGR#129  |
|       245880758 | 1993 | MFGR#121  |
|      1480662753 | 1993 | MFGR#1210 |
|       982292725 | 1993 | MFGR#1211 |
|      1001136766 | 1993 | MFGR#1212 |
|       227144072 | 1993 | MFGR#1213 |
|       291611370 | 1993 | MFGR#1214 |
|       454576668 | 1993 | MFGR#1215 |
|       772027256 | 1993 | MFGR#1216 |
|      1155751851 | 1993 | MFGR#1217 |
|       897883050 | 1993 | MFGR#1218 |
|      1209508962 | 1993 | MFGR#1219 |
|       530577973 | 1993 | MFGR#122  |
|       901650471 | 1993 | MFGR#1220 |
|       739540494 | 1993 | MFGR#1221 |
|       698751459 | 1993 | MFGR#1222 |
|      1327979157 | 1993 | MFGR#1223 |
|      1476697469 | 1993 | MFGR#1224 |
|       792103134 | 1993 | MFGR#1225 |
|      1420277376 | 1993 | MFGR#1226 |
|      1446032835 | 1993 | MFGR#1227 |
|       884375309 | 1993 | MFGR#1228 |
|       522705868 | 1993 | MFGR#1229 |
|       601685233 | 1993 | MFGR#123  |
|       806727248 | 1993 | MFGR#1230 |
|       399477390 | 1993 | MFGR#1231 |
|       894047578 | 1993 | MFGR#1232 |
|       496067089 | 1993 | MFGR#1233 |
|      1050223756 | 1993 | MFGR#1234 |
|       891681399 | 1993 | MFGR#1235 |
|      1402903631 | 1993 | MFGR#1236 |
|       347380448 | 1993 | MFGR#1237 |
|       514853194 | 1993 | MFGR#1238 |
|       410543863 | 1993 | MFGR#1239 |
|       673483594 | 1993 | MFGR#124  |
|       713343630 | 1993 | MFGR#1240 |
|       512610707 | 1993 | MFGR#125  |
|      1228110634 | 1993 | MFGR#126  |
|       936958961 | 1993 | MFGR#127  |
|       579067515 | 1993 | MFGR#128  |
|       636174833 | 1993 | MFGR#129  |
|       370347074 | 1994 | MFGR#121  |
|       483900410 | 1994 | MFGR#1210 |
|      1240306281 | 1994 | MFGR#1211 |
|      1003345253 | 1994 | MFGR#1212 |
|       557310864 | 1994 | MFGR#1213 |
|       314972328 | 1994 | MFGR#1214 |
|      1130260810 | 1994 | MFGR#1215 |
|       401618319 | 1994 | MFGR#1216 |
|       652173601 | 1994 | MFGR#1217 |
|       923612074 | 1994 | MFGR#1218 |
|       469711377 | 1994 | MFGR#1219 |
|       580152115 | 1994 | MFGR#122  |
|       433134653 | 1994 | MFGR#1220 |
|       730569849 | 1994 | MFGR#1221 |
|       607609104 | 1994 | MFGR#1222 |
|       949877669 | 1994 | MFGR#1223 |
|      1644687916 | 1994 | MFGR#1224 |
|       492974194 | 1994 | MFGR#1225 |
|       921499688 | 1994 | MFGR#1226 |
|       719059761 | 1994 | MFGR#1227 |
|      1000497056 | 1994 | MFGR#1228 |
|       486968927 | 1994 | MFGR#1229 |
|       734124906 | 1994 | MFGR#123  |
|       645016873 | 1994 | MFGR#1230 |
|       526638240 | 1994 | MFGR#1231 |
|      1358112405 | 1994 | MFGR#1232 |
|      1167074905 | 1994 | MFGR#1233 |
|      1102915239 | 1994 | MFGR#1234 |
|       693058125 | 1994 | MFGR#1235 |
|      1673392892 | 1994 | MFGR#1236 |
|       849630029 | 1994 | MFGR#1237 |
|       721392705 | 1994 | MFGR#1238 |
|      1237195774 | 1994 | MFGR#1239 |
|      1107832795 | 1994 | MFGR#124  |
|       827906290 | 1994 | MFGR#1240 |
|       682827304 | 1994 | MFGR#125  |
|      1198768141 | 1994 | MFGR#126  |
|      1274148181 | 1994 | MFGR#127  |
|       738849138 | 1994 | MFGR#128  |
|       751136619 | 1994 | MFGR#129  |
|       318978803 | 1995 | MFGR#121  |
|       383199448 | 1995 | MFGR#1210 |
|      1300165814 | 1995 | MFGR#1211 |
|      1550400731 | 1995 | MFGR#1212 |
|       451958158 | 1995 | MFGR#1213 |
|       431434279 | 1995 | MFGR#1214 |
|       713735582 | 1995 | MFGR#1215 |
|       919323722 | 1995 | MFGR#1216 |
|      1542358864 | 1995 | MFGR#1217 |
|       500930092 | 1995 | MFGR#1218 |
|      1208162086 | 1995 | MFGR#1219 |
|       785707989 | 1995 | MFGR#122  |
|       993828211 | 1995 | MFGR#1220 |
|       667253893 | 1995 | MFGR#1221 |
|      1654114297 | 1995 | MFGR#1222 |
|       986528377 | 1995 | MFGR#1223 |
|       755014642 | 1995 | MFGR#1224 |
|      1090300100 | 1995 | MFGR#1225 |
|      1063626454 | 1995 | MFGR#1226 |
|      1382528859 | 1995 | MFGR#1227 |
|       919953351 | 1995 | MFGR#1228 |
|       457795295 | 1995 | MFGR#1229 |
|       953851827 | 1995 | MFGR#123  |
|       807209283 | 1995 | MFGR#1230 |
|       236304454 | 1995 | MFGR#1231 |
|       668449537 | 1995 | MFGR#1232 |
|       240657083 | 1995 | MFGR#1233 |
|       920389826 | 1995 | MFGR#1234 |
|       684096065 | 1995 | MFGR#1235 |
|      1005844219 | 1995 | MFGR#1236 |
|       626170996 | 1995 | MFGR#1237 |
|       984581826 | 1995 | MFGR#1238 |
|       602850634 | 1995 | MFGR#1239 |
|      1172025628 | 1995 | MFGR#124  |
|       489788581 | 1995 | MFGR#1240 |
|       643100327 | 1995 | MFGR#125  |
|       894596661 | 1995 | MFGR#126  |
|       706917239 | 1995 | MFGR#127  |
|       428671983 | 1995 | MFGR#128  |
|       971611472 | 1995 | MFGR#129  |
|       306497573 | 1996 | MFGR#121  |
|       890719726 | 1996 | MFGR#1210 |
|      1761977172 | 1996 | MFGR#1211 |
|       633845765 | 1996 | MFGR#1212 |
|       475801202 | 1996 | MFGR#1213 |
|       271930385 | 1996 | MFGR#1214 |
|       366399844 | 1996 | MFGR#1215 |
|       877472476 | 1996 | MFGR#1216 |
|       970366290 | 1996 | MFGR#1217 |
|       537175690 | 1996 | MFGR#1218 |
|       956970528 | 1996 | MFGR#1219 |
|       711962074 | 1996 | MFGR#122  |
|      1062161683 | 1996 | MFGR#1220 |
|       406293744 | 1996 | MFGR#1221 |
|       785404335 | 1996 | MFGR#1222 |
|       579267044 | 1996 | MFGR#1223 |
|      1220640256 | 1996 | MFGR#1224 |
|       490130196 | 1996 | MFGR#1225 |
|      1603805636 | 1996 | MFGR#1226 |
|      1530646510 | 1996 | MFGR#1227 |
|      1093328922 | 1996 | MFGR#1228 |
|       596520140 | 1996 | MFGR#1229 |
|       450815571 | 1996 | MFGR#123  |
|       315053350 | 1996 | MFGR#1230 |
|       198951017 | 1996 | MFGR#1231 |
|       579778438 | 1996 | MFGR#1232 |
|       480905486 | 1996 | MFGR#1233 |
|      1433336215 | 1996 | MFGR#1234 |
|       560925251 | 1996 | MFGR#1235 |
|      1038766181 | 1996 | MFGR#1236 |
|       783697960 | 1996 | MFGR#1237 |
|       972656445 | 1996 | MFGR#1238 |
|       614528801 | 1996 | MFGR#1239 |
|      1418931894 | 1996 | MFGR#124  |
|       995139591 | 1996 | MFGR#1240 |
|       824028471 | 1996 | MFGR#125  |
|       669475113 | 1996 | MFGR#126  |
|       831704651 | 1996 | MFGR#127  |
|       920514555 | 1996 | MFGR#128  |
|       436162421 | 1996 | MFGR#129  |
|       553684594 | 1997 | MFGR#121  |
|      1317368046 | 1997 | MFGR#1210 |
|      1617056983 | 1997 | MFGR#1211 |
|      1196031005 | 1997 | MFGR#1212 |
|      1056458336 | 1997 | MFGR#1213 |
|       352179650 | 1997 | MFGR#1214 |
|       511058114 | 1997 | MFGR#1215 |
|       658259312 | 1997 | MFGR#1216 |
|      1238450697 | 1997 | MFGR#1217 |
|       376245955 | 1997 | MFGR#1218 |
|       913437812 | 1997 | MFGR#1219 |
|      1114996000 | 1997 | MFGR#122  |
|       814059433 | 1997 | MFGR#1220 |
|       817328516 | 1997 | MFGR#1221 |
|       541428597 | 1997 | MFGR#1222 |
|      1260539052 | 1997 | MFGR#1223 |
|      1766426582 | 1997 | MFGR#1224 |
|      1221271245 | 1997 | MFGR#1225 |
|      1499152922 | 1997 | MFGR#1226 |
|       491586909 | 1997 | MFGR#1227 |
|       911517084 | 1997 | MFGR#1228 |
|       728186585 | 1997 | MFGR#1229 |
|       904363416 | 1997 | MFGR#123  |
|       605369014 | 1997 | MFGR#1230 |
|       290370455 | 1997 | MFGR#1231 |
|       602414397 | 1997 | MFGR#1232 |
|       765339065 | 1997 | MFGR#1233 |
|      1170973957 | 1997 | MFGR#1234 |
|       860319765 | 1997 | MFGR#1235 |
|      1031080311 | 1997 | MFGR#1236 |
|       736404810 | 1997 | MFGR#1237 |
|      1012330790 | 1997 | MFGR#1238 |
|       681055343 | 1997 | MFGR#1239 |
|       601626600 | 1997 | MFGR#124  |
|       920404157 | 1997 | MFGR#1240 |
|      1007678757 | 1997 | MFGR#125  |
|       622347203 | 1997 | MFGR#126  |
|      1215295592 | 1997 | MFGR#127  |
|       822274972 | 1997 | MFGR#128  |
|       643903475 | 1997 | MFGR#129  |
|       470008435 | 1998 | MFGR#121  |
|       568508492 | 1998 | MFGR#1210 |
|       323759101 | 1998 | MFGR#1211 |
|       572013331 | 1998 | MFGR#1212 |
|       448137748 | 1998 | MFGR#1213 |
|       137422458 | 1998 | MFGR#1214 |
|       346491756 | 1998 | MFGR#1215 |
|       454542243 | 1998 | MFGR#1216 |
|       759205210 | 1998 | MFGR#1217 |
|       674544462 | 1998 | MFGR#1218 |
|       735952270 | 1998 | MFGR#1219 |
|       490186568 | 1998 | MFGR#122  |
|       769456686 | 1998 | MFGR#1220 |
|       654540341 | 1998 | MFGR#1221 |
|       800329859 | 1998 | MFGR#1222 |
|       263849231 | 1998 | MFGR#1223 |
|       445461642 | 1998 | MFGR#1224 |
|       387808862 | 1998 | MFGR#1225 |
|       675424382 | 1998 | MFGR#1226 |
|       265906673 | 1998 | MFGR#1227 |
|       585938371 | 1998 | MFGR#1228 |
|       683188537 | 1998 | MFGR#1229 |
|       304403717 | 1998 | MFGR#123  |
|       533781674 | 1998 | MFGR#1230 |
|       304060011 | 1998 | MFGR#1231 |
|       635275907 | 1998 | MFGR#1232 |
|       658295080 | 1998 | MFGR#1233 |
|       524133341 | 1998 | MFGR#1234 |
|       363911877 | 1998 | MFGR#1235 |
|       300885635 | 1998 | MFGR#1236 |
|       532608453 | 1998 | MFGR#1237 |
|       484291410 | 1998 | MFGR#1238 |
|       445336624 | 1998 | MFGR#1239 |
|       719027801 | 1998 | MFGR#124  |
|       518860961 | 1998 | MFGR#1240 |
|       491235383 | 1998 | MFGR#125  |
|       520917638 | 1998 | MFGR#126  |
|      1158787745 | 1998 | MFGR#127  |
|       401190922 | 1998 | MFGR#128  |
|       406656337 | 1998 | MFGR#129  |
+-----------------+------+-----------+

--Q2.2
+-----------------+------+-----------+
| sum(lo_revenue) | year | p_brand   |
+-----------------+------+-----------+
|      1259802358 | 1992 | MFGR#2221 |
|      1728549344 | 1992 | MFGR#2222 |
|      1375260024 | 1992 | MFGR#2223 |
|      1299982475 | 1992 | MFGR#2224 |
|      1541960331 | 1992 | MFGR#2225 |
|      1151853513 | 1992 | MFGR#2226 |
|      1271175264 | 1992 | MFGR#2227 |
|      1726441695 | 1992 | MFGR#2228 |
|      1251460032 | 1993 | MFGR#2221 |
|      1331062515 | 1993 | MFGR#2222 |
|       902809293 | 1993 | MFGR#2223 |
|       980512417 | 1993 | MFGR#2224 |
|      1253088003 | 1993 | MFGR#2225 |
|       959195148 | 1993 | MFGR#2226 |
|       555593932 | 1993 | MFGR#2227 |
|      2186479174 | 1993 | MFGR#2228 |
|      1094092222 | 1994 | MFGR#2221 |
|      1491699323 | 1994 | MFGR#2222 |
|      1501160826 | 1994 | MFGR#2223 |
|      1387107418 | 1994 | MFGR#2224 |
|      1641588884 | 1994 | MFGR#2225 |
|      1387296390 | 1994 | MFGR#2226 |
|      1038341470 | 1994 | MFGR#2227 |
|      1565763138 | 1994 | MFGR#2228 |
|      1412945650 | 1995 | MFGR#2221 |
|      1546178356 | 1995 | MFGR#2222 |
|      1218352073 | 1995 | MFGR#2223 |
|      1052197762 | 1995 | MFGR#2224 |
|      1822921900 | 1995 | MFGR#2225 |
|       728142181 | 1995 | MFGR#2226 |
|       966131607 | 1995 | MFGR#2227 |
|      1379320517 | 1995 | MFGR#2228 |
|      1042767284 | 1996 | MFGR#2221 |
|       994733835 | 1996 | MFGR#2222 |
|      1615788545 | 1996 | MFGR#2223 |
|      1113980216 | 1996 | MFGR#2224 |
|      1622570253 | 1996 | MFGR#2225 |
|      1540226758 | 1996 | MFGR#2226 |
|      1115687883 | 1996 | MFGR#2227 |
|      1716355343 | 1996 | MFGR#2228 |
|       867705636 | 1997 | MFGR#2221 |
|      1529877498 | 1997 | MFGR#2222 |
|      1594444450 | 1997 | MFGR#2223 |
|       587421043 | 1997 | MFGR#2224 |
|      1112274470 | 1997 | MFGR#2225 |
|      1327884722 | 1997 | MFGR#2226 |
|       884180880 | 1997 | MFGR#2227 |
|      1664207656 | 1997 | MFGR#2228 |
|       827743515 | 1998 | MFGR#2221 |
|       662242310 | 1998 | MFGR#2222 |
|       861445539 | 1998 | MFGR#2223 |
|       694538672 | 1998 | MFGR#2224 |
|       675179021 | 1998 | MFGR#2225 |
|       480728720 | 1998 | MFGR#2226 |
|       643763072 | 1998 | MFGR#2227 |
|       994499201 | 1998 | MFGR#2228 |
+-----------------+------+-----------+

--Q2.3
+-----------------+------+-----------+
| sum(lo_revenue) | year | p_brand   |
+-----------------+------+-----------+
|      1428843284 | 1992 | MFGR#2239 |
|      1865666054 | 1993 | MFGR#2239 |
|      2242753254 | 1994 | MFGR#2239 |
|      1446677305 | 1995 | MFGR#2239 |
|       921681503 | 1996 | MFGR#2239 |
|      1549990572 | 1997 | MFGR#2239 |
|       926327433 | 1998 | MFGR#2239 |
+-----------------+------+-----------+

--Q3.1
+-----------+-----------+------+-------------+
| c_nation  | s_nation  | year | revenue     |
+-----------+-----------+------+-------------+
| VIETNAM   | CHINA     | 1992 | 17194479086 |
| JAPAN     | CHINA     | 1992 | 15572594510 |
| JAPAN     | JAPAN     | 1992 | 13861682954 |
| INDONESIA | CHINA     | 1992 | 13499663933 |
| VIETNAM   | INDONESIA | 1992 | 13163103649 |
| JAPAN     | INDONESIA | 1992 | 13035158590 |
| INDIA     | CHINA     | 1992 | 12987688902 |
| INDONESIA | JAPAN     | 1992 | 12939737918 |
| VIETNAM   | JAPAN     | 1992 | 12174715858 |
| JAPAN     | VIETNAM   | 1992 | 11669093753 |
| INDIA     | INDONESIA | 1992 | 11452602145 |
| INDONESIA | INDONESIA | 1992 | 10394407561 |
| INDIA     | JAPAN     | 1992 | 10313084900 |
| JAPAN     | INDIA     | 1992 | 10035511089 |
| CHINA     | CHINA     | 1992 |  9828744666 |
| VIETNAM   | VIETNAM   | 1992 |  9701522505 |
| INDONESIA | INDIA     | 1992 |  9271105764 |
| INDIA     | INDIA     | 1992 |  8879645522 |
| CHINA     | INDONESIA | 1992 |  8373693838 |
| CHINA     | JAPAN     | 1992 |  8051248951 |
| VIETNAM   | INDIA     | 1992 |  7804539029 |
| INDONESIA | VIETNAM   | 1992 |  7615465790 |
| CHINA     | INDIA     | 1992 |  7344868842 |
| INDIA     | VIETNAM   | 1992 |  6830508508 |
| CHINA     | VIETNAM   | 1992 |  6529888238 |
| JAPAN     | CHINA     | 1993 | 18839180326 |
| VIETNAM   | CHINA     | 1993 | 14761890330 |
| JAPAN     | INDONESIA | 1993 | 13648082171 |
| INDONESIA | CHINA     | 1993 | 13518181805 |
| INDIA     | CHINA     | 1993 | 13249555999 |
| JAPAN     | JAPAN     | 1993 | 12667833152 |
| JAPAN     | VIETNAM   | 1993 | 11529854580 |
| CHINA     | CHINA     | 1993 | 11216468573 |
| INDONESIA | INDONESIA | 1993 | 10953284722 |
| VIETNAM   | INDONESIA | 1993 | 10582912267 |
| INDIA     | JAPAN     | 1993 | 10482950584 |
| VIETNAM   | JAPAN     | 1993 | 10370811002 |
| INDIA     | INDONESIA | 1993 | 10145286112 |
| INDONESIA | JAPAN     | 1993 |  9850020303 |
| VIETNAM   | VIETNAM   | 1993 |  9591468153 |
| CHINA     | INDONESIA | 1993 |  9015864524 |
| CHINA     | JAPAN     | 1993 |  8972996729 |
| INDONESIA | INDIA     | 1993 |  8903638786 |
| JAPAN     | INDIA     | 1993 |  8848048514 |
| INDONESIA | VIETNAM   | 1993 |  8024464882 |
| VIETNAM   | INDIA     | 1993 |  7806575746 |
| INDIA     | VIETNAM   | 1993 |  7537331106 |
| INDIA     | INDIA     | 1993 |  7211053846 |
| CHINA     | VIETNAM   | 1993 |  6700022269 |
| CHINA     | INDIA     | 1993 |  6327331541 |
| JAPAN     | CHINA     | 1994 | 15661051644 |
| VIETNAM   | CHINA     | 1994 | 13958591931 |
| JAPAN     | JAPAN     | 1994 | 13566252348 |
| CHINA     | CHINA     | 1994 | 12870010072 |
| VIETNAM   | JAPAN     | 1994 | 12728320716 |
| INDONESIA | CHINA     | 1994 | 12295790872 |
| INDIA     | CHINA     | 1994 | 12166419121 |
| JAPAN     | INDONESIA | 1994 | 11358955025 |
| INDIA     | INDONESIA | 1994 | 11111248365 |
| JAPAN     | INDIA     | 1994 | 10078806371 |
| VIETNAM   | INDONESIA | 1994 |  9923852578 |
| INDIA     | JAPAN     | 1994 |  9839136767 |
| CHINA     | JAPAN     | 1994 |  9836586308 |
| INDONESIA | JAPAN     | 1994 |  9786694572 |
| INDIA     | VIETNAM   | 1994 |  9551081406 |
| JAPAN     | VIETNAM   | 1994 |  9035431932 |
| VIETNAM   | INDIA     | 1994 |  9032319402 |
| INDONESIA | INDONESIA | 1994 |  8876012426 |
| CHINA     | INDONESIA | 1994 |  8375581981 |
| VIETNAM   | VIETNAM   | 1994 |  8095638136 |
| INDONESIA | INDIA     | 1994 |  7943993512 |
| INDONESIA | VIETNAM   | 1994 |  7927236697 |
| INDIA     | INDIA     | 1994 |  7534915457 |
| CHINA     | VIETNAM   | 1994 |  6062387221 |
| CHINA     | INDIA     | 1994 |  5816794324 |
| VIETNAM   | CHINA     | 1995 | 15128423080 |
| INDONESIA | CHINA     | 1995 | 14794647970 |
| INDIA     | CHINA     | 1995 | 14724240804 |
| JAPAN     | CHINA     | 1995 | 14579848516 |
| CHINA     | CHINA     | 1995 | 14296657586 |
| INDIA     | JAPAN     | 1995 | 13511381754 |
| JAPAN     | JAPAN     | 1995 | 12015968288 |
| VIETNAM   | INDONESIA | 1995 | 11290647784 |
| JAPAN     | INDONESIA | 1995 | 10968840402 |
| INDIA     | INDONESIA | 1995 | 10879296370 |
| CHINA     | INDONESIA | 1995 | 10611767914 |
| VIETNAM   | JAPAN     | 1995 | 10493043807 |
| INDONESIA | INDONESIA | 1995 | 10350165199 |
| VIETNAM   | INDIA     | 1995 | 10147175135 |
| CHINA     | JAPAN     | 1995 |  9967113498 |
| JAPAN     | VIETNAM   | 1995 |  9871240910 |
| INDONESIA | JAPAN     | 1995 |  9554798320 |
| JAPAN     | INDIA     | 1995 |  9224478715 |
| INDIA     | INDIA     | 1995 |  8880501531 |
| VIETNAM   | VIETNAM   | 1995 |  8530802028 |
| INDIA     | VIETNAM   | 1995 |  8470249830 |
| CHINA     | INDIA     | 1995 |  8460557790 |
| INDONESIA | VIETNAM   | 1995 |  8393411088 |
| CHINA     | VIETNAM   | 1995 |  7838238263 |
| INDONESIA | INDIA     | 1995 |  7001659338 |
| JAPAN     | CHINA     | 1996 | 14974943391 |
| INDIA     | CHINA     | 1996 | 14236197987 |
| VIETNAM   | CHINA     | 1996 | 13723231674 |
| JAPAN     | INDONESIA | 1996 | 13304501801 |
| INDONESIA | CHINA     | 1996 | 12444022202 |
| CHINA     | CHINA     | 1996 | 12120893189 |
| INDIA     | JAPAN     | 1996 | 11649117519 |
| INDONESIA | JAPAN     | 1996 | 11345350775 |
| VIETNAM   | JAPAN     | 1996 | 11294284203 |
| INDONESIA | INDONESIA | 1996 | 11111201530 |
| JAPAN     | INDIA     | 1996 | 10871364136 |
| JAPAN     | JAPAN     | 1996 | 10836947449 |
| INDIA     | INDONESIA | 1996 | 10568008435 |
| JAPAN     | VIETNAM   | 1996 | 10503890555 |
| VIETNAM   | INDONESIA | 1996 | 10494783196 |
| INDONESIA | VIETNAM   | 1996 |  9940440124 |
| INDONESIA | INDIA     | 1996 |  9864980677 |
| VIETNAM   | VIETNAM   | 1996 |  9560258720 |
| INDIA     | VIETNAM   | 1996 |  9324764214 |
| INDIA     | INDIA     | 1996 |  9023346020 |
| VIETNAM   | INDIA     | 1996 |  8968179949 |
| CHINA     | INDONESIA | 1996 |  8877441837 |
| CHINA     | JAPAN     | 1996 |  8749420872 |
| CHINA     | VIETNAM   | 1996 |  6973983457 |
| CHINA     | INDIA     | 1996 |  6515658476 |
| JAPAN     | CHINA     | 1997 | 15365039212 |
| INDONESIA | CHINA     | 1997 | 14159930904 |
| VIETNAM   | CHINA     | 1997 | 13678288757 |
| INDIA     | CHINA     | 1997 | 13599028484 |
| JAPAN     | JAPAN     | 1997 | 12921870544 |
| CHINA     | CHINA     | 1997 | 12720975220 |
| VIETNAM   | JAPAN     | 1997 | 11929000810 |
| VIETNAM   | INDONESIA | 1997 | 11325447090 |
| JAPAN     | INDONESIA | 1997 | 10764312416 |
| INDONESIA | JAPAN     | 1997 | 10555558162 |
| INDONESIA | INDONESIA | 1997 | 10416928126 |
| CHINA     | INDONESIA | 1997 | 10317902565 |
| INDIA     | JAPAN     | 1997 | 10272590051 |
| JAPAN     | VIETNAM   | 1997 |  9940032294 |
| CHINA     | JAPAN     | 1997 |  9519485461 |
| JAPAN     | INDIA     | 1997 |  9465935835 |
| INDIA     | INDONESIA | 1997 |  9405085270 |
| INDONESIA | INDIA     | 1997 |  8930955270 |
| INDIA     | INDIA     | 1997 |  8295504178 |
| VIETNAM   | VIETNAM   | 1997 |  8293412532 |
| INDONESIA | VIETNAM   | 1997 |  8116443059 |
| INDIA     | VIETNAM   | 1997 |  7960292262 |
| VIETNAM   | INDIA     | 1997 |  7529455873 |
| CHINA     | VIETNAM   | 1997 |  7038413355 |
| CHINA     | INDIA     | 1997 |  6530770558 |
+-----------+-----------+------+-------------+

--Q3.2

+------------+------------+------+-----------+
| c_city     | s_city     | year | revenue   |
+------------+------------+------+-----------+
| CHINA    3 | CHINA    0 | 1992 | 539864249 |
| CHINA    0 | CHINA    6 | 1992 | 471363128 |
| CHINA    8 | CHINA    1 | 1992 | 421384110 |
| CHINA    6 | CHINA    1 | 1992 | 382204882 |
| CHINA    6 | CHINA    7 | 1992 | 355755835 |
| CHINA    8 | CHINA    9 | 1992 | 349006417 |
| CHINA    7 | CHINA    7 | 1992 | 320232842 |
| CHINA    8 | CHINA    3 | 1992 | 296105733 |
| CHINA    5 | CHINA    3 | 1992 | 277283951 |
| CHINA    6 | CHINA    6 | 1992 | 265527771 |
| CHINA    4 | CHINA    1 | 1992 | 237402078 |
| CHINA    8 | CHINA    6 | 1992 | 234720401 |
| CHINA    4 | CHINA    6 | 1992 | 230169075 |
| CHINA    9 | CHINA    1 | 1992 | 223815249 |
| CHINA    1 | CHINA    1 | 1992 | 223467947 |
| CHINA    2 | CHINA    1 | 1992 | 219559691 |
| CHINA    9 | CHINA    6 | 1992 | 205915890 |
| CHINA    7 | CHINA    9 | 1992 | 201288909 |
| CHINA    1 | CHINA    6 | 1992 | 195622902 |
| CHINA    9 | CHINA    7 | 1992 | 190345063 |
| CHINA    8 | CHINA    4 | 1992 | 174478626 |
| CHINA    1 | CHINA    7 | 1992 | 173803257 |
| CHINA    9 | CHINA    9 | 1992 | 162458028 |
| CHINA    6 | CHINA    0 | 1992 | 154260702 |
| CHINA    8 | CHINA    0 | 1992 | 149794069 |
| CHINA    5 | CHINA    9 | 1992 | 149369922 |
| CHINA    8 | CHINA    8 | 1992 | 147607252 |
| CHINA    6 | CHINA    4 | 1992 | 147137516 |
| CHINA    7 | CHINA    8 | 1992 | 139974858 |
| CHINA    5 | CHINA    6 | 1992 | 138467127 |
| CHINA    3 | CHINA    6 | 1992 | 119521008 |
| CHINA    8 | CHINA    7 | 1992 | 109887269 |
| CHINA    6 | CHINA    3 | 1992 | 107201214 |
| CHINA    9 | CHINA    4 | 1992 | 101504450 |
| CHINA    1 | CHINA    3 | 1992 | 101388208 |
| CHINA    7 | CHINA    0 | 1992 |  98475237 |
| CHINA    5 | CHINA    8 | 1992 |  98370738 |
| CHINA    2 | CHINA    6 | 1992 |  93254616 |
| CHINA    2 | CHINA    4 | 1992 |  86394644 |
| CHINA    3 | CHINA    7 | 1992 |  81027008 |
| CHINA    5 | CHINA    4 | 1992 |  78587418 |
| CHINA    3 | CHINA    9 | 1992 |  78114762 |
| CHINA    2 | CHINA    0 | 1992 |  77786892 |
| CHINA    2 | CHINA    8 | 1992 |  75605732 |
| CHINA    4 | CHINA    3 | 1992 |  75101512 |
| CHINA    7 | CHINA    4 | 1992 |  74119240 |
| CHINA    2 | CHINA    9 | 1992 |  73413108 |
| CHINA    5 | CHINA    7 | 1992 |  73199718 |
| CHINA    4 | CHINA    4 | 1992 |  72839118 |
| CHINA    1 | CHINA    9 | 1992 |  68538220 |
| CHINA    0 | CHINA    8 | 1992 |  65856888 |
| CHINA    0 | CHINA    9 | 1992 |  65590624 |
| CHINA    3 | CHINA    8 | 1992 |  64556586 |
| CHINA    2 | CHINA    7 | 1992 |  63336330 |
| CHINA    4 | CHINA    9 | 1992 |  57645963 |
| CHINA    0 | CHINA    7 | 1992 |  55251918 |
| CHINA    0 | CHINA    1 | 1992 |  51774462 |
| CHINA    6 | CHINA    8 | 1992 |  45676858 |
| CHINA    3 | CHINA    3 | 1992 |  41147560 |
| CHINA    3 | CHINA    4 | 1992 |  36838082 |
| CHINA    5 | CHINA    0 | 1992 |  36554488 |
| CHINA    3 | CHINA    1 | 1992 |  32036313 |
| CHINA    4 | CHINA    8 | 1992 |  31517575 |
| CHINA    0 | CHINA    3 | 1992 |  25524054 |
| CHINA    1 | CHINA    4 | 1992 |  12681846 |
| CHINA    7 | CHINA    3 | 1992 |  11395152 |
| CHINA    6 | CHINA    9 | 1992 |   8642375 |
| CHINA    8 | CHINA    6 | 1993 | 638396852 |
| CHINA    7 | CHINA    6 | 1993 | 576731239 |
| CHINA    2 | CHINA    6 | 1993 | 528008729 |
| CHINA    8 | CHINA    9 | 1993 | 522412584 |
| CHINA    8 | CHINA    7 | 1993 | 475478848 |
| CHINA    8 | CHINA    1 | 1993 | 452064153 |
| CHINA    0 | CHINA    1 | 1993 | 425902649 |
| CHINA    9 | CHINA    1 | 1993 | 405252987 |
| CHINA    6 | CHINA    9 | 1993 | 385005953 |
| CHINA    8 | CHINA    8 | 1993 | 382884778 |
| CHINA    0 | CHINA    6 | 1993 | 344911487 |
| CHINA    6 | CHINA    7 | 1993 | 341436211 |
| CHINA    3 | CHINA    6 | 1993 | 291652051 |
| CHINA    7 | CHINA    1 | 1993 | 257769861 |
| CHINA    8 | CHINA    0 | 1993 | 231981252 |
| CHINA    4 | CHINA    6 | 1993 | 215180968 |
| CHINA    3 | CHINA    0 | 1993 | 213320777 |
| CHINA    9 | CHINA    6 | 1993 | 207281000 |
| CHINA    5 | CHINA    9 | 1993 | 206555882 |
| CHINA    6 | CHINA    1 | 1993 | 205665388 |
| CHINA    5 | CHINA    1 | 1993 | 193491875 |
| CHINA    2 | CHINA    9 | 1993 | 193324425 |
| CHINA    5 | CHINA    8 | 1993 | 190521023 |
| CHINA    7 | CHINA    0 | 1993 | 183487919 |
| CHINA    0 | CHINA    9 | 1993 | 170223958 |
| CHINA    6 | CHINA    8 | 1993 | 166821272 |
| CHINA    3 | CHINA    8 | 1993 | 163053528 |
| CHINA    2 | CHINA    0 | 1993 | 158276154 |
| CHINA    3 | CHINA    1 | 1993 | 153652018 |
| CHINA    5 | CHINA    6 | 1993 | 151359347 |
| CHINA    6 | CHINA    0 | 1993 | 140494698 |
| CHINA    8 | CHINA    4 | 1993 | 139857147 |
| CHINA    2 | CHINA    7 | 1993 | 136009418 |
| CHINA    5 | CHINA    7 | 1993 | 133892119 |
| CHINA    9 | CHINA    9 | 1993 | 118965507 |
| CHINA    1 | CHINA    1 | 1993 | 108898379 |
| CHINA    6 | CHINA    6 | 1993 | 100311475 |
| CHINA    0 | CHINA    4 | 1993 |  93483068 |
| CHINA    1 | CHINA    4 | 1993 |  87714152 |
| CHINA    4 | CHINA    1 | 1993 |  87690658 |
| CHINA    4 | CHINA    7 | 1993 |  83701574 |
| CHINA    1 | CHINA    0 | 1993 |  82670983 |
| CHINA    7 | CHINA    4 | 1993 |  77396461 |
| CHINA    5 | CHINA    4 | 1993 |  73556161 |
| CHINA    4 | CHINA    8 | 1993 |  72203335 |
| CHINA    0 | CHINA    7 | 1993 |  70395334 |
| CHINA    3 | CHINA    4 | 1993 |  64771003 |
| CHINA    7 | CHINA    8 | 1993 |  64514099 |
| CHINA    3 | CHINA    7 | 1993 |  62868516 |
| CHINA    8 | CHINA    3 | 1993 |  56504804 |
| CHINA    2 | CHINA    4 | 1993 |  56031779 |
| CHINA    1 | CHINA    7 | 1993 |  48951262 |
| CHINA    7 | CHINA    3 | 1993 |  45962220 |
| CHINA    4 | CHINA    9 | 1993 |  43158138 |
| CHINA    7 | CHINA    9 | 1993 |  42611979 |
| CHINA    2 | CHINA    8 | 1993 |  38092546 |
| CHINA    1 | CHINA    9 | 1993 |  29665374 |
| CHINA    1 | CHINA    3 | 1993 |  23991216 |
| CHINA    6 | CHINA    6 | 1994 | 596294890 |
| CHINA    8 | CHINA    6 | 1994 | 542104721 |
| CHINA    6 | CHINA    1 | 1994 | 504359553 |
| CHINA    3 | CHINA    7 | 1994 | 476727294 |
| CHINA    3 | CHINA    6 | 1994 | 476349724 |
| CHINA    8 | CHINA    9 | 1994 | 427241348 |
| CHINA    6 | CHINA    9 | 1994 | 358191581 |
| CHINA    9 | CHINA    6 | 1994 | 352344057 |
| CHINA    3 | CHINA    0 | 1994 | 351708546 |
| CHINA    8 | CHINA    0 | 1994 | 351131413 |
| CHINA    3 | CHINA    3 | 1994 | 339279574 |
| CHINA    0 | CHINA    1 | 1994 | 298307857 |
| CHINA    0 | CHINA    7 | 1994 | 289536010 |
| CHINA    0 | CHINA    6 | 1994 | 285639032 |
| CHINA    7 | CHINA    6 | 1994 | 263170455 |
| CHINA    2 | CHINA    8 | 1994 | 250332990 |
| CHINA    6 | CHINA    4 | 1994 | 235897763 |
| CHINA    5 | CHINA    1 | 1994 | 234681515 |
| CHINA    8 | CHINA    7 | 1994 | 234390101 |
| CHINA    1 | CHINA    6 | 1994 | 232792764 |
| CHINA    8 | CHINA    1 | 1994 | 223808842 |
| CHINA    4 | CHINA    6 | 1994 | 209522926 |
| CHINA    8 | CHINA    4 | 1994 | 208632636 |
| CHINA    7 | CHINA    3 | 1994 | 202424117 |
| CHINA    4 | CHINA    7 | 1994 | 185487544 |
| CHINA    2 | CHINA    7 | 1994 | 183551771 |
| CHINA    7 | CHINA    1 | 1994 | 178421732 |
| CHINA    4 | CHINA    1 | 1994 | 176262868 |
| CHINA    5 | CHINA    6 | 1994 | 173651872 |
| CHINA    0 | CHINA    4 | 1994 | 173584501 |
| CHINA    8 | CHINA    8 | 1994 | 172179808 |
| CHINA    9 | CHINA    1 | 1994 | 169617585 |
| CHINA    0 | CHINA    9 | 1994 | 167569085 |
| CHINA    5 | CHINA    8 | 1994 | 162066559 |
| CHINA    7 | CHINA    9 | 1994 | 161041255 |
| CHINA    5 | CHINA    4 | 1994 | 154820955 |
| CHINA    7 | CHINA    0 | 1994 | 152844960 |
| CHINA    2 | CHINA    6 | 1994 | 149839190 |
| CHINA    7 | CHINA    8 | 1994 | 149536114 |
| CHINA    1 | CHINA    4 | 1994 | 142403628 |
| CHINA    9 | CHINA    9 | 1994 | 131064832 |
| CHINA    2 | CHINA    1 | 1994 | 124489283 |
| CHINA    2 | CHINA    0 | 1994 | 114263273 |
| CHINA    5 | CHINA    7 | 1994 | 113311766 |
| CHINA    8 | CHINA    3 | 1994 | 112573609 |
| CHINA    3 | CHINA    4 | 1994 | 104903651 |
| CHINA    4 | CHINA    0 | 1994 | 101914439 |
| CHINA    3 | CHINA    1 | 1994 |  98253251 |
| CHINA    1 | CHINA    7 | 1994 |  94582288 |
| CHINA    4 | CHINA    4 | 1994 |  92818317 |
| CHINA    1 | CHINA    9 | 1994 |  85220541 |
| CHINA    6 | CHINA    3 | 1994 |  84604801 |
| CHINA    0 | CHINA    3 | 1994 |  77574978 |
| CHINA    1 | CHINA    3 | 1994 |  74435316 |
| CHINA    4 | CHINA    9 | 1994 |  72622300 |
| CHINA    3 | CHINA    8 | 1994 |  72559366 |
| CHINA    9 | CHINA    0 | 1994 |  69298222 |
| CHINA    3 | CHINA    9 | 1994 |  67472592 |
| CHINA    6 | CHINA    8 | 1994 |  66271372 |
| CHINA    7 | CHINA    4 | 1994 |  59634606 |
| CHINA    2 | CHINA    9 | 1994 |  56882136 |
| CHINA    1 | CHINA    1 | 1994 |  56592337 |
| CHINA    5 | CHINA    9 | 1994 |  52879724 |
| CHINA    9 | CHINA    4 | 1994 |  49324497 |
| CHINA    2 | CHINA    3 | 1994 |  45042384 |
| CHINA    7 | CHINA    7 | 1994 |  44458451 |
| CHINA    5 | CHINA    0 | 1994 |  39091925 |
| CHINA    9 | CHINA    3 | 1994 |  39082405 |
| CHINA    0 | CHINA    8 | 1994 |  28203459 |
| CHINA    6 | CHINA    7 | 1994 |  27243775 |
| CHINA    0 | CHINA    0 | 1994 |  15591040 |
| CHINA    2 | CHINA    6 | 1995 | 832176707 |
| CHINA    8 | CHINA    6 | 1995 | 793322102 |
| CHINA    3 | CHINA    7 | 1995 | 505446788 |
| CHINA    7 | CHINA    9 | 1995 | 483519933 |
| CHINA    4 | CHINA    6 | 1995 | 440320366 |
| CHINA    8 | CHINA    1 | 1995 | 394522570 |
| CHINA    7 | CHINA    1 | 1995 | 393861389 |
| CHINA    5 | CHINA    1 | 1995 | 343166828 |
| CHINA    1 | CHINA    7 | 1995 | 341736584 |
| CHINA    8 | CHINA    7 | 1995 | 323623203 |
| CHINA    6 | CHINA    6 | 1995 | 312876143 |
| CHINA    3 | CHINA    6 | 1995 | 306516324 |
| CHINA    7 | CHINA    6 | 1995 | 294840537 |
| CHINA    3 | CHINA    3 | 1995 | 290066240 |
| CHINA    8 | CHINA    3 | 1995 | 289182495 |
| CHINA    3 | CHINA    1 | 1995 | 288853766 |
| CHINA    0 | CHINA    1 | 1995 | 279082523 |
| CHINA    0 | CHINA    8 | 1995 | 265291443 |
| CHINA    1 | CHINA    6 | 1995 | 262283412 |
| CHINA    4 | CHINA    1 | 1995 | 246559891 |
| CHINA    2 | CHINA    8 | 1995 | 246465167 |
| CHINA    6 | CHINA    7 | 1995 | 246385862 |
| CHINA    9 | CHINA    6 | 1995 | 231314393 |
| CHINA    2 | CHINA    7 | 1995 | 224354491 |
| CHINA    4 | CHINA    7 | 1995 | 222368398 |
| CHINA    0 | CHINA    7 | 1995 | 221334917 |
| CHINA    6 | CHINA    3 | 1995 | 217756587 |
| CHINA    6 | CHINA    9 | 1995 | 215736018 |
| CHINA    4 | CHINA    9 | 1995 | 210496516 |
| CHINA    0 | CHINA    6 | 1995 | 197891458 |
| CHINA    8 | CHINA    9 | 1995 | 192018213 |
| CHINA    7 | CHINA    0 | 1995 | 188804482 |
| CHINA    5 | CHINA    6 | 1995 | 186378531 |
| CHINA    6 | CHINA    1 | 1995 | 165831073 |
| CHINA    1 | CHINA    3 | 1995 | 165118263 |
| CHINA    6 | CHINA    8 | 1995 | 157640218 |
| CHINA    1 | CHINA    1 | 1995 | 150838433 |
| CHINA    1 | CHINA    4 | 1995 | 147632879 |
| CHINA    6 | CHINA    0 | 1995 | 147314401 |
| CHINA    5 | CHINA    4 | 1995 | 142820978 |
| CHINA    5 | CHINA    9 | 1995 | 141416829 |
| CHINA    2 | CHINA    0 | 1995 | 135608473 |
| CHINA    5 | CHINA    7 | 1995 | 131596218 |
| CHINA    0 | CHINA    4 | 1995 | 129159370 |
| CHINA    3 | CHINA    9 | 1995 | 126837748 |
| CHINA    8 | CHINA    0 | 1995 | 126564932 |
| CHINA    0 | CHINA    3 | 1995 | 121337041 |
| CHINA    7 | CHINA    7 | 1995 | 118697587 |
| CHINA    5 | CHINA    8 | 1995 | 116538842 |
| CHINA    8 | CHINA    8 | 1995 | 110161904 |
| CHINA    9 | CHINA    0 | 1995 | 109582187 |
| CHINA    9 | CHINA    1 | 1995 | 103455098 |
| CHINA    2 | CHINA    1 | 1995 | 100264691 |
| CHINA    7 | CHINA    3 | 1995 |  99011859 |
| CHINA    3 | CHINA    0 | 1995 |  90383390 |
| CHINA    4 | CHINA    3 | 1995 |  89908903 |
| CHINA    7 | CHINA    8 | 1995 |  81425699 |
| CHINA    3 | CHINA    4 | 1995 |  77577579 |
| CHINA    4 | CHINA    8 | 1995 |  74805746 |
| CHINA    9 | CHINA    7 | 1995 |  74597020 |
| CHINA    9 | CHINA    9 | 1995 |  73514511 |
| CHINA    5 | CHINA    0 | 1995 |  73274726 |
| CHINA    8 | CHINA    4 | 1995 |  61708487 |
| CHINA    1 | CHINA    0 | 1995 |  58753734 |
| CHINA    3 | CHINA    8 | 1995 |  57133566 |
| CHINA    9 | CHINA    4 | 1995 |  53259334 |
| CHINA    1 | CHINA    9 | 1995 |  46177797 |
| CHINA    2 | CHINA    4 | 1995 |  45147325 |
| CHINA    0 | CHINA    0 | 1995 |  43963173 |
| CHINA    0 | CHINA    9 | 1995 |  40184107 |
| CHINA    1 | CHINA    8 | 1995 |  18859188 |
| CHINA    8 | CHINA    7 | 1996 | 621957444 |
| CHINA    3 | CHINA    9 | 1996 | 530082848 |
| CHINA    8 | CHINA    6 | 1996 | 525755549 |
| CHINA    8 | CHINA    1 | 1996 | 399229343 |
| CHINA    6 | CHINA    7 | 1996 | 365540749 |
| CHINA    8 | CHINA    8 | 1996 | 351864283 |
| CHINA    1 | CHINA    6 | 1996 | 329186504 |
| CHINA    9 | CHINA    6 | 1996 | 321113085 |
| CHINA    3 | CHINA    6 | 1996 | 318264871 |
| CHINA    2 | CHINA    6 | 1996 | 315233397 |
| CHINA    2 | CHINA    9 | 1996 | 285852841 |
| CHINA    9 | CHINA    9 | 1996 | 264510548 |
| CHINA    5 | CHINA    6 | 1996 | 261385523 |
| CHINA    8 | CHINA    9 | 1996 | 259497265 |
| CHINA    6 | CHINA    6 | 1996 | 258200131 |
| CHINA    4 | CHINA    9 | 1996 | 257345949 |
| CHINA    6 | CHINA    9 | 1996 | 247667288 |
| CHINA    2 | CHINA    7 | 1996 | 234569026 |
| CHINA    2 | CHINA    1 | 1996 | 218568966 |
| CHINA    4 | CHINA    1 | 1996 | 207383476 |
| CHINA    0 | CHINA    1 | 1996 | 204596428 |
| CHINA    3 | CHINA    0 | 1996 | 204375870 |
| CHINA    4 | CHINA    0 | 1996 | 202299286 |
| CHINA    4 | CHINA    4 | 1996 | 191983261 |
| CHINA    4 | CHINA    8 | 1996 | 183961012 |
| CHINA    4 | CHINA    6 | 1996 | 183872085 |
| CHINA    6 | CHINA    8 | 1996 | 182132356 |
| CHINA    7 | CHINA    9 | 1996 | 170941341 |
| CHINA    0 | CHINA    6 | 1996 | 168082672 |
| CHINA    1 | CHINA    7 | 1996 | 165942066 |
| CHINA    1 | CHINA    9 | 1996 | 165878775 |
| CHINA    9 | CHINA    8 | 1996 | 156009357 |
| CHINA    7 | CHINA    7 | 1996 | 155842944 |
| CHINA    2 | CHINA    0 | 1996 | 147709906 |
| CHINA    5 | CHINA    7 | 1996 | 147257366 |
| CHINA    1 | CHINA    8 | 1996 | 141840928 |
| CHINA    2 | CHINA    4 | 1996 | 136244052 |
| CHINA    9 | CHINA    0 | 1996 | 130997019 |
| CHINA    1 | CHINA    0 | 1996 | 124362038 |
| CHINA    0 | CHINA    9 | 1996 | 114011231 |
| CHINA    7 | CHINA    3 | 1996 | 112398764 |
| CHINA    4 | CHINA    7 | 1996 | 110567337 |
| CHINA    3 | CHINA    4 | 1996 | 109269982 |
| CHINA    5 | CHINA    1 | 1996 | 107482704 |
| CHINA    6 | CHINA    4 | 1996 | 105485170 |
| CHINA    1 | CHINA    4 | 1996 | 105320270 |
| CHINA    0 | CHINA    7 | 1996 | 102545071 |
| CHINA    2 | CHINA    3 | 1996 | 100407151 |
| CHINA    0 | CHINA    4 | 1996 |  95913303 |
| CHINA    7 | CHINA    0 | 1996 |  94706269 |
| CHINA    6 | CHINA    1 | 1996 |  86949951 |
| CHINA    8 | CHINA    3 | 1996 |  84157344 |
| CHINA    2 | CHINA    8 | 1996 |  83176903 |
| CHINA    5 | CHINA    9 | 1996 |  83104330 |
| CHINA    7 | CHINA    8 | 1996 |  81490639 |
| CHINA    9 | CHINA    3 | 1996 |  79655829 |
| CHINA    5 | CHINA    0 | 1996 |  77489995 |
| CHINA    8 | CHINA    0 | 1996 |  76989056 |
| CHINA    9 | CHINA    1 | 1996 |  72011031 |
| CHINA    7 | CHINA    4 | 1996 |  64764322 |
| CHINA    5 | CHINA    4 | 1996 |  62827767 |
| CHINA    5 | CHINA    8 | 1996 |  62673237 |
| CHINA    7 | CHINA    6 | 1996 |  61880459 |
| CHINA    3 | CHINA    7 | 1996 |  56642844 |
| CHINA    3 | CHINA    1 | 1996 |  50799366 |
| CHINA    3 | CHINA    3 | 1996 |  42601269 |
| CHINA    4 | CHINA    3 | 1996 |  38290290 |
| CHINA    3 | CHINA    8 | 1996 |  21263056 |
| CHINA    7 | CHINA    1 | 1996 |  14836937 |
| CHINA    5 | CHINA    3 | 1996 |  13611339 |
| CHINA    1 | CHINA    3 | 1996 |   8430793 |
| CHINA    1 | CHINA    1 | 1996 |   1601332 |
| CHINA    1 | CHINA    7 | 1997 | 664436721 |
| CHINA    8 | CHINA    9 | 1997 | 585552148 |
| CHINA    8 | CHINA    6 | 1997 | 543571889 |
| CHINA    8 | CHINA    7 | 1997 | 516131917 |
| CHINA    6 | CHINA    7 | 1997 | 467477883 |
| CHINA    3 | CHINA    9 | 1997 | 444914344 |
| CHINA    5 | CHINA    6 | 1997 | 353316321 |
| CHINA    6 | CHINA    4 | 1997 | 338136205 |
| CHINA    0 | CHINA    7 | 1997 | 329137493 |
| CHINA    5 | CHINA    1 | 1997 | 328142466 |
| CHINA    8 | CHINA    4 | 1997 | 308276385 |
| CHINA    6 | CHINA    9 | 1997 | 306814317 |
| CHINA    5 | CHINA    9 | 1997 | 301145803 |
| CHINA    7 | CHINA    1 | 1997 | 299575802 |
| CHINA    8 | CHINA    8 | 1997 | 282083295 |
| CHINA    4 | CHINA    9 | 1997 | 280242025 |
| CHINA    9 | CHINA    1 | 1997 | 253155313 |
| CHINA    4 | CHINA    6 | 1997 | 234247182 |
| CHINA    5 | CHINA    0 | 1997 | 217246162 |
| CHINA    9 | CHINA    4 | 1997 | 215424663 |
| CHINA    0 | CHINA    6 | 1997 | 211152240 |
| CHINA    3 | CHINA    6 | 1997 | 205982217 |
| CHINA    7 | CHINA    6 | 1997 | 196440117 |
| CHINA    1 | CHINA    6 | 1997 | 195757737 |
| CHINA    2 | CHINA    3 | 1997 | 189836909 |
| CHINA    7 | CHINA    8 | 1997 | 189291379 |
| CHINA    9 | CHINA    6 | 1997 | 189236146 |
| CHINA    3 | CHINA    1 | 1997 | 188537684 |
| CHINA    9 | CHINA    7 | 1997 | 182516267 |
| CHINA    0 | CHINA    0 | 1997 | 182459980 |
| CHINA    5 | CHINA    8 | 1997 | 177077882 |
| CHINA    2 | CHINA    6 | 1997 | 176030529 |
| CHINA    2 | CHINA    1 | 1997 | 168770050 |
| CHINA    8 | CHINA    0 | 1997 | 167294093 |
| CHINA    4 | CHINA    3 | 1997 | 161980658 |
| CHINA    3 | CHINA    4 | 1997 | 154433882 |
| CHINA    6 | CHINA    6 | 1997 | 153336736 |
| CHINA    6 | CHINA    3 | 1997 | 151596497 |
| CHINA    8 | CHINA    1 | 1997 | 145432603 |
| CHINA    1 | CHINA    4 | 1997 | 126773981 |
| CHINA    1 | CHINA    0 | 1997 | 120594770 |
| CHINA    7 | CHINA    3 | 1997 | 119618460 |
| CHINA    6 | CHINA    1 | 1997 | 119529805 |
| CHINA    2 | CHINA    9 | 1997 | 114591288 |
| CHINA    7 | CHINA    7 | 1997 | 111335941 |
| CHINA    5 | CHINA    3 | 1997 | 111044153 |
| CHINA    6 | CHINA    0 | 1997 | 104404276 |
| CHINA    1 | CHINA    1 | 1997 |  98869501 |
| CHINA    7 | CHINA    0 | 1997 |  97198605 |
| CHINA    7 | CHINA    9 | 1997 |  92872632 |
| CHINA    0 | CHINA    9 | 1997 |  91097832 |
| CHINA    9 | CHINA    9 | 1997 |  86479272 |
| CHINA    2 | CHINA    7 | 1997 |  79380820 |
| CHINA    9 | CHINA    0 | 1997 |  78499693 |
| CHINA    1 | CHINA    9 | 1997 |  73589328 |
| CHINA    2 | CHINA    8 | 1997 |  71633835 |
| CHINA    8 | CHINA    3 | 1997 |  70505885 |
| CHINA    3 | CHINA    0 | 1997 |  61039282 |
| CHINA    0 | CHINA    3 | 1997 |  58325113 |
| CHINA    5 | CHINA    7 | 1997 |  55476389 |
| CHINA    4 | CHINA    7 | 1997 |  46480159 |
| CHINA    0 | CHINA    1 | 1997 |  38223038 |
| CHINA    4 | CHINA    1 | 1997 |  21636342 |
| CHINA    9 | CHINA    3 | 1997 |  13092788 |
| CHINA    6 | CHINA    8 | 1997 |   2490092 |
+------------+------------+------+-----------+

--Q3.3
+------------+------------+------+-----------+
| c_city     | s_city     | year | revenue   |
+------------+------------+------+-----------+
| UNITED KI0 | UNITED KI7 | 1992 | 251282102 |
| UNITED KI0 | UNITED KI0 | 1992 | 170005406 |
| UNITED KI7 | UNITED KI7 | 1992 |  36835396 |
| UNITED KI0 | UNITED KI7 | 1993 | 560335810 |
| UNITED KI0 | UNITED KI0 | 1993 | 294257692 |
| UNITED KI7 | UNITED KI0 | 1993 | 159005896 |
| UNITED KI7 | UNITED KI7 | 1993 | 139029264 |
| UNITED KI0 | UNITED KI7 | 1994 | 739847089 |
| UNITED KI0 | UNITED KI0 | 1994 | 302339390 |
| UNITED KI7 | UNITED KI7 | 1994 | 275609814 |
| UNITED KI7 | UNITED KI0 | 1994 | 117654093 |
| UNITED KI0 | UNITED KI7 | 1995 | 540994655 |
| UNITED KI0 | UNITED KI0 | 1995 | 230825439 |
| UNITED KI7 | UNITED KI0 | 1995 | 197347696 |
| UNITED KI7 | UNITED KI7 | 1995 | 136620517 |
| UNITED KI0 | UNITED KI7 | 1996 | 448412094 |
| UNITED KI0 | UNITED KI0 | 1996 | 203511607 |
| UNITED KI7 | UNITED KI7 | 1996 |  94528075 |
| UNITED KI7 | UNITED KI0 | 1996 |  35448536 |
| UNITED KI7 | UNITED KI0 | 1997 | 289323850 |
| UNITED KI7 | UNITED KI7 | 1997 | 214791175 |
| UNITED KI0 | UNITED KI7 | 1997 | 196510174 |
| UNITED KI0 | UNITED KI0 | 1997 | 125066127 |
+------------+------------+------+-----------+

--Q3.4
+------------+------------+------+-----------+
| c_city     | s_city     | year | revenue   |
+------------+------------+------+-----------+
| UNITED KI7 | KENYA    4 | 1997 | 170083300 |
| UNITED KI0 | MOZAMBIQU1 | 1997 | 155234463 |
| UNITED KI0 | KENYA    4 | 1997 |  87283610 |
+------------+------------+------+-----------+

--Q4.1
+------+---------------+-------------+
| year | c_nation      | profit      |
+------+---------------+-------------+
| 1992 | ARGENTINA     | 13746243380 |
| 1992 | BRAZIL        | 15762831144 |
| 1992 | CANADA        | 17477043721 |
| 1992 | PERU          | 14698567030 |
| 1992 | UNITED STATES | 14043501956 |
| 1993 | ARGENTINA     | 13992888207 |
| 1993 | BRAZIL        | 15146262693 |
| 1993 | CANADA        | 12463985574 |
| 1993 | PERU          | 11385007831 |
| 1993 | UNITED STATES | 10651361815 |
| 1994 | ARGENTINA     | 13128610315 |
| 1994 | BRAZIL        | 13764866493 |
| 1994 | CANADA        | 13723188154 |
| 1994 | PERU          | 12784683808 |
| 1994 | UNITED STATES | 12554422837 |
| 1995 | ARGENTINA     | 14337205612 |
| 1995 | BRAZIL        | 15068918320 |
| 1995 | CANADA        | 14529005783 |
| 1995 | PERU          | 13086675480 |
| 1995 | UNITED STATES | 11330297649 |
| 1996 | ARGENTINA     | 13659108915 |
| 1996 | BRAZIL        | 12660837584 |
| 1996 | CANADA        | 14558903190 |
| 1996 | PERU          | 14162285166 |
| 1996 | UNITED STATES | 11117076866 |
| 1997 | ARGENTINA     | 12556399750 |
| 1997 | BRAZIL        | 13961587144 |
| 1997 | CANADA        | 15567856947 |
| 1997 | PERU          | 13595325340 |
| 1997 | UNITED STATES | 10779073839 |
| 1998 | ARGENTINA     |  7843424759 |
| 1998 | BRAZIL        |  8853904827 |
| 1998 | CANADA        |  8286104334 |
| 1998 | PERU          |  5822590950 |
| 1998 | UNITED STATES |  8526236814 |
+------+---------------+-------------+

--Q4.2
+------+---------------+------------+------------+
| year | s_nation      | p_category | profit     |
+------+---------------+------------+------------+
| 1997 | ARGENTINA     | MFGR#11    | 1636950553 |
| 1997 | ARGENTINA     | MFGR#12    | 1265547847 |
| 1997 | ARGENTINA     | MFGR#13    | 1505131346 |
| 1997 | ARGENTINA     | MFGR#14    | 1405447137 |
| 1997 | ARGENTINA     | MFGR#15    | 1564085340 |
| 1997 | ARGENTINA     | MFGR#21    | 1335009490 |
| 1997 | ARGENTINA     | MFGR#22    | 1309054179 |
| 1997 | ARGENTINA     | MFGR#23    | 1305213794 |
| 1997 | ARGENTINA     | MFGR#24    | 1089725126 |
| 1997 | ARGENTINA     | MFGR#25    | 1291995512 |
| 1997 | BRAZIL        | MFGR#11    |  721240147 |
| 1997 | BRAZIL        | MFGR#12    |  928318830 |
| 1997 | BRAZIL        | MFGR#13    | 1164674879 |
| 1997 | BRAZIL        | MFGR#14    | 1215622587 |
| 1997 | BRAZIL        | MFGR#15    |  940971658 |
| 1997 | BRAZIL        | MFGR#21    | 1158909618 |
| 1997 | BRAZIL        | MFGR#22    | 1251221641 |
| 1997 | BRAZIL        | MFGR#23    | 1552552455 |
| 1997 | BRAZIL        | MFGR#24    |  929057361 |
| 1997 | BRAZIL        | MFGR#25    |  574645288 |
| 1997 | CANADA        | MFGR#11    | 1170341370 |
| 1997 | CANADA        | MFGR#12    | 1220238121 |
| 1997 | CANADA        | MFGR#13    | 1245774025 |
| 1997 | CANADA        | MFGR#14    | 1032046642 |
| 1997 | CANADA        | MFGR#15    |  738650612 |
| 1997 | CANADA        | MFGR#21    | 1476055209 |
| 1997 | CANADA        | MFGR#22    | 1239005798 |
| 1997 | CANADA        | MFGR#23    |  869393804 |
| 1997 | CANADA        | MFGR#24    | 1466964051 |
| 1997 | CANADA        | MFGR#25    | 1358922727 |
| 1997 | PERU          | MFGR#11    | 1031023174 |
| 1997 | PERU          | MFGR#12    |  731821491 |
| 1997 | PERU          | MFGR#13    | 1044642877 |
| 1997 | PERU          | MFGR#14    |  654877417 |
| 1997 | PERU          | MFGR#15    | 1201769474 |
| 1997 | PERU          | MFGR#21    | 1275496672 |
| 1997 | PERU          | MFGR#22    |  599324545 |
| 1997 | PERU          | MFGR#23    | 1200754744 |
| 1997 | PERU          | MFGR#24    |  942152801 |
| 1997 | PERU          | MFGR#25    | 1064322995 |
| 1997 | UNITED STATES | MFGR#11    | 2365218925 |
| 1997 | UNITED STATES | MFGR#12    | 1132346574 |
| 1997 | UNITED STATES | MFGR#13    | 2460882362 |
| 1997 | UNITED STATES | MFGR#14    | 2190816877 |
| 1997 | UNITED STATES | MFGR#15    | 1687829921 |
| 1997 | UNITED STATES | MFGR#21    | 2125880770 |
| 1997 | UNITED STATES | MFGR#22    | 2013348097 |
| 1997 | UNITED STATES | MFGR#23    | 2570581084 |
| 1997 | UNITED STATES | MFGR#24    | 2724372315 |
| 1997 | UNITED STATES | MFGR#25    | 1480012758 |
| 1998 | ARGENTINA     | MFGR#11    |  783662770 |
| 1998 | ARGENTINA     | MFGR#12    |  472818450 |
| 1998 | ARGENTINA     | MFGR#13    |  585091533 |
| 1998 | ARGENTINA     | MFGR#14    |  507297527 |
| 1998 | ARGENTINA     | MFGR#15    |  549185408 |
| 1998 | ARGENTINA     | MFGR#21    |  972928972 |
| 1998 | ARGENTINA     | MFGR#22    | 1508294213 |
| 1998 | ARGENTINA     | MFGR#23    |  517896738 |
| 1998 | ARGENTINA     | MFGR#24    |  240754731 |
| 1998 | ARGENTINA     | MFGR#25    |  757030162 |
| 1998 | BRAZIL        | MFGR#11    |  826283793 |
| 1998 | BRAZIL        | MFGR#12    |  482293349 |
| 1998 | BRAZIL        | MFGR#13    | 1037202334 |
| 1998 | BRAZIL        | MFGR#14    |  743598666 |
| 1998 | BRAZIL        | MFGR#15    |  584176304 |
| 1998 | BRAZIL        | MFGR#21    |  557259779 |
| 1998 | BRAZIL        | MFGR#22    |  535654445 |
| 1998 | BRAZIL        | MFGR#23    |  403656721 |
| 1998 | BRAZIL        | MFGR#24    | 1305217551 |
| 1998 | BRAZIL        | MFGR#25    | 1109801463 |
| 1998 | CANADA        | MFGR#11    |  936169617 |
| 1998 | CANADA        | MFGR#12    | 1017751308 |
| 1998 | CANADA        | MFGR#13    |  850046376 |
| 1998 | CANADA        | MFGR#14    |  808138010 |
| 1998 | CANADA        | MFGR#15    |  701990010 |
| 1998 | CANADA        | MFGR#21    |  402611051 |
| 1998 | CANADA        | MFGR#22    |  382705122 |
| 1998 | CANADA        | MFGR#23    |  509674722 |
| 1998 | CANADA        | MFGR#24    | 1003021250 |
| 1998 | CANADA        | MFGR#25    |  574602788 |
| 1998 | PERU          | MFGR#11    |  552608732 |
| 1998 | PERU          | MFGR#12    |  500581456 |
| 1998 | PERU          | MFGR#13    |  894607711 |
| 1998 | PERU          | MFGR#14    |  386487826 |
| 1998 | PERU          | MFGR#15    | 1044780577 |
| 1998 | PERU          | MFGR#21    |  184346232 |
| 1998 | PERU          | MFGR#22    |  674942976 |
| 1998 | PERU          | MFGR#23    |  665523956 |
| 1998 | PERU          | MFGR#24    |  631374203 |
| 1998 | PERU          | MFGR#25    |  602609608 |
| 1998 | UNITED STATES | MFGR#11    | 1230069867 |
| 1998 | UNITED STATES | MFGR#12    | 1557720319 |
| 1998 | UNITED STATES | MFGR#13    |  999206739 |
| 1998 | UNITED STATES | MFGR#14    |  605040268 |
| 1998 | UNITED STATES | MFGR#15    |  850219215 |
| 1998 | UNITED STATES | MFGR#21    | 1032550760 |
| 1998 | UNITED STATES | MFGR#22    | 1370141401 |
| 1998 | UNITED STATES | MFGR#23    | 1226632297 |
| 1998 | UNITED STATES | MFGR#24    | 1528135100 |
| 1998 | UNITED STATES | MFGR#25    | 1127867278 |
+------+---------------+------------+------------+

--Q4.3
+------+------------+-----------+-----------+
| year | s_city     | p_brand   | profit    |
+------+------------+-----------+-----------+
| 1997 | UNITED ST0 | MFGR#1410 |  58481513 |
| 1997 | UNITED ST0 | MFGR#1412 |  33582225 |
| 1997 | UNITED ST0 | MFGR#1413 | 135625490 |
| 1997 | UNITED ST0 | MFGR#1414 |  18581969 |
| 1997 | UNITED ST0 | MFGR#142  | 164080005 |
| 1997 | UNITED ST0 | MFGR#1420 |  30831591 |
| 1997 | UNITED ST0 | MFGR#1424 |   4085253 |
| 1997 | UNITED ST0 | MFGR#1425 | 163183170 |
| 1997 | UNITED ST0 | MFGR#1427 |  87578288 |
| 1997 | UNITED ST0 | MFGR#1428 | 109488143 |
| 1997 | UNITED ST0 | MFGR#143  | 198055627 |
| 1997 | UNITED ST0 | MFGR#1430 |  52544552 |
| 1997 | UNITED ST0 | MFGR#1432 | 158742311 |
| 1997 | UNITED ST0 | MFGR#144  |  43479982 |
| 1997 | UNITED ST0 | MFGR#1440 |  40412893 |
| 1997 | UNITED ST0 | MFGR#145  | 175568435 |
| 1997 | UNITED ST1 | MFGR#141  |  11932912 |
| 1997 | UNITED ST1 | MFGR#1411 |  40637463 |
| 1997 | UNITED ST1 | MFGR#1415 |  27562355 |
| 1997 | UNITED ST1 | MFGR#1421 | 100271780 |
| 1997 | UNITED ST1 | MFGR#1422 | 103286764 |
| 1997 | UNITED ST1 | MFGR#1423 | 106114459 |
| 1997 | UNITED ST1 | MFGR#1427 | 157715681 |
| 1997 | UNITED ST1 | MFGR#1428 |  91550168 |
| 1997 | UNITED ST1 | MFGR#1430 |  56560173 |
| 1997 | UNITED ST1 | MFGR#1431 | 248448914 |
| 1997 | UNITED ST1 | MFGR#1435 |    994228 |
| 1997 | UNITED ST1 | MFGR#144  |  55729825 |
| 1997 | UNITED ST1 | MFGR#145  | 118034196 |
| 1997 | UNITED ST1 | MFGR#146  |  99170724 |
| 1997 | UNITED ST1 | MFGR#147  |   5123001 |
| 1997 | UNITED ST2 | MFGR#141  | 111908637 |
| 1997 | UNITED ST2 | MFGR#1414 |  96864725 |
| 1997 | UNITED ST2 | MFGR#1415 | 123601050 |
| 1997 | UNITED ST2 | MFGR#1421 |  21014618 |
| 1997 | UNITED ST2 | MFGR#1427 |  46524767 |
| 1997 | UNITED ST2 | MFGR#1429 |  18800062 |
| 1997 | UNITED ST2 | MFGR#1431 |  79199532 |
| 1997 | UNITED ST2 | MFGR#1432 |  53841788 |
| 1997 | UNITED ST2 | MFGR#1433 | 133842836 |
| 1997 | UNITED ST2 | MFGR#1434 |  96443006 |
| 1997 | UNITED ST2 | MFGR#1435 |  50858424 |
| 1997 | UNITED ST2 | MFGR#1438 |  64571457 |
| 1997 | UNITED ST2 | MFGR#144  |  61319000 |
| 1997 | UNITED ST2 | MFGR#146  |  69558050 |
| 1997 | UNITED ST2 | MFGR#147  |  41160961 |
| 1997 | UNITED ST2 | MFGR#149  |  31735872 |
| 1997 | UNITED ST3 | MFGR#1410 | 306449140 |
| 1997 | UNITED ST3 | MFGR#1411 | 114677189 |
| 1997 | UNITED ST3 | MFGR#1412 |  49229127 |
| 1997 | UNITED ST3 | MFGR#1413 | 174911640 |
| 1997 | UNITED ST3 | MFGR#1415 | 134932298 |
| 1997 | UNITED ST3 | MFGR#1416 |  97111854 |
| 1997 | UNITED ST3 | MFGR#1417 | 176279103 |
| 1997 | UNITED ST3 | MFGR#1418 |  70684147 |
| 1997 | UNITED ST3 | MFGR#1420 |  27591782 |
| 1997 | UNITED ST3 | MFGR#1422 |  39411253 |
| 1997 | UNITED ST3 | MFGR#1424 | 226736650 |
| 1997 | UNITED ST3 | MFGR#1426 |  63997112 |
| 1997 | UNITED ST3 | MFGR#1429 |    556053 |
| 1997 | UNITED ST3 | MFGR#143  |  73550925 |
| 1997 | UNITED ST3 | MFGR#1430 | 218807697 |
| 1997 | UNITED ST3 | MFGR#1431 |  39936281 |
| 1997 | UNITED ST3 | MFGR#1432 |  44356689 |
| 1997 | UNITED ST3 | MFGR#1435 |  49225455 |
| 1997 | UNITED ST3 | MFGR#1436 |  90326644 |
| 1997 | UNITED ST3 | MFGR#1439 |  84615817 |
| 1997 | UNITED ST3 | MFGR#144  |  59081596 |
| 1997 | UNITED ST3 | MFGR#1440 |  59601014 |
| 1997 | UNITED ST3 | MFGR#145  | 100692258 |
| 1997 | UNITED ST3 | MFGR#147  | 142417874 |
| 1997 | UNITED ST3 | MFGR#148  |  38233221 |
| 1997 | UNITED ST5 | MFGR#1416 |  62387773 |
| 1997 | UNITED ST5 | MFGR#1417 |  54974702 |
| 1997 | UNITED ST5 | MFGR#1418 |  87301086 |
| 1997 | UNITED ST5 | MFGR#1421 |   9869673 |
| 1997 | UNITED ST5 | MFGR#1422 |  58912225 |
| 1997 | UNITED ST5 | MFGR#1424 |  80038584 |
| 1997 | UNITED ST5 | MFGR#1428 |  44422717 |
| 1997 | UNITED ST5 | MFGR#1430 |  67186074 |
| 1997 | UNITED ST5 | MFGR#1433 | 105646942 |
| 1997 | UNITED ST5 | MFGR#1434 |  13923867 |
| 1997 | UNITED ST5 | MFGR#145  | 104286534 |
| 1997 | UNITED ST5 | MFGR#146  |  20965182 |
| 1997 | UNITED ST5 | MFGR#148  | 170596496 |
| 1997 | UNITED ST5 | MFGR#149  |  42639213 |
| 1997 | UNITED ST6 | MFGR#1411 |  48199726 |
| 1997 | UNITED ST6 | MFGR#1413 |  28825982 |
| 1997 | UNITED ST6 | MFGR#1414 | 107783723 |
| 1997 | UNITED ST6 | MFGR#1415 |  92119787 |
| 1997 | UNITED ST6 | MFGR#1416 |  35390328 |
| 1997 | UNITED ST6 | MFGR#1417 |  92594053 |
| 1997 | UNITED ST6 | MFGR#1418 |  67638716 |
| 1997 | UNITED ST6 | MFGR#1421 |  98608466 |
| 1997 | UNITED ST6 | MFGR#143  |  23938737 |
| 1997 | UNITED ST6 | MFGR#1432 | 104846191 |
| 1997 | UNITED ST6 | MFGR#1435 | 185809031 |
| 1997 | UNITED ST6 | MFGR#1436 |  82920407 |
| 1997 | UNITED ST6 | MFGR#1438 | 137524730 |
| 1997 | UNITED ST6 | MFGR#146  |  28124052 |
| 1997 | UNITED ST7 | MFGR#141  |  65266383 |
| 1997 | UNITED ST7 | MFGR#1411 |  78295166 |
| 1997 | UNITED ST7 | MFGR#1413 |  37554700 |
| 1997 | UNITED ST7 | MFGR#1414 |  20428356 |
| 1997 | UNITED ST7 | MFGR#1416 |  92381468 |
| 1997 | UNITED ST7 | MFGR#1418 | 105276410 |
| 1997 | UNITED ST7 | MFGR#1419 | 116086880 |
| 1997 | UNITED ST7 | MFGR#1420 |  62010492 |
| 1997 | UNITED ST7 | MFGR#1428 |  50904528 |
| 1997 | UNITED ST7 | MFGR#1430 | 103558679 |
| 1997 | UNITED ST7 | MFGR#1431 |  38342548 |
| 1997 | UNITED ST7 | MFGR#1436 |  59859992 |
| 1997 | UNITED ST7 | MFGR#1437 |  90701341 |
| 1997 | UNITED ST7 | MFGR#147  | 133840269 |
| 1997 | UNITED ST7 | MFGR#148  | 175852097 |
| 1997 | UNITED ST9 | MFGR#1411 |  62786695 |
| 1997 | UNITED ST9 | MFGR#1416 |  25354497 |
| 1997 | UNITED ST9 | MFGR#1417 |  47367797 |
| 1997 | UNITED ST9 | MFGR#1418 |  27220077 |
| 1997 | UNITED ST9 | MFGR#142  |  41015203 |
| 1997 | UNITED ST9 | MFGR#1423 |  41473506 |
| 1997 | UNITED ST9 | MFGR#1424 |  10735092 |
| 1997 | UNITED ST9 | MFGR#1425 |  27926087 |
| 1997 | UNITED ST9 | MFGR#1426 | 136645966 |
| 1997 | UNITED ST9 | MFGR#1430 |  41283531 |
| 1997 | UNITED ST9 | MFGR#1433 |    497505 |
| 1997 | UNITED ST9 | MFGR#1434 | 101147110 |
| 1997 | UNITED ST9 | MFGR#1436 |  30923170 |
| 1997 | UNITED ST9 | MFGR#145  |  18049495 |
| 1997 | UNITED ST9 | MFGR#146  |  43726737 |
| 1998 | UNITED ST0 | MFGR#1413 | 131487843 |
| 1998 | UNITED ST0 | MFGR#1426 |  52942692 |
| 1998 | UNITED ST0 | MFGR#146  |  13567224 |
| 1998 | UNITED ST1 | MFGR#1410 |  65992198 |
| 1998 | UNITED ST1 | MFGR#1416 | 115552383 |
| 1998 | UNITED ST1 | MFGR#1418 |  15646035 |
| 1998 | UNITED ST1 | MFGR#1419 | 129708776 |
| 1998 | UNITED ST1 | MFGR#1428 |  18176281 |
| 1998 | UNITED ST1 | MFGR#1431 |  17985830 |
| 1998 | UNITED ST1 | MFGR#1436 |  16714417 |
| 1998 | UNITED ST1 | MFGR#145  |  48297153 |
| 1998 | UNITED ST2 | MFGR#1418 |   9240384 |
| 1998 | UNITED ST2 | MFGR#1419 |  40909344 |
| 1998 | UNITED ST2 | MFGR#1420 |  78625306 |
| 1998 | UNITED ST2 | MFGR#1426 |  67161050 |
| 1998 | UNITED ST2 | MFGR#1430 |  19028508 |
| 1998 | UNITED ST2 | MFGR#1434 | 127804385 |
| 1998 | UNITED ST2 | MFGR#1435 |  75092689 |
| 1998 | UNITED ST2 | MFGR#1436 |  54579894 |
| 1998 | UNITED ST2 | MFGR#1440 |  29067722 |
| 1998 | UNITED ST2 | MFGR#148  |  78886426 |
| 1998 | UNITED ST3 | MFGR#141  |   4311846 |
| 1998 | UNITED ST3 | MFGR#1412 |  98979253 |
| 1998 | UNITED ST3 | MFGR#1415 | 102275672 |
| 1998 | UNITED ST3 | MFGR#1416 |  50781431 |
| 1998 | UNITED ST3 | MFGR#1419 |  37451476 |
| 1998 | UNITED ST3 | MFGR#1420 |  24660608 |
| 1998 | UNITED ST3 | MFGR#1422 |  98548762 |
| 1998 | UNITED ST3 | MFGR#1424 |  96601854 |
| 1998 | UNITED ST3 | MFGR#1425 |  74508450 |
| 1998 | UNITED ST3 | MFGR#1426 | 330583054 |
| 1998 | UNITED ST3 | MFGR#1427 |  41352585 |
| 1998 | UNITED ST3 | MFGR#1428 |  61979722 |
| 1998 | UNITED ST3 | MFGR#1429 |    869295 |
| 1998 | UNITED ST3 | MFGR#1432 |  66991135 |
| 1998 | UNITED ST3 | MFGR#146  |  35929398 |
| 1998 | UNITED ST3 | MFGR#147  |   8484972 |
| 1998 | UNITED ST3 | MFGR#149  |  11793257 |
| 1998 | UNITED ST5 | MFGR#1410 |  55951811 |
| 1998 | UNITED ST5 | MFGR#1413 |  13403140 |
| 1998 | UNITED ST5 | MFGR#142  |  24156762 |
| 1998 | UNITED ST5 | MFGR#1422 | 105826683 |
| 1998 | UNITED ST5 | MFGR#1430 |  67851607 |
| 1998 | UNITED ST5 | MFGR#1431 |  84833774 |
| 1998 | UNITED ST5 | MFGR#1434 |  45541810 |
| 1998 | UNITED ST5 | MFGR#1437 |  33353745 |
| 1998 | UNITED ST5 | MFGR#146  |  19891496 |
| 1998 | UNITED ST6 | MFGR#1413 | 135522572 |
| 1998 | UNITED ST6 | MFGR#1416 | 185707286 |
| 1998 | UNITED ST6 | MFGR#1417 |  80511133 |
| 1998 | UNITED ST6 | MFGR#1419 | 127132766 |
| 1998 | UNITED ST6 | MFGR#142  |  72629474 |
| 1998 | UNITED ST6 | MFGR#1435 | 158543190 |
| 1998 | UNITED ST7 | MFGR#1412 |  56750777 |
| 1998 | UNITED ST7 | MFGR#1424 |  89508621 |
| 1998 | UNITED ST7 | MFGR#1425 | 160377031 |
| 1998 | UNITED ST7 | MFGR#1434 |  20882477 |
| 1998 | UNITED ST7 | MFGR#146  | 100783548 |
| 1998 | UNITED ST7 | MFGR#147  |  61595522 |
| 1998 | UNITED ST9 | MFGR#1412 |   5049765 |
| 1998 | UNITED ST9 | MFGR#142  |  69919113 |
| 1998 | UNITED ST9 | MFGR#1425 |  11003199 |
| 1998 | UNITED ST9 | MFGR#1426 | 103616972 |
| 1998 | UNITED ST9 | MFGR#1435 |  18879758 |
| 1998 | UNITED ST9 | MFGR#1438 | 101903219 |
+------+------------+-----------+-----------+
```

### 多表查询运行预期结果

```
--Q1.1
+--------------+
| revenue      |
+--------------+
| 218453880421 |
+--------------+

--Q1.2
+---------+
| revenue |
+---------+
|    NULL |
+---------+

--Q1.3
+-------------+
| revenue     |
+-------------+
| 17527575453 |
+-------------+

--Q2.1
+------------+------+-----------+
| lo_revenue | year | p_brand   |
+------------+------+-----------+
| 1135676414 | 1992 | MFGR#121  |
| 1221327580 | 1992 | MFGR#1210 |
| 1101539324 | 1992 | MFGR#1211 |
| 1298411712 | 1992 | MFGR#1212 |
| 1248062482 | 1992 | MFGR#1213 |
| 1340976936 | 1992 | MFGR#1214 |
| 1266304940 | 1992 | MFGR#1215 |
| 1349693562 | 1992 | MFGR#1216 |
| 1350186870 | 1992 | MFGR#1217 |
| 1200404140 | 1992 | MFGR#1218 |
| 1076087188 | 1992 | MFGR#1219 |
| 1310653344 | 1992 | MFGR#122  |
| 1080525764 | 1992 | MFGR#1220 |
| 1112241266 | 1992 | MFGR#1221 |
| 1181525554 | 1992 | MFGR#1222 |
| 1070897302 | 1992 | MFGR#1223 |
| 1407505222 | 1992 | MFGR#1224 |
| 1141665736 | 1992 | MFGR#1225 |
| 1228123186 | 1992 | MFGR#1226 |
| 1163518776 | 1992 | MFGR#1227 |
| 1289285184 | 1992 | MFGR#1228 |
| 1281716860 | 1992 | MFGR#1229 |
| 1579511670 | 1992 | MFGR#123  |
|  937070174 | 1992 | MFGR#1230 |
| 1184873312 | 1992 | MFGR#1231 |
| 1328550304 | 1992 | MFGR#1232 |
| 1227770200 | 1992 | MFGR#1233 |
| 1334798562 | 1992 | MFGR#1234 |
| 1280580140 | 1992 | MFGR#1235 |
| 1003785122 | 1992 | MFGR#1236 |
| 1182963006 | 1992 | MFGR#1237 |
|  954847540 | 1992 | MFGR#1238 |
| 1276518748 | 1992 | MFGR#1239 |
| 1144708392 | 1992 | MFGR#124  |
| 1480958496 | 1992 | MFGR#1240 |
|  957554190 | 1992 | MFGR#125  |
| 1184349232 | 1992 | MFGR#126  |
| 1412303264 | 1992 | MFGR#127  |
| 1084613292 | 1992 | MFGR#128  |
| 1163974704 | 1992 | MFGR#129  |
| 1646175404 | 1993 | MFGR#121  |
| 1296321412 | 1993 | MFGR#1210 |
| 1269487796 | 1993 | MFGR#1211 |
| 1571278566 | 1993 | MFGR#1212 |
| 1276510058 | 1993 | MFGR#1213 |
| 1233674474 | 1993 | MFGR#1214 |
| 1269375950 | 1993 | MFGR#1215 |
| 1276707800 | 1993 | MFGR#1216 |
| 1326745902 | 1993 | MFGR#1217 |
| 1367971710 | 1993 | MFGR#1218 |
| 1293900066 | 1993 | MFGR#1219 |
| 1245065968 | 1993 | MFGR#122  |
| 1061660254 | 1993 | MFGR#1220 |
| 1086692674 | 1993 | MFGR#1221 |
| 1513842406 | 1993 | MFGR#1222 |
| 1067088700 | 1993 | MFGR#1223 |
| 1831832170 | 1993 | MFGR#1224 |
|  946014762 | 1993 | MFGR#1225 |
| 1478072248 | 1993 | MFGR#1226 |
| 1184357774 | 1993 | MFGR#1227 |
| 1167014116 | 1993 | MFGR#1228 |
| 1234906982 | 1993 | MFGR#1229 |
| 1275727736 | 1993 | MFGR#123  |
| 1251068620 | 1993 | MFGR#1230 |
| 1160655270 | 1993 | MFGR#1231 |
| 1394746196 | 1993 | MFGR#1232 |
| 1031142832 | 1993 | MFGR#1233 |
| 1303871516 | 1993 | MFGR#1234 |
| 1151558960 | 1993 | MFGR#1235 |
| 1183757334 | 1993 | MFGR#1236 |
| 1219237152 | 1993 | MFGR#1237 |
|  889228020 | 1993 | MFGR#1238 |
| 1190512654 | 1993 | MFGR#1239 |
| 1321172474 | 1993 | MFGR#124  |
| 1577460118 | 1993 | MFGR#1240 |
| 1232449078 | 1993 | MFGR#125  |
| 1234253508 | 1993 | MFGR#126  |
| 1308876648 | 1993 | MFGR#127  |
| 1463314002 | 1993 | MFGR#128  |
| 1096096790 | 1993 | MFGR#129  |
| 1128811296 | 1994 | MFGR#121  |
| 1290809698 | 1994 | MFGR#1210 |
| 1263241270 | 1994 | MFGR#1211 |
| 1136664696 | 1994 | MFGR#1212 |
| 1357571714 | 1994 | MFGR#1213 |
| 1068004660 | 1994 | MFGR#1214 |
| 1308800484 | 1994 | MFGR#1215 |
| 1117292682 | 1994 | MFGR#1216 |
| 1375691282 | 1994 | MFGR#1217 |
| 1093348694 | 1994 | MFGR#1218 |
| 1134545884 | 1994 | MFGR#1219 |
| 1319768124 | 1994 | MFGR#122  |
| 1125164344 | 1994 | MFGR#1220 |
| 1197237994 | 1994 | MFGR#1221 |
| 1202032882 | 1994 | MFGR#1222 |
| 1110268808 | 1994 | MFGR#1223 |
| 1474844604 | 1994 | MFGR#1224 |
| 1141491910 | 1994 | MFGR#1225 |
| 1492604490 | 1994 | MFGR#1226 |
| 1303414962 | 1994 | MFGR#1227 |
| 1147387094 | 1994 | MFGR#1228 |
| 1295836746 | 1994 | MFGR#1229 |
| 1160899184 | 1994 | MFGR#123  |
|  986540824 | 1994 | MFGR#1230 |
| 1207092296 | 1994 | MFGR#1231 |
| 1439730662 | 1994 | MFGR#1232 |
| 1277964476 | 1994 | MFGR#1233 |
| 1486495354 | 1994 | MFGR#1234 |
| 1197361918 | 1994 | MFGR#1235 |
| 1231452194 | 1994 | MFGR#1236 |
| 1085139630 | 1994 | MFGR#1237 |
| 1147021562 | 1994 | MFGR#1238 |
| 1159711706 | 1994 | MFGR#1239 |
| 1369146644 | 1994 | MFGR#124  |
| 1747471474 | 1994 | MFGR#1240 |
| 1120976608 | 1994 | MFGR#125  |
| 1314073028 | 1994 | MFGR#126  |
| 1245142366 | 1994 | MFGR#127  |
| 1173691328 | 1994 | MFGR#128  |
| 1069083050 | 1994 | MFGR#129  |
| 1412939022 | 1995 | MFGR#121  |
| 1205785606 | 1995 | MFGR#1210 |
| 1290332184 | 1995 | MFGR#1211 |
| 1226578566 | 1995 | MFGR#1212 |
| 1199172958 | 1995 | MFGR#1213 |
| 1125141608 | 1995 | MFGR#1214 |
| 1345057510 | 1995 | MFGR#1215 |
| 1338001944 | 1995 | MFGR#1216 |
| 1450724898 | 1995 | MFGR#1217 |
| 1314053270 | 1995 | MFGR#1218 |
| 1039318006 | 1995 | MFGR#1219 |
| 1449455482 | 1995 | MFGR#122  |
| 1035912262 | 1995 | MFGR#1220 |
| 1271482702 | 1995 | MFGR#1221 |
| 1128736820 | 1995 | MFGR#1222 |
| 1201330298 | 1995 | MFGR#1223 |
| 1525400702 | 1995 | MFGR#1224 |
| 1343339172 | 1995 | MFGR#1225 |
| 1145137496 | 1995 | MFGR#1226 |
| 1060722600 | 1995 | MFGR#1227 |
| 1266714170 | 1995 | MFGR#1228 |
| 1095920488 | 1995 | MFGR#1229 |
| 1321422154 | 1995 | MFGR#123  |
| 1205471716 | 1995 | MFGR#1230 |
|  999704292 | 1995 | MFGR#1231 |
| 1430601506 | 1995 | MFGR#1232 |
| 1114299142 | 1995 | MFGR#1233 |
| 1420046118 | 1995 | MFGR#1234 |
| 1244850478 | 1995 | MFGR#1235 |
| 1269131002 | 1995 | MFGR#1236 |
| 1145694540 | 1995 | MFGR#1237 |
| 1098637824 | 1995 | MFGR#1238 |
| 1187703424 | 1995 | MFGR#1239 |
| 1170843630 | 1995 | MFGR#124  |
| 1414415776 | 1995 | MFGR#1240 |
| 1076493744 | 1995 | MFGR#125  |
| 1211598042 | 1995 | MFGR#126  |
| 1331956224 | 1995 | MFGR#127  |
| 1293921912 | 1995 | MFGR#128  |
| 1017498802 | 1995 | MFGR#129  |
| 1047758290 | 1996 | MFGR#121  |
| 1287290106 | 1996 | MFGR#1210 |
| 1190130678 | 1996 | MFGR#1211 |
| 1349252880 | 1996 | MFGR#1212 |
|  992594174 | 1996 | MFGR#1213 |
| 1166499010 | 1996 | MFGR#1214 |
| 1404369714 | 1996 | MFGR#1215 |
| 1203618668 | 1996 | MFGR#1216 |
| 1409796774 | 1996 | MFGR#1217 |
| 1057686172 | 1996 | MFGR#1218 |
| 1172492660 | 1996 | MFGR#1219 |
| 1424220984 | 1996 | MFGR#122  |
| 1036888430 | 1996 | MFGR#1220 |
|  998638828 | 1996 | MFGR#1221 |
| 1358938712 | 1996 | MFGR#1222 |
| 1257525508 | 1996 | MFGR#1223 |
| 1449689712 | 1996 | MFGR#1224 |
| 1321241174 | 1996 | MFGR#1225 |
| 1335349458 | 1996 | MFGR#1226 |
|  967676170 | 1996 | MFGR#1227 |
| 1219710782 | 1996 | MFGR#1228 |
| 1317919114 | 1996 | MFGR#1229 |
| 1132435704 | 1996 | MFGR#123  |
| 1057759996 | 1996 | MFGR#1230 |
| 1178962388 | 1996 | MFGR#1231 |
| 1405611792 | 1996 | MFGR#1232 |
| 1327359894 | 1996 | MFGR#1233 |
| 1142298900 | 1996 | MFGR#1234 |
|  957296148 | 1996 | MFGR#1235 |
| 1136498730 | 1996 | MFGR#1236 |
| 1185232334 | 1996 | MFGR#1237 |
|  933352296 | 1996 | MFGR#1238 |
| 1341387438 | 1996 | MFGR#1239 |
| 1121335438 | 1996 | MFGR#124  |
| 1642335900 | 1996 | MFGR#1240 |
|  953728666 | 1996 | MFGR#125  |
| 1116061768 | 1996 | MFGR#126  |
| 1271747782 | 1996 | MFGR#127  |
| 1102021236 | 1996 | MFGR#128  |
| 1121141260 | 1996 | MFGR#129  |
| 1174026414 | 1997 | MFGR#121  |
| 1232575784 | 1997 | MFGR#1210 |
| 1097177522 | 1997 | MFGR#1211 |
| 1179187784 | 1997 | MFGR#1212 |
|  848613340 | 1997 | MFGR#1213 |
| 1023943820 | 1997 | MFGR#1214 |
| 1263544492 | 1997 | MFGR#1215 |
| 1384270280 | 1997 | MFGR#1216 |
| 1555989914 | 1997 | MFGR#1217 |
| 1414107440 | 1997 | MFGR#1218 |
| 1122339054 | 1997 | MFGR#1219 |
| 1329832490 | 1997 | MFGR#122  |
| 1188932314 | 1997 | MFGR#1220 |
| 1177696342 | 1997 | MFGR#1221 |
| 1057977920 | 1997 | MFGR#1222 |
| 1074196422 | 1997 | MFGR#1223 |
| 1349526332 | 1997 | MFGR#1224 |
|  900804584 | 1997 | MFGR#1225 |
| 1402721444 | 1997 | MFGR#1226 |
| 1012023140 | 1997 | MFGR#1227 |
| 1171157474 | 1997 | MFGR#1228 |
| 1245488032 | 1997 | MFGR#1229 |
| 1293006336 | 1997 | MFGR#123  |
| 1143601882 | 1997 | MFGR#1230 |
| 1005203580 | 1997 | MFGR#1231 |
| 1355849312 | 1997 | MFGR#1232 |
| 1068911952 | 1997 | MFGR#1233 |
| 1429869430 | 1997 | MFGR#1234 |
| 1534302840 | 1997 | MFGR#1235 |
| 1237754358 | 1997 | MFGR#1236 |
| 1279276114 | 1997 | MFGR#1237 |
|  803906838 | 1997 | MFGR#1238 |
| 1221513428 | 1997 | MFGR#1239 |
| 1086496174 | 1997 | MFGR#124  |
| 1350265384 | 1997 | MFGR#1240 |
|  958198730 | 1997 | MFGR#125  |
| 1141393136 | 1997 | MFGR#126  |
| 1166149184 | 1997 | MFGR#127  |
| 1390266208 | 1997 | MFGR#128  |
| 1311277552 | 1997 | MFGR#129  |
|  689151850 | 1998 | MFGR#121  |
|  834304832 | 1998 | MFGR#1210 |
|  634136336 | 1998 | MFGR#1211 |
|  748683032 | 1998 | MFGR#1212 |
|  665481806 | 1998 | MFGR#1213 |
|  609746004 | 1998 | MFGR#1214 |
|  732202264 | 1998 | MFGR#1215 |
|  758267796 | 1998 | MFGR#1216 |
|  719016994 | 1998 | MFGR#1217 |
|  641246668 | 1998 | MFGR#1218 |
|  692365724 | 1998 | MFGR#1219 |
|  624880054 | 1998 | MFGR#122  |
|  696247922 | 1998 | MFGR#1220 |
|  679690796 | 1998 | MFGR#1221 |
|  710832322 | 1998 | MFGR#1222 |
|  689779644 | 1998 | MFGR#1223 |
|  793813382 | 1998 | MFGR#1224 |
|  580417756 | 1998 | MFGR#1225 |
|  838831414 | 1998 | MFGR#1226 |
|  716932680 | 1998 | MFGR#1227 |
|  503099910 | 1998 | MFGR#1228 |
|  766277720 | 1998 | MFGR#1229 |
|  592661122 | 1998 | MFGR#123  |
|  874362486 | 1998 | MFGR#1230 |
|  797888984 | 1998 | MFGR#1231 |
|  848124910 | 1998 | MFGR#1232 |
|  813934376 | 1998 | MFGR#1233 |
|  857734480 | 1998 | MFGR#1234 |
|  704555562 | 1998 | MFGR#1235 |
|  723654172 | 1998 | MFGR#1236 |
|  683237138 | 1998 | MFGR#1237 |
|  489478462 | 1998 | MFGR#1238 |
|  828303606 | 1998 | MFGR#1239 |
|  660164742 | 1998 | MFGR#124  |
|  830624906 | 1998 | MFGR#1240 |
|  720579248 | 1998 | MFGR#125  |
|  683315160 | 1998 | MFGR#126  |
|  755014122 | 1998 | MFGR#127  |
|  722832994 | 1998 | MFGR#128  |
|  637539146 | 1998 | MFGR#129  |
+------------+------+-----------+

--Q2.2
+------------+------+-----------+
| lo_revenue | year | p_brand   |
+------------+------+-----------+
| 1419049858 | 1992 | MFGR#2221 |
| 1567692788 | 1992 | MFGR#2222 |
| 1530104004 | 1992 | MFGR#2223 |
| 1302977924 | 1992 | MFGR#2224 |
| 1293057178 | 1992 | MFGR#2225 |
| 1419301096 | 1992 | MFGR#2226 |
| 1491112632 | 1992 | MFGR#2227 |
| 1513803750 | 1992 | MFGR#2228 |
| 1533042206 | 1993 | MFGR#2221 |
| 1382951194 | 1993 | MFGR#2222 |
| 1516441504 | 1993 | MFGR#2223 |
| 1339325414 | 1993 | MFGR#2224 |
| 1547708456 | 1993 | MFGR#2225 |
| 1474175036 | 1993 | MFGR#2226 |
| 1563935532 | 1993 | MFGR#2227 |
| 1361760432 | 1993 | MFGR#2228 |
| 1371555036 | 1994 | MFGR#2221 |
| 1333049614 | 1994 | MFGR#2222 |
| 1467987180 | 1994 | MFGR#2223 |
| 1415738080 | 1994 | MFGR#2224 |
| 1442503934 | 1994 | MFGR#2225 |
| 1644991838 | 1994 | MFGR#2226 |
| 1441674256 | 1994 | MFGR#2227 |
| 1652450700 | 1994 | MFGR#2228 |
| 1550874148 | 1995 | MFGR#2221 |
| 1522709584 | 1995 | MFGR#2222 |
| 1275665150 | 1995 | MFGR#2223 |
| 1179531414 | 1995 | MFGR#2224 |
| 1416580078 | 1995 | MFGR#2225 |
| 1494712766 | 1995 | MFGR#2226 |
| 1605005080 | 1995 | MFGR#2227 |
| 1791873572 | 1995 | MFGR#2228 |
| 1400020016 | 1996 | MFGR#2221 |
| 1554620170 | 1996 | MFGR#2222 |
| 1312190628 | 1996 | MFGR#2223 |
| 1313719834 | 1996 | MFGR#2224 |
| 1531641792 | 1996 | MFGR#2225 |
| 1616355468 | 1996 | MFGR#2226 |
| 1459126606 | 1996 | MFGR#2227 |
| 1639331748 | 1996 | MFGR#2228 |
| 1454684764 | 1997 | MFGR#2221 |
| 1329067558 | 1997 | MFGR#2222 |
| 1496576784 | 1997 | MFGR#2223 |
| 1260844162 | 1997 | MFGR#2224 |
| 1514782406 | 1997 | MFGR#2225 |
| 1495778514 | 1997 | MFGR#2226 |
| 1457715798 | 1997 | MFGR#2227 |
| 1550625970 | 1997 | MFGR#2228 |
|  670609008 | 1998 | MFGR#2221 |
|  818694274 | 1998 | MFGR#2222 |
|  918219154 | 1998 | MFGR#2223 |
|  826636144 | 1998 | MFGR#2224 |
|  820804190 | 1998 | MFGR#2225 |
|  907030088 | 1998 | MFGR#2226 |
|  781012810 | 1998 | MFGR#2227 |
|  795878206 | 1998 | MFGR#2228 |
+------------+------+-----------+

--Q2.3
+------------+------+-----------+
| lo_revenue | year | p_brand   |
+------------+------+-----------+
| 1452854972 | 1992 | MFGR#2239 |
| 1410477918 | 1993 | MFGR#2239 |
| 1328290268 | 1994 | MFGR#2239 |
| 1427678672 | 1995 | MFGR#2239 |
| 1456985730 | 1996 | MFGR#2239 |
| 1467793064 | 1997 | MFGR#2239 |
|  760511462 | 1998 | MFGR#2239 |
+------------+------+-----------+

--Q3.1
+-----------+-----------+------+-------------+
| c_nation  | s_nation  | year | lo_revenue  |
+-----------+-----------+------+-------------+
| INDONESIA | INDONESIA | 1992 | 13811397976 |
| CHINA     | INDONESIA | 1992 | 13232157738 |
| CHINA     | CHINA     | 1992 | 12912862954 |
| VIETNAM   | INDONESIA | 1992 | 12680363414 |
| VIETNAM   | CHINA     | 1992 | 12665688780 |
| INDONESIA | CHINA     | 1992 | 12621419066 |
| INDIA     | INDONESIA | 1992 | 12477614708 |
| JAPAN     | INDONESIA | 1992 | 12445131276 |
| CHINA     | INDIA     | 1992 | 12379662702 |
| CHINA     | JAPAN     | 1992 | 12315357786 |
| JAPAN     | CHINA     | 1992 | 12134201310 |
| INDIA     | CHINA     | 1992 | 12132923622 |
| VIETNAM   | JAPAN     | 1992 | 11727572698 |
| JAPAN     | INDIA     | 1992 | 11605499970 |
| INDONESIA | INDIA     | 1992 | 11540406436 |
| VIETNAM   | INDIA     | 1992 | 11397022802 |
| INDONESIA | JAPAN     | 1992 | 11327531220 |
| JAPAN     | JAPAN     | 1992 | 11296069422 |
| INDIA     | JAPAN     | 1992 | 10843918562 |
| CHINA     | VIETNAM   | 1992 | 10824644052 |
| JAPAN     | VIETNAM   | 1992 | 10803385110 |
| INDIA     | INDIA     | 1992 | 10722487510 |
| INDONESIA | VIETNAM   | 1992 | 10605276744 |
| INDIA     | VIETNAM   | 1992 | 10490661242 |
| VIETNAM   | VIETNAM   | 1992 | 10223463556 |
| INDONESIA | INDONESIA | 1993 | 13862726524 |
| INDONESIA | CHINA     | 1993 | 13225782498 |
| CHINA     | INDONESIA | 1993 | 13163026732 |
| VIETNAM   | INDONESIA | 1993 | 13023278704 |
| CHINA     | CHINA     | 1993 | 12889027574 |
| CHINA     | INDIA     | 1993 | 12843388242 |
| VIETNAM   | CHINA     | 1993 | 12827159998 |
| INDIA     | INDONESIA | 1993 | 12662117188 |
| JAPAN     | CHINA     | 1993 | 12584587990 |
| INDIA     | CHINA     | 1993 | 12418707584 |
| CHINA     | JAPAN     | 1993 | 12390933768 |
| VIETNAM   | INDIA     | 1993 | 12322348954 |
| INDONESIA | INDIA     | 1993 | 12303328612 |
| INDONESIA | JAPAN     | 1993 | 12295210498 |
| JAPAN     | INDONESIA | 1993 | 12107892626 |
| INDIA     | JAPAN     | 1993 | 11990417970 |
| CHINA     | VIETNAM   | 1993 | 11770046456 |
| VIETNAM   | JAPAN     | 1993 | 11748533734 |
| INDONESIA | VIETNAM   | 1993 | 11680575444 |
| JAPAN     | INDIA     | 1993 | 11646686314 |
| INDIA     | INDIA     | 1993 | 11143151598 |
| VIETNAM   | VIETNAM   | 1993 | 11108322366 |
| JAPAN     | JAPAN     | 1993 | 10860637166 |
| JAPAN     | VIETNAM   | 1993 | 10813139306 |
| INDIA     | VIETNAM   | 1993 | 10467742974 |
| VIETNAM   | CHINA     | 1994 | 13419766884 |
| CHINA     | CHINA     | 1994 | 13297885930 |
| INDONESIA | CHINA     | 1994 | 12967201820 |
| CHINA     | JAPAN     | 1994 | 12698074042 |
| VIETNAM   | INDONESIA | 1994 | 12694883862 |
| JAPAN     | CHINA     | 1994 | 12640018436 |
| INDONESIA | INDONESIA | 1994 | 12630662172 |
| CHINA     | INDIA     | 1994 | 12595165622 |
| CHINA     | INDONESIA | 1994 | 12469575792 |
| VIETNAM   | JAPAN     | 1994 | 12463946094 |
| INDONESIA | INDIA     | 1994 | 12396824490 |
| INDIA     | INDONESIA | 1994 | 12336379718 |
| INDONESIA | JAPAN     | 1994 | 12282391938 |
| JAPAN     | INDONESIA | 1994 | 12026069236 |
| CHINA     | VIETNAM   | 1994 | 11770637466 |
| INDIA     | CHINA     | 1994 | 11630045428 |
| VIETNAM   | INDIA     | 1994 | 11578797382 |
| JAPAN     | JAPAN     | 1994 | 11507642964 |
| JAPAN     | INDIA     | 1994 | 11291637744 |
| INDONESIA | VIETNAM   | 1994 | 11248692736 |
| INDIA     | INDIA     | 1994 | 11169873030 |
| VIETNAM   | VIETNAM   | 1994 | 10836996318 |
| INDIA     | JAPAN     | 1994 | 10788269948 |
| JAPAN     | VIETNAM   | 1994 | 10551643274 |
| INDIA     | VIETNAM   | 1994 | 10502079630 |
| CHINA     | INDONESIA | 1995 | 14149078888 |
| INDONESIA | CHINA     | 1995 | 13857241240 |
| CHINA     | CHINA     | 1995 | 13249333224 |
| JAPAN     | CHINA     | 1995 | 13039778770 |
| VIETNAM   | CHINA     | 1995 | 12665462536 |
| INDONESIA | INDONESIA | 1995 | 12537062642 |
| VIETNAM   | JAPAN     | 1995 | 12527914040 |
| CHINA     | INDIA     | 1995 | 12493312748 |
| VIETNAM   | INDIA     | 1995 | 12396883914 |
| INDONESIA | INDIA     | 1995 | 12347610366 |
| VIETNAM   | INDONESIA | 1995 | 12115640296 |
| CHINA     | JAPAN     | 1995 | 12043708260 |
| INDONESIA | JAPAN     | 1995 | 12038187742 |
| INDIA     | CHINA     | 1995 | 12021065586 |
| INDIA     | INDONESIA | 1995 | 11951037194 |
| JAPAN     | JAPAN     | 1995 | 11904558258 |
| JAPAN     | INDONESIA | 1995 | 11894001470 |
| VIETNAM   | VIETNAM   | 1995 | 11509455214 |
| JAPAN     | INDIA     | 1995 | 11461486252 |
| INDONESIA | VIETNAM   | 1995 | 11149948132 |
| INDIA     | INDIA     | 1995 | 11131991100 |
| JAPAN     | VIETNAM   | 1995 | 11002627550 |
| CHINA     | VIETNAM   | 1995 | 10979872126 |
| INDIA     | JAPAN     | 1995 | 10938406854 |
| INDIA     | VIETNAM   | 1995 | 10414126568 |
| INDONESIA | INDONESIA | 1996 | 13500112566 |
| CHINA     | INDONESIA | 1996 | 13314250150 |
| INDONESIA | CHINA     | 1996 | 13226878224 |
| CHINA     | CHINA     | 1996 | 13183395830 |
| VIETNAM   | CHINA     | 1996 | 12857307780 |
| VIETNAM   | INDONESIA | 1996 | 12591253464 |
| JAPAN     | INDONESIA | 1996 | 12454895712 |
| INDIA     | CHINA     | 1996 | 12397135638 |
| INDIA     | INDONESIA | 1996 | 12378484116 |
| CHINA     | INDIA     | 1996 | 12307574730 |
| INDONESIA | INDIA     | 1996 | 12277621726 |
| CHINA     | JAPAN     | 1996 | 12211132648 |
| JAPAN     | CHINA     | 1996 | 12177971128 |
| INDONESIA | JAPAN     | 1996 | 12111276444 |
| VIETNAM   | JAPAN     | 1996 | 11839994300 |
| VIETNAM   | VIETNAM   | 1996 | 11721684604 |
| INDIA     | JAPAN     | 1996 | 11683329610 |
| VIETNAM   | INDIA     | 1996 | 11614973966 |
| JAPAN     | INDIA     | 1996 | 11289159232 |
| JAPAN     | JAPAN     | 1996 | 11132409590 |
| INDIA     | INDIA     | 1996 | 11064146206 |
| INDONESIA | VIETNAM   | 1996 | 10877028774 |
| CHINA     | VIETNAM   | 1996 | 10869545636 |
| JAPAN     | VIETNAM   | 1996 | 10668555098 |
| INDIA     | VIETNAM   | 1996 | 10587783062 |
| CHINA     | INDONESIA | 1997 | 13306469392 |
| INDONESIA | CHINA     | 1997 | 13154792628 |
| CHINA     | CHINA     | 1997 | 12927589590 |
| JAPAN     | INDONESIA | 1997 | 12858540252 |
| INDONESIA | INDONESIA | 1997 | 12796855642 |
| VIETNAM   | INDONESIA | 1997 | 12727166240 |
| CHINA     | JAPAN     | 1997 | 12569467036 |
| VIETNAM   | CHINA     | 1997 | 12328437446 |
| INDIA     | CHINA     | 1997 | 12306564428 |
| CHINA     | INDIA     | 1997 | 12168567966 |
| INDONESIA | JAPAN     | 1997 | 12002855912 |
| INDIA     | INDONESIA | 1997 | 11966878600 |
| JAPAN     | CHINA     | 1997 | 11947699374 |
| CHINA     | VIETNAM   | 1997 | 11816508352 |
| JAPAN     | INDIA     | 1997 | 11593843984 |
| JAPAN     | JAPAN     | 1997 | 11580900078 |
| INDONESIA | INDIA     | 1997 | 11578734210 |
| VIETNAM   | INDIA     | 1997 | 11460243216 |
| INDIA     | INDIA     | 1997 | 11386057814 |
| VIETNAM   | JAPAN     | 1997 | 11378690460 |
| INDONESIA | VIETNAM   | 1997 | 11331356264 |
| VIETNAM   | VIETNAM   | 1997 | 11240502648 |
| INDIA     | JAPAN     | 1997 | 11175655826 |
| JAPAN     | VIETNAM   | 1997 | 10499749228 |
| INDIA     | VIETNAM   | 1997 | 10007249674 |
+-----------+-----------+------+-------------+

--Q3.2
+------------+------------+------+------------+
| c_city     | s_city     | year | lo_revenue |
+------------+------------+------+------------+
| UNITED ST4 | UNITED ST1 | 1992 |  204054910 |
| UNITED ST1 | UNITED ST0 | 1992 |  193978982 |
| UNITED ST7 | UNITED ST0 | 1992 |  192156020 |
| UNITED ST9 | UNITED ST0 | 1992 |  189626588 |
| UNITED ST4 | UNITED ST0 | 1992 |  189288484 |
| UNITED ST2 | UNITED ST4 | 1992 |  182361000 |
| UNITED ST5 | UNITED ST0 | 1992 |  180864600 |
| UNITED ST6 | UNITED ST7 | 1992 |  175316534 |
| UNITED ST3 | UNITED ST9 | 1992 |  172284096 |
| UNITED ST6 | UNITED ST5 | 1992 |  171765932 |
| UNITED ST7 | UNITED ST3 | 1992 |  167531332 |
| UNITED ST2 | UNITED ST9 | 1992 |  167411236 |
| UNITED ST4 | UNITED ST6 | 1992 |  163772748 |
| UNITED ST2 | UNITED ST1 | 1992 |  163678330 |
| UNITED ST9 | UNITED ST1 | 1992 |  161590604 |
| UNITED ST6 | UNITED ST3 | 1992 |  157556436 |
| UNITED ST6 | UNITED ST0 | 1992 |  157393912 |
| UNITED ST0 | UNITED ST1 | 1992 |  154534792 |
| UNITED ST0 | UNITED ST0 | 1992 |  151244244 |
| UNITED ST1 | UNITED ST9 | 1992 |  150734118 |
| UNITED ST3 | UNITED ST1 | 1992 |  147274980 |
| UNITED ST2 | UNITED ST0 | 1992 |  144420436 |
| UNITED ST1 | UNITED ST7 | 1992 |  142945946 |
| UNITED ST6 | UNITED ST4 | 1992 |  142173888 |
| UNITED ST4 | UNITED ST4 | 1992 |  140222670 |
| UNITED ST6 | UNITED ST1 | 1992 |  138817376 |
| UNITED ST4 | UNITED ST3 | 1992 |  138003574 |
| UNITED ST5 | UNITED ST7 | 1992 |  136667302 |
| UNITED ST4 | UNITED ST9 | 1992 |  135675940 |
| UNITED ST7 | UNITED ST6 | 1992 |  131026410 |
| UNITED ST4 | UNITED ST5 | 1992 |  130115744 |
| UNITED ST7 | UNITED ST4 | 1992 |  129801776 |
| UNITED ST1 | UNITED ST1 | 1992 |  129338140 |
| UNITED ST3 | UNITED ST5 | 1992 |  128478096 |
| UNITED ST0 | UNITED ST9 | 1992 |  127959992 |
| UNITED ST3 | UNITED ST4 | 1992 |  126289544 |
| UNITED ST5 | UNITED ST6 | 1992 |  125256186 |
| UNITED ST4 | UNITED ST7 | 1992 |  125058752 |
| UNITED ST3 | UNITED ST0 | 1992 |  124883312 |
| UNITED ST9 | UNITED ST4 | 1992 |  122979026 |
| UNITED ST8 | UNITED ST6 | 1992 |  121080880 |
| UNITED ST7 | UNITED ST9 | 1992 |  120652084 |
| UNITED ST7 | UNITED ST7 | 1992 |  120242772 |
| UNITED ST5 | UNITED ST1 | 1992 |  119890574 |
| UNITED ST5 | UNITED ST4 | 1992 |  115251254 |
| UNITED ST7 | UNITED ST5 | 1992 |  115133604 |
| UNITED ST2 | UNITED ST5 | 1992 |  114042730 |
| UNITED ST9 | UNITED ST7 | 1992 |  113766718 |
| UNITED ST0 | UNITED ST3 | 1992 |  112718634 |
| UNITED ST1 | UNITED ST3 | 1992 |  111454948 |
| UNITED ST5 | UNITED ST3 | 1992 |  107927106 |
| UNITED ST0 | UNITED ST7 | 1992 |  101166818 |
| UNITED ST5 | UNITED ST9 | 1992 |  100382182 |
| UNITED ST7 | UNITED ST1 | 1992 |  100334416 |
| UNITED ST0 | UNITED ST8 | 1992 |   99465280 |
| UNITED ST0 | UNITED ST4 | 1992 |   99353614 |
| UNITED ST9 | UNITED ST3 | 1992 |   95362330 |
| UNITED ST8 | UNITED ST4 | 1992 |   93514038 |
| UNITED ST3 | UNITED ST3 | 1992 |   90174432 |
| UNITED ST8 | UNITED ST0 | 1992 |   88737678 |
| UNITED ST0 | UNITED ST6 | 1992 |   84943612 |
| UNITED ST6 | UNITED ST8 | 1992 |   84927380 |
| UNITED ST8 | UNITED ST7 | 1992 |   83795802 |
| UNITED ST3 | UNITED ST8 | 1992 |   82551528 |
| UNITED ST6 | UNITED ST9 | 1992 |   81183442 |
| UNITED ST0 | UNITED ST5 | 1992 |   80241772 |
| UNITED ST1 | UNITED ST4 | 1992 |   78652692 |
| UNITED ST3 | UNITED ST7 | 1992 |   78057158 |
| UNITED ST3 | UNITED ST6 | 1992 |   77597430 |
| UNITED ST9 | UNITED ST9 | 1992 |   72096686 |
| UNITED ST2 | UNITED ST8 | 1992 |   72092898 |
| UNITED ST2 | UNITED ST3 | 1992 |   71963926 |
| UNITED ST8 | UNITED ST1 | 1992 |   71361504 |
| UNITED ST1 | UNITED ST6 | 1992 |   70809980 |
| UNITED ST8 | UNITED ST5 | 1992 |   70375220 |
| UNITED ST1 | UNITED ST5 | 1992 |   67942502 |
| UNITED ST5 | UNITED ST8 | 1992 |   67756106 |
| UNITED ST2 | UNITED ST7 | 1992 |   67405558 |
| UNITED ST8 | UNITED ST3 | 1992 |   61898648 |
| UNITED ST8 | UNITED ST8 | 1992 |   58618216 |
| UNITED ST5 | UNITED ST5 | 1992 |   58559136 |
| UNITED ST1 | UNITED ST8 | 1992 |   57131158 |
| UNITED ST9 | UNITED ST5 | 1992 |   56150008 |
| UNITED ST2 | UNITED ST6 | 1992 |   55627478 |
| UNITED ST0 | UNITED ST2 | 1992 |   55437466 |
| UNITED ST2 | UNITED ST2 | 1992 |   51487308 |
| UNITED ST8 | UNITED ST9 | 1992 |   45368942 |
| UNITED ST4 | UNITED ST8 | 1992 |   43856884 |
| UNITED ST9 | UNITED ST8 | 1992 |   42772200 |
| UNITED ST5 | UNITED ST2 | 1992 |   40991634 |
| UNITED ST6 | UNITED ST6 | 1992 |   36274210 |
| UNITED ST9 | UNITED ST6 | 1992 |   31759136 |
| UNITED ST4 | UNITED ST2 | 1992 |   24123690 |
| UNITED ST7 | UNITED ST8 | 1992 |   23791404 |
| UNITED ST6 | UNITED ST2 | 1992 |   23641396 |
| UNITED ST9 | UNITED ST2 | 1992 |   23246354 |
| UNITED ST8 | UNITED ST2 | 1992 |   21943122 |
| UNITED ST1 | UNITED ST2 | 1992 |   15413456 |
| UNITED ST7 | UNITED ST2 | 1992 |    9886408 |
| UNITED ST3 | UNITED ST2 | 1992 |    2194416 |
| UNITED ST0 | UNITED ST9 | 1993 |  219668080 |
| UNITED ST7 | UNITED ST0 | 1993 |  219576048 |
| UNITED ST5 | UNITED ST0 | 1993 |  213645194 |
| UNITED ST0 | UNITED ST0 | 1993 |  213485096 |
| UNITED ST1 | UNITED ST0 | 1993 |  198611904 |
| UNITED ST4 | UNITED ST4 | 1993 |  196300930 |
| UNITED ST3 | UNITED ST4 | 1993 |  184987840 |
| UNITED ST0 | UNITED ST1 | 1993 |  182393186 |
| UNITED ST4 | UNITED ST1 | 1993 |  177042846 |
| UNITED ST8 | UNITED ST0 | 1993 |  176712742 |
| UNITED ST4 | UNITED ST7 | 1993 |  176344396 |
| UNITED ST4 | UNITED ST0 | 1993 |  173836916 |
| UNITED ST6 | UNITED ST3 | 1993 |  166834322 |
| UNITED ST6 | UNITED ST1 | 1993 |  166691878 |
| UNITED ST7 | UNITED ST9 | 1993 |  160621402 |
| UNITED ST3 | UNITED ST1 | 1993 |  156460556 |
| UNITED ST6 | UNITED ST7 | 1993 |  156394588 |
| UNITED ST5 | UNITED ST9 | 1993 |  152573078 |
| UNITED ST0 | UNITED ST3 | 1993 |  152342566 |
| UNITED ST5 | UNITED ST8 | 1993 |  148718558 |
| UNITED ST9 | UNITED ST1 | 1993 |  148118838 |
| UNITED ST4 | UNITED ST9 | 1993 |  146593918 |
| UNITED ST5 | UNITED ST1 | 1993 |  142909246 |
| UNITED ST6 | UNITED ST4 | 1993 |  139293826 |
| UNITED ST2 | UNITED ST1 | 1993 |  139263402 |
| UNITED ST6 | UNITED ST0 | 1993 |  136495078 |
| UNITED ST7 | UNITED ST7 | 1993 |  136219640 |
| UNITED ST2 | UNITED ST3 | 1993 |  133944876 |
| UNITED ST3 | UNITED ST0 | 1993 |  133253852 |
| UNITED ST9 | UNITED ST7 | 1993 |  133250966 |
| UNITED ST1 | UNITED ST8 | 1993 |  132292396 |
| UNITED ST2 | UNITED ST7 | 1993 |  128370028 |
| UNITED ST5 | UNITED ST4 | 1993 |  126831278 |
| UNITED ST9 | UNITED ST9 | 1993 |  126521526 |
| UNITED ST1 | UNITED ST4 | 1993 |  125768694 |
| UNITED ST7 | UNITED ST4 | 1993 |  123313226 |
| UNITED ST3 | UNITED ST6 | 1993 |  117169616 |
| UNITED ST2 | UNITED ST4 | 1993 |  113300782 |
| UNITED ST3 | UNITED ST5 | 1993 |  111814610 |
| UNITED ST6 | UNITED ST9 | 1993 |  109801884 |
| UNITED ST1 | UNITED ST7 | 1993 |  109702366 |
| UNITED ST3 | UNITED ST9 | 1993 |  109525192 |
| UNITED ST8 | UNITED ST6 | 1993 |  109266124 |
| UNITED ST8 | UNITED ST3 | 1993 |  108099748 |
| UNITED ST5 | UNITED ST7 | 1993 |  105491076 |
| UNITED ST0 | UNITED ST5 | 1993 |  105402104 |
| UNITED ST1 | UNITED ST9 | 1993 |  105029804 |
| UNITED ST8 | UNITED ST5 | 1993 |  104475674 |
| UNITED ST1 | UNITED ST3 | 1993 |  104195892 |
| UNITED ST8 | UNITED ST4 | 1993 |  102838712 |
| UNITED ST0 | UNITED ST6 | 1993 |  100864564 |
| UNITED ST5 | UNITED ST5 | 1993 |  100714378 |
| UNITED ST3 | UNITED ST7 | 1993 |  100270896 |
| UNITED ST0 | UNITED ST4 | 1993 |   98520134 |
| UNITED ST0 | UNITED ST7 | 1993 |   97592720 |
| UNITED ST2 | UNITED ST9 | 1993 |   96377014 |
| UNITED ST1 | UNITED ST1 | 1993 |   95077220 |
| UNITED ST9 | UNITED ST3 | 1993 |   93887294 |
| UNITED ST7 | UNITED ST5 | 1993 |   89527384 |
| UNITED ST1 | UNITED ST6 | 1993 |   89457080 |
| UNITED ST8 | UNITED ST1 | 1993 |   88830868 |
| UNITED ST7 | UNITED ST8 | 1993 |   87805256 |
| UNITED ST9 | UNITED ST6 | 1993 |   87734320 |
| UNITED ST2 | UNITED ST0 | 1993 |   85690970 |
| UNITED ST3 | UNITED ST8 | 1993 |   84503696 |
| UNITED ST0 | UNITED ST8 | 1993 |   84005364 |
| UNITED ST4 | UNITED ST8 | 1993 |   83315164 |
| UNITED ST1 | UNITED ST5 | 1993 |   81387026 |
| UNITED ST9 | UNITED ST5 | 1993 |   79370538 |
| UNITED ST7 | UNITED ST3 | 1993 |   79047722 |
| UNITED ST8 | UNITED ST8 | 1993 |   77580470 |
| UNITED ST8 | UNITED ST9 | 1993 |   77032722 |
| UNITED ST2 | UNITED ST5 | 1993 |   74813690 |
| UNITED ST9 | UNITED ST8 | 1993 |   74369392 |
| UNITED ST8 | UNITED ST7 | 1993 |   73804436 |
| UNITED ST6 | UNITED ST8 | 1993 |   72913482 |
| UNITED ST7 | UNITED ST1 | 1993 |   68782318 |
| UNITED ST6 | UNITED ST5 | 1993 |   68458164 |
| UNITED ST5 | UNITED ST3 | 1993 |   68063622 |
| UNITED ST2 | UNITED ST8 | 1993 |   66890892 |
| UNITED ST4 | UNITED ST3 | 1993 |   66258824 |
| UNITED ST6 | UNITED ST6 | 1993 |   66101326 |
| UNITED ST9 | UNITED ST0 | 1993 |   65306610 |
| UNITED ST4 | UNITED ST6 | 1993 |   61398510 |
| UNITED ST9 | UNITED ST4 | 1993 |   61289374 |
| UNITED ST4 | UNITED ST5 | 1993 |   58239188 |
| UNITED ST7 | UNITED ST6 | 1993 |   54201004 |
| UNITED ST4 | UNITED ST2 | 1993 |   54025356 |
| UNITED ST2 | UNITED ST6 | 1993 |   52964452 |
| UNITED ST5 | UNITED ST6 | 1993 |   50715358 |
| UNITED ST3 | UNITED ST3 | 1993 |   43554288 |
| UNITED ST3 | UNITED ST2 | 1993 |   43118146 |
| UNITED ST5 | UNITED ST2 | 1993 |   41220484 |
| UNITED ST7 | UNITED ST2 | 1993 |   40438608 |
| UNITED ST6 | UNITED ST2 | 1993 |   37628734 |
| UNITED ST9 | UNITED ST2 | 1993 |   35436780 |
| UNITED ST1 | UNITED ST2 | 1993 |   33689076 |
| UNITED ST0 | UNITED ST2 | 1993 |   30084290 |
| UNITED ST2 | UNITED ST2 | 1993 |   29043990 |
| UNITED ST8 | UNITED ST2 | 1993 |   19968732 |
| UNITED ST8 | UNITED ST0 | 1994 |  198441578 |
| UNITED ST3 | UNITED ST9 | 1994 |  194952370 |
| UNITED ST6 | UNITED ST1 | 1994 |  193874294 |
| UNITED ST6 | UNITED ST9 | 1994 |  189366618 |
| UNITED ST9 | UNITED ST1 | 1994 |  180881896 |
| UNITED ST0 | UNITED ST9 | 1994 |  179730404 |
| UNITED ST5 | UNITED ST7 | 1994 |  178179922 |
| UNITED ST9 | UNITED ST0 | 1994 |  175341146 |
| UNITED ST3 | UNITED ST1 | 1994 |  171047306 |
| UNITED ST4 | UNITED ST9 | 1994 |  167644786 |
| UNITED ST0 | UNITED ST0 | 1994 |  167053754 |
| UNITED ST7 | UNITED ST0 | 1994 |  164531072 |
| UNITED ST2 | UNITED ST1 | 1994 |  162600178 |
| UNITED ST5 | UNITED ST0 | 1994 |  157296114 |
| UNITED ST4 | UNITED ST7 | 1994 |  153908280 |
| UNITED ST4 | UNITED ST4 | 1994 |  153674762 |
| UNITED ST0 | UNITED ST1 | 1994 |  153226758 |
| UNITED ST1 | UNITED ST3 | 1994 |  151984918 |
| UNITED ST7 | UNITED ST1 | 1994 |  150641598 |
| UNITED ST4 | UNITED ST0 | 1994 |  147438680 |
| UNITED ST5 | UNITED ST1 | 1994 |  147016836 |
| UNITED ST4 | UNITED ST1 | 1994 |  144439114 |
| UNITED ST2 | UNITED ST9 | 1994 |  139342108 |
| UNITED ST6 | UNITED ST5 | 1994 |  132923068 |
| UNITED ST2 | UNITED ST3 | 1994 |  131241520 |
| UNITED ST3 | UNITED ST0 | 1994 |  131045454 |
| UNITED ST5 | UNITED ST3 | 1994 |  130669822 |
| UNITED ST7 | UNITED ST4 | 1994 |  129557430 |
| UNITED ST3 | UNITED ST4 | 1994 |  126824730 |
| UNITED ST8 | UNITED ST4 | 1994 |  124283362 |
| UNITED ST0 | UNITED ST4 | 1994 |  123039488 |
| UNITED ST0 | UNITED ST7 | 1994 |  122961640 |
| UNITED ST0 | UNITED ST6 | 1994 |  122577556 |
| UNITED ST2 | UNITED ST0 | 1994 |  120364306 |
| UNITED ST6 | UNITED ST4 | 1994 |  119659978 |
| UNITED ST4 | UNITED ST5 | 1994 |  118794056 |
| UNITED ST8 | UNITED ST9 | 1994 |  117333812 |
| UNITED ST4 | UNITED ST6 | 1994 |  117266964 |
| UNITED ST5 | UNITED ST5 | 1994 |  112470426 |
| UNITED ST6 | UNITED ST3 | 1994 |  112246476 |
| UNITED ST2 | UNITED ST4 | 1994 |  111358754 |
| UNITED ST8 | UNITED ST3 | 1994 |  110407682 |
| UNITED ST1 | UNITED ST1 | 1994 |  108766348 |
| UNITED ST1 | UNITED ST7 | 1994 |  107706212 |
| UNITED ST6 | UNITED ST0 | 1994 |  107457706 |
| UNITED ST5 | UNITED ST9 | 1994 |  106734662 |
| UNITED ST9 | UNITED ST9 | 1994 |  103961698 |
| UNITED ST5 | UNITED ST4 | 1994 |  103599186 |
| UNITED ST7 | UNITED ST9 | 1994 |  100288170 |
| UNITED ST7 | UNITED ST7 | 1994 |   92892884 |
| UNITED ST6 | UNITED ST6 | 1994 |   92399444 |
| UNITED ST7 | UNITED ST5 | 1994 |   91790728 |
| UNITED ST3 | UNITED ST3 | 1994 |   91254306 |
| UNITED ST8 | UNITED ST5 | 1994 |   89106112 |
| UNITED ST9 | UNITED ST4 | 1994 |   87821522 |
| UNITED ST1 | UNITED ST0 | 1994 |   86450402 |
| UNITED ST1 | UNITED ST9 | 1994 |   86000074 |
| UNITED ST7 | UNITED ST8 | 1994 |   85552934 |
| UNITED ST0 | UNITED ST5 | 1994 |   83616602 |
| UNITED ST2 | UNITED ST6 | 1994 |   83052210 |
| UNITED ST1 | UNITED ST4 | 1994 |   82763116 |
| UNITED ST3 | UNITED ST7 | 1994 |   81870262 |
| UNITED ST8 | UNITED ST1 | 1994 |   80304192 |
| UNITED ST9 | UNITED ST8 | 1994 |   78557616 |
| UNITED ST5 | UNITED ST6 | 1994 |   77316902 |
| UNITED ST2 | UNITED ST5 | 1994 |   75280634 |
| UNITED ST8 | UNITED ST7 | 1994 |   75201374 |
| UNITED ST9 | UNITED ST5 | 1994 |   74293452 |
| UNITED ST6 | UNITED ST7 | 1994 |   74115616 |
| UNITED ST8 | UNITED ST6 | 1994 |   73553138 |
| UNITED ST3 | UNITED ST6 | 1994 |   72580514 |
| UNITED ST9 | UNITED ST3 | 1994 |   71693000 |
| UNITED ST2 | UNITED ST8 | 1994 |   67535548 |
| UNITED ST0 | UNITED ST8 | 1994 |   63690866 |
| UNITED ST4 | UNITED ST3 | 1994 |   63198866 |
| UNITED ST9 | UNITED ST7 | 1994 |   63172346 |
| UNITED ST1 | UNITED ST6 | 1994 |   62574652 |
| UNITED ST1 | UNITED ST8 | 1994 |   60490306 |
| UNITED ST7 | UNITED ST3 | 1994 |   58849680 |
| UNITED ST9 | UNITED ST6 | 1994 |   58425854 |
| UNITED ST0 | UNITED ST3 | 1994 |   54655658 |
| UNITED ST6 | UNITED ST8 | 1994 |   53185992 |
| UNITED ST3 | UNITED ST5 | 1994 |   52395750 |
| UNITED ST6 | UNITED ST2 | 1994 |   51618000 |
| UNITED ST1 | UNITED ST5 | 1994 |   49878276 |
| UNITED ST7 | UNITED ST6 | 1994 |   49263874 |
| UNITED ST1 | UNITED ST2 | 1994 |   47113172 |
| UNITED ST4 | UNITED ST2 | 1994 |   46071784 |
| UNITED ST2 | UNITED ST7 | 1994 |   44365516 |
| UNITED ST0 | UNITED ST2 | 1994 |   44035908 |
| UNITED ST4 | UNITED ST8 | 1994 |   41370704 |
| UNITED ST7 | UNITED ST2 | 1994 |   39310162 |
| UNITED ST5 | UNITED ST8 | 1994 |   37863782 |
| UNITED ST2 | UNITED ST2 | 1994 |   36137314 |
| UNITED ST3 | UNITED ST8 | 1994 |   31872102 |
| UNITED ST8 | UNITED ST8 | 1994 |   20046824 |
| UNITED ST3 | UNITED ST2 | 1994 |   19990468 |
| UNITED ST9 | UNITED ST2 | 1994 |   19401978 |
| UNITED ST5 | UNITED ST2 | 1994 |   14325592 |
| UNITED ST8 | UNITED ST2 | 1994 |    7579252 |
| UNITED ST5 | UNITED ST1 | 1995 |  239587338 |
| UNITED ST4 | UNITED ST9 | 1995 |  198980136 |
| UNITED ST7 | UNITED ST0 | 1995 |  196062590 |
| UNITED ST6 | UNITED ST0 | 1995 |  183436942 |
| UNITED ST4 | UNITED ST1 | 1995 |  181757306 |
| UNITED ST0 | UNITED ST1 | 1995 |  181527198 |
| UNITED ST8 | UNITED ST9 | 1995 |  177710178 |
| UNITED ST7 | UNITED ST7 | 1995 |  173143248 |
| UNITED ST3 | UNITED ST0 | 1995 |  168925466 |
| UNITED ST9 | UNITED ST1 | 1995 |  165877934 |
| UNITED ST2 | UNITED ST4 | 1995 |  164864610 |
| UNITED ST1 | UNITED ST0 | 1995 |  163353246 |
| UNITED ST5 | UNITED ST4 | 1995 |  162033522 |
| UNITED ST7 | UNITED ST1 | 1995 |  159928724 |
| UNITED ST5 | UNITED ST3 | 1995 |  156198260 |
| UNITED ST5 | UNITED ST0 | 1995 |  155231492 |
| UNITED ST9 | UNITED ST9 | 1995 |  153031916 |
| UNITED ST7 | UNITED ST9 | 1995 |  150635418 |
| UNITED ST4 | UNITED ST4 | 1995 |  149174142 |
| UNITED ST9 | UNITED ST4 | 1995 |  145051372 |
| UNITED ST1 | UNITED ST9 | 1995 |  144941740 |
| UNITED ST4 | UNITED ST7 | 1995 |  138528814 |
| UNITED ST6 | UNITED ST3 | 1995 |  135026124 |
| UNITED ST2 | UNITED ST3 | 1995 |  130436258 |
| UNITED ST2 | UNITED ST9 | 1995 |  130110356 |
| UNITED ST7 | UNITED ST6 | 1995 |  130041342 |
| UNITED ST3 | UNITED ST1 | 1995 |  129525630 |
| UNITED ST1 | UNITED ST1 | 1995 |  128398664 |
| UNITED ST6 | UNITED ST9 | 1995 |  126914210 |
| UNITED ST0 | UNITED ST9 | 1995 |  126506998 |
| UNITED ST5 | UNITED ST9 | 1995 |  124729794 |
| UNITED ST4 | UNITED ST5 | 1995 |  124163010 |
| UNITED ST1 | UNITED ST7 | 1995 |  123031482 |
| UNITED ST2 | UNITED ST7 | 1995 |  120000416 |
| UNITED ST8 | UNITED ST6 | 1995 |  117980808 |
| UNITED ST1 | UNITED ST4 | 1995 |  115071198 |
| UNITED ST0 | UNITED ST3 | 1995 |  112721416 |
| UNITED ST8 | UNITED ST0 | 1995 |  110463328 |
| UNITED ST5 | UNITED ST7 | 1995 |  107481518 |
| UNITED ST2 | UNITED ST0 | 1995 |  105121676 |
| UNITED ST3 | UNITED ST7 | 1995 |  103159096 |
| UNITED ST9 | UNITED ST0 | 1995 |  103097242 |
| UNITED ST6 | UNITED ST6 | 1995 |  101909354 |
| UNITED ST5 | UNITED ST5 | 1995 |  100788014 |
| UNITED ST7 | UNITED ST4 | 1995 |   99799090 |
| UNITED ST3 | UNITED ST3 | 1995 |   96316178 |
| UNITED ST6 | UNITED ST4 | 1995 |   95394482 |
| UNITED ST9 | UNITED ST7 | 1995 |   92929178 |
| UNITED ST4 | UNITED ST0 | 1995 |   92285798 |
| UNITED ST1 | UNITED ST3 | 1995 |   91646112 |
| UNITED ST2 | UNITED ST1 | 1995 |   90874680 |
| UNITED ST6 | UNITED ST5 | 1995 |   90856304 |
| UNITED ST8 | UNITED ST5 | 1995 |   89989726 |
| UNITED ST7 | UNITED ST3 | 1995 |   87399468 |
| UNITED ST9 | UNITED ST6 | 1995 |   86964988 |
| UNITED ST2 | UNITED ST5 | 1995 |   86764834 |
| UNITED ST6 | UNITED ST8 | 1995 |   83947840 |
| UNITED ST0 | UNITED ST6 | 1995 |   81437884 |
| UNITED ST3 | UNITED ST5 | 1995 |   80115630 |
| UNITED ST7 | UNITED ST5 | 1995 |   78030586 |
| UNITED ST0 | UNITED ST0 | 1995 |   77969004 |
| UNITED ST6 | UNITED ST1 | 1995 |   76656704 |
| UNITED ST4 | UNITED ST6 | 1995 |   76219048 |
| UNITED ST3 | UNITED ST9 | 1995 |   74729246 |
| UNITED ST4 | UNITED ST3 | 1995 |   74712792 |
| UNITED ST2 | UNITED ST6 | 1995 |   74292576 |
| UNITED ST9 | UNITED ST5 | 1995 |   72019848 |
| UNITED ST1 | UNITED ST8 | 1995 |   69837586 |
| UNITED ST8 | UNITED ST1 | 1995 |   68435560 |
| UNITED ST0 | UNITED ST7 | 1995 |   66790626 |
| UNITED ST1 | UNITED ST5 | 1995 |   63714904 |
| UNITED ST8 | UNITED ST7 | 1995 |   61836404 |
| UNITED ST2 | UNITED ST8 | 1995 |   61008378 |
| UNITED ST3 | UNITED ST4 | 1995 |   60844692 |
| UNITED ST5 | UNITED ST6 | 1995 |   60409474 |
| UNITED ST8 | UNITED ST3 | 1995 |   58699876 |
| UNITED ST0 | UNITED ST4 | 1995 |   58340076 |
| UNITED ST1 | UNITED ST6 | 1995 |   54278806 |
| UNITED ST7 | UNITED ST8 | 1995 |   52888980 |
| UNITED ST6 | UNITED ST7 | 1995 |   47667954 |
| UNITED ST4 | UNITED ST8 | 1995 |   46106472 |
| UNITED ST4 | UNITED ST2 | 1995 |   45574006 |
| UNITED ST3 | UNITED ST8 | 1995 |   45010478 |
| UNITED ST9 | UNITED ST8 | 1995 |   42585054 |
| UNITED ST8 | UNITED ST4 | 1995 |   38574622 |
| UNITED ST8 | UNITED ST2 | 1995 |   36565980 |
| UNITED ST9 | UNITED ST3 | 1995 |   35078204 |
| UNITED ST3 | UNITED ST6 | 1995 |   33477060 |
| UNITED ST0 | UNITED ST8 | 1995 |   32786498 |
| UNITED ST5 | UNITED ST2 | 1995 |   29902046 |
| UNITED ST2 | UNITED ST2 | 1995 |   26910062 |
| UNITED ST5 | UNITED ST8 | 1995 |   26693864 |
| UNITED ST3 | UNITED ST2 | 1995 |   25773658 |
| UNITED ST9 | UNITED ST2 | 1995 |   25306724 |
| UNITED ST0 | UNITED ST5 | 1995 |   22907418 |
| UNITED ST6 | UNITED ST2 | 1995 |   22727102 |
| UNITED ST8 | UNITED ST8 | 1995 |   22571734 |
| UNITED ST1 | UNITED ST2 | 1995 |   15983352 |
| UNITED ST0 | UNITED ST2 | 1995 |    9552920 |
| UNITED ST7 | UNITED ST2 | 1995 |    7947130 |
| UNITED ST6 | UNITED ST0 | 1996 |  264573526 |
| UNITED ST4 | UNITED ST0 | 1996 |  213795126 |
| UNITED ST5 | UNITED ST0 | 1996 |  209003958 |
| UNITED ST0 | UNITED ST4 | 1996 |  206457498 |
| UNITED ST9 | UNITED ST1 | 1996 |  203967654 |
| UNITED ST1 | UNITED ST0 | 1996 |  189723108 |
| UNITED ST0 | UNITED ST1 | 1996 |  183897554 |
| UNITED ST6 | UNITED ST1 | 1996 |  179411740 |
| UNITED ST2 | UNITED ST1 | 1996 |  176512310 |
| UNITED ST1 | UNITED ST1 | 1996 |  174531696 |
| UNITED ST4 | UNITED ST7 | 1996 |  167355628 |
| UNITED ST6 | UNITED ST3 | 1996 |  164336458 |
| UNITED ST2 | UNITED ST7 | 1996 |  160936954 |
| UNITED ST8 | UNITED ST1 | 1996 |  157943512 |
| UNITED ST7 | UNITED ST4 | 1996 |  155882022 |
| UNITED ST1 | UNITED ST3 | 1996 |  155221810 |
| UNITED ST9 | UNITED ST9 | 1996 |  154603480 |
| UNITED ST0 | UNITED ST9 | 1996 |  151870418 |
| UNITED ST7 | UNITED ST0 | 1996 |  151204890 |
| UNITED ST3 | UNITED ST1 | 1996 |  149493398 |
| UNITED ST7 | UNITED ST7 | 1996 |  148081288 |
| UNITED ST4 | UNITED ST1 | 1996 |  145639734 |
| UNITED ST5 | UNITED ST9 | 1996 |  145228228 |
| UNITED ST1 | UNITED ST9 | 1996 |  139647538 |
| UNITED ST9 | UNITED ST4 | 1996 |  139233228 |
| UNITED ST6 | UNITED ST4 | 1996 |  138592010 |
| UNITED ST2 | UNITED ST0 | 1996 |  134190244 |
| UNITED ST5 | UNITED ST1 | 1996 |  130692778 |
| UNITED ST6 | UNITED ST9 | 1996 |  126512364 |
| UNITED ST4 | UNITED ST6 | 1996 |  124378656 |
| UNITED ST0 | UNITED ST0 | 1996 |  123057710 |
| UNITED ST8 | UNITED ST9 | 1996 |  120933382 |
| UNITED ST3 | UNITED ST0 | 1996 |  120453680 |
| UNITED ST8 | UNITED ST6 | 1996 |  119493310 |
| UNITED ST2 | UNITED ST3 | 1996 |  119297196 |
| UNITED ST0 | UNITED ST5 | 1996 |  115525790 |
| UNITED ST8 | UNITED ST7 | 1996 |  115047850 |
| UNITED ST2 | UNITED ST4 | 1996 |  114974114 |
| UNITED ST6 | UNITED ST7 | 1996 |  114181238 |
| UNITED ST3 | UNITED ST4 | 1996 |  109676518 |
| UNITED ST4 | UNITED ST9 | 1996 |  108269680 |
| UNITED ST1 | UNITED ST6 | 1996 |  108112732 |
| UNITED ST3 | UNITED ST7 | 1996 |  107974436 |
| UNITED ST2 | UNITED ST9 | 1996 |  106982830 |
| UNITED ST4 | UNITED ST8 | 1996 |  106071324 |
| UNITED ST9 | UNITED ST5 | 1996 |  105651844 |
| UNITED ST7 | UNITED ST3 | 1996 |  104713772 |
| UNITED ST6 | UNITED ST8 | 1996 |  104273568 |
| UNITED ST1 | UNITED ST5 | 1996 |  102379298 |
| UNITED ST8 | UNITED ST4 | 1996 |  102066108 |
| UNITED ST1 | UNITED ST4 | 1996 |  100271094 |
| UNITED ST3 | UNITED ST9 | 1996 |   99224608 |
| UNITED ST9 | UNITED ST0 | 1996 |   99181402 |
| UNITED ST3 | UNITED ST3 | 1996 |   98527592 |
| UNITED ST9 | UNITED ST7 | 1996 |   97597518 |
| UNITED ST7 | UNITED ST1 | 1996 |   97568350 |
| UNITED ST9 | UNITED ST6 | 1996 |   97370126 |
| UNITED ST2 | UNITED ST5 | 1996 |   94057952 |
| UNITED ST9 | UNITED ST3 | 1996 |   94042036 |
| UNITED ST2 | UNITED ST8 | 1996 |   93730226 |
| UNITED ST4 | UNITED ST3 | 1996 |   92921880 |
| UNITED ST6 | UNITED ST5 | 1996 |   92060208 |
| UNITED ST2 | UNITED ST6 | 1996 |   90833298 |
| UNITED ST8 | UNITED ST5 | 1996 |   86960946 |
| UNITED ST5 | UNITED ST5 | 1996 |   86041444 |
| UNITED ST6 | UNITED ST6 | 1996 |   85846064 |
| UNITED ST4 | UNITED ST5 | 1996 |   85616824 |
| UNITED ST3 | UNITED ST6 | 1996 |   83763256 |
| UNITED ST1 | UNITED ST7 | 1996 |   83443012 |
| UNITED ST5 | UNITED ST7 | 1996 |   81892660 |
| UNITED ST8 | UNITED ST0 | 1996 |   79690854 |
| UNITED ST8 | UNITED ST3 | 1996 |   79071880 |
| UNITED ST1 | UNITED ST8 | 1996 |   78861764 |
| UNITED ST5 | UNITED ST6 | 1996 |   76664088 |
| UNITED ST0 | UNITED ST6 | 1996 |   74464124 |
| UNITED ST7 | UNITED ST6 | 1996 |   73071256 |
| UNITED ST9 | UNITED ST8 | 1996 |   72224602 |
| UNITED ST3 | UNITED ST8 | 1996 |   67849464 |
| UNITED ST3 | UNITED ST5 | 1996 |   67434878 |
| UNITED ST5 | UNITED ST4 | 1996 |   66849718 |
| UNITED ST5 | UNITED ST3 | 1996 |   65839852 |
| UNITED ST4 | UNITED ST4 | 1996 |   65575990 |
| UNITED ST7 | UNITED ST5 | 1996 |   65568448 |
| UNITED ST5 | UNITED ST8 | 1996 |   64831364 |
| UNITED ST0 | UNITED ST7 | 1996 |   62782362 |
| UNITED ST0 | UNITED ST3 | 1996 |   59591330 |
| UNITED ST7 | UNITED ST9 | 1996 |   50056182 |
| UNITED ST7 | UNITED ST8 | 1996 |   48697702 |
| UNITED ST6 | UNITED ST2 | 1996 |   40895694 |
| UNITED ST8 | UNITED ST8 | 1996 |   32681206 |
| UNITED ST0 | UNITED ST8 | 1996 |   30336524 |
| UNITED ST4 | UNITED ST2 | 1996 |   24903734 |
| UNITED ST1 | UNITED ST2 | 1996 |   20165072 |
| UNITED ST5 | UNITED ST2 | 1996 |   17088466 |
| UNITED ST7 | UNITED ST2 | 1996 |   16780940 |
| UNITED ST9 | UNITED ST2 | 1996 |   16216070 |
| UNITED ST8 | UNITED ST2 | 1996 |   14056668 |
| UNITED ST0 | UNITED ST2 | 1996 |   13814398 |
| UNITED ST3 | UNITED ST2 | 1996 |    8623600 |
| UNITED ST5 | UNITED ST0 | 1997 |  242915532 |
| UNITED ST0 | UNITED ST9 | 1997 |  239712536 |
| UNITED ST5 | UNITED ST1 | 1997 |  213800322 |
| UNITED ST9 | UNITED ST9 | 1997 |  212445590 |
| UNITED ST5 | UNITED ST4 | 1997 |  206865854 |
| UNITED ST7 | UNITED ST1 | 1997 |  202653880 |
| UNITED ST5 | UNITED ST9 | 1997 |  194785280 |
| UNITED ST8 | UNITED ST0 | 1997 |  178869690 |
| UNITED ST1 | UNITED ST3 | 1997 |  170351276 |
| UNITED ST4 | UNITED ST1 | 1997 |  169222376 |
| UNITED ST4 | UNITED ST7 | 1997 |  169213992 |
| UNITED ST1 | UNITED ST4 | 1997 |  166185138 |
| UNITED ST0 | UNITED ST1 | 1997 |  160334278 |
| UNITED ST4 | UNITED ST9 | 1997 |  159395854 |
| UNITED ST1 | UNITED ST0 | 1997 |  155335732 |
| UNITED ST2 | UNITED ST0 | 1997 |  155182940 |
| UNITED ST1 | UNITED ST7 | 1997 |  154091444 |
| UNITED ST2 | UNITED ST7 | 1997 |  152967604 |
| UNITED ST1 | UNITED ST1 | 1997 |  152680888 |
| UNITED ST0 | UNITED ST7 | 1997 |  145154980 |
| UNITED ST4 | UNITED ST0 | 1997 |  139751608 |
| UNITED ST6 | UNITED ST3 | 1997 |  139451012 |
| UNITED ST2 | UNITED ST9 | 1997 |  139087968 |
| UNITED ST7 | UNITED ST0 | 1997 |  138708624 |
| UNITED ST9 | UNITED ST7 | 1997 |  138105260 |
| UNITED ST8 | UNITED ST3 | 1997 |  133836788 |
| UNITED ST0 | UNITED ST0 | 1997 |  132617032 |
| UNITED ST9 | UNITED ST0 | 1997 |  132133582 |
| UNITED ST2 | UNITED ST3 | 1997 |  130858906 |
| UNITED ST2 | UNITED ST1 | 1997 |  130792270 |
| UNITED ST4 | UNITED ST4 | 1997 |  125064692 |
| UNITED ST9 | UNITED ST1 | 1997 |  124836812 |
| UNITED ST3 | UNITED ST7 | 1997 |  122190600 |
| UNITED ST7 | UNITED ST4 | 1997 |  120246988 |
| UNITED ST4 | UNITED ST3 | 1997 |  119268306 |
| UNITED ST3 | UNITED ST4 | 1997 |  116712282 |
| UNITED ST6 | UNITED ST9 | 1997 |  116462526 |
| UNITED ST6 | UNITED ST4 | 1997 |  114430044 |
| UNITED ST2 | UNITED ST4 | 1997 |  114025222 |
| UNITED ST5 | UNITED ST3 | 1997 |  113579864 |
| UNITED ST9 | UNITED ST5 | 1997 |  112183840 |
| UNITED ST6 | UNITED ST0 | 1997 |  111649838 |
| UNITED ST6 | UNITED ST1 | 1997 |  110235418 |
| UNITED ST7 | UNITED ST9 | 1997 |  110079940 |
| UNITED ST5 | UNITED ST7 | 1997 |  109068630 |
| UNITED ST3 | UNITED ST1 | 1997 |  108301366 |
| UNITED ST3 | UNITED ST0 | 1997 |  108100344 |
| UNITED ST3 | UNITED ST9 | 1997 |  102740616 |
| UNITED ST1 | UNITED ST5 | 1997 |  102104220 |
| UNITED ST6 | UNITED ST7 | 1997 |   99591698 |
| UNITED ST5 | UNITED ST6 | 1997 |   98060032 |
| UNITED ST1 | UNITED ST9 | 1997 |   97888222 |
| UNITED ST3 | UNITED ST3 | 1997 |   96770466 |
| UNITED ST0 | UNITED ST5 | 1997 |   95976836 |
| UNITED ST2 | UNITED ST8 | 1997 |   92783818 |
| UNITED ST4 | UNITED ST6 | 1997 |   92473698 |
| UNITED ST9 | UNITED ST3 | 1997 |   92243448 |
| UNITED ST8 | UNITED ST9 | 1997 |   91705592 |
| UNITED ST7 | UNITED ST8 | 1997 |   90952532 |
| UNITED ST8 | UNITED ST1 | 1997 |   86568278 |
| UNITED ST7 | UNITED ST7 | 1997 |   85133206 |
| UNITED ST0 | UNITED ST4 | 1997 |   82387606 |
| UNITED ST8 | UNITED ST7 | 1997 |   81756858 |
| UNITED ST8 | UNITED ST8 | 1997 |   81498800 |
| UNITED ST2 | UNITED ST5 | 1997 |   81325772 |
| UNITED ST0 | UNITED ST3 | 1997 |   80157016 |
| UNITED ST6 | UNITED ST8 | 1997 |   75976890 |
| UNITED ST9 | UNITED ST6 | 1997 |   75193764 |
| UNITED ST6 | UNITED ST5 | 1997 |   75143576 |
| UNITED ST2 | UNITED ST2 | 1997 |   74068666 |
| UNITED ST7 | UNITED ST5 | 1997 |   73779472 |
| UNITED ST8 | UNITED ST4 | 1997 |   73201168 |
| UNITED ST3 | UNITED ST6 | 1997 |   72151688 |
| UNITED ST7 | UNITED ST3 | 1997 |   70337844 |
| UNITED ST2 | UNITED ST6 | 1997 |   68548934 |
| UNITED ST5 | UNITED ST8 | 1997 |   65821892 |
| UNITED ST3 | UNITED ST5 | 1997 |   65623926 |
| UNITED ST4 | UNITED ST8 | 1997 |   65199472 |
| UNITED ST5 | UNITED ST5 | 1997 |   65137776 |
| UNITED ST4 | UNITED ST5 | 1997 |   63991736 |
| UNITED ST9 | UNITED ST4 | 1997 |   63530956 |
| UNITED ST7 | UNITED ST2 | 1997 |   62819180 |
| UNITED ST9 | UNITED ST8 | 1997 |   62544770 |
| UNITED ST0 | UNITED ST8 | 1997 |   60482740 |
| UNITED ST3 | UNITED ST8 | 1997 |   58204440 |
| UNITED ST7 | UNITED ST6 | 1997 |   55079862 |
| UNITED ST8 | UNITED ST5 | 1997 |   53347486 |
| UNITED ST6 | UNITED ST6 | 1997 |   49966582 |
| UNITED ST0 | UNITED ST2 | 1997 |   47168458 |
| UNITED ST0 | UNITED ST6 | 1997 |   45848092 |
| UNITED ST1 | UNITED ST2 | 1997 |   41198260 |
| UNITED ST8 | UNITED ST6 | 1997 |   40146000 |
| UNITED ST1 | UNITED ST6 | 1997 |   36410652 |
| UNITED ST1 | UNITED ST8 | 1997 |   30750516 |
| UNITED ST6 | UNITED ST2 | 1997 |   29493360 |
| UNITED ST5 | UNITED ST2 | 1997 |   27726876 |
| UNITED ST8 | UNITED ST2 | 1997 |   24107412 |
| UNITED ST3 | UNITED ST2 | 1997 |   15783756 |
| UNITED ST4 | UNITED ST2 | 1997 |    5696314 |
| UNITED ST9 | UNITED ST2 | 1997 |    5323304 |
+------------+------------+------+------------+

--Q3.3
+------------+------------+------+------------+
| c_city     | s_city     | year | lo_revenue |
+------------+------------+------+------------+
| UNITED KI1 | UNITED KI1 | 1992 |   93471990 |
| UNITED KI5 | UNITED KI1 | 1992 |   72554110 |
| UNITED KI5 | UNITED KI5 | 1992 |   50710534 |
| UNITED KI1 | UNITED KI5 | 1992 |   43835692 |
| UNITED KI5 | UNITED KI1 | 1993 |  122035214 |
| UNITED KI1 | UNITED KI1 | 1993 |   91339070 |
| UNITED KI5 | UNITED KI5 | 1993 |   68198784 |
| UNITED KI1 | UNITED KI5 | 1993 |   42888412 |
| UNITED KI5 | UNITED KI1 | 1994 |   72564326 |
| UNITED KI1 | UNITED KI1 | 1994 |   69736882 |
| UNITED KI5 | UNITED KI5 | 1994 |   69014568 |
| UNITED KI1 | UNITED KI5 | 1994 |   42443560 |
| UNITED KI5 | UNITED KI1 | 1995 |  165911792 |
| UNITED KI1 | UNITED KI1 | 1995 |   71762372 |
| UNITED KI5 | UNITED KI5 | 1995 |   41079610 |
| UNITED KI1 | UNITED KI5 | 1995 |   34353020 |
| UNITED KI5 | UNITED KI1 | 1996 |  131534098 |
| UNITED KI1 | UNITED KI1 | 1996 |  119846074 |
| UNITED KI5 | UNITED KI5 | 1996 |   92154684 |
| UNITED KI1 | UNITED KI5 | 1996 |   27400508 |
| UNITED KI1 | UNITED KI1 | 1997 |  140686266 |
| UNITED KI5 | UNITED KI1 | 1997 |  129956718 |
| UNITED KI5 | UNITED KI5 | 1997 |   54664054 |
| UNITED KI1 | UNITED KI5 | 1997 |   32821336 |
+------------+------------+------+------------+

--Q3.4
+------------+------------+------+------------+
| c_city     | s_city     | year | lo_revenue |
+------------+------------+------+------------+
| UNITED KI5 | UNITED KI1 | 1997 |   18235692 |
| UNITED KI5 | UNITED KI5 | 1997 |   12407602 |
| UNITED KI1 | UNITED KI5 | 1997 |    3740140 |
+------------+------------+------+------------+

--Q4.1
+------+---------------+-------------+
| year | c_nation      | profit      |
+------+---------------+-------------+
| 1992 | ARGENTINA     | 19317928938 |
| 1992 | BRAZIL        | 18453966110 |
| 1992 | CANADA        | 19286353574 |
| 1992 | PERU          | 18821353194 |
| 1992 | UNITED STATES | 19698855306 |
| 1993 | ARGENTINA     | 19952665706 |
| 1993 | BRAZIL        | 18937598458 |
| 1993 | CANADA        | 19794604840 |
| 1993 | PERU          | 18618891672 |
| 1993 | UNITED STATES | 20007970172 |
| 1994 | ARGENTINA     | 19880610430 |
| 1994 | BRAZIL        | 18697303354 |
| 1994 | CANADA        | 19165295192 |
| 1994 | PERU          | 18590530026 |
| 1994 | UNITED STATES | 19039760850 |
| 1995 | ARGENTINA     | 20287682760 |
| 1995 | BRAZIL        | 18312154700 |
| 1995 | CANADA        | 19125224320 |
| 1995 | PERU          | 19556174422 |
| 1995 | UNITED STATES | 18621130488 |
| 1996 | ARGENTINA     | 20003855790 |
| 1996 | BRAZIL        | 18336970302 |
| 1996 | CANADA        | 20123208406 |
| 1996 | PERU          | 18710271348 |
| 1996 | UNITED STATES | 19539424348 |
| 1997 | ARGENTINA     | 19709120522 |
| 1997 | BRAZIL        | 18243142094 |
| 1997 | CANADA        | 20194743556 |
| 1997 | PERU          | 18631051834 |
| 1997 | UNITED STATES | 21013447758 |
| 1998 | ARGENTINA     | 11668480814 |
| 1998 | BRAZIL        | 10712796190 |
| 1998 | CANADA        | 10846422392 |
| 1998 | PERU          | 11452371940 |
| 1998 | UNITED STATES | 12018924038 |
+------+---------------+-------------+

--Q4.2
+------+---------------+------------+------------+
| year | s_nation      | p_category | profit     |
+------+---------------+------------+------------+
| 1997 | ARGENTINA     | MFGR#11    | 1814143132 |
| 1997 | ARGENTINA     | MFGR#12    | 1848231124 |
| 1997 | ARGENTINA     | MFGR#13    | 1945723642 |
| 1997 | ARGENTINA     | MFGR#14    | 1950820690 |
| 1997 | ARGENTINA     | MFGR#15    | 1877734750 |
| 1997 | ARGENTINA     | MFGR#21    | 2029565148 |
| 1997 | ARGENTINA     | MFGR#22    | 1746033566 |
| 1997 | ARGENTINA     | MFGR#23    | 2060714604 |
| 1997 | ARGENTINA     | MFGR#24    | 1786921158 |
| 1997 | ARGENTINA     | MFGR#25    | 2012622806 |
| 1997 | BRAZIL        | MFGR#11    | 2146438656 |
| 1997 | BRAZIL        | MFGR#12    | 1979717666 |
| 1997 | BRAZIL        | MFGR#13    | 2256960758 |
| 1997 | BRAZIL        | MFGR#14    | 2388513444 |
| 1997 | BRAZIL        | MFGR#15    | 2188838248 |
| 1997 | BRAZIL        | MFGR#21    | 1820053664 |
| 1997 | BRAZIL        | MFGR#22    | 1986284096 |
| 1997 | BRAZIL        | MFGR#23    | 2215345748 |
| 1997 | BRAZIL        | MFGR#24    | 2116027298 |
| 1997 | BRAZIL        | MFGR#25    | 1989467528 |
| 1997 | CANADA        | MFGR#11    | 1709450040 |
| 1997 | CANADA        | MFGR#12    | 1877436328 |
| 1997 | CANADA        | MFGR#13    | 1918531780 |
| 1997 | CANADA        | MFGR#14    | 2005624900 |
| 1997 | CANADA        | MFGR#15    | 1696366026 |
| 1997 | CANADA        | MFGR#21    | 1999610544 |
| 1997 | CANADA        | MFGR#22    | 1556839526 |
| 1997 | CANADA        | MFGR#23    | 1856719290 |
| 1997 | CANADA        | MFGR#24    | 1699790256 |
| 1997 | CANADA        | MFGR#25    | 1809175930 |
| 1997 | PERU          | MFGR#11    | 2200485754 |
| 1997 | PERU          | MFGR#12    | 1988730700 |
| 1997 | PERU          | MFGR#13    | 1694972210 |
| 1997 | PERU          | MFGR#14    | 1895539366 |
| 1997 | PERU          | MFGR#15    | 1998791356 |
| 1997 | PERU          | MFGR#21    | 1735846788 |
| 1997 | PERU          | MFGR#22    | 1977494918 |
| 1997 | PERU          | MFGR#23    | 2133290172 |
| 1997 | PERU          | MFGR#24    | 1871331450 |
| 1997 | PERU          | MFGR#25    | 1962908258 |
| 1997 | UNITED STATES | MFGR#11    | 2093412096 |
| 1997 | UNITED STATES | MFGR#12    | 1818427418 |
| 1997 | UNITED STATES | MFGR#13    | 2192557812 |
| 1997 | UNITED STATES | MFGR#14    | 1868564222 |
| 1997 | UNITED STATES | MFGR#15    | 1925521686 |
| 1997 | UNITED STATES | MFGR#21    | 2001352948 |
| 1997 | UNITED STATES | MFGR#22    | 2153895230 |
| 1997 | UNITED STATES | MFGR#23    | 1874576204 |
| 1997 | UNITED STATES | MFGR#24    | 2006772726 |
| 1997 | UNITED STATES | MFGR#25    | 2107332104 |
| 1998 | ARGENTINA     | MFGR#11    | 1135224454 |
| 1998 | ARGENTINA     | MFGR#12    | 1054050084 |
| 1998 | ARGENTINA     | MFGR#13    | 1165583584 |
| 1998 | ARGENTINA     | MFGR#14    | 1047452736 |
| 1998 | ARGENTINA     | MFGR#15    | 1044156534 |
| 1998 | ARGENTINA     | MFGR#21    | 1009425370 |
| 1998 | ARGENTINA     | MFGR#22    | 1012123472 |
| 1998 | ARGENTINA     | MFGR#23    | 1120959602 |
| 1998 | ARGENTINA     | MFGR#24    | 1049158236 |
| 1998 | ARGENTINA     | MFGR#25    | 1095680422 |
| 1998 | BRAZIL        | MFGR#11    | 1277156976 |
| 1998 | BRAZIL        | MFGR#12    | 1292625362 |
| 1998 | BRAZIL        | MFGR#13    | 1310323544 |
| 1998 | BRAZIL        | MFGR#14    | 1105352340 |
| 1998 | BRAZIL        | MFGR#15    | 1327625418 |
| 1998 | BRAZIL        | MFGR#21    | 1337644896 |
| 1998 | BRAZIL        | MFGR#22    | 1183583836 |
| 1998 | BRAZIL        | MFGR#23    | 1381297754 |
| 1998 | BRAZIL        | MFGR#24    | 1124724440 |
| 1998 | BRAZIL        | MFGR#25    | 1408364752 |
| 1998 | CANADA        | MFGR#11    | 1018172250 |
| 1998 | CANADA        | MFGR#12    |  976179544 |
| 1998 | CANADA        | MFGR#13    |  973066594 |
| 1998 | CANADA        | MFGR#14    | 1055674454 |
| 1998 | CANADA        | MFGR#15    | 1071738598 |
| 1998 | CANADA        | MFGR#21    |  911737302 |
| 1998 | CANADA        | MFGR#22    | 1188554616 |
| 1998 | CANADA        | MFGR#23    | 1148250140 |
| 1998 | CANADA        | MFGR#24    | 1017060848 |
| 1998 | CANADA        | MFGR#25    | 1095515984 |
| 1998 | PERU          | MFGR#11    | 1135677094 |
| 1998 | PERU          | MFGR#12    | 1081089514 |
| 1998 | PERU          | MFGR#13    | 1182663766 |
| 1998 | PERU          | MFGR#14    |  962670128 |
| 1998 | PERU          | MFGR#15    | 1140492276 |
| 1998 | PERU          | MFGR#21    | 1067466660 |
| 1998 | PERU          | MFGR#22    | 1055581312 |
| 1998 | PERU          | MFGR#23    | 1272786442 |
| 1998 | PERU          | MFGR#24    | 1178150524 |
| 1998 | PERU          | MFGR#25    | 1086502230 |
| 1998 | UNITED STATES | MFGR#11    | 1112552464 |
| 1998 | UNITED STATES | MFGR#12    | 1224771964 |
| 1998 | UNITED STATES | MFGR#13    | 1244827854 |
| 1998 | UNITED STATES | MFGR#14    | 1110013774 |
| 1998 | UNITED STATES | MFGR#15    | 1050239138 |
| 1998 | UNITED STATES | MFGR#21    | 1126813672 |
| 1998 | UNITED STATES | MFGR#22    | 1160957470 |
| 1998 | UNITED STATES | MFGR#23    | 1312160930 |
| 1998 | UNITED STATES | MFGR#24    | 1076890116 |
| 1998 | UNITED STATES | MFGR#25    | 1178223904 |
+------+---------------+------------+------------+

--Q4.3
+------+------------+-----------+----------+-------------+----------+------------+
| year | s_city     | p_brand   | profit   | c_region    | s_nation | p_category |
+------+------------+-----------+----------+-------------+----------+------------+
| 1997 | ALGERIA  0 | MFGR#111  |  1845848 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#111  | 10847138 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1110 |  6035219 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1110 |   432257 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1111 |  2107179 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1111 |  4071438 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1111 |  3057167 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1112 |  2311386 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1112 |   948054 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1113 |  1602699 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1113 |  1829091 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1113 |  3219216 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1114 |  4755426 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1114 |  5240408 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1115 |  4338301 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1115 |  6297251 | MIDDLE EAST | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1116 |  4030507 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1116 |  1375765 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1116 |  7716780 | MIDDLE EAST | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1117 |  7095003 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1117 |  4806253 | MIDDLE EAST | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1119 |  3020204 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1119 |  3898889 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1119 |   636154 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#112  |  6913968 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#112  |  4635551 | MIDDLE EAST | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1120 |  3789881 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1120 |  5375347 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1121 |  2962488 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1121 |   546333 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1121 | 12922204 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1122 |  9912733 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1122 |  5158697 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1123 |  3024512 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1123 |  2393758 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1124 |  2049203 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1125 | 10168898 | MIDDLE EAST | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1125 |  3169896 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1126 |   364696 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1126 |  3089145 | MIDDLE EAST | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1126 |   695856 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1127 |  6379893 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1128 |   356710 | AMERICA     | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1128 |  4804429 | MIDDLE EAST | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1128 | 10020597 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1129 |  4396845 | ASIA        | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1129 |   577228 | AFRICA      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#113  |  6419788 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1130 |  3840822 | EUROPE      | ALGERIA  | MFGR#11    |
| 1997 | ALGERIA  0 | MFGR#1130 |  6045720 | MIDDLE EAST | ALGERIA  | MFGR#11    |
+------+------------+-----------+----------+-------------+----------+------------+
```
