# **完成 TPCH 测试**

TPC Benchmark™H（TPC-H）是决策支持基准。它由一套面向业务的即时查询（ad-hoc）和并发数据修改组成。选择查询和填充数据库的数据具有广泛的行业相关性。该基准测试解释说明了决策支持系统，该系统可检查大量数据，执行高度复杂的查询并为关键业务问题提供答案。TPC-H 是 OLAP 数据库广泛使用的基准测试。

通过阅读本教程，您将学习如何使用 MatrixOne 完成 TPC-H 测试。

## **准备工作**

确保你已经安装了[单机版MatrixOne](../Get-Started/install-standalone-matrixone.md)并[连接到MatrixOne服务](..Get-Started//connect-to-matrixone-server.md).

## **1. 编译dbgen**

默认情况下，tpch dbgen 实用程序是用来生成测试数据集表格的工具，它根据比例因子 Scale Factor（SF)的大小确定数据集的大小，并生成一组平面文件（Flat File)，这些文件适合加载到 tpch 模式中。

当使用 `-s 1` 时 `dbgen` 命令会产生 1GB 的完整数据集，当使用`-s 10`时会产生大约 10GB 的数据集，以此类推。

```
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make
```

## **2. 生成数据**

运行 `dbgen`，获得适当的数据库大小因子(在示例中为 1GB)。

```
./dbgen -s 1
```

生成完整数据集可能需要一段时间。完成后，您可以看到结果文件。

```
total 2150000
-rw-r--r--  1 deister  staff   24346144 13 may 12:05 customer.tbl
-rw-r--r--  1 deister  staff  759863287 13 may 12:05 lineitem.tbl
-rw-r--r--  1 deister  staff       2224 13 may 12:05 nation.tbl
-rw-r--r--  1 deister  staff  171952161 13 may 12:05 orders.tbl
-rw-r--r--  1 deister  staff   24135125 13 may 12:05 part.tbl
-rw-r--r--  1 deister  staff  118984616 13 may 12:05 partsupp.tbl
-rw-r--r--  1 deister  staff        389 13 may 12:05 region.tbl
-rw-r--r--  1 deister  staff    1409184 13 may 12:05 supplier.tbl
```

我们同时也准备了 1GB 的数据集供您下载。您可以在以下链接中直接获取数据文件:

```
https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/tpch/tpch-1g.zip
```

## **3. 在MatrixOne中建表**

MatrixOne 暂不支持复合主键和分区，`PARTSUPP` 和 `LINEITEM` 表的创建代码有以下修改：

- 移除了 `PARTSUPP` 和 `LINEITEM` 表的复合主键。

- 移除了 `LINEITEM` 表的 `PARTITION BY KEY()`。

```
drop database if exists TPCH;
create database if not exists TPCH;
use tpch;
CREATE TABLE NATION(
N_NATIONKEY  INTEGER NOT NULL,
N_NAME       CHAR(25) NOT NULL,
N_REGIONKEY  INTEGER NOT NULL,
N_COMMENT    VARCHAR(152),
PRIMARY KEY (N_NATIONKEY)
);

CREATE TABLE REGION(
R_REGIONKEY  INTEGER NOT NULL,
R_NAME       CHAR(25) NOT NULL,
R_COMMENT    VARCHAR(152),
PRIMARY KEY (R_REGIONKEY)
);

CREATE TABLE PART(
P_PARTKEY     INTEGER NOT NULL,
P_NAME        VARCHAR(55) NOT NULL,
P_MFGR        CHAR(25) NOT NULL,
P_BRAND       CHAR(10) NOT NULL,
P_TYPE        VARCHAR(25) NOT NULL,
P_SIZE        INTEGER NOT NULL,
P_CONTAINER   CHAR(10) NOT NULL,
P_RETAILPRICE DECIMAL(15,2) NOT NULL,
P_COMMENT     VARCHAR(23) NOT NULL,
PRIMARY KEY (P_PARTKEY)
);

CREATE TABLE SUPPLIER(
S_SUPPKEY     INTEGER NOT NULL,
S_NAME        CHAR(25) NOT NULL,
S_ADDRESS     VARCHAR(40) NOT NULL,
S_NATIONKEY   INTEGER NOT NULL,
S_PHONE       CHAR(15) NOT NULL,
S_ACCTBAL     DECIMAL(15,2) NOT NULL,
S_COMMENT     VARCHAR(101) NOT NULL,
PRIMARY KEY (S_SUPPKEY)
);

CREATE TABLE PARTSUPP(
PS_PARTKEY     INTEGER NOT NULL,
PS_SUPPKEY     INTEGER NOT NULL,
PS_AVAILQTY    INTEGER NOT NULL,
PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
PS_COMMENT     VARCHAR(199) NOT NULL
);

CREATE TABLE CUSTOMER(
C_CUSTKEY     INTEGER NOT NULL,
C_NAME        VARCHAR(25) NOT NULL,
C_ADDRESS     VARCHAR(40) NOT NULL,
C_NATIONKEY   INTEGER NOT NULL,
C_PHONE       CHAR(15) NOT NULL,
C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
C_MKTSEGMENT  CHAR(10) NOT NULL,
C_COMMENT     VARCHAR(117) NOT NULL,
PRIMARY KEY (C_CUSTKEY)
);

CREATE TABLE ORDERS(
O_ORDERKEY       BIGINT NOT NULL,
O_CUSTKEY        INTEGER NOT NULL,
O_ORDERSTATUS    CHAR(1) NOT NULL,
O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
O_ORDERDATE      DATE NOT NULL,
O_ORDERPRIORITY  CHAR(15) NOT NULL,
O_CLERK          CHAR(15) NOT NULL,
O_SHIPPRIORITY   INTEGER NOT NULL,
O_COMMENT        VARCHAR(79) NOT NULL,
PRIMARY KEY (O_ORDERKEY)
);

CREATE TABLE LINEITEM(
L_ORDERKEY    BIGINT NOT NULL,
L_PARTKEY     INTEGER NOT NULL,
L_SUPPKEY     INTEGER NOT NULL,
L_LINENUMBER  INTEGER NOT NULL,
L_QUANTITY    DECIMAL(15,2) NOT NULL,
L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
L_DISCOUNT    DECIMAL(15,2) NOT NULL,
L_TAX         DECIMAL(15,2) NOT NULL,
L_RETURNFLAG  VARCHAR(1) NOT NULL,
L_LINESTATUS  VARCHAR(1) NOT NULL,
L_SHIPDATE    DATE NOT NULL,
L_COMMITDATE  DATE NOT NULL,
L_RECEIPTDATE DATE NOT NULL,
L_SHIPINSTRUCT CHAR(25) NOT NULL,
L_SHIPMODE     CHAR(10) NOT NULL,
L_COMMENT      VARCHAR(44) NOT NULL
);
```

## **4. 导入数据**

在 MatrixOne 中使用以下命令将数据加载到相关的表中。

```
load data infile '/YOUR_TPCH_DATA_PATH/nation.tbl' into table NATION FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/YOUR_TPCH_DATA_PATH/region.tbl' into table REGION FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/YOUR_TPCH_DATA_PATH/part.tbl' into table PART FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/YOUR_TPCH_DATA_PATH/supplier.tbl' into table SUPPLIER FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/YOUR_TPCH_DATA_PATH/partsupp.tbl' into table PARTSUPP FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/YOUR_TPCH_DATA_PATH/orders.tbl' into table ORDERS FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/YOUR_TPCH_DATA_PATH/customer.tbl' into table CUSTOMER FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

load data infile '/YOUR_TPCH_DATA_PATH/lineitem.tbl' into table LINEITEM FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

加载完成后，可以使用创建的表查询 MatrixOne 中的数据。

## **5. 运行 TPCH 测试命令**

```sql
--Q1
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '112' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
;

--Q2
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 48
    and p_type like '%TIN'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'MIDDLE EAST'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp,
            supplier,
            nation,
            region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'MIDDLE EAST'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100
;


--Q3
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = 'HOUSEHOLD'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-29'
    and l_shipdate > date '1995-03-29'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10
;

--Q4
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1997-07-01'
    and o_orderdate < date '1997-07-01' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority
;


--Q5
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AMERICA'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc
;


--Q6
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.03 - 0.01 and 0.03 + 0.01
    and l_quantity < 24;

--Q7
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'ARGENTINA')
                or (n1.n_name = 'ARGENTINA' and n2.n_name = 'FRANCE')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year
;

--Q8
select
    o_year,
    (sum(case
        when nation = 'ARGENTINA' then volume
        else 0
    end) / sum(volume)) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY BURNISHED TIN'
    ) as all_nations
group by
    o_year
order by
    o_year
;

--Q9
select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%pink%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc
;


--Q10
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-03-01'
    and o_orderdate < date '1993-03-01' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20
;


--Q11
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'JAPAN'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
        select
            sum(ps_supplycost * ps_availqty) * 0.0001000000
                from
                partsupp,
                supplier,nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'JAPAN'
        )
order by
    value desc
;

--Q12
select
        l_shipmode,
        sum(case
                when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
from
        orders,
        lineitem
where
        o_orderkey = l_orderkey
        and l_shipmode in ('FOB', 'TRUCK')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '1996-01-01'
        and l_receiptdate < date '1996-01-01' + interval '1' year
group by
        l_shipmode
order by
        l_shipmode
;

--Q13
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%pending%accounts%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc
;

--Q14
select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1996-04-01'
    and l_shipdate < date '1996-04-01' + interval '1' month;

--Q15
with q15_revenue0 as (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        lineitem
    where
        l_shipdate >= date '1995-12-01'
        and l_shipdate < date '1995-12-01' + interval '3' month
    group by
        l_suppkey
    )
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    q15_revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            q15_revenue0
    )
order by
    s_suppkey
;

--Q16
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#35'
    and p_type not like 'ECONOMY BURNISHED%'
    and p_size in (14, 7, 21, 24, 35, 33, 2, 20)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size
;

--Q17
select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#54'
    and p_container = 'LG BAG'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );

--Q18
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 314
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100
;

--Q19
select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 5 and l_quantity <= 5 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#15'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 14 and l_quantity <= 14 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#44'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 28 and l_quantity <= 28 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );

--Q20
select
    s_name,
    s_address
from
    supplier,
    nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                    p_name like 'lime%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1993-01-01'
                    and l_shipdate < date '1993-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey
    and n_name = 'VIETNAM'
order by s_name
;

--Q21
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select
            *
        from
            lineitem l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select
            *
        from
            lineitem l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and n_name = 'BRAZIL'
group by
    s_name
order by
    numwait desc,
    s_name
limit 100
;

--Q22
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            customer
        where
            substring(c_phone from 1 for 2) in
                ('10', '11', '26', '22', '19', '20', '27')
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal > 0.00
                    and substring(c_phone from 1 for 2) in
                        ('10', '11', '26', '22', '19', '20', '27')
            )
            and not exists (
                select
                    *
                from
                    orders
                where
                    o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode
;

```

## **6. 运行预期结果**

以下为22个 TPCH 查询的预期结果。

说明：由于 Q16 的结果段落过长，无法在下文展示，请参阅此连结的完整结果：[Q16运行预期结果](https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/tpch/tpch1g_result_matrixone.md)

```
Q1
+--------------+--------------+-------------+-----------------+-------------------+---------------------+---------+-----------+----------+-------------+
| l_returnflag | l_linestatus | sum_qty     | sum_base_price  | sum_disc_price    | sum_charge          | avg_qty | avg_price | avg_disc | count_order |
+--------------+--------------+-------------+-----------------+-------------------+---------------------+---------+-----------+----------+-------------+
| A            | F            | 37734107.00 |  56586554400.73 |  53758257134.8700 |  55909065222.827692 |   25.52 |  38273.12 |     0.04 |     1478493 |
| N            | F            |   991417.00 |   1487504710.38 |   1413082168.0541 |   1469649223.194375 |   25.51 |  38284.46 |     0.05 |       38854 |
| N            | O            | 76633518.00 | 114935210409.19 | 109189591897.4720 | 113561024263.013782 |   25.50 |  38248.01 |     0.05 |     3004998 |
| R            | F            | 37719753.00 |  56568041380.90 |  53741292684.6040 |  55889619119.831932 |   25.50 |  38250.85 |     0.05 |     1478870 |
+--------------+--------------+-------------+-----------------+-------------------+---------------------+---------+-----------+----------+-------------+
4 rows in set

Q2
+-----------+--------------------+--------------+-----------+----------------+------------------------------------------+-----------------+-----------------------------------------------------------------------------------------------------+
| s_acctbal | s_name             | n_name       | p_partkey | p_mfgr         | s_address                                | s_phone         | s_comment                                                                                           |
+-----------+--------------------+--------------+-----------+----------------+------------------------------------------+-----------------+-----------------------------------------------------------------------------------------------------+
|   9973.93 | Supplier#000004194 | JORDAN       |     14193 | Manufacturer#1 | A8AoX9AK,qhf,CpEF                        | 23-944-413-2681 | t fluffily. regular requests about the regular, unusual somas play of the furiously busy            |
|   9956.34 | Supplier#000005108 | IRAN         |    140079 | Manufacturer#5 | d3PLCdpPP3uE4GzbbAh4bWmU 7ecOifL9e1mNnzG | 20-842-882-7047 | ronic accounts. carefully bold accounts sleep beyond                                                |
|   9836.43 | Supplier#000000489 | IRAN         |    190488 | Manufacturer#2 | y9NMoYGxDUPfrB1GwjYhLtCeV7pOt            | 20-375-500-2226 | quickly carefully pending accounts. fina                                                            |
|   9825.95 | Supplier#000007554 | IRAQ         |     40041 | Manufacturer#5 | Huq0k qKET hByp3RcMcr                    | 21-787-637-9651 | ending, final requests thrash pending,                                                              |
|   9806.52 | Supplier#000005276 | IRAQ         |    132762 | Manufacturer#2 | inh0KOhRfLM7WRhdRNvJJDQx                 | 21-834-496-7360 | the slyly unusual theodolites; carefully even accounts use slyly. sl                                |
|   9796.31 | Supplier#000005847 | IRAQ         |    188292 | Manufacturer#1 | obol3bfh0zWi                             | 21-530-950-2847 | equests. blithely regular deposits should have to impress. final platelets integrate fluffily       |
|   9775.37 | Supplier#000007245 | IRAQ         |    169696 | Manufacturer#5 | 5VOUnvxlJeOJ                             | 21-663-724-2985 | ic deposits about the slyly bold requests                                                           |
|   9755.43 | Supplier#000002439 | EGYPT        |    172438 | Manufacturer#5 | rXxojWV0VefSK7A0dhF                      | 14-410-168-5354 | p quickly packages. carefully pending pinto beans after the                                         |
|   9714.60 | Supplier#000007317 | EGYPT        |     29810 | Manufacturer#4 | nJGsPl2ruoq4Ydtv0fwWG385eOFV6  VUokbCZ   | 14-666-351-2592 | ons integrate furiously? slyly ironic requests sl                                                   |
|   9557.33 | Supplier#000007367 | EGYPT        |    197366 | Manufacturer#3 | bTP8DLvaRAB0n                            | 14-234-934-5255 | ep slyly regular accounts-- regular, regular excuses dazzle furiously about the reg                 |
|   9538.15 | Supplier#000000979 | IRAN         |     55968 | Manufacturer#1 | cdvHjrKZR7iDlmSWU2a                      | 20-151-688-1408 | ckages cajole quietly carefully regular in                                                          |
|   9513.31 | Supplier#000004163 | SAUDI ARABIA |    109142 | Manufacturer#4 | 2VnQypC7pJPJRu6HzYRg7nAvhzckcYAQFbI      | 30-544-852-3772 | he regular requests. blithely final                                                                 |
|   9450.15 | Supplier#000002067 | EGYPT        |      9566 | Manufacturer#3 | 9dO68x0XLMCUDuFk1J6k9hpvLoKx 4qasok4lIb  | 14-252-246-5791 | rding to the regular dolphins. quickly ir                                                           |
|   9359.59 | Supplier#000005087 | JORDAN       |     55086 | Manufacturer#4 | q0c6r9wYVQx31IeGBZKfe                    | 23-335-392-5204 | osits. quickly regular packages haggle among the quickly silent ins                                 |
|   9343.35 | Supplier#000006337 | IRAN         |    173819 | Manufacturer#1 | ilQgNOdCAysclt3SgODb6LeJ7d4RzYD          | 20-950-345-8173 | quickly ironic packages affix aft                                                                   |
|   9318.47 | Supplier#000003834 | SAUDI ARABIA |     11332 | Manufacturer#5 | cxGQnW3nShks59xA5bPoaC bIvcBWUt          | 30-823-353-6520 | regular instructions. express packages run slyly pending                                            |
|   9318.47 | Supplier#000003834 | SAUDI ARABIA |    108813 | Manufacturer#2 | cxGQnW3nShks59xA5bPoaC bIvcBWUt          | 30-823-353-6520 | regular instructions. express packages run slyly pending                                            |
|   9315.33 | Supplier#000003009 | IRAN         |     40504 | Manufacturer#2 | oTbwfQ,Yfdcf3ysgc60NKtTpSIc              | 20-306-556-2450 | the carefully special requests. express instructions wake                                           |
|   9296.31 | Supplier#000008213 | JORDAN       |    163180 | Manufacturer#2 | YhdN9ESxYvhJp9ngr12Bbeo4t3zLPD,          | 23-197-507-9431 | g to the blithely regular accounts! deposit                                                         |
|   9284.57 | Supplier#000009781 | EGYPT        |      4780 | Manufacturer#4 | 49NAd1iDiY4,                             | 14-410-636-4312 | its. ironic pinto beans are blithely. express depths use caref                                      |
|   9261.13 | Supplier#000000664 | EGYPT        |    125639 | Manufacturer#5 | ln6wISAnC8Bpj q4V                        | 14-244-772-4913 | ly special foxes cajole slyly ironic reque                                                          |
|   9260.78 | Supplier#000001949 | IRAN         |     86932 | Manufacturer#2 | W79M2lpYiSY76Ujo6fSRUQiu                 | 20-531-767-2819 | thinly ironic excuses haggle slyly. f                                                               |
|   9227.16 | Supplier#000009461 | EGYPT        |    126948 | Manufacturer#2 | Eweba 0sfaF,l4sAxXGTgEjzsJsNWWIGjYhFkRWV | 14-983-137-4957 | lly bold packages. carefully express deposits integrate about the unusual accounts. regular,        |
|   9185.89 | Supplier#000007888 | EGYPT        |     27887 | Manufacturer#1 | nq06Y48amPfS8YBuduy1RYu                  | 14-462-704-3828 | ole slyly-- blithely careful foxes wake against the furiously ironic accounts. pending ideas        |
|   9185.89 | Supplier#000007888 | EGYPT        |    190330 | Manufacturer#4 | nq06Y48amPfS8YBuduy1RYu                  | 14-462-704-3828 | ole slyly-- blithely careful foxes wake against the furiously ironic accounts. pending ideas        |
|   9132.92 | Supplier#000007878 | IRAN         |     92859 | Manufacturer#3 | aq6T3tUVq1,                              | 20-861-274-6282 | waters cajole ironic, ironic requests. furi                                                         |
|   9058.94 | Supplier#000002789 | IRAN         |    142788 | Manufacturer#4 | 7EkIldjP7M6psSWcJ11tf65GT7ZC7UaiCh       | 20-842-716-4307 | s. platelets use carefully. busily regular accounts cajole fluffily above the slowly final pinto be |
|   9026.80 | Supplier#000005436 | SAUDI ARABIA |     92926 | Manufacturer#3 | 3HiusYZGYmHItExgX5VfNCdJwkW8W7R          | 30-453-280-6340 | . blithely unusual requests                                                                         |
|   9007.16 | Supplier#000001747 | EGYPT        |    121746 | Manufacturer#3 | UyDlFjVxanP,ifej7L5jtNaubC               | 14-141-360-9722 | maintain bravely across the busily express pinto beans. sometimes                                   |
|   8846.35 | Supplier#000005446 | EGYPT        |     57930 | Manufacturer#2 | Nusoq0ckv9SwnJZV8Rw2dUqE,V0ylm Bon       | 14-468-853-6477 | deposits. ironic, bold ideas wake. fluffily ironic deposits must have to sleep furiously pending    |
|   8837.21 | Supplier#000007210 | JORDAN       |    144695 | Manufacturer#3 | G7MYkWkkJDVu,rr23aXjQCwNqZ2Vk6           | 23-560-295-1805 | en, express foxes use across the blithely bold                                                      |
|   8800.91 | Supplier#000008182 | EGYPT        |    143153 | Manufacturer#5 | KGMyipBiAF00tSB99DwH                     | 14-658-338-3635 | thely even excuses integrate blithel                                                                |
|   8788.46 | Supplier#000003437 | IRAN         |    118414 | Manufacturer#4 | JfgZDO9fsP4ljfzsi,s7431Ld3A7zXtHfrF74    | 20-547-871-1712 | ickly unusual dependencies. carefully regular dolphins ha                                           |
|   8750.12 | Supplier#000001064 | IRAQ         |     31063 | Manufacturer#5 | QgmUIaEs5KpuW ,oREZV2b4wr3HEC1z4F        | 21-440-809-7599 | sly even deposits? furiously regular pack                                                           |
|   8594.80 | Supplier#000007553 | IRAN         |      5052 | Manufacturer#4 | wAU2Lui w9                               | 20-663-409-7956 | old, stealthy accounts are blithely. fluffily final                                                 |
|   8594.80 | Supplier#000007553 | IRAN         |    195033 | Manufacturer#1 | wAU2Lui w9                               | 20-663-409-7956 | old, stealthy accounts are blithely. fluffily final                                                 |
|   8588.63 | Supplier#000008094 | SAUDI ARABIA |    148093 | Manufacturer#1 | SNS6FCscBNZBFecA                         | 30-465-665-6735 | ake quickly blithely ironic theodolites. quickly ironic ideas boost. furiously iro                  |
|   8522.70 | Supplier#000004208 | IRAQ         |    166659 | Manufacturer#5 | x3jZYF7ZYN 8a4LY1c1kEsh                  | 21-468-998-1571 | furiously regular accounts!                                                                         |
|   8514.86 | Supplier#000006347 | JORDAN       |    181310 | Manufacturer#5 | wwR5j4kdIAwLe33uBwo                      | 23-340-942-3641 | uests breach blithely ironic deposi                                                                 |
|   8473.01 | Supplier#000003912 | IRAQ         |     33911 | Manufacturer#3 | Op7,1zt3MAxs34Qo4O W                     | 21-474-809-6508 | es. regular, brave instructions wa                                                                  |
|   8405.28 | Supplier#000007886 | IRAQ         |    192847 | Manufacturer#4 | sFTj5nzc5EIVmzjXwenFTXD U                | 21-735-778-5786 | ven dependencies boost blithely ironic de                                                           |
|   8375.58 | Supplier#000001259 | IRAQ         |     38755 | Manufacturer#2 | 32cJBGFFpGEkEjx1sF8JZAy0A72uXL5qU        | 21-427-422-4993 | ironic accounts haggle slyly alongside of the carefully ironic deposit                              |
|   8351.75 | Supplier#000007495 | IRAQ         |    114983 | Manufacturer#4 | 3jQQGvfs,5Aryhn0Z                        | 21-953-463-7239 | requests. carefully final accounts after the qui                                                    |
|   8230.12 | Supplier#000001058 | SAUDI ARABIA |     68551 | Manufacturer#2 | fJ8egP,xkLygXGv8bmtc9T1FJ                | 30-496-504-3341 | requests haggle? regular, regular pinto beans integrate fluffily. dependenc                         |
|   8195.44 | Supplier#000009805 | IRAQ         |      4804 | Manufacturer#4 | dTTmLRYJNat,JbhlQlxwWp HjMR              | 21-838-243-3925 | lets. quickly even theodolites dazzle slyly even a                                                  |
|   8175.17 | Supplier#000003172 | IRAN         |     55656 | Manufacturer#5 | 8ngbGS7BQoTDmJyMa5WV9XbaM31u5FAayd2vT3   | 20-834-374-7746 | ss deposits use furiously after the quickly final sentiments. fluffily ruthless ideas believe regu  |
|   8159.13 | Supplier#000007486 | EGYPT        |     17485 | Manufacturer#1 | AjfdzbrrJE1                              | 14-970-643-1521 | ld accounts. enticingly furious requests cajole. final packages s                                   |
|   8111.40 | Supplier#000007567 | IRAN         |    197566 | Manufacturer#1 | 7W4k2qEVoBkRehprGliXRSYVOQEh             | 20-377-181-7435 | gular foxes. silent attainments boost furiousl                                                      |
|   8046.55 | Supplier#000001625 | IRAQ         |     14121 | Manufacturer#2 | yKlKMbENR6bfmIu7aCFmbs                   | 21-769-404-7617 | deposits. ideas boost blithely. slyly even Tiresias according to the platelets are q                |
|   8040.16 | Supplier#000001925 | SAUDI ARABIA |      4424 | Manufacturer#4 | Cu5Ub AAdXT                              | 30-969-417-1108 | pending packages across the regular req                                                             |
|   8031.68 | Supplier#000002370 | SAUDI ARABIA |    147341 | Manufacturer#5 | xGQB9xSPqRtCuMZaJavOrFuTY7km             | 30-373-388-2352 | dependencies. carefully express deposits use slyly among the slyly unusual pearls. dogge            |
|   8031.42 | Supplier#000008216 | IRAN         |     83199 | Manufacturer#2 | jsqlyr1ViAo                              | 20-224-305-7298 | to the carefully even excuses haggle blithely against the pending pinto be                          |
|   8007.83 | Supplier#000006266 | JORDAN       |     81249 | Manufacturer#1 | XWBf5Jd2V5SOurbn11Tt1                    | 23-363-445-7184 | as cajole carefully against the quickly special ac                                                  |
|   7995.78 | Supplier#000006957 | IRAN         |    161924 | Manufacturer#1 | 8lvRhU5xtXv                              | 20-312-173-2216 | ly ironic accounts. stealthily regular foxes about the blithely ironic requests play blithely abo   |
|   7913.40 | Supplier#000003148 | JORDAN       |     58137 | Manufacturer#1 | CpCJWI4PHeiwYuq0                         | 23-767-770-9172 | ove the quickly final packages boost fluffily among the furiously final platelets. carefully s      |
|   7910.16 | Supplier#000002102 | IRAQ         |     99592 | Manufacturer#2 | 1kuyUn5q6czLOGB60fAVgpv68M2suwchpmp2nK   | 21-367-198-9930 | accounts after the blithely                                                                         |
|   7893.58 | Supplier#000000918 | SAUDI ARABIA |     13414 | Manufacturer#1 | e0sB7xAU3,cWF7pzXrpIbATUNydCUZup         | 30-303-831-1662 | ependencies wake carefull                                                                           |
|   7885.17 | Supplier#000004001 | JORDAN       |     38994 | Manufacturer#2 | 3M39sZY1XeQXPDRO                         | 23-109-632-6806 | efully express packages integrate across the regular pearls. blithely unusual packages mainta       |
|   7880.20 | Supplier#000005352 | JORDAN       |       351 | Manufacturer#3 | PP9gHTn946hXqUF5E7idIPLkhnN              | 23-557-756-7951 | egular frays. final instructions sleep a                                                            |
|   7844.31 | Supplier#000006987 | IRAQ         |     44482 | Manufacturer#5 | UH1zBxTNjTminnmHRe h YUT1eR              | 21-963-444-7397 | nag quickly carefully regular requests. ironic theo                                                 |
|   7812.27 | Supplier#000006967 | SAUDI ARABIA |    151936 | Manufacturer#4 | S4i1HfrSM4m3di3R9Cxxp59M1                | 30-193-457-6365 | ely. dependencies cajole quickly. final warhorses across the furiously ironic foxes integr          |
|   7767.63 | Supplier#000004306 | IRAN         |     31802 | Manufacturer#2 | SkZkJZflW5mDg9wL fJ                      | 20-911-180-1895 | uickly regular ideas. blithely express accounts along the carefully sile                            |
|   7741.42 | Supplier#000000899 | IRAQ         |     53383 | Manufacturer#5 | oLlkiVghtro IwzcwFuzwMCG94rRpux          | 21-980-994-3905 | equests wake quickly special, express accounts. courts promi                                        |
|   7741.42 | Supplier#000000899 | IRAQ         |    105878 | Manufacturer#3 | oLlkiVghtro IwzcwFuzwMCG94rRpux          | 21-980-994-3905 | equests wake quickly special, express accounts. courts promi                                        |
|   7741.10 | Supplier#000001059 | IRAN         |    103528 | Manufacturer#4 | 4tBenOMokWbWVRB8i8HwENeO cQjM9           | 20-620-710-8984 | to the carefully special courts.                                                                    |
|   7599.20 | Supplier#000006596 | SAUDI ARABIA |    184077 | Manufacturer#2 | k8qeFxfXKIGYdQ82RXAfCwddSrc              | 30-804-947-3851 | ously unusual deposits boost carefully after the enticing                                           |
|   7598.31 | Supplier#000008857 | IRAQ         |     63844 | Manufacturer#4 | dP2th8vneyOLIUFwNBwqixkFD6               | 21-691-170-4769 | s. quickly ironic frays detect carefully                                                            |
|   7591.79 | Supplier#000009723 | JORDAN       |    104702 | Manufacturer#2 | Q1CkkpDdlLOpCJiV,zIf,Mv86otWhxj7slGc     | 23-710-907-3873 | e fluffily even instructions. packages impress enticingly.                                          |
|   7575.12 | Supplier#000007557 | IRAQ         |     77556 | Manufacturer#1 | udLvpjNvIx9qeRNdjL1ZAO0OZNOBo6h          | 21-629-935-9941 | ally special accounts nod; f                                                                        |
|   7496.91 | Supplier#000005828 | IRAN         |    103317 | Manufacturer#1 | Xt0EqDCNU6X00sNsIO7nd0ws3H               | 20-435-850-8703 | furiously about the fluffily careful idea                                                           |
|   7472.88 | Supplier#000004204 | EGYPT        |     14203 | Manufacturer#1 | 0rGZJ6VZXdH                              | 14-520-667-4690 | y pending pinto beans. even, final requests sleep care                                              |
|   7472.88 | Supplier#000004204 | EGYPT        |    161687 | Manufacturer#3 | 0rGZJ6VZXdH                              | 14-520-667-4690 | y pending pinto beans. even, final requests sleep care                                              |
|   7467.63 | Supplier#000003270 | IRAN         |     45757 | Manufacturer#2 | 7j4n5FnNEHVJxFhiyz                       | 20-450-599-9591 | regular, even instructions boost deposits                                                           |
|   7465.41 | Supplier#000008686 | EGYPT        |    188685 | Manufacturer#4 | 4Onf4yxuNwHCRIC0y                        | 14-454-946-4151 | ly final ideas. bravely unusual deposits doze carefully. expr                                       |
|   7460.80 | Supplier#000008701 | IRAQ         |     83684 | Manufacturer#3 | PLR2QehcW08                              | 21-747-984-4244 | ideas use carefully pending, final deposits. ironic, pe                                             |
|   7447.86 | Supplier#000005877 | JORDAN       |    120852 | Manufacturer#2 | EyqOHClZZMJkq grnOX9 4alZx6P7B2fq        | 23-419-288-6451 | lar pinto beans breach carefully final pinto                                                        |
|   7445.03 | Supplier#000009802 | IRAQ         |    164769 | Manufacturer#5 | y6wLN KiZuTf5HT9Hbm0BELn1GUTD6yl         | 21-116-708-2013 | nic requests. pinto beans across the carefully regular grouches snooze among the final pin          |
|   7401.46 | Supplier#000008677 | IRAN         |    123652 | Manufacturer#5 | WNa780JZzivxuGBEsDszqoT1Pj               | 20-899-256-5288 | onic instructions along the furiously ironic accounts haggle fluffily silently un                   |
|   7393.50 | Supplier#000007056 | IRAQ         |     54550 | Manufacturer#1 | M5cAJQvW9D5zwC7o2qkoe                    | 21-175-383-4727 | slyly even requests. forges haggle boldly express requests. furio                                   |
|   7376.11 | Supplier#000003982 | IRAQ         |    118959 | Manufacturer#1 | jx9EloF33Ez                              | 21-890-236-4160 | s the furiously special warhorses affix after the car                                               |
|   7264.42 | Supplier#000001565 | IRAQ         |     14061 | Manufacturer#4 | bOwKHdBteMkZoZcxdigk4Tnu07w1gDztmV7hvCw  | 21-791-273-8592 | to beans. express accounts nag around the                                                           |
|   7256.46 | Supplier#000009116 | IRAQ         |      4115 | Manufacturer#3 | ULjaQwNbcB XUG9dvbZDHvJVwLo08utswt       | 21-241-469-8343 | ending deposits. slyly ironic dependencies breach. blithely speci                                   |
|   7256.46 | Supplier#000009116 | IRAQ         |     99115 | Manufacturer#1 | ULjaQwNbcB XUG9dvbZDHvJVwLo08utswt       | 21-241-469-8343 | ending deposits. slyly ironic dependencies breach. blithely speci                                   |
|   7256.46 | Supplier#000009116 | IRAQ         |    131576 | Manufacturer#4 | ULjaQwNbcB XUG9dvbZDHvJVwLo08utswt       | 21-241-469-8343 | ending deposits. slyly ironic dependencies breach. blithely speci                                   |
|   7254.81 | Supplier#000005664 | EGYPT        |     35663 | Manufacturer#2 | b8VWuTXRt66wF9bfrgTmNGuxf1PU0x3O9e       | 14-214-171-8987 | ts across the quickly pending pin                                                                   |
|   7186.63 | Supplier#000006958 | IRAN         |     71943 | Manufacturer#4 | 0n9BD,gRzUc3B,PsFcxDBGp4BFf4P            | 20-185-413-5590 | against the instructions. requests are. speci                                                       |
|   7166.36 | Supplier#000003541 | EGYPT        |    116007 | Manufacturer#1 | DbwyOxoaMEdhEtIB3y045QrKCi2fQpGRu,       | 14-508-763-1850 | ages. carefully unusual requests across the pending instructions aff                                |
|   7128.81 | Supplier#000000677 | JORDAN       |     50676 | Manufacturer#4 | 8mhrffG7D2WJBSQbOGstQ                    | 23-290-639-3315 | nder blithely. slyly unusual theod                                                                  |
|   7051.73 | Supplier#000003349 | IRAQ         |    125812 | Manufacturer#3 | wtTK9df9kY7mQ5QUM0Xe5bHLMRLgwE           | 21-614-525-7451 | ar theodolites cajole fluffily across the pending requests. slyly final requests a                  |
|   7023.47 | Supplier#000009543 | SAUDI ARABIA |     47038 | Manufacturer#1 | VYKinyOBNXRr Hdqn8kOxfTw                 | 30-785-782-6088 | sts. furiously pending packages sleep slyly even requests. final excuses print deposits. final pac  |
|   6985.93 | Supplier#000006409 | IRAQ         |    131382 | Manufacturer#1 | eO8JDNM19HrlQMR                          | 21-627-356-3992 | sts. slyly final deposits around the regular accounts are along the furiously final pac             |
|   6964.75 | Supplier#000009931 | EGYPT        |     57425 | Manufacturer#1 | peQYiRFk G0xZKfJ                         | 14-989-166-5782 | deposits according to the sometimes silent requests wake along the packages-- blithely f            |
|   6964.04 | Supplier#000007399 | IRAQ         |     77398 | Manufacturer#2 | zdxjENOGR4QiCFP                          | 21-859-733-1999 | e blithely after the even requests. carefully ironic packages use slyly a                           |
|   6913.81 | Supplier#000002625 | IRAQ         |     22624 | Manufacturer#3 | a4V0rWemgbsT ZMj w7DB8rUbZ4F4lqqW5VKljQF | 21-136-564-3910 | . asymptotes among the express requests cajole furiously after the ca                               |
|   6880.18 | Supplier#000006704 | IRAN         |     26703 | Manufacturer#4 | 97rxJlAImbO1 sUlChUWoOJ0ZzvQ2NI3KI6VDOwk | 20-588-916-1286 | old accounts wake quickly. ca                                                                       |
|   6878.62 | Supplier#000001697 | IRAQ         |    146668 | Manufacturer#5 | 37nm ODTeHy0xWTWegplgdWQqelh             | 21-377-544-4864 | ironic theodolites. furiously regular d                                                             |
|   6790.39 | Supplier#000008703 | IRAN         |    123678 | Manufacturer#4 | wMslK1A8SEUTIIdApQ                       | 20-782-266-2552 | eep blithely regular, pending w                                                                     |
|   6763.46 | Supplier#000007882 | EGYPT        |    137881 | Manufacturer#5 | JDv8BZiYG0UlZ                            | 14-111-252-9120 | the silent accounts wake foxes. furious                                                             |
|   6751.81 | Supplier#000003156 | EGYPT        |    165607 | Manufacturer#2 | alRWaW4FTFERMM4vf2rHKIKE                 | 14-843-946-7775 | are furiously. final theodolites affix slyly bold deposits. even packages haggle idly slyly specia  |
|   6702.07 | Supplier#000006276 | EGYPT        |     31269 | Manufacturer#2 | ,dE1anEjKQGZfgquYfkx2fkGcXH              | 14-896-626-7847 | ze about the carefully regular pint                                                                 |
+-----------+--------------------+--------------+-----------+----------------+------------------------------------------+-----------------+-----------------------------------------------------------------------------------------------------+
100 rows in set

Q3
+------------+-------------+-------------+----------------+
| l_orderkey | revenue     | o_orderdate | o_shippriority |
+------------+-------------+-------------+----------------+
|    2152675 | 431309.8065 | 1995-03-28  |              0 |
|    4994400 | 423834.7976 | 1995-03-09  |              0 |
|    2160291 | 401149.7805 | 1995-03-18  |              0 |
|    2845094 | 401094.1393 | 1995-03-06  |              0 |
|    1902471 | 400497.3847 | 1995-03-01  |              0 |
|    5624358 | 395710.6806 | 1995-03-20  |              0 |
|    2346242 | 392580.0394 | 1995-03-17  |              0 |
|    2529826 | 387365.1560 | 1995-02-17  |              0 |
|    5168933 | 385433.6198 | 1995-03-20  |              0 |
|    2839239 | 380503.7310 | 1995-03-22  |              0 |
+------------+-------------+-------------+----------------+
10 rows in set

Q4
+-----------------+-------------+
| o_orderpriority | order_count |
+-----------------+-------------+
| 1-URGENT        |       10623 |
| 2-HIGH          |       10465 |
| 3-MEDIUM        |       10309 |
| 4-NOT SPECIFIED |       10618 |
| 5-LOW           |       10541 |
+-----------------+-------------+
5 rows in set

Q5
+---------------+---------------+
| n_name        | revenue       |
+---------------+---------------+
| PERU          | 56206762.5035 |
| CANADA        | 56052846.0161 |
| ARGENTINA     | 54595012.8076 |
| BRAZIL        | 53601776.5201 |
| UNITED STATES | 50890580.8962 |
+---------------+---------------+
5 rows in set

Q6
+---------------+
| revenue       |
+---------------+
| 61660051.7967 |
+---------------+

Q7
+-------------+-------------+--------+---------------+
| supp_nation | cust_nation | l_year | revenue       |
+-------------+-------------+--------+---------------+
| ARGENTINA   | FRANCE      |   1995 | 57928886.8015 |
| ARGENTINA   | FRANCE      |   1996 | 55535134.8474 |
| FRANCE      | ARGENTINA   |   1995 | 52916227.7375 |
| FRANCE      | ARGENTINA   |   1996 | 51077995.8841 |
+-------------+-------------+--------+---------------+
4 rows in set

Q8
+--------+----------------------+
| o_year | mkt_share            |
+--------+----------------------+
|   1995 | 0.035094304475112484 |
|   1996 |  0.03724375099464825 |
+--------+----------------------+
2 rows in set

Q9
+----------------+--------+---------------+
| nation         | o_year | sum_profit    |
+----------------+--------+---------------+
| ALGERIA        |   1998 | 29931671.4862 |
| ALGERIA        |   1997 | 49521023.1139 |
| ALGERIA        |   1996 | 51283603.7356 |
| ALGERIA        |   1995 | 50206939.3447 |
| ALGERIA        |   1994 | 48738988.5891 |
| ALGERIA        |   1993 | 48084070.1204 |
| ALGERIA        |   1992 | 49725592.1793 |
| ARGENTINA      |   1998 | 26407044.9262 |
| ARGENTINA      |   1997 | 46224601.0785 |
| ARGENTINA      |   1996 | 44579611.0571 |
| ARGENTINA      |   1995 | 45081953.2540 |
| ARGENTINA      |   1994 | 48291282.8512 |
| ARGENTINA      |   1993 | 48063838.9130 |
| ARGENTINA      |   1992 | 45277890.2991 |
| BRAZIL         |   1998 | 28577022.6384 |
| BRAZIL         |   1997 | 46808660.3688 |
| BRAZIL         |   1996 | 47119265.0765 |
| BRAZIL         |   1995 | 47706399.9100 |
| BRAZIL         |   1994 | 48377469.9386 |
| BRAZIL         |   1993 | 46933565.7471 |
| BRAZIL         |   1992 | 47272215.5408 |
| CANADA         |   1998 | 30500303.6521 |
| CANADA         |   1997 | 50046257.5687 |
| CANADA         |   1996 | 52638586.9029 |
| CANADA         |   1995 | 50433911.3289 |
| CANADA         |   1994 | 51605251.7124 |
| CANADA         |   1993 | 50117218.8464 |
| CANADA         |   1992 | 50347111.2789 |
| CHINA          |   1998 | 26956001.9487 |
| CHINA          |   1997 | 48311246.7866 |
| CHINA          |   1996 | 51133929.1033 |
| CHINA          |   1995 | 48024289.1049 |
| CHINA          |   1994 | 50027433.6557 |
| CHINA          |   1993 | 48240226.3801 |
| CHINA          |   1992 | 47769117.6007 |
| EGYPT          |   1998 | 26972573.1604 |
| EGYPT          |   1997 | 46708654.7666 |
| EGYPT          |   1996 | 46095050.4457 |
| EGYPT          |   1995 | 44901908.2949 |
| EGYPT          |   1994 | 48522762.8892 |
| EGYPT          |   1993 | 49055807.7642 |
| EGYPT          |   1992 | 46909796.1083 |
| ETHIOPIA       |   1998 | 26364411.6457 |
| ETHIOPIA       |   1997 | 44889623.0645 |
| ETHIOPIA       |   1996 | 47554295.2892 |
| ETHIOPIA       |   1995 | 44747639.5440 |
| ETHIOPIA       |   1994 | 46497570.0631 |
| ETHIOPIA       |   1993 | 43853718.5460 |
| ETHIOPIA       |   1992 | 44005773.0397 |
| FRANCE         |   1998 | 27033406.6353 |
| FRANCE         |   1997 | 45763555.5515 |
| FRANCE         |   1996 | 47178544.9301 |
| FRANCE         |   1995 | 48821282.1929 |
| FRANCE         |   1994 | 46444640.9397 |
| FRANCE         |   1993 | 46602311.0590 |
| FRANCE         |   1992 | 47769356.5113 |
| GERMANY        |   1998 | 26165681.8305 |
| GERMANY        |   1997 | 46600844.4431 |
| GERMANY        |   1996 | 44873520.1979 |
| GERMANY        |   1995 | 47761215.6058 |
| GERMANY        |   1994 | 42283120.0209 |
| GERMANY        |   1993 | 46954873.9820 |
| GERMANY        |   1992 | 46263626.6361 |
| INDIA          |   1998 | 27651103.0250 |
| INDIA          |   1997 | 46000888.8340 |
| INDIA          |   1996 | 43993476.7354 |
| INDIA          |   1995 | 44015709.1914 |
| INDIA          |   1994 | 44281439.6282 |
| INDIA          |   1993 | 45367255.7857 |
| INDIA          |   1992 | 45350810.5330 |
| INDONESIA      |   1998 | 27120545.3120 |
| INDONESIA      |   1997 | 45745362.3667 |
| INDONESIA      |   1996 | 45347554.8232 |
| INDONESIA      |   1995 | 45685709.4978 |
| INDONESIA      |   1994 | 44738603.1901 |
| INDONESIA      |   1993 | 45172063.2033 |
| INDONESIA      |   1992 | 44623924.3942 |
| IRAN           |   1998 | 27876287.0949 |
| IRAN           |   1997 | 47184621.5647 |
| IRAN           |   1996 | 47397859.7878 |
| IRAN           |   1995 | 49579120.6991 |
| IRAN           |   1994 | 48032316.8744 |
| IRAN           |   1993 | 48295593.2066 |
| IRAN           |   1992 | 50531453.3934 |
| IRAQ           |   1998 | 29997323.2927 |
| IRAQ           |   1997 | 52851471.1377 |
| IRAQ           |   1996 | 53671825.6297 |
| IRAQ           |   1995 | 53251012.1025 |
| IRAQ           |   1994 | 50934553.4361 |
| IRAQ           |   1993 | 51961214.1186 |
| IRAQ           |   1992 | 50840364.3833 |
| JAPAN          |   1998 | 26054615.4955 |
| JAPAN          |   1997 | 43557394.2595 |
| JAPAN          |   1996 | 46531743.0980 |
| JAPAN          |   1995 | 41688293.4741 |
| JAPAN          |   1994 | 45526719.0728 |
| JAPAN          |   1993 | 45619475.4478 |
| JAPAN          |   1992 | 44545639.3069 |
| JORDAN         |   1998 | 24793092.4101 |
| JORDAN         |   1997 | 42050730.7748 |
| JORDAN         |   1996 | 42562783.8663 |
| JORDAN         |   1995 | 42253019.5330 |
| JORDAN         |   1994 | 45027034.7721 |
| JORDAN         |   1993 | 44797510.9808 |
| JORDAN         |   1992 | 41313405.2890 |
| KENYA          |   1998 | 24550926.4693 |
| KENYA          |   1997 | 42767120.5848 |
| KENYA          |   1996 | 45000095.1105 |
| KENYA          |   1995 | 43250458.0109 |
| KENYA          |   1994 | 42891596.7158 |
| KENYA          |   1993 | 43599201.5126 |
| KENYA          |   1992 | 45286145.8141 |
| MOROCCO        |   1998 | 23482053.5970 |
| MOROCCO        |   1997 | 41503033.0020 |
| MOROCCO        |   1996 | 45645555.9409 |
| MOROCCO        |   1995 | 44462858.7689 |
| MOROCCO        |   1994 | 44768368.8310 |
| MOROCCO        |   1993 | 44611871.2477 |
| MOROCCO        |   1992 | 43057959.1352 |
| MOZAMBIQUE     |   1998 | 28824737.9244 |
| MOZAMBIQUE     |   1997 | 48682746.5995 |
| MOZAMBIQUE     |   1996 | 50816940.9909 |
| MOZAMBIQUE     |   1995 | 50010039.0178 |
| MOZAMBIQUE     |   1994 | 48794892.1253 |
| MOZAMBIQUE     |   1993 | 48451128.3332 |
| MOZAMBIQUE     |   1992 | 50113858.5449 |
| PERU           |   1998 | 30575758.1899 |
| PERU           |   1997 | 49323405.6808 |
| PERU           |   1996 | 50063490.6085 |
| PERU           |   1995 | 51272843.6555 |
| PERU           |   1994 | 50690589.2334 |
| PERU           |   1993 | 49086129.3668 |
| PERU           |   1992 | 50067216.3450 |
| ROMANIA        |   1998 | 27367992.9903 |
| ROMANIA        |   1997 | 45668932.7094 |
| ROMANIA        |   1996 | 46594220.7498 |
| ROMANIA        |   1995 | 44576835.1623 |
| ROMANIA        |   1994 | 45640971.0684 |
| ROMANIA        |   1993 | 46374545.0712 |
| ROMANIA        |   1992 | 47130533.3076 |
| RUSSIA         |   1998 | 27486839.8755 |
| RUSSIA         |   1997 | 44050712.6907 |
| RUSSIA         |   1996 | 45604597.4983 |
| RUSSIA         |   1995 | 48972490.6009 |
| RUSSIA         |   1994 | 45652045.5872 |
| RUSSIA         |   1993 | 47139548.1597 |
| RUSSIA         |   1992 | 47159990.1221 |
| SAUDI ARABIA   |   1998 | 29766229.7961 |
| SAUDI ARABIA   |   1997 | 51473031.6922 |
| SAUDI ARABIA   |   1996 | 52859666.6646 |
| SAUDI ARABIA   |   1995 | 50946175.0229 |
| SAUDI ARABIA   |   1994 | 53085288.9954 |
| SAUDI ARABIA   |   1993 | 50907571.2046 |
| SAUDI ARABIA   |   1992 | 50334063.0381 |
| UNITED KINGDOM |   1998 | 27904712.8220 |
| UNITED KINGDOM |   1997 | 48170994.4362 |
| UNITED KINGDOM |   1996 | 46498116.9611 |
| UNITED KINGDOM |   1995 | 43210619.0456 |
| UNITED KINGDOM |   1994 | 47339709.9122 |
| UNITED KINGDOM |   1993 | 44308436.3275 |
| UNITED KINGDOM |   1992 | 45870809.6693 |
| UNITED STATES  |   1998 | 25856187.3719 |
| UNITED STATES  |   1997 | 44934753.2208 |
| UNITED STATES  |   1996 | 44826974.2915 |
| UNITED STATES  |   1995 | 44160425.4086 |
| UNITED STATES  |   1994 | 43193241.6843 |
| UNITED STATES  |   1993 | 45126307.2619 |
| UNITED STATES  |   1992 | 44205926.3317 |
| VIETNAM        |   1998 | 28289193.6726 |
| VIETNAM        |   1997 | 48284585.4019 |
| VIETNAM        |   1996 | 48360225.9084 |
| VIETNAM        |   1995 | 48742082.6165 |
| VIETNAM        |   1994 | 49035537.3894 |
| VIETNAM        |   1993 | 47222674.6352 |
| VIETNAM        |   1992 | 48628336.9011 |
+----------------+--------+---------------+
175 rows in set

Q10
+-----------+--------------------+-------------+-----------+----------------+------------------------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | revenue     | c_acctbal | n_name         | c_address                                | c_phone         | c_comment                                                                                                       |
+-----------+--------------------+-------------+-----------+----------------+------------------------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------+
|     95962 | Customer#000095962 | 704336.0774 |     -9.33 | MOZAMBIQUE     | 83wOMt9iAb9OJ0HbkQ1PaX3odXVBNEIMXaE      | 26-127-693-7436 | nusual theodolites maintain furiously fluffily iro                                                              |
|     87064 | Customer#000087064 | 684037.4349 |   5244.68 | BRAZIL         | 0xej6ldT8zi7MwLdDJ1II3YWwprkvwB1 I0kwsf  | 12-930-206-2571 | de of the ironic, silent warthogs. bold, r                                                                      |
|     56416 | Customer#000056416 | 661218.0492 |   4303.82 | INDIA          | CEuBN,xZVmP                              | 18-212-984-8331 | al waters cajole along the slyly unusual dugouts. carefully regular deposits use slyly? packages h              |
|     46450 | Customer#000046450 | 646205.6835 |   2400.59 | UNITED STATES  | rzWQxB9iFpd8i4KUCAPdv                    | 34-765-320-4326 | ss, final deposits cajole sly                                                                                   |
|    128713 | Customer#000128713 | 643240.1183 |   7200.30 | ARGENTINA      | mm0kxtHFCchaZX4eYSCCyQHno7vq,SRmv4       | 11-174-994-6880 | ording to the express accounts cajole carefully across the bravely special packages. carefully regular account  |
|    102187 | Customer#000102187 | 637493.0787 |   -896.03 | ETHIOPIA       | EAi6vcGnWHUMb6rJwn,PtUgSH74tR Aixa       | 15-877-462-6534 | gular packages. carefully regular deposits cajole carefully of the regular requests. carefully special accou    |
|     42541 | Customer#000042541 | 634546.9756 |   8082.14 | IRAN           | IccOGHgp8g                               | 20-442-159-1337 | cross the final asymptotes. final packages wake furiously ironic dec                                            |
|     51595 | Customer#000051595 | 611926.8265 |   7236.80 | UNITED STATES  | wQFWZk 7JCpeg50O0KCzSmUFnNNwX1aEQ7V3Q    | 34-844-269-9070 | sts. always express accounts use carefully along the quickly speci                                              |
|     66391 | Customer#000066391 | 608385.5852 |   9404.57 | UNITED STATES  | V0XvU1Nh9NU4zsyOkm,RBa                   | 34-149-224-8119 | ages cajole carefully carefully bold deposits: fluffily unusual deposits promise slyly carefully ironic co      |
|     48358 | Customer#000048358 | 603621.4823 |   -611.15 | ETHIOPIA       | ycg3uMG7iDdwQvJ1irr                      | 15-687-936-5181 | the slyly unusual foxes-- carefully regular                                                                     |
|     99175 | Customer#000099175 | 602125.3304 |   2218.76 | INDONESIA      | 9wbW52xx9T84E0dZ Rvz1ozQ1                | 19-125-912-6494 | ide of the slyly ironic foxes boost silently ironic, even instructions. blithe                                  |
|    122509 | Customer#000122509 | 601580.1203 |   2613.83 | KENYA          | ZN1sc0eJrkD8t6X5Q1d3                     | 24-421-308-3881 | brave deposits haggle across the even deposits. instr                                                           |
|    148055 | Customer#000148055 | 601003.6812 |    455.31 | PERU           | Y,RCZ3Bislx64nTsPaRL,5gjx7xgC6y, yKYnCw  | 27-473-476-4382 | uickly final accounts wake carefully sl                                                                         |
|    117451 | Customer#000117451 | 599792.7063 |   1090.48 | UNITED STATES  | bSwr7mNPiaf1f lNK9 uTJxWCL2sn1Lak5NIB    | 34-354-586-6011 | ding to the furiously express accounts boost carefully af                                                       |
|    104110 | Customer#000104110 | 588194.3118 |   2762.52 | JORDAN         | mm7 ZuDX5Z5nAQbKObB 80XBCy,1nyW          | 23-639-800-5768 | urts sleep furiously alongside of the packages! slyly ironic packages sleep                                     |
|     13666 | Customer#000013666 | 579926.1679 |   7453.98 | EGYPT          | DLRUWGcprmWqdROJvmZwpE                   | 14-316-135-4381 | ross the silent requests. special theodolit                                                                     |
|     96202 | Customer#000096202 | 571017.3398 |   4703.04 | CANADA         | 4Vcxcx3w4zMjVYNQaqrweweQY6TJO AP9rdvQaLl | 13-194-779-9597 | en packages use. fluffily regular dependencies boost. never pending requ                                        |
|     70279 | Customer#000070279 | 561369.3650 |   9109.34 | CHINA          | ltie8o3ihwffMrqMrkvN957KZVWmH5           | 28-842-825-1717 | theodolites sleep: blithely final requests are fur                                                              |
|     16972 | Customer#000016972 | 560435.8065 |   6408.66 | ROMANIA        | X6T8vRKy6kSO0f2wJJt                      | 29-483-958-3347 | sts. pending deposits are across the regular, express instructions. carefully daring foxes cajol                |
|    113443 | Customer#000113443 | 557272.6706 |    -72.67 | UNITED KINGDOM | SUHbS85cYxgVkKbfh9sUpEa6ezVSlQuCKe3CV    | 33-819-742-6112 | ic foxes cajole thinly furiously stealthy instructions. pinto beans are. quickly regular accounts integrate car |
+-----------+--------------------+-------------+-----------+----------------+------------------------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------+
20 rows in set

Q11
+------------+-------------+
| ps_partkey | value       |
+------------+-------------+
|     131630 | 17882680.37 |
|     104150 | 17613017.18 |
|     128284 | 16502418.74 |
|       8978 | 16470438.59 |
|     147193 | 16462742.12 |
|      78788 | 16010246.37 |
|      76331 | 15776882.05 |
|     137287 | 15770471.15 |
|      51302 | 15730620.22 |
|     141553 | 15333540.19 |
|     137196 | 15035435.60 |
|     186531 | 14818272.68 |
|     103818 | 14690943.63 |
|      80080 | 14626441.35 |
|       1312 | 14330729.50 |
|       6531 | 14267308.08 |
|      96162 | 14154396.04 |
|      69605 | 14018927.25 |
|      30118 | 13854726.38 |
|      17006 | 13731495.60 |
|      95347 | 13716648.60 |
|      18722 | 13707978.71 |
|     122875 | 13640341.00 |
|     105499 | 13532912.80 |
|     165560 | 13509536.95 |
|       1531 | 13337454.55 |
|      34732 | 13304041.48 |
|     173221 | 13038078.41 |
|     180975 | 13038039.17 |
|      24703 | 12957050.80 |
|      72036 | 12939426.90 |
|     124814 | 12849842.04 |
|     174453 | 12814999.00 |
|      14209 | 12814858.56 |
|     185186 | 12657201.05 |
|     187868 | 12647101.80 |
|     125085 | 12639931.63 |
|      80331 | 12625007.00 |
|     118685 | 12515185.68 |
|     163988 | 12484272.80 |
|     124685 | 12432747.32 |
|      92838 | 12410071.57 |
|     140928 | 12396673.84 |
|       1218 | 12362877.75 |
|      39201 | 12328085.10 |
|      33237 | 12180622.98 |
|     183791 | 12150040.50 |
|       3243 | 12136315.74 |
|      62740 | 12131313.60 |
|     154171 | 12105470.89 |
|      49034 | 11982382.52 |
|      88673 | 11925499.04 |
|      52527 | 11923653.16 |
|      83974 | 11871084.73 |
|      88254 | 11870393.22 |
|        411 | 11806670.95 |
|      14320 | 11800136.02 |
|     164979 | 11794760.03 |
|     166149 | 11778499.72 |
|      74105 | 11750224.34 |
|     169104 | 11708532.18 |
|      15542 | 11687293.42 |
|     161538 | 11661769.80 |
|      63337 | 11592505.40 |
|     117197 | 11508165.60 |
|     102989 | 11497056.75 |
|      10836 | 11465875.43 |
|     199561 | 11431793.36 |
|     134683 | 11384564.54 |
|     136318 | 11351893.30 |
|     166270 | 11336004.81 |
|      32200 | 11324838.00 |
|      57033 | 11281026.52 |
|      18098 | 11245398.24 |
|     135174 | 11189782.12 |
|     181616 | 11183947.65 |
|      85064 | 11175761.43 |
|     120719 | 11164342.08 |
|      99670 | 11140257.47 |
|      46096 | 11034143.76 |
|     195124 | 11030197.30 |
|      78838 | 11012446.40 |
|     151656 | 11010376.90 |
|     156956 | 10996384.80 |
|      34028 | 10942671.24 |
|      15778 | 10937778.75 |
|     199707 | 10924333.33 |
|     118776 | 10920609.31 |
|      27640 | 10919693.42 |
|      15237 | 10918145.54 |
|     148243 | 10916765.29 |
|     111498 | 10867707.51 |
|     132024 | 10834280.47 |
|      35124 | 10806898.50 |
|     196818 | 10787371.25 |
|     197669 | 10779504.60 |
|     110042 | 10778828.37 |
|     197422 | 10770092.44 |
|      75160 | 10746976.60 |
|     191567 | 10642430.39 |
|      34225 | 10574664.41 |
|     102588 | 10567012.05 |
|      44148 | 10505249.34 |
|     126607 | 10484944.29 |
|     172625 | 10444857.62 |
|     157054 | 10406203.24 |
|      19322 | 10378704.98 |
|     136541 | 10371536.77 |
|     167526 | 10320346.58 |
|     136011 | 10302146.84 |
|     107431 | 10273992.76 |
|      16485 | 10257703.67 |
|      52580 | 10250264.05 |
|        839 | 10238243.36 |
|      31704 | 10196678.94 |
|     122558 | 10137326.18 |
|     180386 | 10123318.07 |
|      97705 | 10089163.37 |
|      96327 | 10087851.88 |
|     136143 | 10082137.97 |
|      15174 | 10057277.55 |
|     193324 | 10039922.93 |
|      33593 | 10019952.10 |
|     126288 | 10014855.05 |
|      64123 |  9985650.90 |
|     183712 |  9973256.18 |
|     138831 |  9963069.10 |
|     123694 |  9959096.38 |
|      51734 |  9952439.73 |
|      11861 |  9949647.12 |
|     119127 |  9942105.69 |
|     173308 |  9932264.52 |
|      40986 |  9921554.40 |
|     176970 |  9919708.65 |
|      54316 |  9913595.16 |
|      62644 |  9903936.27 |
|     185354 |  9895956.52 |
|      81468 |  9885132.60 |
|     104687 |  9883888.05 |
|     198959 |  9875351.28 |
|     179767 |  9872309.86 |
|     102835 |  9870743.52 |
|     163221 |  9856173.04 |
|      32633 |  9852565.04 |
|      19605 |  9850164.48 |
|      47378 |  9826135.11 |
|      44026 |  9822433.44 |
|     126629 |  9816227.30 |
|     199665 |  9812400.23 |
|      30989 |  9812295.52 |
|     102177 |  9810372.32 |
|      25765 |  9806344.88 |
|     110721 |  9804895.23 |
|     159532 |  9803738.34 |
|     101640 |  9801375.65 |
|     151569 |  9792489.20 |
|     180629 |  9782164.34 |
|     165528 |  9769074.10 |
|      23772 |  9766084.22 |
|     149727 |  9765190.96 |
|     189605 |  9761887.80 |
|      74703 |  9758757.28 |
|      83382 |  9758144.21 |
|      93775 |  9726901.71 |
|      56192 |  9725508.16 |
|      50060 |  9712714.65 |
|      15409 |  9706898.91 |
|     139104 |  9701070.72 |
|     177435 |  9686566.09 |
|      31351 |  9675197.98 |
|      20495 |  9672566.31 |
|      24537 |  9654516.03 |
|     160528 |  9650804.70 |
|      34706 |  9647241.90 |
|     149039 |  9643498.32 |
|     147139 |  9642356.34 |
|     118629 |  9624960.80 |
|      35359 |  9621549.92 |
|      33854 |  9616857.73 |
|      33707 |  9609988.84 |
|     149055 |  9599364.32 |
|     127429 |  9580670.49 |
|      67575 |  9579613.26 |
|      80727 |  9576545.81 |
|     181650 |  9574445.40 |
|      50176 |  9573389.08 |
|     171093 |  9571625.20 |
|     151342 |  9569230.21 |
|     123052 |  9561903.68 |
|     132633 |  9545052.14 |
|     130419 |  9524936.49 |
|      89241 |  9512992.32 |
|     138255 |  9503515.93 |
|      31680 |  9502841.07 |
|     151986 |  9500862.59 |
|     146390 |  9490242.96 |
|      62275 |  9475584.10 |
|      33518 |  9475074.40 |
|       5286 |  9473739.88 |
|      39020 |  9467701.22 |
|     113281 |  9466510.94 |
|     138789 |  9464407.24 |
|     165040 |  9462153.75 |
|     150766 |  9461855.88 |
|      54341 |  9459425.45 |
|      33464 |  9459377.37 |
|      15251 |  9455980.84 |
|     145308 |  9454189.29 |
|     192621 |  9449324.14 |
|     175218 |  9448987.35 |
|      58992 |  9446144.40 |
|      24548 |  9442739.03 |
|     177563 |  9440891.04 |
|     184482 |  9431486.10 |
|      78961 |  9430401.05 |
|     174167 |  9428622.96 |
|      88265 |  9423143.28 |
|       6057 |  9405359.37 |
|      85387 |  9402175.55 |
|      47053 |  9399707.66 |
|     128973 |  9399265.92 |
|      65668 |  9395584.45 |
|      50222 |  9394502.96 |
|     116534 |  9388011.08 |
|     140959 |  9386284.56 |
|      46897 |  9385056.21 |
|     141872 |  9383820.48 |
|     177181 |  9383551.92 |
|     168265 |  9376664.16 |
|      48974 |  9374769.12 |
|      46218 |  9364135.50 |
|     104039 |  9363227.03 |
|      61538 |  9360159.08 |
|      94688 |  9359604.98 |
|     122393 |  9357937.19 |
|       7323 |  9356712.30 |
|     197892 |  9356573.44 |
|     194056 |  9352381.73 |
|      61285 |  9348480.54 |
|     180336 |  9347874.15 |
|     121930 |  9347784.74 |
|      80652 |  9347143.50 |
|      18549 |  9346038.72 |
|      23992 |  9339908.16 |
|     136583 |  9337299.56 |
|     156151 |  9337138.10 |
|     160572 |  9336553.40 |
|     113391 |  9335558.10 |
|      48068 |  9334317.92 |
|      20409 |  9331093.65 |
|      39712 |  9324685.28 |
|      59364 |  9322249.86 |
|       1344 |  9308304.39 |
|      60549 |  9308293.20 |
|      83854 |  9307387.25 |
|      92092 |  9307165.64 |
|     193306 |  9306177.31 |
|     118265 |  9300250.20 |
|     107568 |  9296254.34 |
|     109127 |  9293552.10 |
|     184688 |  9291647.92 |
|       8718 |  9287337.37 |
|      80433 |  9286295.52 |
|      26670 |  9284963.44 |
|     139548 |  9283605.21 |
|      14736 |  9280119.20 |
|      97886 |  9273852.42 |
|     181442 |  9273130.50 |
|     172360 |  9272824.92 |
|     192714 |  9268366.36 |
|     106726 |  9264879.90 |
|      72157 |  9263498.40 |
|      70445 |  9257553.92 |
|      75148 |  9257420.83 |
|      26170 |  9256074.12 |
|     116531 |  9249721.71 |
|     133665 |  9245464.80 |
|     129041 |  9244629.48 |
|     136486 |  9240748.92 |
|     198924 |  9239976.06 |
|     115254 |  9233580.37 |
|     168135 |  9232693.98 |
|      22480 |  9232190.78 |
|     192018 |  9230386.58 |
|     111889 |  9228204.96 |
|     151661 |  9227926.90 |
|      96482 |  9226960.85 |
|      49198 |  9226436.40 |
|      41219 |  9222883.52 |
|     113502 |  9222208.59 |
|      84009 |  9218703.22 |
|     192788 |  9213468.00 |
|     160251 |  9206353.32 |
|     188162 |  9200537.88 |
|     167589 |  9195835.03 |
|     132673 |  9194021.22 |
|     191105 |  9192417.12 |
|     128748 |  9189941.55 |
|     130423 |  9184710.96 |
|      22639 |  9182963.16 |
|     199034 |  9180909.86 |
|     187644 |  9180350.20 |
|        970 |  9175757.70 |
|      59070 |  9170000.64 |
|      66568 |  9166070.04 |
|      52715 |  9161221.80 |
|     130276 |  9161201.57 |
|      24189 |  9160740.15 |
|     132402 |  9144498.48 |
|      37799 |  9142271.24 |
|     173337 |  9140566.68 |
|     176552 |  9135054.51 |
|     195714 |  9133679.77 |
|     119363 |  9123261.90 |
|     161160 |  9122259.60 |
|     196968 |  9111592.20 |
|      61943 |  9111527.33 |
|      79766 |  9109534.89 |
|     178082 |  9105694.92 |
|      38800 |  9105468.72 |
|      83608 |  9099493.68 |
|     146346 |  9098628.00 |
|     116690 |  9098099.93 |
|      64690 |  9095441.10 |
|      82061 |  9095381.18 |
|      89015 |  9092660.48 |
|     188457 |  9091400.40 |
|     125177 |  9090455.55 |
|     114776 |  9088177.68 |
|       4486 |  9087487.20 |
|     176940 |  9086842.84 |
|      93157 |  9084361.81 |
|     148624 |  9083370.78 |
|       4441 |  9079520.58 |
|      63590 |  9079125.44 |
|     174189 |  9078023.39 |
|      63054 |  9075441.98 |
|      14950 |  9073156.19 |
|     175646 |  9072322.47 |
|      63712 |  9067710.48 |
|     157197 |  9067452.77 |
|     147196 |  9064699.80 |
|      50551 |  9062434.72 |
|      43035 |  9061782.03 |
|     187679 |  9056529.40 |
|      96673 |  9056525.94 |
|     130148 |  9054217.06 |
|     159007 |  9053155.29 |
|      41544 |  9052820.94 |
|     109476 |  9048012.09 |
|      60092 |  9045562.44 |
|     197490 |  9044579.88 |
|      47311 |  9037223.52 |
|      87230 |  9033227.61 |
|       3860 |  9030622.02 |
|       5466 |  9029841.66 |
|     171537 |  9024699.30 |
|      39707 |  9022833.12 |
|     167048 |  9022709.18 |
|     109006 |  9022258.40 |
|      17910 |  9019688.45 |
|     132826 |  9017286.74 |
|     157502 |  9016444.08 |
|     142309 |  9016270.60 |
|      78891 |  9005693.25 |
|      88301 |  9002414.82 |
|      11496 |  9000803.97 |
|     163633 |  8996162.06 |
|     151809 |  8993104.95 |
|     131555 |  8988340.68 |
|      72812 |  8985370.68 |
|      77047 |  8981489.79 |
|       1553 |  8977226.10 |
|     162531 |  8973689.92 |
|     154026 |  8973320.24 |
|     125499 |  8969667.84 |
|      34547 |  8966116.43 |
|      41301 |  8965350.42 |
|      12853 |  8959403.59 |
|      27736 |  8957933.23 |
|     162817 |  8956868.20 |
|     155389 |  8955349.85 |
|     130360 |  8952928.25 |
|     120878 |  8952393.10 |
|     150671 |  8952112.72 |
|     190365 |  8951671.57 |
|      72364 |  8950587.82 |
|      71615 |  8949277.07 |
|      95277 |  8947796.58 |
|      78180 |  8946814.80 |
|      97062 |  8945057.46 |
|     170013 |  8944660.40 |
|     113426 |  8943016.29 |
|     173751 |  8942914.28 |
|       1478 |  8941906.24 |
|      26061 |  8941022.48 |
|     152527 |  8939654.10 |
|     148360 |  8939589.40 |
|      44057 |  8939101.36 |
|      13595 |  8936720.10 |
|      33337 |  8935366.48 |
|     169698 |  8931507.20 |
|      26155 |  8927283.11 |
|      17185 |  8927218.40 |
|      51996 |  8926661.08 |
|     101869 |  8919281.70 |
|      14561 |  8910653.92 |
|     190047 |  8909427.90 |
|     104143 |  8909328.40 |
|     133330 |  8907195.90 |
|     169144 |  8904989.34 |
|      87067 |  8900079.44 |
|     176075 |  8898845.64 |
|      25076 |  8895274.12 |
|      80838 |  8895205.30 |
|      40387 |  8890891.55 |
|      88004 |  8888748.80 |
|     105527 |  8888672.72 |
|      40741 |  8886674.24 |
|      76690 |  8880622.61 |
|      86485 |  8880488.57 |
|      75736 |  8877666.06 |
|      48704 |  8876626.52 |
|      56450 |  8872277.59 |
|      61683 |  8870173.93 |
|      24067 |  8867814.12 |
|     108012 |  8863632.38 |
|     180971 |  8862007.20 |
|     132986 |  8861335.20 |
|      35839 |  8859344.64 |
|     191553 |  8857411.14 |
|     163492 |  8855825.91 |
|     112101 |  8851904.10 |
|      27050 |  8847924.19 |
|      57481 |  8845309.59 |
|     163252 |  8842276.65 |
|      87958 |  8840221.67 |
|      60162 |  8838927.08 |
|     131928 |  8838900.48 |
|     123514 |  8833601.14 |
|      42891 |  8830401.37 |
|      71547 |  8829540.72 |
|      13975 |  8826582.48 |
|      31577 |  8825371.40 |
|      86165 |  8816308.38 |
|     164646 |  8815470.18 |
|     150176 |  8814992.11 |
|     152464 |  8814533.82 |
|     183434 |  8813941.24 |
|      58839 |  8808010.20 |
|      59952 |  8801497.32 |
|     151038 |  8800215.80 |
|     139523 |  8800032.57 |
|       8828 |  8798704.66 |
|      14080 |  8797032.12 |
|     194080 |  8792825.27 |
|      87199 |  8788933.64 |
|      91747 |  8785811.64 |
|     194429 |  8776185.03 |
|     118998 |  8776071.00 |
|     179467 |  8771474.74 |
|      68715 |  8771302.80 |
|     180572 |  8771095.68 |
|      19821 |  8770770.82 |
|      41702 |  8770565.71 |
|      27916 |  8769001.47 |
|     121302 |  8763598.50 |
|     107013 |  8762893.37 |
|      37287 |  8761196.43 |
|     117050 |  8758230.00 |
|      58547 |  8757757.40 |
|     197088 |  8749026.12 |
|      55839 |  8747234.02 |
|      71829 |  8744546.91 |
|      30961 |  8743416.92 |
|     134548 |  8741635.28 |
|     179833 |  8738680.00 |
|      79721 |  8737857.70 |
|     144577 |  8736427.08 |
|      29051 |  8729063.28 |
|     131481 |  8728799.64 |
|      73271 |  8727985.25 |
|      89553 |  8725727.19 |
|      31306 |  8724451.12 |
|      82181 |  8724017.16 |
|      95549 |  8723460.30 |
|      31507 |  8722094.40 |
|      21302 |  8722054.95 |
|     137953 |  8721611.83 |
|     195768 |  8721020.99 |
|     180105 |  8718021.20 |
|      98241 |  8717935.36 |
|      59431 |  8715482.28 |
|     143694 |  8713267.63 |
|     109020 |  8713043.36 |
|      46732 |  8711642.04 |
|     144172 |  8711013.10 |
|     139056 |  8710786.50 |
|     107543 |  8706135.75 |
|      89127 |  8705410.56 |
|     146544 |  8704812.86 |
|     195524 |  8699333.14 |
|     133563 |  8698060.14 |
|     112707 |  8694322.84 |
|      98951 |  8690376.70 |
|     132635 |  8689305.24 |
|      69056 |  8688980.25 |
|     134143 |  8688695.26 |
|     148150 |  8687553.16 |
|      89122 |  8686767.31 |
|      15085 |  8685772.26 |
|     196686 |  8682783.57 |
|       3076 |  8672940.78 |
|     137428 |  8672547.80 |
|      27263 |  8671719.36 |
|     101561 |  8667962.72 |
|      12597 |  8662223.52 |
|     143329 |  8661688.72 |
|     130813 |  8659409.04 |
|     183679 |  8658698.30 |
|      47449 |  8658493.58 |
|     164677 |  8658220.00 |
|      51437 |  8654713.02 |
|     116162 |  8649713.36 |
|      71889 |  8645159.67 |
|       6486 |  8639891.76 |
|     192102 |  8638102.72 |
|     101660 |  8634451.80 |
|     124703 |  8633146.86 |
|     150469 |  8631948.60 |
|     197467 |  8630739.78 |
|      97621 |  8630453.32 |
|     150354 |  8630288.15 |
|     179544 |  8630121.63 |
|      38972 |  8626072.00 |
|     110732 |  8625761.16 |
|     170791 |  8625203.06 |
|     149414 |  8617070.17 |
|      59527 |  8616079.20 |
|     157580 |  8615676.04 |
|      16268 |  8615087.46 |
|      76464 |  8610219.38 |
|      44474 |  8607934.92 |
|     125527 |  8607708.08 |
|     118076 |  8602251.65 |
|     180362 |  8601367.05 |
|       5808 |  8599851.04 |
|      28703 |  8599486.36 |
|     113373 |  8597996.36 |
|     118918 |  8597063.80 |
|      44868 |  8596304.52 |
|      43419 |  8596265.35 |
|      89763 |  8595248.64 |
|     119232 |  8594224.56 |
|     108649 |  8590683.68 |
|      10396 |  8588398.05 |
|      79536 |  8587117.83 |
|     149800 |  8587058.86 |
|     165839 |  8582991.20 |
|     115397 |  8581524.77 |
|     104394 |  8581384.42 |
|     142569 |  8581127.40 |
|      63676 |  8580930.08 |
|      29029 |  8580613.53 |
|     156604 |  8580477.00 |
|       7310 |  8579949.50 |
|     105381 |  8576164.24 |
|      84306 |  8573960.40 |
|      61217 |  8570393.04 |
|     164438 |  8569616.36 |
|      28073 |  8565639.60 |
|     125743 |  8563258.90 |
|     190032 |  8561620.55 |
|     147122 |  8561245.68 |
|       5384 |  8558830.08 |
|      70172 |  8558319.64 |
|     161966 |  8556193.38 |
|      69530 |  8554377.60 |
|     111243 |  8553627.55 |
|      72590 |  8551077.51 |
|     134423 |  8550604.77 |
|      44509 |  8547134.31 |
|     160707 |  8546000.68 |
|      54123 |  8545976.26 |
|      36547 |  8540333.04 |
|      48715 |  8537983.35 |
|     103078 |  8537142.60 |
|     137613 |  8536278.96 |
|      44995 |  8532416.72 |
|     191159 |  8532173.37 |
|     119345 |  8532070.56 |
|     109941 |  8531904.79 |
|       5449 |  8528034.35 |
|     134116 |  8526854.95 |
|     199268 |  8523599.58 |
|     168520 |  8523360.67 |
|     154189 |  8521620.13 |
|     108771 |  8513853.87 |
|     198651 |  8511238.80 |
|      93681 |  8510935.14 |
|     170680 |  8509087.68 |
|     106409 |  8506859.19 |
|      27110 |  8499811.75 |
|      43224 |  8499539.52 |
|     153225 |  8499434.28 |
|      16681 |  8498021.66 |
|     117983 |  8496934.32 |
|     192158 |  8492372.03 |
|      33900 |  8491139.64 |
|      37006 |  8489126.28 |
|     176554 |  8488633.92 |
|      69234 |  8484937.26 |
|     176652 |  8484496.02 |
|      41660 |  8480585.65 |
|     129104 |  8480411.17 |
|      66960 |  8478978.86 |
|      36296 |  8472438.75 |
|      98665 |  8471241.57 |
|     134173 |  8467888.57 |
|      60496 |  8467019.22 |
|     197520 |  8466553.20 |
|     116746 |  8465792.60 |
|     187394 |  8458248.24 |
|     140377 |  8455546.68 |
|      97326 |  8450501.67 |
|      26770 |  8449625.64 |
|     104884 |  8446152.26 |
|     143109 |  8443547.19 |
|     127361 |  8441094.08 |
|     104754 |  8436883.50 |
|     183676 |  8436165.76 |
|        906 |  8434608.12 |
|      55768 |  8433763.69 |
|     118654 |  8433465.57 |
|      39310 |  8433214.55 |
|     173261 |  8432992.53 |
|      93976 |  8432605.20 |
|      63318 |  8432149.26 |
|     128243 |  8424182.94 |
|     156063 |  8422743.54 |
|     195087 |  8421279.30 |
|      67668 |  8417594.98 |
|      49882 |  8417237.80 |
|     105631 |  8412628.07 |
|      40987 |  8406033.41 |
|     185735 |  8404112.83 |
|     173986 |  8403050.34 |
|      87372 |  8402838.40 |
|      24509 |  8398807.24 |
|     180522 |  8394989.75 |
|      76215 |  8394433.35 |
|     193872 |  8390435.23 |
|     141234 |  8390180.92 |
|      91138 |  8386645.20 |
|      28097 |  8385577.38 |
|       4053 |  8384952.75 |
|      17050 |  8380304.40 |
|      64050 |  8377921.56 |
|      80836 |  8375803.16 |
|      86084 |  8373551.95 |
|     168499 |  8373348.72 |
|     178642 |  8372218.52 |
|       8498 |  8370557.16 |
|     156312 |  8366249.30 |
|     136803 |  8361949.92 |
|      92109 |  8359503.23 |
|     138625 |  8358135.21 |
|     137540 |  8358031.08 |
|     176531 |  8355437.00 |
|      53783 |  8352395.63 |
|     106977 |  8352334.98 |
|      21385 |  8351786.37 |
|     114885 |  8351582.40 |
|     113643 |  8350530.65 |
|      89061 |  8349422.08 |
|      77752 |  8348730.24 |
|      28623 |  8348321.44 |
|      74478 |  8348064.27 |
|      41383 |  8347223.45 |
|     147632 |  8346967.80 |
|      40948 |  8346743.30 |
|     154324 |  8346521.91 |
|      89724 |  8346034.80 |
|     119083 |  8338084.92 |
|     124143 |  8335841.76 |
|      80512 |  8335705.69 |
|     105047 |  8332249.86 |
|      38243 |  8329017.19 |
|      42583 |  8328613.91 |
|      44240 |  8327684.64 |
|      57611 |  8321693.94 |
|       9730 |  8319725.70 |
|      91655 |  8318837.40 |
|      13140 |  8316216.96 |
|     112257 |  8315169.85 |
|      27182 |  8314740.99 |
|     166654 |  8314332.64 |
|      40572 |  8312654.55 |
|      26680 |  8311626.68 |
|     138947 |  8311347.29 |
|     184982 |  8310393.08 |
|      35540 |  8308058.43 |
|     181446 |  8304851.76 |
|      65160 |  8299581.90 |
|       9533 |  8299139.42 |
|      67836 |  8294228.46 |
|     159414 |  8293114.90 |
|     115025 |  8291746.65 |
|      30780 |  8291580.00 |
|     164680 |  8290263.02 |
|       4599 |  8288816.03 |
|      73366 |  8286818.96 |
|     135625 |  8284930.92 |
|      46497 |  8284638.88 |
|      63781 |  8284447.60 |
|      84332 |  8283372.14 |
|     196269 |  8276407.36 |
|     166651 |  8275663.35 |
|        142 |  8273960.31 |
|      56904 |  8272891.44 |
|      46821 |  8272603.71 |
|      76051 |  8272300.75 |
|      19666 |  8270192.64 |
|      92723 |  8267074.20 |
|     125843 |  8266816.38 |
|     158722 |  8266634.88 |
|      28941 |  8266245.12 |
|      39968 |  8265605.53 |
|      41429 |  8265317.84 |
|      61601 |  8264074.31 |
|     179159 |  8260137.47 |
|      15969 |  8259835.96 |
|     121125 |  8253912.49 |
|      66486 |  8253743.66 |
|     181031 |  8253570.14 |
|      43712 |  8250825.78 |
|      13842 |  8245765.00 |
|      76203 |  8245412.16 |
|      68992 |  8243081.46 |
|     119704 |  8241363.06 |
|      86109 |  8240377.92 |
|      29534 |  8239914.00 |
|      68596 |  8239825.29 |
|     168291 |  8237626.32 |
|     183308 |  8235947.21 |
|      78657 |  8233481.64 |
|     193545 |  8233037.49 |
|      23658 |  8232306.18 |
|     179945 |  8231365.25 |
|      53391 |  8231252.10 |
|      71380 |  8231125.68 |
|      53666 |  8226715.00 |
|     118592 |  8226181.00 |
|      67203 |  8225355.99 |
|       1178 |  8224625.05 |
|     147876 |  8224189.62 |
|      80042 |  8220826.70 |
|      48950 |  8218611.22 |
|      43331 |  8218448.04 |
|     177706 |  8215723.50 |
|     145442 |  8215706.16 |
|     197042 |  8215536.00 |
|     169952 |  8214698.43 |
|      57907 |  8211740.04 |
|     145741 |  8210316.57 |
|      91144 |  8209855.02 |
|     160266 |  8209468.80 |
|      31602 |  8209366.90 |
|      98672 |  8208412.85 |
|     199012 |  8207897.50 |
|     151148 |  8207645.16 |
|     116545 |  8207573.24 |
|     122176 |  8207508.04 |
|      11021 |  8206766.10 |
|      47752 |  8203436.82 |
|        124 |  8203209.30 |
|     148126 |  8202846.66 |
|      15753 |  8202695.55 |
|      50833 |  8200880.16 |
|      11523 |  8196478.02 |
|      71478 |  8195930.68 |
|     129262 |  8190520.80 |
|      43023 |  8186451.85 |
|     119193 |  8184853.14 |
|      85067 |  8182638.86 |
|     164534 |  8181563.04 |
|      82556 |  8180455.14 |
|      31813 |  8179417.14 |
|      81345 |  8173128.69 |
|      38413 |  8172464.04 |
|     106014 |  8171418.35 |
|     191180 |  8170663.97 |
|      43274 |  8169669.72 |
|       5837 |  8166123.50 |
|      63332 |  8161839.60 |
|      47668 |  8161790.04 |
|     112468 |  8160728.40 |
|     132541 |  8160680.00 |
|      59457 |  8160393.33 |
|      71751 |  8159865.19 |
|     118395 |  8156795.00 |
|     132390 |  8154867.54 |
|      44792 |  8153384.22 |
|     128838 |  8153018.30 |
|      87197 |  8152281.72 |
|     187978 |  8150832.56 |
|     147419 |  8150063.60 |
|     149166 |  8149406.78 |
|     196012 |  8147307.42 |
|     190519 |  8145402.96 |
|     151511 |  8144276.58 |
|      88891 |  8140166.24 |
|     168056 |  8139101.96 |
|     189186 |  8136933.25 |
|     117326 |  8136047.82 |
|      60575 |  8133316.80 |
|      75452 |  8130427.37 |
|     194126 |  8129751.80 |
|     130199 |  8129270.88 |
|      41680 |  8128823.40 |
|     107624 |  8125799.20 |
|     135069 |  8123999.10 |
|     119032 |  8123770.24 |
|      27635 |  8123076.65 |
|      14317 |  8121553.23 |
|     148018 |  8119898.16 |
|      51152 |  8118370.26 |
|     112643 |  8117331.37 |
|     119526 |  8116075.80 |
|     192084 |  8114896.38 |
|     151385 |  8114711.28 |
|     160836 |  8112053.68 |
|      91468 |  8111785.50 |
|      58877 |  8108256.25 |
|      41885 |  8107026.81 |
|     155542 |  8106757.18 |
|     149968 |  8104953.78 |
|     168380 |  8103576.00 |
|     134641 |  8101092.32 |
|      92470 |  8100877.70 |
|     113610 |  8098591.93 |
|     198538 |  8097343.20 |
|     122506 |  8096090.76 |
|      29082 |  8093543.55 |
|     161345 |  8093157.93 |
|     105743 |  8093045.53 |
|     103572 |  8091573.66 |
|      59514 |  8089470.48 |
|       8801 |  8088454.15 |
|     129062 |  8088206.58 |
|     155464 |  8086115.79 |
|      86363 |  8082561.00 |
|     180836 |  8082087.30 |
|      92558 |  8081407.80 |
|      85120 |  8073164.00 |
|     149026 |  8072285.40 |
|      51138 |  8072074.48 |
|      36306 |  8071648.86 |
|     102380 |  8070503.00 |
|     147597 |  8069397.60 |
|      41382 |  8059995.35 |
|     121856 |  8059809.11 |
|      86644 |  8058667.76 |
|     108481 |  8058214.81 |
|      41685 |  8057355.39 |
|     175712 |  8054878.30 |
|      72815 |  8052294.24 |
|      58794 |  8047848.00 |
|     118769 |  8047465.14 |
|     157192 |  8046501.96 |
|     195708 |  8045001.94 |
|     163683 |  8044727.02 |
|     189018 |  8043927.54 |
|      62904 |  8043011.65 |
|      80095 |  8042575.59 |
|      90500 |  8042502.65 |
|      73281 |  8040167.52 |
|     150710 |  8035910.80 |
|     139282 |  8034489.36 |
|     172904 |  8033791.68 |
|      38881 |  8032557.38 |
|      53055 |  8030796.15 |
|     105816 |  8025318.24 |
|      88304 |  8024637.06 |
|     115565 |  8023928.25 |
|      55376 |  8021432.16 |
|      56334 |  8019313.12 |
|      58875 |  8016065.00 |
|       4688 |  8012303.00 |
|      49117 |  8009207.80 |
|      57173 |  8008116.27 |
|      48176 |  8006765.85 |
|     112191 |  8003883.39 |
|      33265 |  8002391.76 |
|     181788 |  8002030.50 |
|     172799 |  8001050.55 |
|       2084 |  7999172.30 |
|     174747 |  7997167.48 |
|     171184 |  7996930.11 |
|     113271 |  7992683.04 |
|      68662 |  7991426.30 |
|     179375 |  7991170.88 |
|     188383 |  7990226.27 |
|      50208 |  7989363.27 |
|      23653 |  7988890.87 |
|     159419 |  7988841.36 |
|      74581 |  7987356.50 |
|     133590 |  7986046.81 |
|     195820 |  7985473.14 |
|      87903 |  7983482.88 |
|      69032 |  7981908.18 |
|     113975 |  7980561.00 |
|     178678 |  7975116.93 |
|      52316 |  7973618.16 |
|     135546 |  7972669.80 |
|      89425 |  7970077.44 |
|     115937 |  7966015.20 |
|     151483 |  7964850.88 |
|      73974 |  7964186.23 |
|      39976 |  7964104.24 |
|     130168 |  7961690.88 |
|      58973 |  7957416.76 |
|      16354 |  7956051.07 |
|      23988 |  7955837.92 |
|     138467 |  7955481.05 |
|      26096 |  7955212.32 |
|     192216 |  7953429.18 |
|     112833 |  7952279.26 |
|      60599 |  7951261.80 |
|     129116 |  7948811.85 |
|      79529 |  7947581.91 |
|      71616 |  7944476.54 |
|     136821 |  7942188.24 |
|     116204 |  7941096.90 |
|     165298 |  7939933.31 |
|      44009 |  7939859.65 |
|     194487 |  7938247.20 |
|      11299 |  7938135.81 |
|      76488 |  7935926.86 |
|      58998 |  7934414.04 |
|      25175 |  7931035.11 |
|     136144 |  7929283.23 |
|     132829 |  7926841.62 |
|      84176 |  7925781.05 |
|      68592 |  7922872.98 |
|     139280 |  7922119.48 |
|     160669 |  7921588.43 |
|      42938 |  7917524.56 |
|     183183 |  7915624.86 |
|      95449 |  7914292.08 |
|     115390 |  7912655.54 |
|     173723 |  7911329.40 |
|      48992 |  7911153.12 |
|     173464 |  7910458.65 |
|      26098 |  7910217.75 |
|     141115 |  7909496.38 |
|     195218 |  7906315.56 |
|     116608 |  7906302.60 |
|     163793 |  7905477.33 |
|      10419 |  7904598.30 |
|     106312 |  7901466.72 |
|      48674 |  7901010.24 |
|      35198 |  7899974.88 |
|      88954 |  7899573.52 |
|      41505 |  7897709.99 |
|     115586 |  7897301.88 |
|     167431 |  7895826.00 |
|     158787 |  7894948.50 |
|     161712 |  7893410.70 |
|      46930 |  7892707.77 |
|      58633 |  7892088.15 |
|      10599 |  7892067.69 |
|      99523 |  7891485.16 |
|      70126 |  7890247.41 |
|      32476 |  7890149.34 |
|     152617 |  7890136.50 |
|     162639 |  7889822.70 |
|      82056 |  7889345.05 |
|     186450 |  7887873.56 |
|      39082 |  7886019.89 |
|     183217 |  7885948.48 |
|     192551 |  7884432.48 |
|     164801 |  7882870.10 |
|     112804 |  7882772.00 |
|       5956 |  7878805.04 |
|      73054 |  7878479.63 |
|      62593 |  7878401.44 |
|     137687 |  7873755.91 |
|      80526 |  7871839.50 |
|     195354 |  7869617.75 |
|       4122 |  7867967.09 |
|       4057 |  7865176.80 |
|      63195 |  7864322.16 |
|     143370 |  7863444.54 |
|      41473 |  7862926.89 |
|     155060 |  7860900.96 |
|      76875 |  7858529.64 |
|     135778 |  7857660.51 |
|      30534 |  7855226.08 |
|      99405 |  7853410.95 |
|     161551 |  7852244.40 |
|     185034 |  7850752.00 |
|      17264 |  7850704.88 |
|      23652 |  7848909.16 |
|     123681 |  7848265.36 |
|     186170 |  7845527.50 |
|      81496 |  7840427.40 |
|      25407 |  7840234.72 |
|      96662 |  7839907.41 |
|     156407 |  7839647.75 |
|     165843 |  7839562.80 |
|     153361 |  7838813.07 |
|     149362 |  7838282.52 |
|      46057 |  7835709.81 |
|     114341 |  7835492.25 |
|     154823 |  7834898.61 |
|     139538 |  7834690.64 |
|      42853 |  7833252.60 |
|     177659 |  7831803.58 |
|      29158 |  7829880.80 |
|      85583 |  7825996.64 |
|     165714 |  7825006.46 |
|      58662 |  7821977.76 |
|     185839 |  7821640.74 |
|      93559 |  7821137.52 |
|      58481 |  7818648.16 |
|     162217 |  7817923.47 |
|     130014 |  7815929.34 |
|     125640 |  7815262.90 |
|      83723 |  7815021.48 |
|      54314 |  7813732.94 |
|     146652 |  7809817.39 |
|     189256 |  7808972.00 |
|      87994 |  7808660.48 |
|     157067 |  7806217.25 |
|      56859 |  7805947.60 |
|     118132 |  7804423.69 |
|     189457 |  7802777.91 |
|       1509 |  7802315.42 |
|     129101 |  7801994.70 |
|     162285 |  7801859.52 |
|     182358 |  7801430.46 |
|       6288 |  7800363.30 |
|      68972 |  7799224.95 |
|      51684 |  7795455.46 |
|     148645 |  7794585.92 |
|      94359 |  7794358.92 |
|      40451 |  7791437.70 |
|      44019 |  7790053.76 |
|      81470 |  7788716.85 |
|      12731 |  7786998.38 |
|     114393 |  7784963.34 |
|      69323 |  7783583.08 |
|     169794 |  7780968.30 |
|      25378 |  7778569.60 |
|     104509 |  7777137.62 |
|      81874 |  7775216.80 |
|      70859 |  7771185.07 |
|     135768 |  7769704.84 |
|     181960 |  7768847.90 |
|      28481 |  7768516.61 |
|     191604 |  7765367.68 |
|        754 |  7762507.02 |
|     127702 |  7761776.05 |
|      36488 |  7761744.00 |
|     183906 |  7759864.80 |
|      90365 |  7759602.50 |
|      60725 |  7759495.78 |
|      69436 |  7759033.52 |
|      12963 |  7756623.52 |
|      64571 |  7755731.04 |
|     160111 |  7753787.70 |
|     107970 |  7753735.88 |
|     132036 |  7753401.36 |
|      79965 |  7748656.15 |
|     149862 |  7747239.10 |
|      73218 |  7745499.42 |
|     161036 |  7742807.45 |
|     152467 |  7742471.40 |
|     163358 |  7742034.00 |
|     197951 |  7741768.84 |
|      15820 |  7740003.00 |
|      31444 |  7739519.60 |
|     151208 |  7738273.85 |
|      20410 |  7737192.99 |
|      45462 |  7736792.55 |
|     128966 |  7736467.65 |
|     118945 |  7735275.00 |
|     106458 |  7734069.72 |
|     162706 |  7730189.88 |
|      70528 |  7730088.25 |
|     107998 |  7728273.45 |
|     163110 |  7728042.40 |
|      74591 |  7727297.76 |
|     121454 |  7726200.56 |
|     181252 |  7724464.38 |
|      29154 |  7724129.66 |
|      63854 |  7720353.88 |
|      34157 |  7719803.30 |
|      30684 |  7718307.84 |
|       3985 |  7715042.96 |
|      29387 |  7714858.80 |
|     184703 |  7712545.12 |
|     124679 |  7712528.72 |
|      15606 |  7710658.46 |
|     123814 |  7709872.95 |
|      83760 |  7709633.92 |
|      22084 |  7707219.79 |
|     123210 |  7706030.42 |
|      75066 |  7704727.51 |
|      16337 |  7704517.80 |
|      47109 |  7704111.51 |
|       8232 |  7702887.50 |
|      11222 |  7702535.62 |
|      84961 |  7701923.72 |
|     157118 |  7700132.88 |
|     118362 |  7699210.20 |
|     193755 |  7698545.20 |
|       1520 |  7697759.37 |
|     114599 |  7697377.50 |
|     168842 |  7696152.00 |
|     172245 |  7694286.06 |
|       4584 |  7693352.79 |
|     113651 |  7689659.67 |
|     183207 |  7687955.66 |
|     175802 |  7686604.70 |
|      59066 |  7685120.43 |
|     130726 |  7684159.25 |
|      89672 |  7684049.50 |
|       7224 |  7683446.40 |
|      97533 |  7680694.62 |
|      59941 |  7680100.80 |
|      29298 |  7676823.42 |
|     163962 |  7675924.96 |
|      41086 |  7674518.14 |
|     185483 |  7673376.60 |
|     165010 |  7672469.70 |
|       3708 |  7671744.18 |
|     192994 |  7671712.00 |
|      79968 |  7668060.48 |
|     118494 |  7666659.00 |
|      59236 |  7666625.98 |
|     149509 |  7665930.67 |
|       3793 |  7664981.28 |
|      28979 |  7664632.93 |
|     178389 |  7662544.96 |
|      65315 |  7661085.88 |
|      59710 |  7657442.00 |
|     170276 |  7656813.89 |
|     182707 |  7656387.06 |
|     129170 |  7655820.48 |
|      59765 |  7655009.92 |
|      23337 |  7654271.94 |
|      90396 |  7653568.35 |
|      68842 |  7652742.72 |
|      16315 |  7652630.70 |
|        956 |  7652174.81 |
|      10639 |  7651375.80 |
|     112886 |  7649534.08 |
|       9561 |  7648502.73 |
|      65484 |  7647789.30 |
|      68677 |  7646879.14 |
|     196529 |  7645482.24 |
|       6556 |  7642116.06 |
|       9113 |  7640163.68 |
|     128139 |  7638760.00 |
|     143264 |  7635499.56 |
|      21569 |  7634785.86 |
|     193402 |  7633576.06 |
|      35545 |  7632210.69 |
|      65068 |  7632188.76 |
|      25515 |  7630952.93 |
|     180189 |  7630887.10 |
|     131680 |  7629593.64 |
|      80162 |  7629440.93 |
|     139054 |  7629417.37 |
|       8028 |  7629134.04 |
|      76804 |  7626731.00 |
|      74179 |  7624974.03 |
|     122507 |  7623903.87 |
|     141889 |  7623552.30 |
|     184279 |  7623048.17 |
|       8076 |  7620897.81 |
|     192681 |  7619802.09 |
|      21398 |  7617942.52 |
|      14825 |  7617843.60 |
|      17969 |  7617524.64 |
|     170764 |  7616119.96 |
|     115303 |  7615914.17 |
|      67708 |  7615306.08 |
|      33317 |  7613417.24 |
|     190782 |  7613203.42 |
|     113818 |  7612852.48 |
|     178091 |  7611457.30 |
|      87603 |  7611343.68 |
|     108317 |  7610509.71 |
|     106552 |  7609868.84 |
|      28679 |  7609292.20 |
|     192350 |  7609140.81 |
|     154801 |  7607944.38 |
|       5768 |  7607785.68 |
|     127689 |  7606313.94 |
|      62847 |  7605651.45 |
|     111212 |  7605052.00 |
|     156065 |  7603327.60 |
|     115140 |  7601161.68 |
|      19597 |  7601153.46 |
|      55233 |  7600940.23 |
|      89353 |  7600929.84 |
|      75701 |  7600492.60 |
|      64974 |  7599754.80 |
|     116156 |  7597452.48 |
|      59491 |  7596352.84 |
|       6138 |  7594861.54 |
|      62317 |  7594854.10 |
|     106575 |  7594520.08 |
|     161092 |  7594454.40 |
|       9872 |  7593734.34 |
|      77711 |  7593431.60 |
|      61206 |  7593153.00 |
|     123776 |  7592736.80 |
|     185141 |  7592617.12 |
|       5542 |  7592513.04 |
|     185296 |  7591439.31 |
|      72597 |  7591142.40 |
+------------+-------------+
1225 rows in set

Q12
+------------+-----------------+----------------+
| l_shipmode | high_line_count | low_line_count |
+------------+-----------------+----------------+
| FOB        |            6273 |           9429 |
| TRUCK      |            6336 |           9300 |
+------------+-----------------+----------------+

Q13
+---------+----------+
| c_count | custdist |
+---------+----------+
|       0 |    50005 |
|      10 |     6574 |
|       9 |     6554 |
|      11 |     6072 |
|       8 |     5934 |
|      12 |     5598 |
|      13 |     5032 |
|      19 |     4685 |
|       7 |     4663 |
|      20 |     4607 |
|      17 |     4550 |
|      18 |     4515 |
|      14 |     4480 |
|      15 |     4476 |
|      16 |     4341 |
|      21 |     4176 |
|      22 |     3710 |
|       6 |     3303 |
|      23 |     3172 |
|      24 |     2670 |
|      25 |     2111 |
|       5 |     1954 |
|      26 |     1605 |
|      27 |     1195 |
|       4 |     1030 |
|      28 |      898 |
|      29 |      620 |
|       3 |      408 |
|      30 |      353 |
|      31 |      225 |
|      32 |      135 |
|       2 |      128 |
|      33 |       82 |
|      34 |       54 |
|      35 |       33 |
|       1 |       18 |
|      36 |       17 |
|      37 |        7 |
|      41 |        3 |
|      40 |        3 |
|      38 |        3 |
|      39 |        1 |
+---------+----------+
42 rows in set


Q14
+-------------------+
| promo_revenue     |
+-------------------+
| 16.65118731292792 |
+-------------------+

Q15
+-----------+--------------------+----------------------------------+-----------------+---------------+
| s_suppkey | s_name             | s_address                        | s_phone         | total_revenue |
+-----------+--------------------+----------------------------------+-----------------+---------------+
|      7895 | Supplier#000007895 | NYl,i8UhxTykLxGJ2voIRn20Ugk1KTzz | 14-559-808-3306 |  1678635.2636 |
+-----------+--------------------+----------------------------------+-----------------+---------------+


Q16
+----------+---------------------------+--------+--------------+
| p_brand  | p_type                    | p_size | supplier_cnt |
+----------+---------------------------+--------+--------------+
| Brand#55 | LARGE BURNISHED TIN       |     21 |           36 |
| Brand#25 | PROMO BRUSHED STEEL       |     24 |           28 |
| Brand#54 | STANDARD BRUSHED COPPER   |     14 |           27 |
| Brand#12 | MEDIUM PLATED BRASS       |     21 |           24 |
| Brand#14 | ECONOMY PLATED TIN        |     33 |           24 |
| Brand#24 | ECONOMY PLATED TIN        |     33 |           24 |
| Brand#25 | MEDIUM PLATED STEEL       |     35 |           24 |
| Brand#32 | MEDIUM POLISHED COPPER    |     20 |           24 |
| Brand#32 | SMALL ANODIZED BRASS      |      7 |           24 |
| Brand#33 | ECONOMY PLATED STEEL      |      7 |           24 |
| Brand#33 | MEDIUM PLATED COPPER      |     20 |           24 |
| Brand#33 | PROMO POLISHED STEEL      |     14 |           24 |
...
| Brand#31 | PROMO ANODIZED COPPER     |     20 |            3 |
| Brand#41 | LARGE BURNISHED STEEL     |     20 |            3 |
| Brand#43 | SMALL BRUSHED COPPER      |      7 |            3 |
| Brand#52 | MEDIUM POLISHED BRASS     |     21 |            3 |
| Brand#52 | SMALL POLISHED TIN        |      2 |            3 |
+----------+---------------------------+--------+--------------+
18341 rows in set

Q17
+-------------------+
| avg_yearly        |
+-------------------+
| 348406.0542857143 |
+-------------------+

Q18
+--------------------+-----------+------------+-------------+--------------+-----------------+
| c_name             | c_custkey | o_orderkey | o_orderdate | o_totalprice | sum(l_quantity) |
+--------------------+-----------+------------+-------------+--------------+-----------------+
| Customer#000128120 |    128120 |    4722021 | 1994-04-07  |    544089.09 |          323.00 |
| Customer#000144617 |    144617 |    3043270 | 1997-02-12  |    530604.44 |          317.00 |
| Customer#000066790 |     66790 |    2199712 | 1996-09-30  |    515531.82 |          327.00 |
| Customer#000015619 |     15619 |    3767271 | 1996-08-07  |    480083.96 |          318.00 |
| Customer#000147197 |    147197 |    1263015 | 1997-02-02  |    467149.67 |          320.00 |
| Customer#000117919 |    117919 |    2869152 | 1996-06-20  |    456815.92 |          317.00 |
| Customer#000126865 |    126865 |    4702759 | 1994-11-07  |    447606.65 |          320.00 |
| Customer#000036619 |     36619 |    4806726 | 1995-01-17  |    446704.09 |          328.00 |
| Customer#000119989 |    119989 |    1544643 | 1997-09-20  |    434568.25 |          320.00 |
+--------------------+-----------+------------+-------------+--------------+-----------------+
9 rows in set

Q19
+--------------+
| revenue      |
+--------------+
| 3083843.0578 |
+--------------+

Q20
+--------------------+------------------------------------------+
| s_name             | s_address                                |
+--------------------+------------------------------------------+
| Supplier#000000035 | QymmGXxjVVQ5OuABCXVVsu,4eF gU0Qc6        |
| Supplier#000000068 | Ue6N50wH2CwE4PPgTGLmat,ibGYYlDoOb3xQwtgb |
| Supplier#000000080 | cJ2MHSEJ13rIL2Wj3D5i6hRo30,ZiNUXhqn      |
| Supplier#000000100 | rIlN li8zvW22l2slbcx ECP4fL              |
| Supplier#000000274 | usxbl9KSW41DTE6FAglxHU                   |
| Supplier#000000406 | zMhU58CDF4aHTeodxg9IgRZgq                |
| Supplier#000000444 | mHr2VcUpRkvyQ9rjKMaPkeWbVZmEIhxhb8F      |
| Supplier#000000453 | bpt98PxU5HSQt61bVB695JPjBmJKUv hNzQeHvC  |
| Supplier#000000458 | IFNkUK1H53HwUHabiONkMFAUDb               |
| Supplier#000000622 | gCQimU1jYHoQiglDmW1FkQM9wzi YC1P15pMy1   |
| Supplier#000000713 | DBMIf1HiYY8OyRFcbtHpKIz                  |
| Supplier#000000767 | bHEuqKKdmCMEKOV                          |
| Supplier#000000776 | nklfFoSkCwf,ooSuF                        |
| Supplier#000000823 |  gC0DrEG5U,v893fp3nj mmXa6rYhJ0tjpJ      |
| Supplier#000000828 | 0B2aPqJ6KTEr2fqxuC7z                     |
| Supplier#000000941 | gqG2XEnVlzUhjjfQGYGlwk,jcaNsplI8Rleg     |
| Supplier#000000973 | 5 nhBZ 03rG6EcOEDkZXvt                   |
| Supplier#000000984 | 6H6qqye iYbYzCmwWhj                      |
| Supplier#000001149 | Nuno37wiZOjNGHF                          |
| Supplier#000001201 | Seh4D7pi9UdK,XQkF46A0O2N                 |
| Supplier#000001309 | 72RNUzKzbniUnnsSs24ZzGDvmcv2Pd           |
| Supplier#000001344 | 6iF,zVDNTykohVKcb7FKvn82s74ez            |
| Supplier#000001351 | zXdoBMmmRx1wOD7GKoHHBtemXGuYKLDb,U2KP    |
| Supplier#000001391 | hkWoAM561QlLjBNk,SdFdIgFx                |
| Supplier#000001481 | ARqVvJHMxBNKl2LrfPsR  Wq9ZUXh,14         |
| Supplier#000001584 | gJbTkijteJxSMLmdzBSzeMAH                 |
| Supplier#000001651 | 6rJNoWL9YL                               |
| Supplier#000001710 | J,sdOOJwUhwPv2mrEiNEA0UZlmu5IRmgz        |
| Supplier#000001755 | QstBVfnY,93NsbWXCqO                      |
| Supplier#000001869 | nogoCdaFQii,ri9rs3P8f5rPt1wVOMw9I7TmypxK |
| Supplier#000001895 | lywAGDbk37fYPDS                          |
| Supplier#000001910 | vih,zrhclXX1O9x                          |
| Supplier#000001930 | 2jCSw3KOLHol7y5omVO13                    |
| Supplier#000001979 | UNW7nA,IC 5igvVsgUHA7OaLL,jOzUcT         |
| Supplier#000002046 | BiTDgHknmvQGT6FpZXfRX,xlnR               |
| Supplier#000002071 | zLH3QAtZuuOq8AoVNM                       |
| Supplier#000002270 | HIscbvhw8N94djn,3UbPaY4R                 |
| Supplier#000002350 | TWsO2iJGOl7v3vSwiscXp6X                  |
| Supplier#000002409 | oy39SaSQ,FIP pzLqblhxj                   |
| Supplier#000002520 | 5y55UzYQKByZP3                           |
| Supplier#000002618 | 3UtbE1kKm29kKyx09hSEBMhRLM               |
| Supplier#000002643 | eDN6YjGtp2dcj0IF,BKEEYjElO,sUjjcNI       |
| Supplier#000002649 | agDQi9iCt1cUaS                           |
| Supplier#000002655 | i6v8dkQBuK0NSCeqQCE8                     |
| Supplier#000002812 | Q9sO3wZkBU5QBe0VITRWShv                  |
| Supplier#000002888 | 3AtRoxBFh6HIBa9kdBX,6,Ml2SZGUA           |
| Supplier#000002910 | nlH1gjApxHkQe5SU4iVZwi2xWk88wwhTWRkSvOBB |
| Supplier#000002914 | fUC4IkGB8pt1S                            |
| Supplier#000003000 | JtDvRf4iWHJkj54PYxl                      |
| Supplier#000003011 | vfL mV0MTdyozfRIPZkJbM1Z7Lcm2NCPIj6qSgBz |
| Supplier#000003038 | F5Tz7P juuCbABDuW8JGomRFxqVHBWyQrsLwg4i  |
| Supplier#000003150 | XwSjsmzEnANK,wAQUp4Xf5xJDqR              |
| Supplier#000003305 | GLZJimfuzKoQcqcv4                        |
| Supplier#000003394 | R6D7n3WrQjWNGSQTb7eN ,X0oCMkhyuTHBOSPw   |
| Supplier#000003452 | 7tMycIKhE,pe4OL3Du                       |
| Supplier#000003666 | ENS fE9iSrSzw,iTwA,zGorkflw              |
| Supplier#000003698 | lnSEu64ca4B53BfznJPg                     |
| Supplier#000003773 | UWjSotAjkAD                              |
| Supplier#000003837 | SYXpXaKop3                               |
| Supplier#000003846 | wl076KfcEpYLRegb1LfIf93b3n5HBabFK2R,mEM  |
| Supplier#000003862 | 0XXFhF1IDBh                              |
| Supplier#000003868 | 5aP4VBn0t666NbGYB                        |
| Supplier#000003880 | DZo80mSznrhCpb8                          |
| Supplier#000003955 | piECPB8qbn7s3XP                          |
| Supplier#000004007 | cvlSgCCKGOwpaB iFIPx4vU2qA5b6K hz9Z91    |
| Supplier#000004066 | TNBnJFDScUmsjBy6pSWTS sfMg9jpfKx         |
| Supplier#000004127 | EduKm3NcCc75Cd                           |
| Supplier#000004174 | Bk97olQYwXmjYdQjwyt N                    |
| Supplier#000004328 | euddbWZRcVMD3W                           |
| Supplier#000004341 | ea8KZYvO7amq8A                           |
| Supplier#000004360 | w 7kM5J,fqjiqBu4SU0UPEDqspaUEm           |
| Supplier#000004375 | Cmr952zcJJuW0xAYc0W0MA7N6vMcCjy          |
| Supplier#000004391 | pcsiJBhSEHuFHNAxR3K c                    |
| Supplier#000004398 | khZZ0CmLip49Zncec                        |
| Supplier#000004402 | acagGfDWzwmS,,WVBsszubFs3LOA8rDRS0I      |
| Supplier#000004714 | IKRla2xArMmR4p3Mbn8JV8g0                 |
| Supplier#000004717 | H,Suh5pN230Ol,ggx0QEh3rrvzyQsq050Lat     |
| Supplier#000004740 | yM0TXkhfjpObafbQhuWU                     |
| Supplier#000004763 | W 7kS9LLh4ZgLpk2                         |
| Supplier#000004837 | tYHMZS4XlJjzvj34mH2PCoj                  |
| Supplier#000004882 | e,V Bo1KZEt                              |
| Supplier#000004913 | em,yC41xEl Fst9LwEik                     |
| Supplier#000005005 | K6 GI4WzmbsGEOh                          |
| Supplier#000005238 | jmtI76 8RNG8Z2BZu                        |
| Supplier#000005289 | 62XeOur9SnXgbdjGwb9E1aJIEBr5PA9          |
| Supplier#000005317 | lPOPHufNjwZaUJGVNHCC2DE FYQcKZBzHltL5    |
| Supplier#000005401 | eEOlCEAaIfVexStlrgTuzwQx7vjPF6ZT dm      |
| Supplier#000005449 | fhc8lUuZdqWUujcVaWogowEq1WVL9Y8m1efwCl3G |
| Supplier#000005472 | LlyLSmvY9GFvMN4QhHzMokW0k5d              |
| Supplier#000005572 | o0VYozeSbEyqck                           |
| Supplier#000005579 | ACVEMP4IwRf                              |
| Supplier#000005661 | pq5wuxmkIW0DyWU                          |
| Supplier#000005676 | HInJHZisl5svSU1oKsr                      |
| Supplier#000005815 | S6cu6cspYxHlTz2                          |
| Supplier#000005835 | rYoXzV3EZ77Z                             |
| Supplier#000006103 | l32l8iaPdbHgRXoq,kdjFAj3hZk2d            |
| Supplier#000006173 | hBdratcVfL4LpWxsEpCRP g0AksN0CDhBZ       |
| Supplier#000006226 | CKuDyeGAxPHeRHwC4a                       |
| Supplier#000006254 | g7OY1vWNUb1vxIRgEl                       |
| Supplier#000006348 | f2KDn2rLnadX8I DZR                       |
| Supplier#000006359 | QyUuVHYBp8sTd7Y9WveNfsz                  |
| Supplier#000006430 | F2RrkeaNcs6po8x2PyYvcPa1rtKd,fT2AMxP     |
| Supplier#000006516 | 89XwFOC,hLRxGq5rL0txv0EM9F               |
| Supplier#000006700 | BWjerJH5kbEPu 8h9                        |
| Supplier#000006785 | lyo6PpwulTeN9ZfIkvWag5NucL,XMC  89Kn7U   |
| Supplier#000006998 | r2i3HfkSQh9dvho, NpoabdMsPBG             |
| Supplier#000007019 | 2GQsALzRiTt2BQum6bocdeGawkOrsjNIZ        |
| Supplier#000007114 | s9s4YLeLWo7fLRO3rdQKFfUnZhrZUPjOC        |
| Supplier#000007170 | 9vABqu hZaciXSCQrbTj                     |
| Supplier#000007171 | DXerxFIhNRpqF9dWNRw hDOlLX gEJFxh0       |
| Supplier#000007213 | 2Nrby3JJHDJyWwVNiqPtm2U JGWlZpU          |
| Supplier#000007219 | p5Ui3IGPcmotYu                           |
| Supplier#000007229 | iwNoWdaURFzLAsQHxK,BeOPpI5TOTo           |
| Supplier#000007263 | malQPdYc8xiup2MiFuKHa                    |
| Supplier#000007270 | TksERECGdYZRPUjkUdDRZv5pW26cOTaA1        |
| Supplier#000007276 | Vi9,aBg2ychZf                            |
| Supplier#000007334 | NPXYWdJ8L9EDr20tw9CZQsEMqXlgXzI2JC Y     |
| Supplier#000007400 | 7r9zZj8J,,hN2GRfWtDxzuGa                 |
| Supplier#000007442 | DzycM1,T6kh2EutfPeFpv0Ro                 |
| Supplier#000007456 | ITYEeccPVJi0HvnAwVs2Z                    |
| Supplier#000007559 | Wmzx1vskciC                              |
| Supplier#000007677 | OoTYQdxQyd7NukSaSRv                      |
| Supplier#000007712 | DyTQD 3ajuOtHQTpI4LsWSF kSd2SE6U4COgYHQ  |
| Supplier#000007715 | gZHd7Yzbtv7yb7DYCCAQPJH8FRHTqi6T4w       |
| Supplier#000007816 | 1ejcJ545bwLWLuY6Qq4qyEExZIsp0SG          |
| Supplier#000007845 | agwGVTzLyRKOsZxLVi,mPWZ08Qxb             |
| Supplier#000007875 | E0CkoBYngcIoH                            |
| Supplier#000007908 | ghhHapj7GK                               |
| Supplier#000007972 | WW0GuiWP2N3kUo4f                         |
| Supplier#000008162 | XASpbn08mRV0kgHRmUSKx                    |
| Supplier#000008235 | TjVWq6bTdGJB                             |
| Supplier#000008249 | PwUjvlMk y72zaMRtZQ8trbCmu4j             |
| Supplier#000008309 | 6P,FQbW6sJouqunvttVO6vEeY                |
| Supplier#000008339 | uWw8 P6u,S                               |
| Supplier#000008343 |  BbHngAVqj0J8                            |
| Supplier#000008349 | 8Hkx1IDd0mZCTX                           |
| Supplier#000008377 | ,Yk0mflw2LqQCTxMYR sU2juj5DorUAG4w6i     |
| Supplier#000008468 | 5R4jsweitleustYlE3w,u5otW                |
| Supplier#000008523 | C4ocdfNu5I2nnnVG2xSd3016J6KNLIg          |
| Supplier#000008580 | t5ri71bM6Sox3riP4JUZsMMNC                |
| Supplier#000008638 | yxj50B 8aMql                             |
| Supplier#000008642 | qnN9N9du9Dg2arf6kjD xW0DjMT9cM           |
| Supplier#000008651 | pfw32RGA7BPXrUiavYqE                     |
| Supplier#000008679 | JWFVoSsCwn9p8o                           |
| Supplier#000008704 | a6DjHp0B6mifKBtqUk,C                     |
| Supplier#000008737 | MsdGxF9Xoq9 8s                           |
| Supplier#000008820 | uAsBvPBNsEsO                             |
| Supplier#000008829 | lNcY7xNLDonCw TuRYL                      |
| Supplier#000008947 | 1Ij3T0egGHnVbLich98HzY,UeCdVbxzYa ZpKDVc |
| Supplier#000008964 | U2YJW,Y1xCbUWbjuovtzsLfsl                |
| Supplier#000008974 | 4JCXOJ3MyPfa51mIf,MQu                    |
| Supplier#000008997 | KY MmMEcyQ6FEDCooFj xa uCwF2GbaeA8       |
| Supplier#000009065 | ZELuiqWrWbJV9zAuco1OnXKTJClhR            |
| Supplier#000009114 | nkn6bcPvlP5w,lUpO0nZTBSj                 |
| Supplier#000009125 | IQbCXbN1mmght                            |
| Supplier#000009131 | gDBXgWtg4rTxu0WUJhhV                     |
| Supplier#000009149 | yKX,bKryD6YtvF,cVLIKC0Z6rN               |
| Supplier#000009182 | z56kNgeqaWQ1kHFBp                        |
| Supplier#000009220 | N4y,vP kdArpcmdypBh,fJVVB                |
| Supplier#000009226 | yzT10vNTFJ                               |
| Supplier#000009288 |  251AA4ziZ3d7TTWXLGnXjb4BnXv             |
| Supplier#000009360 | 1NVjjX8zMjyBX2UapDTP0Sz                  |
| Supplier#000009381 | rhCTm7QehIznqd8 Np7VT,H5J5zSGr           |
| Supplier#000009403 | 70841REghyWBrHyyg762Jh4sjCG7CKaIc        |
| Supplier#000009504 | Rqt07,ANI92kj1oU                         |
| Supplier#000009598 | PnTAz7rNRLVDFO3zoo2QRTlh4o               |
| Supplier#000009609 | LV2rJUGfr0k3dPNRqufG1IoYHzV              |
| Supplier#000009619 | K0RwcJ9S75Xil jqKukFoDNkD                |
| Supplier#000009626 | Nm1FnIh4asUR3EnXv2Pvy3gXqI9es            |
| Supplier#000009738 | 15RRSVTuOzwdMP LmfCtIguMGXK              |
| Supplier#000009770 | Ag, SZfowit580QPDdbP8kmFHdpZ9ASI         |
| Supplier#000009865 | extcOh9ZrdDCMsHhhsFTkTUAh,HM2UQ2qa8sRo   |
| Supplier#000009866 | Auh6aZnOnQG1pPYKZ5o9ATramJBA             |
| Supplier#000009890 | izJXemCM Ikpgxk                          |
| Supplier#000009937 | edZ9HQJ0KJAU6EWknTiDghKfRLHq6vtFqdey,0l  |
| Supplier#000009954 | VzElx9ihlXFJLIQw2Hn4bC2                  |
| Supplier#000009958 | ggiiSA4CSyvhwQUYjdJhWlKEY9PAfs           |
+--------------------+------------------------------------------+
177 rows in set


Q21
+--------------------+---------+
| s_name             | numwait |
+--------------------+---------+
| Supplier#000009302 |      21 |
| Supplier#000000342 |      20 |
| Supplier#000000632 |      19 |
| Supplier#000002196 |      19 |
| Supplier#000003325 |      18 |
| Supplier#000003915 |      18 |
| Supplier#000005045 |      18 |
| Supplier#000006442 |      18 |
| Supplier#000003093 |      17 |
| Supplier#000004498 |      17 |
| Supplier#000000906 |      16 |
| Supplier#000001183 |      16 |
| Supplier#000001477 |      16 |
| Supplier#000006043 |      16 |
| Supplier#000000689 |      15 |
| Supplier#000001955 |      15 |
| Supplier#000002066 |      15 |
| Supplier#000002146 |      15 |
| Supplier#000003253 |      15 |
| Supplier#000003527 |      15 |
| Supplier#000003947 |      15 |
| Supplier#000004915 |      15 |
| Supplier#000005248 |      15 |
| Supplier#000006718 |      15 |
| Supplier#000007773 |      15 |
| Supplier#000008121 |      15 |
| Supplier#000008169 |      15 |
| Supplier#000008645 |      15 |
| Supplier#000008684 |      15 |
| Supplier#000009079 |      15 |
| Supplier#000009956 |      15 |
| Supplier#000000737 |      14 |
| Supplier#000000775 |      14 |
| Supplier#000001474 |      14 |
| Supplier#000001502 |      14 |
| Supplier#000003196 |      14 |
| Supplier#000004415 |      14 |
| Supplier#000004940 |      14 |
| Supplier#000005253 |      14 |
| Supplier#000005703 |      14 |
| Supplier#000006308 |      14 |
| Supplier#000006789 |      14 |
| Supplier#000007161 |      14 |
| Supplier#000007952 |      14 |
| Supplier#000008062 |      14 |
| Supplier#000008414 |      14 |
| Supplier#000008442 |      14 |
| Supplier#000008508 |      14 |
| Supplier#000000300 |      13 |
| Supplier#000000727 |      13 |
| Supplier#000000921 |      13 |
| Supplier#000000992 |      13 |
| Supplier#000001282 |      13 |
| Supplier#000001582 |      13 |
| Supplier#000001662 |      13 |
| Supplier#000001683 |      13 |
| Supplier#000002933 |      13 |
| Supplier#000003177 |      13 |
| Supplier#000003428 |      13 |
| Supplier#000003640 |      13 |
| Supplier#000004842 |      13 |
| Supplier#000004951 |      13 |
| Supplier#000005795 |      13 |
| Supplier#000005981 |      13 |
| Supplier#000006118 |      13 |
| Supplier#000006433 |      13 |
| Supplier#000006484 |      13 |
| Supplier#000007268 |      13 |
| Supplier#000008599 |      13 |
| Supplier#000008675 |      13 |
| Supplier#000009474 |      13 |
| Supplier#000009521 |      13 |
| Supplier#000009853 |      13 |
| Supplier#000000021 |      12 |
| Supplier#000000211 |      12 |
| Supplier#000000743 |      12 |
| Supplier#000000951 |      12 |
| Supplier#000001654 |      12 |
| Supplier#000001868 |      12 |
| Supplier#000002089 |      12 |
| Supplier#000002879 |      12 |
| Supplier#000003060 |      12 |
| Supplier#000003215 |      12 |
| Supplier#000003365 |      12 |
| Supplier#000003873 |      12 |
| Supplier#000003985 |      12 |
| Supplier#000004452 |      12 |
| Supplier#000004639 |      12 |
| Supplier#000005122 |      12 |
| Supplier#000005633 |      12 |
| Supplier#000005671 |      12 |
| Supplier#000005782 |      12 |
| Supplier#000006088 |      12 |
| Supplier#000006477 |      12 |
| Supplier#000006508 |      12 |
| Supplier#000006750 |      12 |
| Supplier#000006802 |      12 |
| Supplier#000008236 |      12 |
| Supplier#000009294 |      12 |
| Supplier#000009329 |      12 |
+--------------------+---------+
100 rows in set

Q22
+-----------+---------+------------+
| cntrycode | numcust | totacctbal |
+-----------+---------+------------+
| 10        |     882 | 6606081.31 |
| 11        |     899 | 6702253.34 |
| 19        |     963 | 7230776.82 |
| 20        |     916 | 6824676.02 |
| 22        |     894 | 6636740.03 |
| 26        |     861 | 6404695.86 |
| 27        |     877 | 6565078.99 |
+-----------+---------+------------+
7 rows in set

```
