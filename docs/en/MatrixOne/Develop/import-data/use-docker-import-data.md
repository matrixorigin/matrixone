# Import data when launching MatrixOne using Docker

This document will guide you on how to import data when launching MatrixOne using Docker.

## Before you start

- Make sure you have already [installed MatrixOne using Docker](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#method-3-using-docker).
- Make sure you have already [connected MatrixOne Server](../../Get-Started/connect-to-matrixone-server.md).

## Steps

1. Mount the data directory to the container directory.

2. Use the `source` command or the `load data` command to import data.

For more information on the details of steps, see **Practice Example** as below.

## Practice Example

- **Applicable scenarios**: Launch MatrixOne using docker, and a large number of datasets need to be imported into the MatrixOne.
- **Scenario description**: We have also prepared a 1GB dataset for downloading.  You can get the data files directly.

1. Download the dataset file and store the data in *~/tmp/docker_loaddata_demo/*:

```
cd ~/tmp/docker_loaddata_demo/
wget https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/lineorder_flat.tar.bz2
```

2. Unzip the dataset:

```
tar -jxvf lineorder_flat.tar.bz2
```

3. Use Docker to launch MatrixOne, and mount the directory *~/tmp/docker_loaddata_demo/* that stores data files to a directory in the container. The container directory is */sb-dbgen-path* as an example:

```
docker run -d -p 6001:6001 -v ~/tmp/docker_loaddata_demo:/ssb-dbgen-path:rw --name matrixone matrixorigin/matrixone:0.5.1
```

4. Connect to MatrixOne server:

```
mysql -h 127.0.0.1 -P 6001 -udump -p111
```

5. Create *lineorder_flat* tables in MatrixOne, and import the dataset into MatriOne:

```
create database if not exists ssb;
use ssb;
drop table if exists lineorder_flat;
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
load data infile '/ssb-dbgen-path/lineorder_flat.tbl' into table lineorder_flat FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

5. After the import is successful, you can run SQL statements to check the amount of imported data:

```
select count(*) from lineorder_flat;
/*
    expected results:
 */
+----------+
| count(*) |
+----------+
| 10272594 |
+----------+
```
