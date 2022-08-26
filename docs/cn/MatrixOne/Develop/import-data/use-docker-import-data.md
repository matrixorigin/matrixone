# Docker 启动 MatrixOne 时导入数据

本篇文档将指导你在 Docker 启动 MatrixOne 时如何完成数据导入。

## 开始前准备

- 已通过[Docker](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#3-docker)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

## 步骤

基本步骤：

1. 将数据目录挂载到容器目录下。
2. 使用命令行（`source` 命令或者 `load data` 命令）将数据导入。

具体操作可以参见下述 **实践示例**。

## 实践示例

- **适用场景**： 使用 Docker 启动了 MatrixOne 服务，并且有大量数据集需要导入到 MatrixOne 数据库中。
- **场景描述**：本示例为你准备了 1GB 的数据集可直接下载使用，完成导入数据的步骤。

1. 下载数据集文件，并将数据存放在目录 *~/tmp/docker_loaddata_demo/* 下：

```
cd ~/tmp/docker_loaddata_demo/
wget https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/lineorder_flat.tar.bz2
```

2. 解压数据集：

```
tar -jxvf lineorder_flat.tar.bz2
```

3. 使用 Docker 启动 MatrixOne，启动时将存放了数据文件的目录 *~/tmp/docker_loaddata_demo/* 挂载到容器的某个目录下，这里容器目录以 */ssb-dbgen-path* 为例：

```
docker run -d -p 6001:6001 -v ~/tmp/docker_loaddata_demo:/ssb-dbgen-path:rw --name matrixone matrixorigin/matrixone:0.5.1
```

4. 连接 MatrixOne 服务：

```
mysql -h 127.0.0.1 -P 6001 -udump -p111
```

5. 创建测试数据库和 *lineorder_flat* 表，并导入数据集：

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

5. 导入成功后可执行 SQL 语句检查导入数据量：

```
select count(*) from lineorder_flat;
/*
    执行结果如下：
 */
+----------+
| count(*) |
+----------+
| 10272594 |
+----------+
```
