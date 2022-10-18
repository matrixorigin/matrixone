# 导入数据

本篇文档将指导你在 **MySQL 客户端启动 MatrixOne 时如何完成数据导入**。

## 开始前准备

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

如果你使用 Docker 安装启动 MatrixOne，确保你已将数据文件目录挂载到容器目录下，示例如下：

```
docker run -d -p 6001:6001 -v ~/tmp/docker_loaddata_demo:/ssb-dbgen-path:rw --name matrixone matrixorigin/matrixone:0.5.1
```

上述示例为典型的安装和挂载方式，将其本地路径 *~/tmp/docker_loaddata_demo* 挂载到内部容器路径 */ssb-dbgen-path*。

## 方式一：MySQL Client 中使用 `source` 命令，将 SQL 脚本导入 MatrixOne 中

您可以使用 `source` 命令或 `\.` 命令来执行 SQL 脚本文件。

```
mysql> source file_name
mysql> \. file_name
```

通常 `source` 命令可以被用来执行很多 SQL 语句，你可以在你的数据库写入或创建表，插入数据到一个 SQL 文件中，并在 MySQL 客户端中使用 `source` 命令执行这个文件。每个 SQL 语句都是单独的一行，以 `--` 开头或是以 `/*` 换行的语句皆为注释，在执行 SQL 的过程中将被忽略。

如果你的 SQL 来自 `mysqldump`，参考[教程](../../Migrate/migrate-from-mysql-to-matrixone.md)，了解如何修改 SQL 文件以适应 MatrixOne 格式。

## 方式二：MySQL Client 中使用 `Load data` 命令导入数据

在 MySQL Client 中使用 `Load data` 命令将外部大数据文件导入，但目前只支持 *.csv* 格式的文件导入。

1. 在 MatrixOne 中执行 `Load Data` 之前，需要提前在 MatrixOne 中创建完成数据表。目前，数据文件需要与 MatrixOne 服务器在同一台机器上，如果它们在不同的机器上，则需要进行文件传输。

    !!! note
        如果你使用 Docker 启动 MatrixOne，需要先将数据文件挂载到容器的目录中。

2. 在 MatrixOne 本地服务器中启动 MySQL 客户端以访问本地文件系统。

    ```
    mysql -h 127.0.0.1 -P 6001 -udump -p111
    ```

3. 在 MySQL 客户端对对应的文件路径执行 `LOAD DATA`：

    ```
    mysql> LOAD DATA INFILE '/tmp/xxx.csv'
    INTO TABLE xxxxxx
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY "\r\n";
    ```

    !!! note
        如果你使用 Docker 启动 MatrixOne，需要先将这个数据文件路径挂载到容器的目录中。

### 示例：使用 *docker* 执行 `Load data`

本示例将介绍如何使用 Docker 启动 MatrixOne 0.5.1 加载数据。

1. 下载数据集，并且将数据集存储到本地 *~/tmp/docker_loaddata_demo/* 路径下：

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

5. 在 MatrixOne 中新建表 *lineorder_flat*，并且将数据集导入至 MatriOne：

    ```
    mysql> create database if not exists ssb;
    mysql> use ssb;
    mysql> drop table if exists lineorder_flat;
    mysql> CREATE TABLE lineorder_flat(
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
    mysql> load data infile '/ssb-dbgen-path/lineorder_flat.tbl' into table lineorder_flat FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
    ```

6. 导入成功后，可以使用 SQL 语句查看导入数据的行数:

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
