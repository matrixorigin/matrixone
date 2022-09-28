# Import data

This document will guide you on how to import large amounts of data to MatrixOne.

## Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).
- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/). 

If you use the `docker` install, please make sure that you have a data directory mounted to the container. For example,

```T
docker run -d -p 6001:6001 -v ~/tmp/docker_loaddata_demo:/ssb-dbgen-path:rw --name matrixone matrixorigin/matrixone:0.5.1
```

This typical installation maps its local path `~/tmp/docker_loaddata_demo` to a inner-container path `/ssb-dbgen-path`.

## Method 1: Using the `Source` command in MySQL Client

You can execute an SQL script file using the `source` command or `\.` command in MySQL client. 

```
mysql> source file_name
mysql> \. file_name
```

Usually `source` is used to execute many SQL statements, you can write your database&table creation, insert data in one SQL file and execute this file with `source` in MySQL Client. Each statement is a separate line, the lines starting with `--` or wrapped with `/*`  are considered as comments and will be ignored. 

If your SQL is from `mysqldump`, please refer to this [tutorial](../../Migrate/migrate-from-mysql-to-matrixone.md) about how to modify the SQL file to adapt to MatrixOne format. 

## Method 2: Using the `Load data` command in MySQL Client

You can use `Load Data` to import data from big data files. Currently, MatrixOne only supports `csv` files.

1. Before executing `Load Data` in MatrixOne, the table needs to be created in advance. For now, the data file is required to be at the same machine with MatrixOne server, a file transfer is necessary if they are in separate machines. 

   !Note: if you are working with MatrixOne `docker` version, please put the data file in the directory mounted to the container, otherwise the container cann't perceive. 

2. Launch the MySQL Client in the MatrixOne local server for accessing the local file system. 

```
mysql -h 127.0.0.1 -P 6001 -udump -p111
```

3. Execute `LOAD DATA` with the corresponding file path in MySQL client. 

```
mysql> LOAD DATA INFILE '/tmp/xxx.csv'
INTO TABLE xxxxxx
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY "\r\n";
```

!Note: For the `docker` version, the file path needs to be in the mounted directory.

### Example using `Load data` with `docker` version

We will walk through the whole process of loading data with MatrixOne 0.5.1 docker version.

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

6. After the import is successful, you can run SQL statements to check the rows of imported data:

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
