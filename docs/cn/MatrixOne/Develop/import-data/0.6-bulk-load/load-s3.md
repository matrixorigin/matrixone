# 从 S3 导入数据

## 概述

MatrixOne 支持从对象存储服务（Simple Storage Service, S3) 中读取文件，导入数据到 MatrixOne 数据库表中。

目前的 S3 支持 AWS，以及国内的主流云厂商，此外 S3 上的文件支持压缩格式，此外还支持文件路径的正则表达式规则，来读取多个文件，例如 "/Users/\*.txt" 就会去读取以 *.txt* 结尾的所有文件。

## 开始前准备

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

如果你使用 Docker 安装启动 MatrixOne，确保你已将数据文件目录挂载到容器目录下，示例如下：

```
docker run -d -p 6001:6001 -v ~/tmp/docker_loaddata_demo:/ssb-dbgen-path:rw --name matrixone matrixorigin/matrixone:0.5.1
```

上述示例为典型的安装和挂载方式，将其本地路径 *~/tmp/docker_loaddata_demo* 挂载到内部容器路径 */ssb-dbgen-path*。

## 导入语法介绍

```sql
LOAD DATA
    [ INFILE 'string'
    | INFILE {"filepath"='<string>', "compression"='<string>'}
    | URL s3options {"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'}
    [IGNORE]
    INTO TABLE tbl_name
    [CHARACTER SET charset_name]
    [{FIELDS | COLUMNS}
        [[OPTIONALLY] ENCLOSED BY 'char']
    ]
    ]
    [IGNORE number {LINES | ROWS}]
```

<!--待确认-heni-->

**参数说明**

|参数|描述|
|:-:|:-:|
|endpoint|终端节点是作为 AWS Web 服务的入口点的 URL。例如：s3.us-west-2.amazonaws.com|
|access_key_id| S3 的 Access key ID|
|secret_access_key| S3 的 Secret access key|
|bucket| 需要访问的桶|
|filepath| 访问文件的相对路径 |
|region| s3 所在的区域|
|compression| S3 文件的压缩格式，为空表示非压缩文件，支持的字段或压缩格式为"auto", "none", "gzip", "bzip2", "flate", "zlib", "lz4"|
|auto|压缩格式，表示通过文件后缀名自动检查文件的压缩格式|
|none|压缩格式，表示为非压缩格式，其余表示文件的压缩格式|

更多信息，参考 [LOAD DATA](../../../Reference/SQL-Reference/Data-Manipulation-Statements/load-data.md)。

**代码示例**：

```sql
## 非指定文件压缩格式
LOAD DATA INFILE URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>'} INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';

## 指定文件压缩格式
LOAD DATA INFILE URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'} INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';
```

## 示例

为方便举例说明，我们准备了 AWS S3 上的数据，如果你还未开通 AWS S3 上的账号，请先开通账号。

!!! note
    涉及到账号隐私，此处代码示例不展示有关 access_id 等账号信息。
    你可以通过本篇文档了解主要步骤，对于具体数据和具体账号信息，将不做展示。

### 方式一：直接将 S3 文件导入到 MatrixOne

1. 准备数据。进入到 **AWS S3 > buckets** 找到表文件 *file-demo*。

2. 打开 MySQL 客户端，启动 MatrixOne，在 MatrixOne 中建表，例如：

    ```sql
    create database db1;
    use db1;
    drop table if exists t1;
    create table t1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19));
    ```

2. 将表文件导入到 MatrixOne：

    ```
    LOAD DATA INFILE URL s3option{"endpoint"='s3.us-west-2.amazonaws.com', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'} INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';
    ```

3. 导入成功后，可以使用 SQL 语句查看导入结果：

    ```sql
    select * from t1;
    ```

### 方式二：直接将 S3 文件指定到外部表

1. 准备数据。进入到 **AWS S3 > Buckes** 找到表文件 *file-demo*。

2. 打开 MySQL 客户端将 S3 表数据指定到外部表：

    ```
    create external table ex_table_s3_1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19)) URL s3option{"endpoint"='s3.us-west-2.amazonaws.com',"access_key_id"='AKIAW2D4ZBGTXW2S2PVR', "secret_access_key"='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', "bucket"='heni-test', "filepath"='s3://heni-test/ex_table_number.csv', "region"='us-west-2'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
    ```

3. 导入成功后，可以使用 SQL 语句查看导入结果：

    ```sql
    select * from ex_table_s3_1;
    ```

4. (选做)如果需要将外部表数据导入到 MatrixOne 中的数据表，可以使用以下 SQL 语句：

    在 MatrixOne 中新建表 *t1*：

    ```sql
    create table t1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19));
    ```

    将外部表 *ex_table_s3_1* 导入到 *t1*：

    ```sql
    insert into t1 select * from ex_table_s3_1;
    ```

#### 关于外部表 `Load data` 说明

外部表 `Load data` 语法示例如下：

```sql
## 创建指向S3文件的外表（指定压缩格式）
create external table t(...) URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';

## 创建指向S3文件的外表（不指定压缩格式，则为auto格式，自动检查文件的格式）
create external table t(...) URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';
```

!!! note
    MatrixOne 对于外部表仅支持进行 `select` 操作，`delete`，`insert`，`update` 暂不支持。
    外部表 `select` 的操作与目前普通表的操作相同，支持 `where`，`limit` 等条件操作。

如果 S3 文件指向外部表，可以使用 `select` 操作读取外表指向文件里的内容。在 MatrixOne 普通表中导入数据时，可以使用如下 SQL 语句，示例：

```sql
## t1为普通表，t为外表
insert into t1 select * from t;

## 也可以选取外部表文件中某一列导入内表
insert into t1 select a from t;
```
