# **CREATE EXTERNAL TABLE**

## **语法说明**

外部表是指不在数据库里的表，是操作系统上的一个按照一定格式分割的文本文件，或是其他类型的表，对 MatrixOne 来说类似于视图，可以在数据库中像视图一样进行查询等操作，但是外部表在数据库中只有表结构，而数据存放在操作系统中。

本篇文档将讲述如何在 MatrixOne 数据库外建表。

## **语法结构**

### 通用语法

```
> CREATE EXTERNAL TABLE [IF NOT EXISTS] [db.]table_name;
(
    name1 type1,
    name2 type2,
    ...
)
```

### 语法示例

```
## 创建指向本地文件的外表（指定压缩格式）
create external table t(...) localfile{"filepath"='<string>', "compression"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';

## 创建指向本地文件的外表（不指定压缩格式，则为auto格式，自动检查文件的格式）
create external table t(...) localfile{"filepath"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';


## 创建指向S3文件的外表（指定压缩格式）
create external table t(...) URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';

## 创建指向S3文件的外表（不指定压缩格式，则为auto格式，自动检查文件的格式）
create external table t(...) URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';
```

### 语法说明

#### 参数说明

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

## 示例

```sql
> create external table ex_table_cpk(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/cpk_table_1.csv'} ;
```

更多关于使用外表指定 S3 文件，参见[从 S3 导入数据](../../../Develop/import-data/0.6-bulk-load/load-s3.md)。

## **限制**

当前 MatrixOne 仅支持对外部表进行 `select` 操作，暂时还不支持使用 `delete`、`insert`、`update` 对外部表插入数据。
