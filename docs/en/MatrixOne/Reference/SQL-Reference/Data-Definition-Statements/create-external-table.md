# **CREATE EXTERNAL TABLE**

## **Description**

**External table** access data in external sources as if it were in a table in the database.

You can connect to the database and create metadata for the external table using DDL.

The DDL for an external table consists of two parts: one part that describes the MatrixOne column types, and another part (the access parameters) that describes the mapping of the external data to the MatrixOne data columns.

This document describe how to create a new tables outside of the MatrixOne databases.

## **Syntax**

### Common syntax

```
> CREATE EXTERNAL TABLE [IF NOT EXISTS] [db.]table_name;
(
    name1 type1,
    name2 type2,
    ...
)
```

### Current Usage syntax

```
## Create a external table for a local file (specify the compression format)
create external table t(...) localfile{"filepath"='<string>', "compression"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';

## Create a external table for a local file (if no compression format is specified, the format is auto, and the file format is automatically checked)
create external table t(...) localfile{"filepath"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';


## Create a external table for an S3 file (specify the compression format)
create external table t(...) URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';

## Create a external table for an S3 file (if no compression format is specified, the format is auto, and the file format is automatically checked)
create external table t(...) URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';
```

### Explanations

#### Parameter Description

|Parameter|Description|
|:-:|:-:|
|endpoint|A endpoint is a URL that can conncect to AWS Web service. For example: s3.us-west-2.amazonaws.com|
|access_key_id| S3 Access key ID |
|secret_access_key| S3 Secret access key |
|bucket| S3 Bucket to access |
|filepath| relative file path |
|region| AWS S3 Area|
|compression| Compressed format of S3 files. If empty, it indicates uncompressed files. Supported fields or Compressed format are "auto", "none", "gzip", "bzip2", "flate", "zlib", and "lz4".|
|auto|Compressed format: indicates that the file name extension automatically checks the compressed format of a file|
|none|Compressed format: indicates the uncompressed format, and the rest indicates the compressed format of the file|

## Example

```sql
> create external table ex_table_cpk(clo1 tinyint,clo2 smallint,clo3 int,clo4 bigint,clo5 tinyint unsigned,clo6 smallint unsigned,clo7 int unsigned,clo8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 varchar(255))infile{"filepath"='$resources/external_table_file/cpk_table_1.csv'} ;
```

For more information on using an external table to import the data from S3, see [Import the data from S3](../../../Develop/import-data/0.6-bulk-load/load-s3.md).

## **Constraints**

MatrixOne only supports `select` on **EXTERNAL TABLE**, `delete`, `insert`, and `update` is not supported.
