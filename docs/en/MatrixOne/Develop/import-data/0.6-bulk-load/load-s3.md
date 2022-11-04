# Import the data from S3

## Overview

MatrixOne can read the Simple Storage Service (S3) files and import data to MatrixOne database.

Currently, S3 supports AWS and mainstream cloud vendors in China. In addition, files on S3 support compressed formats and regular expression rules for file paths to read multiple files. For example, "/Users/\*.txt" will read all files ending with *.txt*.

## Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).
- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/).

If you use the `docker` install, please make sure that you have a data directory mounted to the container. For example,

```T
docker run -d -p 6001:6001 -v ~/tmp/docker_loaddata_demo:/ssb-dbgen-path:rw --name matrixone matrixorigin/matrixone:0.5.1
```

This typical installation maps its local path *~/tmp/docker_loaddata_demo* to a inner-container path */ssb-dbgen-path*.

## Basic command

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

**Parameter Description**

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

For more information, see [LOAD DATA](../../../Reference/SQL-Reference/Data-Manipulation-Statements/load-data.md).

**Code Example**：

```sql
## Non - specified file Compressed format
LOAD DATA INFILE URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>'} INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';

## Specifies the file Compressed format
LOAD DATA INFILE URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'} INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';
```

## Example

For example purposes, we have prepared data from AWS S3. If you do not already have an account on AWS S3, please sign up first.

!!! note
    This code example does not show account information such as access_id because of account privacy.
    You can read this document to understand the main steps; specific data and account information will not be shown.

### Method 1: Import S3 file to MatrixOne

1. Prepare the data file. Enter into **AWS S3 > buckets** and find the file *file-demo*.

2. Launch the MySQL Client, create tables in MatrixOne, for example:

    ```sql
    create database db1;
    use db1;
    drop table if exists t1;
    create table t1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19));
    ```

2. Import the file into MatrixOne:

    ```
    LOAD DATA INFILE URL s3option{"endpoint"='s3.us-west-2.amazonaws.com', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'} INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';
    ```

3. After the import is successful, you can run SQL statements to check the result of imported data:

    ```sql
    select * from t1;
    ```

### Method 2: Specify S3 file to an external table

1. Prepare the data file. Enter into **AWS S3 > buckets** and find the file *file-demo*.

2. Launch the MySQL Client, specify S3 file to an external table:

    ```
    create external table ex_table_s3_1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19)) URL s3option{"endpoint"='s3.us-west-2.amazonaws.com',"access_key_id"='AKIAW2D4ZBGTXW2S2PVR', "secret_access_key"='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', "bucket"='heni-test', "filepath"='s3://heni-test/ex_table_number.csv', "region"='us-west-2'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
    ```

3. After the import is successful, you can run SQL statements to check the result of imported data:

    ```sql
    select * from ex_table_s3_1;
    ```

4. (Optional)If you need to import external table data into a data table in MatrixOne, you can use the following SQL statement:

    Create a new table t1 in MatrixOne:

    ```sql
    create table t1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19));
    ```

    Import the external table *ex_table_s3_1* to *t1*:

    ```sql
    insert into t1 select * from ex_table_s3_1;
    ```

#### About external table with `Load data`

An example of the `Load data` syntax for an external table is:

```sql
## Create a external table for an S3 file (specify the compression format)
create external table t(...) URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>', "compression"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';

## Create a external table for an S3 file (if no compression format is specified, the format is auto, and the file format is automatically checked)
create external table t(...) URL s3option{"endpoint"='<string>', "access_key_id"='<string>', "secret_access_key"='<string>', "bucket"='<string>', "filepath"='<string>', "region"='<string>'} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';
```

!!! note
    MatrixOne only supports `select` on external tables. `Delete`, `insert`, and `update` are not supported.

If the S3 file is specified to an external table, you can use the `select` to read the the external table file. To import data into a MatrixOne table, use the following SQL statements as an example:

```sql
## t1 is the MatrixOne's table, t is the external table
insert into t1 select * from t;

## You can also select a column from the external table file to import into the MatrixOne's table
insert into t1 select a from t;
```
