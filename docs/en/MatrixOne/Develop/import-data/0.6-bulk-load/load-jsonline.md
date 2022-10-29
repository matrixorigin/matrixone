## Import the jsonlines data

This document will guide you on how to import jsonlines (that is *.jl* file) data to MatrixOne.

## Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).
- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/).

If you use the `docker` install, please make sure that you have a data directory mounted to the container. For example,

```T
docker run -d -p 6001:6001 -v ~/tmp/docker_loaddata_demo:/ssb-dbgen-path:rw --name matrixone matrixorigin/matrixone:0.5.1
```

This typical installation maps its local path *~/tmp/docker_loaddata_demo* to a inner-container path */ssb-dbgen-path*.

### Basic command

```
load data infile {'filepath'='data.txt', 'compression'='BZIP2','format'='jsonline','jsondata'='object'} into table db.a

load data infile {'filepath'='data.txt', 'format'='jsonline','jsondata'='object'} into table db.a
```

**Parameter Description**

* filepath: The file path.
* compression: Compression format, BZIP2, GZIP, LZO, SNAPPY, ZLIB are supported.
* format: format, the file format *.csv* and *.jsonline* are supported.
* jsondata: jsondata format. object and array are supported. If `format` is *jsonline*, must specify *jsondata*.

**Import Principles**

- Read a line of jsonline using `simdcsv`

- Convert jsonline into a json object

- Converts a json object to a row of data

- The import method must be the same as importing data in *.csv*

## Example

1. Prepare the data files. You can also download and use the *.jl* file we prepared. The following steps are illustrated with sample data.

    - Example data file 1：*[jsonline_object.jl](https://github.com/matrixorigin/matrixone/blob/main/test/resources/load_data/jsonline_object.jl)*
    - Example data file  2：*[jsonline_array.jl](https://github.com/matrixorigin/matrixone/blob/main/test/resources/load_data/jsonline_array.jl)*

2. Open your terminal, enter into the directory where the *.jl* file resides, and run the following command line to display the contents of the file:

    ```shell
    > cd /$filepath
    > head jsonline_object.jl
    {"col1":true,"col2":1,"col3":"var","col4":"2020-09-07","col5":"2020-09-07 00:00:00","col6":"2020-09-07 00:00:00","col7":"18","col8":121.11}
    {"col1":"true","col2":"1","col3":"var","col4":"2020-09-07","col5":"2020-09-07 00:00:00","col6":"2020-09-07 00:00:00","col7":"18","col8":"121.11"}
    {"col6":"2020-09-07 00:00:00","col7":"18","col8":"121.11","col4":"2020-09-07","col5":"2020-09-07 00:00:00","col1":"true","col2":"1","col3":"var"}
    {"col2":1,"col3":"var","col1":true,"col6":"2020-09-07 00:00:00","col7":"18","col4":"2020-09-07","col5":"2020-09-07 00:00:00","col8":121.11}
    > head jsonline_array.jl
    [true,1,"var","2020-09-07","2020-09-07 00:00:00","2020-09-07 00:00:00","18",121.11]
    ["true","1","var","2020-09-07","2020-09-07 00:00:00","2020-09-07 00:00:00","18","121.11"]
    ```

3. Launch the MySQL Client in the MatrixOne local server for accessing the local file system.

    ```
    mysql -h 127.0.0.1 -P 6001 -udump -p111
    ```

4. Create tables in MatrixOne:

    ```sql
    create database db1;
    use db1;
    drop table if exists t1;
    create table t1(col1 bool,col2 int,col3 varchar, col4 date,col5 datetime,col6 timestamp,col7 decimal,col8 float);
    drop table if exists t2;
    create table t2(col1 bool,col2 int,col3 varchar, col4 date,col5 datetime,col6 timestamp,col7 decimal,col8 float);
    ```

5. Execute `LOAD DATA` with the corresponding file path in MySQL client, import the *jsonline_object.jl*  and the file *jsonline_array.jl* into MatrixOne:

    ```sql
    load data infile {'filepath'='$filepath/jsonline_object.jl','format'='jsonline','jsondata'='object'} into table t1;
    load data infile {'filepath'='$filepath/jsonline_array.jl','format'='jsonline','jsondata'='array'} into table t2;
    ```

6. After the import is successful, you can run SQL statements to check the result of imported data:

    ```sql
    select * from t1;
    col1	col2	col3	col4	col5	col6	col7	col8
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    select * from t2;
    col1	col2	col3	col4	col5	col6	col7	col8
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    ```

!!! note
    If you use Docker to launch MatrixOne, when you try to import the jsonline file, please make sure that you have a data directory mounted to the container. For more information on backing up the store directory, see[Back up the store directory in the container to the host](../../../Maintain/upgrade-standalone-matrixone/#1back-up-the-store-directory-in-the-container-to-the-host).
