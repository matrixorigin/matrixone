## 导入 jsonlines 数据

MatrixOne 支持 jsonlines 格式数据（即 *.jl* 文件）导入。

## 开始前准备

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

如果你使用 Docker 安装启动 MatrixOne，确保你已将数据文件目录挂载到容器目录下，示例如下：

```
docker run -d -p 6001:6001 -v ~/tmp/docker_loaddata_demo:/ssb-dbgen-path:rw --name matrixone matrixorigin/matrixone:0.5.1
```

上述示例为典型的安装和挂载方式，将其本地路径 *~/tmp/docker_loaddata_demo* 挂载到内部容器路径 */ssb-dbgen-path*。

### 基本命令

```
load data infile {'filepath'='data.txt', 'compression'='BZIP2','format'='jsonline','jsondata'='object'} into table db.a

load data infile {'filepath'='data.txt', 'format'='jsonline','jsondata'='object'} into table db.a
```

**参数说明**

* filepath：文件路径。
* compression：压缩格式，支持 BZIP2、GZIP、LZO、SNAPPY、ZLIB。
* format：文件格式，支持 *.csv* 和 *.jsonline*
* jsondata：json 数据格式，支持 object 和 array，如果 `format` 为 *jsonline*，则**必须**指定 *jsondata*。

**导入原理说明**

- 使用 `simdcsv` 读入一行 jsonline
- 将 jsonline 转换成 json 对象
- 将 json 对象转换为一行数据
- 导入方式与导入 *.csv* 格式数据一致

## 示例

1. 准备数据。你也可以下载使用我们准备好的 *.jl* 文件。以下步骤将以示例数据讲述。

    - 示例数据 1：*[jsonline_object.jl](https://github.com/matrixorigin/matrixone/blob/main/test/resources/load_data/jsonline_object.jl)*
    - 示例数据 2：*[jsonline_array.jl](https://github.com/matrixorigin/matrixone/blob/main/test/resources/load_data/jsonline_array.jl)*

2. 打开终端，进入到 *.jl* 文件所在目录，输入下面的命令行，显示文件内的具体内容：

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

3. 在 MatrixOne 本地服务器中启动 MySQL 客户端以访问本地文件系统。

    ```
    mysql -h 127.0.0.1 -P 6001 -udump -p111
    ```
    
4. 在 MatrixOne 建表：

    ```sql
    create database db1;
    use db1;
    drop table if exists t1;
    create table t1(col1 bool,col2 int,col3 varchar, col4 date,col5 datetime,col6 timestamp,col7 decimal,col8 float);
    drop table if exists t2;
    create table t2(col1 bool,col2 int,col3 varchar, col4 date,col5 datetime,col6 timestamp,col7 decimal,col8 float);
    ```

5. 在 MySQL 客户端对对应的文件路径执行 `LOAD DATA`，导入 *jsonline_object.jl* 和 *jsonline_array.jl* 文件：

    ```sql
    load data infile {'filepath'='$filepath/jsonline_object.jl','format'='jsonline','jsondata'='object'} into table t1;
    load data infile {'filepath'='$filepath/jsonline_array.jl','format'='jsonline','jsondata'='array'} into table t2;
    ```

6. 导入成功后，可以使用 SQL 语句查看导入结果：

    ```sql
    select * from t1;
    col1	col2	col3	col4	col5	col6	col7	col8
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    true	1	var	2020-09-07	2020-09-07 00:00:00	2020-09-07 00:00:00	18	121.11
    ```

!!! note
    Docker 启动 MatrixOne 导入 jsonline 文件，启动时将存放了数据文件的目录挂载到容器的某个目录下，然后再进行数据导入。挂载目录的方式，参见[将容器中的 *store* 目录备份到宿主机](../../../Maintain/upgrade-standalone-matrixone/#1back-up-the-store-directory-in-the-container-to-the-host)。
