# Docker 启动 MatrixOne 时导出数据

## 开始前准备

- 已安装 Docker。

!!! note
    MatrixOne 暂时不支持整库 dump 导出，仅支持使用 `SELECT...INTO OUTFILE` 语句导出表数据。

## 方式一：直接导出数据表到本地

使用 [Docker 运行 MatrixOne](../../Get-Started/install-standalone-matrixone.md)，若仅需导出数据表，方式与 [通用方式导出数据](use-source-export-data.md)一致。

## 方式二：挂载目录

### 步骤

1. 使用 Docker 启动 MatrixOne，启动时将存放了数据文件的目录 *~/tmp/docker_export_demo/* 挂载到容器的某个目录下，这里容器目录以 */store* 为例：

    ```
    docker run -d -p 6001:6001 -v ~/tmp/docker_export_demo/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
    ```

2. 连接 MatrixOne 服务：

    ```
    mysql -h 127.0.0.1 -P 6001 -udump -p111
    ```

2. 准备数据：

    ```sql
    create database aaa;
    use aaa;
    CREATE TABLE `user` (`id` int(11) ,`user_name` varchar(255) ,`sex` varchar(255));
    insert into user(id,user_name,sex) values('1', 'weder', 'man'), ('2', 'tom', 'man'), ('3', 'wederTom', 'man');
    select * from user;
    +------+-----------+------+
    | id   | user_name | sex  |
    +------+-----------+------+
    |    1 | weder     | man  |
    |    2 | tom       | man  |
    |    3 | wederTom  | man  |
    +------+-----------+------+
    ```
    
3. 导出表数据到 *~/tmp/docker_export_demo/store/export_datatable.txt*

    ```
    select * from user into outfile 'store/export_datatable.txt';
    ```

4. 到你本地 *~/tmp/docker_export_demo/store/* 下查看导出情况。

    导出成功，打开本地目录 *~/tmp/docker_export_demo/store/*，可见新建了一个 *export_datatable.txt* 文件，打开文件，展示结果如下：导出成功结果如下：

    ```
    id,user_name,sex
    1,"weder","man"
    2,"tom","man"
    3,"wederTom","man"
    ```
