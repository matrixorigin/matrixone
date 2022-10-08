# 导出数据

本篇文档将指导你使用 MatrixOne 如何完成数据导出。

MatrixOne 支持导出数据保存为 *.txt* 及 *.csv* 类型。

## 开始前准备

- 已通过[源代码](../../Get-Started/install-standalone-matrixone/#1)或[二进制包](../../Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

如果你使用 Docker 安装启动 MatrixOne，确保你已将数据文件目录挂载到容器目录下，示例如下：

```
docker run -d -p 6001:6001 -v ~/tmp/docker_export_demo/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
```

!!! note
    MatrixOne 暂时不支持整库 dump 导出，仅支持使用 `SELECT...INTO OUTFILE` 语句导出表数据。

### 步骤

1. 在 MatrixOne 中新建一个数据表：

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

2. 导出表数据：

    - 如果你是通过源码或二进制包安装 MatrixOne，导出数据到 *~/tmp/export_demo/export_datatable.txt*

       ```
       select * from user into outfile '~/tmp/export_demo/export_datatable.txt'
       ```

    - 如果你是通过 Docker 安装启动 MatrixOne，导出数据到 *~/tmp/docker_export_demo/store/export_datatable.txt*

       ```
       select * from user into outfile 'store/export_datatable.txt';
       ```

3. 到你本地 *export_datatable.txt* 文件下查看导出情况：

    ```
    id,user_name,sex
    1,"weder","man"
    2,"tom","man"
    3,"wederTom","man"
    ```
