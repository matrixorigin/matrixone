# 通用方式导出数据

本篇文档将指导你使用 MatrixOne 如何完成数据导出。

## 开始前准备

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../Get-Started/connect-to-matrixone-server.md)

!!! note
    MatrixOne 暂时不支持整库 dump 导出，仅支持使用 `SELECT...INTO OUTFILE` 语句导出表数据。

- **场景描述**：在 MatrixOne 里新建一个表，把表数据导出到你指定的文件夹目录下，例如 *~/tmp/export_demo/export_datatable.txt*。

### 步骤

1. 准备数据：

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

2. 导出表数据到 *~/tmp/export_demo/export_datatable.txt*

    ```
    select * from user into outfile '~/tmp/export_demo/export_datatable.txt'
    ```

3. 到你本地 *~/tmp/export_demo/export_datatable.txt* 下查看导出情况。

    导出成功，打开本地目录 *~/tmp/export_demo/*，可见新建了一个 *export_datatable.txt* 文件，打开文件，展示结果如下：

    ```
    id,user_name,sex
    1,"weder","man"
    2,"tom","man"
    3,"wederTom","man"
    ```
