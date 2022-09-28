# Export data

This document will guide you how to export data in MatrixOne.

## Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).

- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/). 

If you use the `docker` install, please make sure that you have a data directory mounted to the container. For example,

```
docker run -d -p 6001:6001 -v ~/tmp/docker_export_demo/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
```

!!! note
    MatrixOne does not support using `mysqldump` to export table structures and data, it only supports `SELECT...INTO OUTFILE`  exporting to text files.

### Steps

1. Create tables in MatrixOne:

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

2. For installation with source code or binary file, export the table to your local directory, for example, *~/tmp/export_demo/export_datatable.txt*

    ```
    select * from user into outfile '~/tmp/export_demo/export_datatable.txt'
    ```

    For installation with docker, export the your mounted directory path of container as the following example. The directory `store` refers to the local path of `~/tmp/docker_export_demo/store`. 

    ```
    select * from user into outfile 'store/export_datatable.txt';
    ```

3. Check the table in your directory `export_datatable.txt`, the result is as below:

    ```
    id,user_name,sex
    1,"weder","man"
    2,"tom","man"
    3,"wederTom","man"
    ```
