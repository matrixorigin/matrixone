# Export data when launch MatrixOne using Docker

## Before you start

- Make sure you have already [installed MatrixOne using Docker](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#method-3-using-docker).

!!! note
    MatrixOne does not support using `dump` to export the tables, only supports using `SELECT...INTO OUTFILE` to export the table.

## Method 1: Export the table to your local directory

[Launch MatrixOne using docker](../../Get-Started/install-standalone-matrixone.md), then export the table to your local directory, see [Export data in general method](use-source-export-data.md).

## Method 2: Export the table to your container directory

### Steps

1. To mount the directory *~/tmp/docker_export_demo/* that stores data files to a directory in the container before launching MatrixOne using docker. For example, the container directory is */store*.

    ```
    docker run -d -p 6001:6001 -v ~/tmp/docker_export_demo/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
    ```

2. Connect to MatrixOne server:

    ```
    mysql -h 127.0.0.1 -P 6001 -udump -p111
    ```

2. Create tables in MatrixOne:

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
3. Export the table to your container directory path，for example, *~/tmp/docker_export_demo/store/export_datatable.txt*

    ```
    select * from user into outfile 'store/export_datatable.txt';
    ```

4. Check the table in your local directory*~/tmp/docker_export_demo/store/*.

    Open the *~/tmp/docker_export_demo/store/* directory, *export_datatable.txt* file is created, then open the file, check the result as below:

    ```
    id,user_name,sex
    1,"weder","man"
    2,"tom","man"
    3,"wederTom","man"
    ```
