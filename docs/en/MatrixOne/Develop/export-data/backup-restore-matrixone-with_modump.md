# How to backup and restore MatrixOne databases using the `modump` command

In this article, we'll explain how to generate the backup in the MatrixOne database server using the `modump` command. As we know, data is a valuable asset to the organization. As database administrators, it is our primary and crucial job to keep the data available and safe. If the system or data center fails, database corruption, and data loss, we must be able to recover it within the defined SLA.

`modump` is a command-line utility that is used to generate the logical backup of the MatrixOne database. It produces the SQL Statements that can be used to recreate the database objects and data.

## modump syntax

```
./mo-dump -u ${user} -p ${password} -h ${host} -P ${port} -db ${database} [-tbl ${table}...] > {dumpfilename.sql}
```

The parameters are as following:

- **-u [user]**: It is a username to connect to the MatrixOne server. Only the users with database and table read privileges can use `modump` utility. Default value: dump

- **-p [password]**: The valid password of the MatrixOne user. Default value: 111

- **-h [host]**: The host ip address of MatrixOne server. Default value: 127.0.0.1

- **-P [port]**: The port of MatrixOne server. Default value: 6001

- **-db [database name]**: Required paratemer. Name of the database that you want to take backup.

- **-tbl [table name]**: Optional parameter. If the parameter is empty, the whole database will be exported. If you want to take the backup specific tables, then you can specify multiple `-tbl` and table names in the command.

## Build the modump binary

`modump` utility is embedded in the MatrixOne source code. You can build the binary from the source code.

```
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone
make build modump
```

You can find the `modump` executable file in the MatrixOne folder.

## Generate the backup of a single database

For example, we have a database “**t**” which is created by the following SQL.

```
DROP DATABASE IF EXISTS `t`;
CREATE DATABASE `t`;
USE `t`;
create table t1
(
    c1  int primary key auto_increment,
    c2  tinyint not null default 4,
    c3  smallint,
    c4  bigint,
    c5  tinyint unsigned,
    c6  smallint unsigned,
    c7  int unsigned,
    c8  bigint unsigned,
    c9  float,
    c10 double,
    c11 date,
    c12 datetime,
    c13 timestamp on update current_timestamp,
    c14 char,
    c15 varchar,
    c16 json,
    c17 decimal,
    c18 text,
    c19 blob,
    c20 uuid
);
insert into t1 values (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, '2019-01-01', '2019-01-01 00:00:00', '2019-01-01 00:00:00', 'a', 'a', '{"a":1}','1212.1212', 'a', 'aza', '00000000-0000-0000-0000-000000000000');
```

If you want to generate the backup of the single database, run the following command. The command will generate the backup of the “**t**” database with structure and data in the`t.sql` file.

```
./mo-dump -u dump -p 111 -h 127.0.0.1 -P 6001 -db t > t.sql
```

If you want to generate the backup of a single table in a database, run the following command. The command will generate the backup of the `t1` table of  `t` database with structure and data in the `t.sql` file.

```
./mo-dump -u dump -p 111 -db t -tbl t1 > t1.sql
```

## Restore the backup to MatrixOne server

Restoring a MatrixOne database using the exported *.sql* file is simple. To restore the database, you must create an empty database and use `mysql client` to restore.

Connect to MatrixOne with MySQL client in the same server, and make sure the exported *.sql* file is also in the same machine as the MySQL client.

```
mysql> create database t if not exists;
mysql> source /YOUR_SQL_FILE_PATH/t.sql
```

Once command executes successfully, execute the following command to verify that all objects have been created on the `t` database.

```
mysql> use t;
mysql> show tables;
mysql> select count(*) from t1;
```

## Constraints

* `modump` only supports exporting the backup of a single database, if you have several databases to backup, you need to manually run `modump` for several times.

* `modump` doesn't support exporting only the structure or data of databases. If you want to generate the backup of the data without the database structure or vise versa, you need to manually split the `.sql` file.
