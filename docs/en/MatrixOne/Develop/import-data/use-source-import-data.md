# Import data when launching MatrixOne using MySQL client

This document will guide you on how to import data when launching MatrixOne using the MySQL client.

## Before you start

- Make sure you have already [installed and launched MatrixOne using source](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#method-1-building-from-source) or [installed and launched MatrixOne using binary packages](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#method-2-downloading-binary-packages).

- Make sure you have already [connected MatrixOne Server](../../Get-Started/connect-to-matrixone-server.md).

## Method 1: Using the source command in MySQL Client to import the SQL script to a MatrixOne

- **Applicable scenarios**: Migrate data from MySQL to MatrixOne.
- **Scenario description**: Use the [mysqldump](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html) tool to export existing data from the MySQL database to an SQL script. The exported SQL script contains table statements and `INSERT` data statements.
- **Modify parameters**: Before importing data into a MatrixOne, modify the following parameters:

  + MatrixOne currently does not support setting `AUTO_INCREMENT` in DDL statements. Statements with `AUTO_INCREMENT` will not occur `Syntax Error`, but it will not work. The application layer needs to be modified if this logic is used.

  + `mysqldump` exported SQL script will automatically add \`\` to all table and column names, such as `table`, `name`; MatrixOne will recognize the case in \`\`. If the table name or column name is mixed with the case, it is recommended to remove all \`\`. Otherwise, the column name may not be found during the query.

  + MatrixOne supports UTF8 CHARACTER SET by default, but does not support setting `CHARACTER SET/CHARSET`, `COLLATE` of the column level or the table level. Therefore, these operators and the related contents of operators can be deleted without affecting the imported content.

  + `mysqldump` export the end of the built table statements usually with such as `ENGINE = InnoDB`, `AUTO_INCREMENT = 187`, `ROW_FORMAT = DYNAMIC/COMPACT`, MatrixOne currently dose not support these configuration items, and these configuration items can be deleted.

  + MatrixOne currently supports only a single primary key index, if you want to use a composite primary key index or secondary index, you needs to modify the SQL: the secondary index can be deleted, the composite primary key index can be deleted or be rewritten.

  + Some columns in the exported SQL script are declared to use the `BTREE` index. MatrixOne currently dose not support the `BTREE` index, and the `BTREE` index can be deleted.

  + MatrixOne currently does not support the `TEXT` and `BLOB` types. If the `TEXT` -related type is used, it can be overwritten as `VARCHAR`.

  + MatrixOne currently does not support `LOCK TABLE`, so the `LOCK/UNLOCK TABLE` statement can be deleted.

  + MatrixOne currently does not support system variable modification, when using such as `SET SYSTEM_VARIABLE = XX;` statement can be deleted.

  + MatrixOne currently does not support `COMMENT`, the syntax will not report errors, but the actual effect is not effective, `SHOW CREATE TABLE` will not have `COMMENT` information.

To change the SQL of the exported table, see the following example:

- An original `mysqldump` is exported as below:

```
CREATE TABLE `roles` (
`role_id` int(11) NOT NULL AUTO_INCREMENT,
`role_name` varchar(128) DEFAULT NULL,
`state` int(11) DEFAULT NULL,
`company_id` int(11) DEFAULT NULL,
`remark` varchar(1024) DEFAULT NULL,
`created_by` varchar(64) DEFAULT NULL,
`created_time` datetime DEFAULT NULL,
`updated_by` varchar(64) DEFAULT NULL,
`updated_time` datetime DEFAULT NULL,
`is_deleted` int(11) DEFAULT '0',
`deleted_by` varchar(64) DEFAULT NULL,
`deleted_time` datetime DEFAULT NULL,
PRIMARY KEY (`role_id`),
KEY `idx_company_state` (`company_id`,`state`)
) ENGINE=InnoDB AUTO_INCREMENT=395 DEFAULT CHARSET=utf8;
```

- To import MatrixOne, rewrite it as below:

```
CREATE TABLE roles (
role_id int(11) NOT NULL AUTO_INCREMENT,
role_name varchar(128) DEFAULT NULL,
state int(11) DEFAULT NULL,
company_id int(11) DEFAULT NULL,
remark varchar(1024) DEFAULT NULL,
created_by varchar(64) DEFAULT NULL,
created_time datetime DEFAULT NULL,
updated_by varchar(64) DEFAULT NULL,
updated_time datetime DEFAULT NULL,
is_deleted int(11) DEFAULT '0',
deleted_by varchar(64) DEFAULT NULL,
deleted_time datetime DEFAULT NULL,
PRIMARY KEY (role_id)
);
```

## Method 2: Using the `Load data` command in MySQL Client to import data

- **Applicable scenario** : Import external data in *.csv* format to MatrixOne

- **Scenario description** : Run the `Load data` command on the MySQL Client to import external data files. Currently, only *.csv* files are supported.

### Steps

The appropriate raw data to import into MatrixOne is the form type, or the *.csv* file which is generated through using `SELECT INTO... OUTFILE` from MySQL.

1. The MySQL Client export statement is as follows:

```
mysql> SELECT * FROM xxxx
INTO OUTFILE "/tmp/xxx.csv"
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY "\r\n";
```

2. In MatrixOne, you need to build the corresponding table first, and then import data through `LOAD DATA`:

```
mysql> LOAD DATA INFILE '/tmp/xxx.csv'
INTO TABLE xxxxxx
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY "\r\n";
```
