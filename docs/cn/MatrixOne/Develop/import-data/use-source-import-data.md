# MySQL 客户端启动 MatrixOne 时导入数据

本篇文档将指导你在 MySQL 客户端启动 MatrixOne 时如何完成数据导入。

## 开始前准备

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

## 方式一：MySQL Client 中使用 source 命令，将 SQL 脚本导入 MatrixOne 中

这里将以示例的形式介绍 *MySQL Client 中使用 source 命令，将 SQL 脚本导入 MatrixOne 中*。

- **适用场景**： 从 MySQL 中迁移数据到 MatrixOne
- **场景描述**：通过 *mysqldump* 工具从 MySQL 中已有数据导出成 SQL 脚本，导出的 SQL 脚本包含建表语句与 `INSERT` 数据语句。
- **修改参数**：在将数据导入 MatrixOne 之前，需要修改以下参数：

  + MatrixOne 目前不支持 DDL 语句中设置 `AUTO_INCREMENT`，带有 `AUTO_INCREMENT` 的语句不会有 `Syntax Error`，但是不会生效。应用层如果用到这个逻辑的需要一定修改。

  + `mysqldump` 导出的 SQL 脚本会自动将表名和列名全部加上\`\`，例如`table`, `name`；并且 MatrixOne 会对\`\`中的大小写进行识别，如果在表名或者列名中大小写混用，那么建议将所有的\`\`都去除，否则在查询的时候可能会出现 *该列名找不到* 的错误。

  + MatrixOne 默认支持 UTF8 字符集，但是不支持列级别或表级别的 `CHARACTER SET/CHARSET`, `COLLATE` 的设置。因此这些操作符相关内容均可以删除，不会影响导入内容。

  + mysqldump 导出的建表语句结尾处一般会带有如 `ENGINE=InnoDB`， `AUTO_INCREMENT=187`, `ROW_FORMAT=DYNAMIC/COMPACT`，这些配置项在 MatrixOne 目前均不存在，可以删除。

  + MatrixOne 目前仅支持单列主键索引，如果用到复合主键索引或者次级索引的情况需要对 SQL 进行修改，次级索引可以去除，复合主键索引也可以去除或者改写。

  + mysqldump 导出 SQL 脚本中某些列会声明使用 `BTREE` 索引，MatrixOne 暂不支持 `BTREE` 索引，需要删除。

  + 目前 MatrixOne 还不支持 `TEXT` 和 `BLOB` 类型，如果用到 `TEXT` 相关类型，可以改写成 `VARCHAR`。

    <!--0.6.0支持text-->

  + MatrixOne 不支持 `LOCK TABLE` 操作，因此用到 `LOCK/UNLOCK TABLE` 的语句可以去除。

  + MatrixOne 目前不支持系统变量的修改，因为用到如 `SET SYSTEM_VARIABLE = XX;` 的语句可以去除。

  + MatrixOne 目前不支持 `COMMENT`，语法上不会报错但是实际并不生效，`SHOW CREATE TABLE` 的时候不会有 `COMMENT` 信息。

    <!--0.6.0支持comment-->

参考下面示例，了解 mysqldump 导出的表需要做出的修改：

- 一个原始的 mysqldump 导出的建表语句如下：

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

- 导入 MatrixOne 的时候需要改写成：

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

## 方式二：MySQL Client 中使用 Load data 命令导入数据

这里将以示例的形式介绍 *MySQL Client 中使用 Load data 命令导入数据*。

- **适用场景**：将 *.csv** 格式的外部数据导入到 MatrixOne
- **场景描述**：在 MySQL Client 中使用 Load data 命令将外部数据文件导入，但目前只支持 *.csv* 格式的文件导入。

### 步骤

适合导入到 MatrixOne 的原始数据就是表单类数据或者从 MySQL 使用 `SELECT INTO...OUTFILE` 生成的 *.csv* 格式文件。

1. MySQL Client 的导出语句如下:

```
mysql> SELECT * FROM xxxx
INTO OUTFILE "/tmp/xxx.csv"
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY "\r\n";
```

2. 在 MatrixOne 中需要先把相应的表建好，再通过 `LOAD DATA` 导入数据：

```
mysql> LOAD DATA INFILE '/tmp/xxx.csv'
INTO TABLE xxxxxx
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY "\r\n";
```
