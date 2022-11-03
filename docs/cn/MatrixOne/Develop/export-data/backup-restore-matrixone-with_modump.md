# 如何使用 `modump` 命令备份和恢复 MatrixOne 数据库

在本篇文档中，介绍了如何使用 `modump` 命令在 MatrixOne 数据库服务器中生成备份。正如我们所知，数据是组织的宝贵资产。保持数据可用和安全是数据库管理员的主要工作。如果系统或数据中心发生故障、数据库损坏或数据丢失，我们必须能够在定义的 SLA 内对数据进行恢复。

`modump` 是一个命令行实用程序，用于生成 MatrixOne 数据库的逻辑备份。它生成可用于重新创建数据库对象和数据的SQL语句。

## modump 语法

```
./mo-dump -u ${user} -p ${password} -h ${host} -P ${port} -db ${database} [-tbl ${table}...] > {dumpfilename.sql}
```

**参数释义**

- **-u [user]**：连接 MatrixOne 服务器的用户名。只有具有数据库和表读取权限的用户才能使用 `modump` 实用程序，默认值 dump。

- **-p [password]**：MatrixOne 用户的有效密码。默认值：111。

- **-h [host]**：MatrixOne 服务器的主机 IP 地址。默认值：127.0.0.1

- **-P [port]**：MatrixOne 服务器的端口。默认值：6001

- **-db [数据库名称]**：必需参数。要备份的数据库的名称。

- **-tbl [表名]**：可选参数。如果参数为空，则导出整个数据库。如果要备份指定表，则可以在命令中指定多个 `-tbl` 和表名。

## 构建 modump 二进制包

`modump` 命令程序嵌入在 MatrixOne 源代码中，你可以从源代码构建二进制文件。

```
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone
make build modump
```

你可以在 MatrixOne 文件夹中找到 `modump` 可执行文件。

## 生成单个数据库的备份

示例如下，使用以下 SQL 创建的数据库 *t* 及其表 *t1*：

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

如果要生成单个数据库的备份，可以运行以下命令。该命令将生成命名为 *t* 的数据库的备份，其中包含`t.sql`文件中的结构和数据。

```
./mo-dump -u dump -p 111 -h 127.0.0.1 -P 6001 -db t > t.sql
```

如果要在数据库中生成单个表的备份，可以运行以下命令。该命令将生成命名为 *t* 的数据库的 *t1* 表的备份，其中包含 `t.sql` 文件中的结构和数据。

```
./mo-dump -u dump -p 111 -db t -tbl t1 > t1.sql
```

## 恢复备份到 MatrixOne 服务器

将导出的 *.sql* 文件恢复至 MatrixOne 数据库相对简单。要恢复你的数据库，你必须先创建一个空数据库，并使用 *MySQL 客户端* 进行恢复。

将 MatrixOne 与 MySQL 客户端连接至同一服务器上，并确保导出的 *.sql* 文件也在同一服务器上。

```
mysql> create database t if not exists;
mysql> source /YOUR_SQL_FILE_PATH/t.sql
```

成功执行以上命令后，执行以下命令，检查是否在命名为 *t* 数据库上创建了所有对象。

```
mysql> use t;
mysql> show tables;
mysql> select count(*) from t1;
```

## 限制

* `modump` 仅支持导出单个数据库的备份，如果你有多个数据库需要备份，需要手动运行 `modump` 多次。

* `modump` 暂不支持只导出数据库的结构或数据。如果你想在没有数据库结构的情况下生成数据的备份，或者仅想导出数据库结构，那么，你需要手动拆分 `.sql` 文件。
