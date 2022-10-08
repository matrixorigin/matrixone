# 将数据从 MySQL 迁移至 MatrixOne

本篇文档将指导你如何将数据从 MySQL 迁移至 MatrixOne。

## MySQL 数据转储

你需要拥有对 MySQL 实例的完全访问权限。

首先，使用 `mysqldump` 将 MySQL 表结构和数据通过以下命令转储到一个文件中。

```
mysqldump -h IP_ADDRESS -uUSERNAME -pPASSWORD -d DB_NAME1 DB_NAME2 ... OUTPUT_FILE_NAME.SQL
```

示例如下，使用命令将所有表结构和数据转储到一个名为 *a.sql* 的文件中。

```
mysqldump -h 127.0.0.1 -uroot -proot -d test a.sql
```

## 修改 *.sql* 文件

*.sql* 文件当前还不完全适用于 MatrixOne。你需要修改几个元素，以使 *.sql* 文件适应 MatrixOne 的格式。

* 需删除不支持的语法或功能：`AUTO_INCREMENT`，`UNIQUE KEY`，`KEY`，`CHARACTER SET/CHARSET`，`COLLATE`，`ROW_FORMAT`，`USING BTREE`，`LOCK TABLE`，`SET SYSTEM_VARIABLE`，`ENGINE`。

* 暂不支持的数据类型：如果你使用的数据类型为 `TEXT` 或 `BLOB` 类型，那么你可以将它们修改为存储大小合适的 `VARCHAR` 类型。

* 有限的支持：如果你使用复合主键，需将复合主键修改为单一主键，或删除单一主键。

下面示例是一个典型的 `mysqldump` 表：

```
CREATE TABLE `roles` (
	`role_id` int(11) NOT NULL AUTO_INCREMENT,
	`role_name` varchar(128) DEFAULT NULL,
	`state` int(11) DEFAULT NULL,
	`company_id` int(11) DEFAULT NULL,
	`remark` varchar(1024) DEFAULT NULL,
	`created_by` text DEFAULT NULL,
	`created_time` datetime DEFAULT NULL,
	`updated_by` varchar(64) DEFAULT NULL,
	`updated_time` datetime DEFAULT NULL,
	`is_deleted` int(11) DEFAULT '0',
	`deleted_by` varchar(64) DEFAULT NULL,
	`deleted_time` datetime DEFAULT NULL,
	PRIMARY KEY (`role_id`),
	UNIQUE KEY(`state`),
	KEY `idx_company_state` (`company_id`,`state`)
) ENGINE=InnoDB AUTO_INCREMENT=395 DEFAULT CHARSET=utf8;
```

在 MatrixOne 中成功创建这个表，它需要被修改为:

```
CREATE TABLE roles (
	role_id int(11) NOT NULL,
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

### 导入至 MatrixOne

转储的 *.sql* 文件修改完成之后，就可以将整个表结构和数据导入到 MatrixOne 中。

1. 打开 MySQL 终端并连接到 MatrixOne。

2. 通过 `source` 命令将 *.sql* 文件导入 MatrixOne。

```
mysql> source '/YOUR_PATH/a.sql'
```

如果 *.sql* 文件较大，可以使用如下命令在后台运行导入任务：

```
nohup mysql -h 127.0.0.1 -P 6001 -udump -p111 -e 'source /YOUR_PATH/a.sql' &
```
