# From MySQL to MatrixOne

This document describes how to migrate data from MySQL to MatrixOne. 

### Dump MySQL data

We suppose you have full access to your MySQL instances. 

Firstly, we use `mysqldump` to dump MySQL table structures and data to a single file with the following command.

```
mysqldump -h IP_ADDRESS -uUSERNAME -pPASSWORD -d DB_NAME1 DB_NAME2 ... OUTPUT_FILE_NAME.SQL
```

For example, this following command dumps all table structures and data to a single file named `a.sql`.

```
mysqldump -h 127.0.0.1 -uroot -proot -d test a.sql
```

### Modify SQL file

The SQL file doesn't fully fit with MatrixOne yet. We'll need to remove and modify several element to adapt the SQL file to MatrixOne's format.

* Unsupported syntax or features need to be removed: AUTO_INCREMENT, UNIQUE KEY, KEY, CHARACTER SET/CHARSET, COLLATE, ROW_FORMAT, USING BTREE, LOCK TABLE, SET SYSTEM_VARIABLE, ENGINE.
* Unsupported data type: If you use TEXT or BLOB type, you can modify them to VARCHAR type, with a estimated size. 
* Limited support: If you use composite primary key, you should modify them to a single primary key or completely remove it.

We take a typical `mysqldump` table as an example:

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

To be able to successfully create this table in MatrixOne, it will be modifed as:

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

### Import into MatrixOne

Once your dumped SQL file was ready, you can import the whole table structures and data into MatrixOne. 

1. Open a MySQL terminal and connect to MatrixOne.
2. Import the SQL file into MatrixOne by the `source` command. 

```
mysql> source '/YOUR_PATH/a.sql'
```

If your SQL file is big, you can use the following command to run the import task in the background. For example:

```
nohup mysql -h 127.0.0.1 -P 6001 -udump -p111 -e 'source /YOUR_PATH/a.sql' &
```
