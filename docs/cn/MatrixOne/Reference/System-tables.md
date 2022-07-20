# MatrixOne Catalog

MatrixOne Catalog 是 MatrixOne 存储系统信息的组件。你可以通过 `mo_catalog` 数据库访问系统信息。`mo_catalog` 数据库由 `MatrixOne` 在初始化时创建。

在 0.5.0 中，`mo_catalog` 还未完全实现，仅支持查看数据库和数据表的基本信息，暂不支持将值写入 `mo_catalog` 。

在 `mo_catalog` 数据库中有三个表:

## mo_database 表

mo_database 表中包含数据库信息。

* 表结构：

| 属性       | 类型          | 主键 | 描述   |
| ---------------- | ------------- | ----------- | ------------- |
| datname          | varchar(256)  | Primary key | database name |
| dat_catalog_name | varchar(256)  |             | catalog name  |
| dat_createsql    | varchar(4096) |             | create sql    |

* 表初始行

在初始化时，`mo_database` 有一些默认行。

| datname            | dat_catalog_name | dat_createsql                                    |
| ------------------ | ---------------- | ------------------------------------------------ |
| mo_catalog         | def              | hardcode                                         |
| information_schema | def              | create database if not exists information_schema |
| system_metrics     | def              |                                                  |

## mo_tables 表

mo_tables 表包含表、索引、视图等信息。

* 表结构：

| 属性     | 类型         | 主键 | 描述                                                  |
| -------------- | ------------- | ----------- | ------------------------------------------------------------ |
| relname        | varchar(256)  | PK          | Name of the table, index, view, etc.                         |
| reldatabase    | varchar(256)  | PK,FK       | The database that contains this relation. reference mo_database.datname |
| relpersistence | char(1)       |             | p = permanent table, t = temporary table                     |
| relkind        | char(1)       |             | r = ordinary table, i = index, S = sequence, v = view, m = materialized view |
| rel_comment    | varchar(1024) |             | comment                                                      |
| rel_createsql  | varchar(4096) |             | create sql                                                   |

* 表初始行：

在初始化时，`mo_tables` 有一些默认行。

| relname             | reldatabase        | repersistence | relkind | rel_createsql | rel_comment                                            |
| ------------------- | ------------------ | ------------- | ------- | ------------- | ------------------------------------------------------ |
| mo_database         | mo_catalog         | 'p'           | 'r'     | tae hardcode  | databases                                              |
| mo_tables           | mo_catalog         | 'p'           | 'r'     | tae hardcode  | tables                                                 |
| mo_columns          | mo_catalog         | 'p'           | 'r'     | tae hardcode  | columns                                                |
| mo_global_variables | mo_catalog         | 'p'           | 'r'     | create sql    | system variables                                       |
| mo_user             | mo_catalog         | 'p'           | 'r'     | create sql    | user                                                   |
| schemata            | information_schema | 'p'           | 'v'     | create sql    | schemas                                                |
| tables              | information_schema | 'p'           | 'v'     | create sql    | tables                                                 |
| columns             | information_schema | 'p'           | 'v'     | create sql    | columns                                                |
| key_column_usage    | information_schema | 'p'           | 'v'     | create sql    | columns related to unique, primary key, or foreign key |
| views               | information_schema | 'p'           | 'v'     | create sql    | views                                                  |

## mo_columns table

mo_columns 表包含表属性信息。

* 表结构：

| 属性            | 类型          | 主键 | 描述                                                 |
| --------------------- | ------------- | ----------- | ------------------------------------------------------------ |
| att_database          | varchar(256)  | PK          | database                                                     |
| att_relname           | varchar(256)  | PK,UK       | The table this column belongs to.(references mo_tables.relname) |
| attname               | varchar(256)  | PK          | The column name                                              |
| atttyp                | int           |             | The data type of this column (zero for a dropped column).    |
| attnum                | int           | UK          | The number of the column. Ordinary columns are numbered from 1 up. |
| att_length            | int           |             | bytes count for the type.                                    |
| attnotnull            | tinyint(1)    |             | This represents a not-null constraint.                       |
| atthasdef             | tinyint(1)    |             | This column has a default expression or generation expression. |
| att_default           | varchar(1024) |             | default expression                                           |
| attisdropped          | tinyint(1)    |             | This column has been dropped and is no longer valid. A dropped column is still physically present in the table, but is ignored by the parser and so cannot be accessed via SQL. |
| att_constraint_type   | char(1)       |             | p = primary key constraint, n=no constraint                  |
| att_is_unsigned       | tinyint(1)    |             | unsigned or not                                              |
| att_is_auto_increment | tinyint       |             | auto increment or not                                        |
| att_comment           | varchar(1024) |             | comment                                                      |
| att_is_hidden         | tinyint(1)    |             | hidden or not                                                |

* 表初始行：

| att_database | att_relname | attname               | atttyp  | attnum | attnotnull | atthasdef | att_default | attisdropped | att_constraint_type | att_is_unsigned | att_is_auto_increment | att_comment     | att_is_hidden |
| ------------ | ----------- | --------------------- | ------- | ------ | ---------- | --------- | ----------- | ------------ | ------------------- | --------------- | --------------------- | --------------- | ------------- |
| mo_catalog   | mo_database | datname               | varchar | 0      | 1          | 0         | ''          | 0            | 'p'                 | 0               | 0                     | 'database name' | 0             |
| mo_catalog   | mo_database | dat_catalog_name      | varchar | 1      | 1          | 0         | ''          | 0            | 'p'                 | 0               | 0                     | 'catalog'       | 0             |
| mo_catalog   | mo_database | dat_createsql         | varchar | 2      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_tables   | relname               | varchar | 0      | 1          |           |             |              | 'p'                 |                 |                       |                 |               |
| mo_catalog   | mo_tables   | reldatabase           | varchar | 1      | 1          |           |             |              | 'p'                 |                 |                       |                 |               |
| mo_catalog   | mo_tables   | relpersistence        | char    | 2      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_tables   | relkind               | char    | 3      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_tables   | rel_createsql         | varchar | 4      | 0          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_tables   | rel_comment           | varchar | 5      | 0          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | att_database          | varchar | 0      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | att_relname           | varchar | 1      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | attname               | varchar | 2      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | atttyp                | int     | 3      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | attnum                | int     | 4      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | attnotnull            | tinyint | 5      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | atthasdef             | tinyint | 6      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | att_default           | varchar | 7      | 0          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | attisdropped          | tinyint | 8      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | att_constraint_type   | char    | 9      | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | att_is_unsigned       | tinyint | 10     | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | att_is_auto_increment | tinyint | 11     | 1          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | att_comment           | varchar | 12     | 0          |           |             |              |                     |                 |                       |                 |               |
| mo_catalog   | mo_columns  | att_is_hidden         | tinyint | 13     | 1          |           |             |              |                     |                 |                       |                 |               |
| ...          |             |                       |         |        |            |           |             |              |                     |                 |                       |                 |               |

## mo_global_variables 表

mo_global_variables 表包含系统变量信息。

* 表结构：

| Attributes        | Type          | Primary key | Description    |
| ----------------- | ------------- | ----------- | -------------- |
| gv_variable_name  | varchar(256)  | PK          | variable name  |
| gv_variable_value | varchar(1024) |             | variable value |

* 表初始行：

| gv_variable_name    | gv_variable_value |
| ------------------- | ----------------- |
| gv_variable_name    | gv_variable_value |
| max_allowed_packet  | 67108864          |
| version_comment     | MatrixOne         |
| port                | 6001              |
| host                | 0.0.0.0           |
| storePath           | ./store           |
| batchSizeInLoadData | 40000             |
| ...                 |                   |

## mo_user 表

mo_user 表包含用户账户信息。

* 表结构：

| Attributes             | Type          | Primary key | Description |
| ---------------------- | ------------- | ----------- | ----------- |
| user_host              | varchar(256)  | PK          | user host   |
| user_name              | varchar(256)  | PK          | user name   |
| Select_priv            | char          |             | 'N','Y'     |
| Insert_priv            | char          |             | 'N','Y'     |
| Update_priv            | char          |             | 'N','Y'     |
| Delete_priv            | char          |             | 'N','Y'     |
| Create_priv            | char          |             | 'N','Y'     |
| Drop_priv              | char          |             | 'N','Y'     |
| Reload_priv            | char          |             | 'N','Y'     |
| Shutdown_priv          | char          |             | 'N','Y'     |
| Process_priv           | char          |             | 'N','Y'     |
| File_priv              | char          |             | 'N','Y'     |
| Grant_priv             | char          |             | 'N','Y'     |
| References_priv        | char          |             | 'N','Y'     |
| Index_priv             | char          |             | 'N','Y'     |
| Alter_priv             | char          |             | 'N','Y'     |
| Show_db_priv           | char          |             | 'N','Y'     |
| Super_priv             | char          |             | 'N','Y'     |
| Create_tmp_table_priv  | char          |             | 'N','Y'     |
| Lock_tables_priv       | char          |             | 'N','Y'     |
| Execute_priv           | char          |             | 'N','Y'     |
| Repl_slave_priv        | char          |             | 'N','Y'     |
| Repl_client_priv       | char          |             | 'N','Y'     |
| Create_view_priv       | char          |             | 'N','Y'     |
| Show_view_priv         | char          |             | 'N','Y'     |
| Create_routine_priv    | char          |             | 'N','Y'     |
| Alter_routine_priv     | char          |             | 'N','Y'     |
| Create_user_priv       | char          |             | 'N','Y'     |
| Event_priv             | char          |             | 'N','Y'     |
| Trigger_priv           | char          |             | 'N','Y'     |
| Create_tablespace_priv | char          |             | 'N','Y'     |
| max_questions          | int unsigned  |             |             |
| max_updates            | int unsigned  |             |             |
| max_connections        | int unsigned  |             |             |
| max_user_connections   | int unsigned  |             |             |
| authentication_string  | varchar(4096) |             | password    |
| account_locked         | char          |             | 'N','Y'     |
| Create_role_priv       | char          |             | 'N','Y'     |
| Drop_role_priv         | char          |             | 'N','Y'     |

* 表初始行：

| user_host | user_name | authentication_string |
| --------- | --------- | --------------------- |
| localhost | root      | ''                    |
| localhost | dump      | 111                   |
| ...       |           |                       |
