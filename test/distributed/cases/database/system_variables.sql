-- @suit

-- @case
-- @desc:test for Some System variables and new variables like ERRORS, INDEXES and so on
-- @label:bvt

-- auto_increment_increment
show variables like 'auto%';
show variables like 'auto_increment_increment';
set auto_increment_increment = 2;
show variables like 'auto_increment_increment';
set auto_increment_increment = 1+1;
show variables like 'auto_increment_increment';
set auto_increment_increment = 2*3;
show variables like 'auto_increment_increment';

-- init_connect
show variables like 'init%';
show variables like 'init_connect';

-- interactive_timeout
show variables like 'interactive%';
show variables like 'interactive_timeout';
set interactive_timeout = 36600;
show variables like 'interactive_timeout';
set interactive_timeout = 30000+100;
show variables like 'interactive_timeout';
set global interactive_timeout = 30000+100;
show variables like 'interactive_timeout';

-- lower_case_table_names, this is a system variable, read only
show variables like 'lower%';
show variables like 'lower_case_table_names';

-- net_write_timeout
show variables like 'net_write_timeout';
set net_write_timeout = 70;
show variables like 'net_write_timeout';
set net_write_timeout = 20*20;
show variables like 'net_write_timeout';
set net_write_timeout = 60;
show variables like 'net_write_timeout';

-- system_time_zone, this is a system variable, read only
show variables like 'system%';
show variables like 'system_time_zone';

-- transaction_isolation, enum type
show variables like 'trans%';
show variables like 'transaction_isolation';

-- wait_timeout
show variables like 'wait%';
show variables like 'wait_timeout';
set wait_timeout = 33600;
show variables like 'wait_timeout';
set wait_timeout = 10;
show variables like 'wait_timeout';


drop table if exists t;
create table t(
                  a int,
                  b int,
                  c int,
                  primary key(a)
);
show indexes from t;

create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=1&user=acc_idx:root&password=123456
create database db1;
use db1;
drop table if exists t;
create table t(
                  a int,
                  b int,
                  c int,
                  primary key(a)
);
show indexes from t;
drop database db1;
-- @session
drop account acc_idx;


-- Support More System Views
use information_schema;
show tables;
desc key_column_usage;
select table_name, column_name from key_column_usage limit 2;
desc columns;
select table_name, column_name from columns where table_schema = 'mo_catalog' order by table_name, column_name limit 5;
desc profiling;
select seq, state from profiling;

desc `PROCESSLIST`;
select * from `PROCESSLIST` limit 2;

desc user_privileges;
select grantee, table_catalog from user_privileges limit 2;
desc schemata;
select catalog_name, schema_name from schemata where schema_name = 'mo_catalog' or schema_name = 'mo_task' order by catalog_name, schema_name;
desc character_sets;
select character_set_name, description, maxlen from character_sets limit 5;
desc triggers;
select trigger_name, action_order from triggers limit 3;

use mysql;
desc user;
select host, user from user limit 2;
desc db;
select db, user from db limit 5;
desc procs_priv;
select routine_name, routine_type from procs_priv limit 5;
desc columns_priv;
select table_name, column_name from columns_priv limit 5;
desc tables_priv;
select host, table_name from tables_priv limit 5;

-- sql_select_limit
show variables like 'sql_select_limit';
set sql_select_limit = 100000;
show variables like 'sql_select_limit';
set sql_select_limit = 1;
show variables like 'sql_select_limit';
SET SQL_SELECT_LIMIT = Default;
show variables like 'sql_select_limit';

--int type
show variables like 'max_allowed_packet';
set max_allowed_packet = 10000;
show variables like 'max_allowed_packet';
set max_allowed_packet = default;
show variables like 'max_allowed_packet';

show variables like 'wait_timeout';
set wait_timeout = 10000;
show variables like 'wait_timeout';
set wait_timeout = default;
show variables like 'wait_timeout';

--string type
show variables like 'character_set_results';
set character_set_server = default;
show variables like 'character_set_results';

show variables like 'character_set_server';
set character_set_server = default;
show variables like 'character_set_server';

--enum type
show variables like 'transaction_isolation';
set transaction_isolation = default;
show variables like 'transaction_isolation';

show variables like 'tx_isolation';
set tx_isolation = default;
show variables like 'tx_isolation';

select @@sql_mode;
set @@sql_mode = ONLY_FULL_GROUP_BY;
select @@sql_mode;
set @@sql_mode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES";
select @@sql_mode;
set @@sql_mode = default;
select @@sql_mode;

drop database if exists test;
create database test;
select `configuration` from mo_catalog.mo_mysql_compatbility_mode where dat_name ="test";
drop database test;
select `configuration` from mo_catalog.mo_mysql_compatbility_mode where dat_name ="test";

drop database if exists test;
create database test;
select `configuration` from mo_catalog.mo_mysql_compatbility_mode where dat_name ="test";
alter database test set mysql_compatbility_mode = '{"version_compatibility": "8.0.30-MatrixOne-v0.7.0"}';
select `configuration` from mo_catalog.mo_mysql_compatbility_mode where dat_name ="test";
drop database test;

drop database if exists test;
create database test;
use test;
select version();
alter database test set mysql_compatbility_mode = '{"version_compatibility": "8.0.30-MatrixOne-v0.7.0"}';
select version();
drop database test;

create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=abc:admin&password=123456
drop database if exists test;
drop database if exists test1;
create database test;
create database test1;
use test;
select version();
alter database test set mysql_compatbility_mode = '{"version_compatibility": "8.0.30-MatrixOne-v0.7.0"}';
select version();
use test1;
select version();
alter account config abc set mysql_compatbility_mode = '{"version_compatibility": "0.7"}';
select version();
use test1;
select version();
drop database test;
drop database test1;
-- @session