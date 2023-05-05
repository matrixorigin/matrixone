create database sys_db_1;
use sys_db_1;
create table sys_tbl_1(a int primary key );
insert into sys_tbl_1 values(1),(2),(3);
create publication sys_pub_1 database sys_db_1;
set global syspublications = "sys_pub_1";
create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=1&user=acc_idx:root&password=123456
show subscriptions;
-- @session
drop account acc_idx;
drop publication sys_pub_1;
drop database sys_db_1;

create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=2&user=acc_idx:root&password=123456
create database db7;
-- @session
create publication pubname7 database db7 account acc_idx;
drop account acc_idx;