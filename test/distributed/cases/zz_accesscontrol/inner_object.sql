set global enable_privilege_cache = off;
-- env prepare statement
drop account if exists account1;
drop account if exists inner_account;
drop role if exists revoke_role_1;

--验证访问控制表中内置对象数据正确性
select user_name,owner from mo_catalog.mo_user where user_name="root";
select role_id,role_name,owner from mo_catalog.mo_role where role_name in ("moadmin","public");

--验证moadminaccount初始化，sys租户root下创建普通租户下管理员用户查看
create account account1 ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=account1:admin&password=123456
select role_id,role_name,owner from mo_catalog.mo_role;
show databases;
show grants;
use system;
show triggers;
use mo_catalog;
show columns from mo_tables;
select datname, dat_createsql from mo_database;
select relname from mo_tables where relname="sql_statement_total";
select relname from mo_tables where relname="mo_user";
select relname from mo_tables where relname="tables";
select user_name,authentication_string from mo_user;
select role_name from mo_role;
create database account_db;
use account_db;
show tables;
create table a(col int);
show create table a;
show tables;
-- @session

--public只有连接权限
-- @session:id=2&user=account1:admin:public&password=123456
show databases;
-- @session

--内置表不能增删改
update mo_catalog.mo_tables set relname='mo_aaaa';
insert into mo_catalog.mo_role values (1763,'apple',0,1,'2022-09-22 06:53:34','');
delete from mo_catalog.mo_user;
drop table mo_catalog.mo_account;
delete from mo_catalog.mo_user_grant;
delete from mo_catalog.mo_role_grant;
delete from mo_catalog.mo_role_privs;
delete from mo_catalog.mo_database;
delete from mo_catalog.mo_columns;
delete from mo_catalog.mo_indexes;
delete from mo_catalog.mo_table_partitions;

--内置数据库不能删除
drop database information_schema;
drop database mo_catalog;
drop database system;
drop database system_metrics;

--moadmin,public删除/回收
revoke moadmin,public from root;
select count(*) from mo_catalog.mo_role_privs where role_name in ('moadmin','public');
drop role if exists moadmin,public;
select role_name from mo_role where role_name in('moadmin','public');

--root/admin user修改/删除/授权
drop user if exists admin,root;

--accountadmin删除/回收,切换到普通account验证
create account inner_account ADMIN_NAME 'admin' IDENTIFIED BY '111';
-- @session:id=2&user=inner_account:admin&password=123456
revoke accountadmin from admin;
select count(*) from mo_catalog.mo_role_privs where role_name in ('accountadmin');
drop role if exists accountadmin;
select role_name from mo_catalog.mo_role where role_name in('accountadmin');
-- @session

create table tb1(
deptno int unsigned,
dname varchar(15),
loc varchar(50),
unique key(deptno)
);
select `name`,`type`,`name`,`is_visible`,`hidden`,`comment`,`column_name`,`ordinal_position`,`options` from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 'tb1');
desc mo_catalog.mo_indexes;

CREATE TABLE trp (
id INT NOT NULL,
fname VARCHAR(30),
lname VARCHAR(30),
hired DATE NOT NULL DEFAULT '1970-01-01',
separated DATE NOT NULL DEFAULT '9999-12-31',
job_code INT,
store_id INT
)
PARTITION BY RANGE ( YEAR(separated) ) (
	PARTITION p0 VALUES LESS THAN (1991),
	PARTITION p1 VALUES LESS THAN (1996),
	PARTITION p2 VALUES LESS THAN (2001),
	PARTITION p3 VALUES LESS THAN MAXVALUE
);
select tbl.relname, part.number, part.name, part.description_utf8, part.comment, part.options, part.partition_table_name
from mo_catalog.mo_tables tbl left join mo_catalog.mo_table_partitions part on tbl.rel_id = part.table_id
where tbl.relname = 'trp';
desc mo_catalog.mo_table_partitions;

--accountadmin删除/回收,切换到普通account验证
create account accx11 ADMIN_NAME 'admin' IDENTIFIED BY '111';
-- @session:id=2&user=accx11:admin&password=123456
select `name`,`type`,`name`,`is_visible`,`hidden`,`comment`,`column_name`,`ordinal_position`,`options` from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 'tb1');
desc mo_catalog.mo_indexes;
-- @session

drop account if exists account1;
drop account if exists inner_account;
drop account if exists accx11;
drop role if exists revoke_role_1;
set global enable_privilege_cache = on;
desc mo_catalog.mo_stages;