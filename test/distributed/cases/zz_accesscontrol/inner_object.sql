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

-- sys and non sys account admin user information_schema:columns，schemata,tables，views，partitions isolation
create account ac_1 ADMIN_NAME 'admin' IDENTIFIED BY '111';
create database sys_db1;
create table sys_db1.sys_t1(c1 char);
create view sys_db1.sys_v1  as select * from sys_db1.sys_t1;
create table sys_db1.test01 (
emp_no      int             not null,
birth_date  date            not null,
first_name  varchar(14)     not null,
last_name   varchar(16)     not null,
gender      varchar(5)      not null,
hire_date   date            not null,
primary key (emp_no)
) partition by range columns (emp_no)(
partition p01 values less than (100001),
partition p02 values less than (200001),
partition p03 values less than (300001),
partition p04 values less than (400001)
);
-- @session:id=3&user=ac_1:admin&password=111
create database ac_db;
create table ac_db.ac_t1(c1 int);
create view ac_db.ac_v1  as select * from ac_db.ac_t1;
create table ac_db.test02 (
emp_no      int             not null,
birth_date  date            not null,
first_name  varchar(14)     not null,
last_name   varchar(16)     not null,
gender      varchar(5)      not null,
hire_date   date            not null,
primary key (emp_no)
) partition by range columns (emp_no)(
partition p01 values less than (100001),
partition p02 values less than (200001),
partition p03 values less than (300001),
partition p04 values less than (400001)
);
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_schema="ac_db" and table_name='ac_t1';
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_schema="ac_db" and table_name='test02';
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_schema="sys_db1";
select count(*),table_name, column_name  from information_schema.columns group by table_name, column_name having count(*)>1;
select * from information_schema.schemata where schema_name='ac_db';
select * from information_schema.schemata where schema_name='sys_db1';
select count(*),schema_name from information_schema.schemata group by schema_name having count(*)>1;
select table_schema,table_name  from information_schema.tables where table_name='sys_t1';
select table_schema,table_name from information_schema.tables where table_name='ac_t1';
select count(*),table_name from information_schema.tables group by table_name having count(*) >1;
select * from information_schema.views where table_name='ac_v1';
select * from information_schema.views where table_name='sys_v1';
select count(*),table_name from information_schema.views group by table_name having count(*)>1;
select count(*) from information_schema.partitions where table_schema='ac_db' and table_name='test02';
select table_schema,table_name,partition_name  from information_schema.partitions where table_schema='sys_db1';
select count(*),table_schema,table_name,partition_name  from information_schema.partitions group by table_schema,table_name,partition_name having count(*) >1;
-- @session
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_name='ac_t1';
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_name='sys_t1';
select count(*),table_name, column_name  from information_schema.columns group by table_name, column_name having count(*)>1;
select * from information_schema.schemata where schema_name='ac_db';
select * from information_schema.schemata where schema_name='sys_db1';
select count(*),schema_name from information_schema.schemata group by schema_name having count(*)>1;
select table_schema,table_name from information_schema.tables where table_name='sys_t1';
select table_schema,table_name from information_schema.tables where table_name='ac_t1';
select count(*),table_name from information_schema.tables group by table_name having count(*) >1;
select * from information_schema.views where table_name='sys_v1';
select * from information_schema.views where table_name='ac_v1';
select count(*),table_name from information_schema.views group by table_name having count(*)>1;
select count(*) from information_schema.partitions where table_schema='sys_db1' and table_name='test01';
select table_schema,table_name from information_schema.partitions where table_schema='ac_db';
select count(*),table_schema,table_name,partition_name  from information_schema.partitions group by table_schema,table_name,partition_name having count(*) >1;

-- sys and non sys account non admin user information_schema:columns，schemata,tables，views，partitions isolation
create user 'sys_user' identified by '123456';
create role 'sys_role';
grant  all on account *  to 'sys_role';
grant OWNERSHIP on database *.* to sys_role;
grant select on table *.* to sys_role;
grant sys_role to sys_user;
-- @session:id=3&user=ac_1:admin&password=111
create user 'ac_user' identified by '123456';
create role 'ac_role';
grant  all on account *  to 'ac_role';
grant OWNERSHIP on database *.* to ac_role;
grant select on table *.* to ac_role;
grant ac_role to ac_user;
-- @session
-- @session:id=4&user=sys:sys_user:sys_role&password=123456
create database user_db;
create table user_db.user_t1(c1 int,c2 varchar);
create view user_db.sysuser_v1  as select * from user_db.user_t1;
create table user_db.test02 (
emp_no      int             not null,
birth_date  date            not null,
first_name  varchar(14)     not null,
last_name   varchar(16)     not null,
gender      varchar(5)      not null,
hire_date   date            not null,
primary key (emp_no)
) partition by range columns (emp_no)(
partition p01 values less than (100001),
partition p02 values less than (200001),
partition p03 values less than (300001),
partition p04 values less than (400001)
);
-- @session
-- @session:id=5&user=ac_1:ac_user:ac_role&password=123456
create database acuser_db;
create table acuser_db.acuser_t1(c1 int,c2 varchar);
create view acuser_db.acuser_v1  as select * from acuser_db.acuser_t1;
create table acuser_db.test (
emp_no      int             not null,
birth_date  date            not null,
first_name  varchar(14)     not null,
last_name   varchar(16)     not null,
gender      varchar(5)      not null,
hire_date   date            not null,
primary key (emp_no)
) partition by range columns (emp_no)(
partition p01 values less than (100001),
partition p02 values less than (200001),
partition p03 values less than (300001),
partition p04 values less than (400001)
);
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_schema="acuser_db" and table_name='acuser_t1';
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_schema="user_db";
select count(*),table_name, column_name  from information_schema.columns group by table_name, column_name having count(*)>1;
select * from information_schema.schemata where schema_name='acuser_db';
select * from information_schema.schemata where schema_name='user_db1';
select count(*),schema_name from information_schema.schemata group by schema_name having count(*)>1;
select table_schema,table_name from information_schema.tables where table_name='user_t1';
select table_schema,table_name from information_schema.tables where table_name='acuser_t1';
select count(*),table_name from information_schema.tables group by table_name having count(*) >1;
select table_schema,table_name from information_schema.views where table_name='acuser_v1';
select table_schema,table_name from information_schema.views where table_name='sysuser_v1';
select count(*),table_name from information_schema.views group by table_name having count(*)>1;
select table_schema,table_name,partition_name from information_schema.partitions where table_schema='acuser_db';
select table_schema,table_name,partition_name from information_schema.partitions where table_schema='user_db';
select count(*),table_schema,table_name,partition_name  from information_schema.partitions group by table_schema,table_name,partition_name having count(*) >1;
-- @session
-- @session:id=4&user=sys:sys_user:sys_role&password=123456
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_schema="user_db" and table_name='user_t1';
select table_catalog,table_schema,table_name,column_name from information_schema.columns where table_schema="acuser_db";
select count(*),table_name, column_name  from information_schema.columns group by table_name, column_name having count(*)>1;
select * from information_schema.schemata where schema_name='acuser_db';
select * from information_schema.schemata where schema_name='user_db';
select count(*),schema_name from information_schema.schemata group by schema_name having count(*)>1;
select table_schema,table_name from information_schema.tables where table_name='user_t1';
select table_schema,table_name from information_schema.tables where table_name='acuser_t1';
select count(*),table_name from information_schema.tables group by table_name having count(*) >1;
select table_schema,table_name from information_schema.views where table_name='acuser_v1';
select table_schema,table_name from information_schema.views where table_name='sysuser_v1';
select count(*),table_name from information_schema.views group by table_name having count(*)>1;
select table_schema,table_name,partition_name from information_schema.partitions where table_schema='acuser_db';
select table_schema,table_name,partition_name from information_schema.partitions where table_schema='user_db';
select count(*),table_schema,table_name,partition_name  from information_schema.partitions group by table_schema,table_name,partition_name having count(*) >1;
-- @session

drop database sys_db1;
drop database user_db;
drop account ac_1;
drop user sys_user;
drop role sys_role;

drop database if exists etao;
create database etao;
use etao;
CREATE TABLE `users` (
  `id` VARCHAR(128) NOT NULL,
  `username` VARCHAR(255) DEFAULT NULL,
  `password` VARCHAR(512) DEFAULT NULL,
  `user_status` TINYINT DEFAULT NULL,
  `user_role` TINYINT DEFAULT NULL,
  `created_at` DATETIME DEFAULT NULL,
  `updated_at` DATETIME DEFAULT NULL,
  `user_source` TINYINT DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_mocadmin_users_username` (`username`)
);
SELECT column_name, column_default, is_nullable = 'YES', data_type, character_maximum_length, column_type, column_key, extra, column_comment, numeric_precision, numeric_scale , datetime_precision FROM information_schema.columns WHERE table_schema = 'etao' AND table_name = 'users' ORDER BY ORDINAL_POSITION;
drop database etao;

drop account if exists acc1;
create account acc1 admin_name 'root' identified by '111';
-- @session:id=6&user=acc1:root&password=111
drop database if exists etao;
create database etao;
use etao;
CREATE TABLE `users` (
  `id` VARCHAR(128) NOT NULL,
  `username` VARCHAR(255) DEFAULT NULL,
  `password` VARCHAR(512) DEFAULT NULL,
  `user_status` TINYINT DEFAULT NULL,
  `user_role` TINYINT DEFAULT NULL,
  `created_at` DATETIME DEFAULT NULL,
  `updated_at` DATETIME DEFAULT NULL,
  `user_source` TINYINT DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_mocadmin_users_username` (`username`)
);
SELECT column_name, column_default, is_nullable = 'YES', data_type, character_maximum_length, column_type, column_key, extra, column_comment, numeric_precision, numeric_scale , datetime_precision FROM information_schema.columns WHERE table_schema = 'etao' AND table_name = 'users' ORDER BY ORDINAL_POSITION;
drop database etao;
-- @session
drop account acc1;
