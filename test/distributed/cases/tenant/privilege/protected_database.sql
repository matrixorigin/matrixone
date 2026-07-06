set global enable_privilege_cache = off;

drop database if exists protected_bvt_sys;
drop database if exists protected_bvt_sys_normal;
drop database if exists protected_bvt_sys_new;
drop user if exists protected_bvt_sys_user;
drop role if exists protected_bvt_sys_writer;
set global protected_databases = 'protected_bvt_sys,protected_bvt_sys_new';
select @@global.protected_databases;
create database protected_bvt_sys;
create table protected_bvt_sys.t1(a int primary key, b int);
insert into protected_bvt_sys.t1 values(1, 1);
create role protected_bvt_sys_writer;
grant create database,drop database on account * to protected_bvt_sys_writer;
grant all on database protected_bvt_sys to protected_bvt_sys_writer;
grant ownership on database protected_bvt_sys to protected_bvt_sys_writer;
grant all on table protected_bvt_sys.* to protected_bvt_sys_writer with grant option;
grant ownership on table protected_bvt_sys.t1 to protected_bvt_sys_writer;
create user protected_bvt_sys_user identified by '111' default role protected_bvt_sys_writer;
-- @session:id=3&user=sys:protected_bvt_sys_user:protected_bvt_sys_writer&password=111
set enable_privilege_cache = off;
select a from protected_bvt_sys.t1;
create database protected_bvt_sys_normal;
drop database protected_bvt_sys_normal;
set global protected_databases = 'protected_bvt_sys_user_try';
create database protected_bvt_sys_new;
create table protected_bvt_sys.t2(a int);
insert into protected_bvt_sys.t1 values(2, 2);
drop database protected_bvt_sys;
-- @session
drop user if exists protected_bvt_sys_user;
drop role if exists protected_bvt_sys_writer;
drop database protected_bvt_sys;
set global protected_databases = 'protected_bvt_unused';

drop account if exists protected_bvt_acc;
create account protected_bvt_acc ADMIN_NAME 'admin' IDENTIFIED BY '111';

-- @session:id=1&user=protected_bvt_acc:admin&password=111
set enable_privilege_cache = off;
set global protected_databases = 'protected_bvt_db,protected_bvt_new,protected_bvt_clone,CamelDB';
set global protected_databases = '';
set global protected_databases = ',';
select @@global.protected_databases;
set global protected_databases = 'protected_bvt_db,protected_bvt_new,protected_bvt_clone,CamelDB';
create database protected_bvt_db;
create database protected_bvt_later;
create table protected_bvt_db.t1(a int primary key, b int);
insert into protected_bvt_db.t1 values(1, 1);
create index protected_bvt_idx on protected_bvt_db.t1(b);
create view protected_bvt_db.v1 as select * from protected_bvt_db.t1;
create table protected_bvt_db.pk_single(id int primary key, v varchar(10));
insert into protected_bvt_db.pk_single values(1, 'a');
create table protected_bvt_db.pk_compound(k1 int, k2 int, v varchar(10), primary key(k1, k2));
insert into protected_bvt_db.pk_compound values(1, 1, 'a');
create table protected_bvt_db.auto_pk(id int auto_increment primary key, v varchar(10));
insert into protected_bvt_db.auto_pk(v) values('a');
create table protected_bvt_db.uk_table(id int primary key, email varchar(20), unique key uk_email(email), key idx_email(email));
insert into protected_bvt_db.uk_table values(1, 'a@b');
create table protected_bvt_db.part_hash(id int, v varchar(10)) partition by hash(id) partitions 4;
insert into protected_bvt_db.part_hash values(1, 'a');
create table protected_bvt_db.part_range(id int, v varchar(10)) partition by range(id)(
    partition p0 values less than (10),
    partition p1 values less than (maxvalue)
);
insert into protected_bvt_db.part_range values(1, 'a');
create table protected_bvt_db.fk_parent(id int primary key, name varchar(10));
create table protected_bvt_db.fk_child(id int primary key, parent_id int, constraint fk_child_parent foreign key(parent_id) references protected_bvt_db.fk_parent(id));
insert into protected_bvt_db.fk_parent values(1, 'p1');
insert into protected_bvt_db.fk_child values(1, 1);
create table protected_bvt_db.fk_self(id int primary key, parent_id int, constraint fk_self_parent foreign key(parent_id) references protected_bvt_db.fk_self(id));
create table protected_bvt_later.t1(a int);
insert into protected_bvt_later.t1 values(10);
create role protected_bvt_writer;
grant create database,drop database on account * to protected_bvt_writer;
grant all on database protected_bvt_db to protected_bvt_writer;
grant ownership on database protected_bvt_db to protected_bvt_writer;
grant all on table protected_bvt_db.* to protected_bvt_writer with grant option;
grant ownership on table protected_bvt_db.t1 to protected_bvt_writer;
grant all on database protected_bvt_later to protected_bvt_writer;
grant all on table protected_bvt_later.* to protected_bvt_writer;
grant connect on account * to protected_bvt_writer;

create user protected_bvt_user identified by '111' default role protected_bvt_writer;

-- @session:id=1&user=protected_bvt_acc:admin&password=111
set enable_privilege_cache = off;
set global protected_databases = 'protected_bvt_db,protected_bvt_new,protected_bvt_clone,CamelDB,protected_bvt_later';
-- @session

-- @session:id=4&user=protected_bvt_acc:protected_bvt_user:protected_bvt_writer&password=111
set enable_privilege_cache = off;
select @@global.protected_databases;
select database();
use protected_bvt_later;
select a from t1;
-- @session

-- @session:id=2&user=protected_bvt_acc:protected_bvt_user:protected_bvt_writer&password=111
set enable_privilege_cache = off;
use protected_bvt_db;
show tables;
select a from t1;
select a from protected_bvt_db.t1;
select a from protected_bvt_db.v1;
-- @session

-- @session:id=1&user=protected_bvt_acc:admin&password=111
set enable_privilege_cache = off;
create sequence protected_bvt_db.s1;
create function protected_bvt_db.f1(a int) returns int language sql as '$1 + 1';
create procedure protected_bvt_db.p1() 'begin select 1; end';
-- @session

-- @session:id=2&user=protected_bvt_acc:protected_bvt_user:protected_bvt_writer&password=111
set enable_privilege_cache = off;
create database protected_bvt_normal;
drop database protected_bvt_normal;
create database protected_bvt_new;
create table protected_bvt_db.t2(a int);
create external table protected_bvt_db.ext1(a int) infile{"filepath"='$resources/external_table_file/extable.csv'} fields terminated by ',' lines terminated by '\n';
create dynamic table protected_bvt_db.dt1 as select * from protected_bvt_db.t1;
create view protected_bvt_db.v2 as select * from protected_bvt_db.t1;
alter view protected_bvt_db.v1 as select a from protected_bvt_db.t1;
drop view protected_bvt_db.v1;
alter database protected_bvt_db set mysql_compatibility_mode = '0.8.0';
alter table protected_bvt_db.t1 add column c int;
rename table protected_bvt_db.t1 to protected_bvt_db.t2;
create index protected_bvt_idx2 on protected_bvt_db.t1(b);
drop index protected_bvt_idx on protected_bvt_db.t1;
create table protected_bvt_db.t_clone clone protected_bvt_db.t1;
create database protected_bvt_clone clone protected_bvt_db;
create sequence protected_bvt_db.s2;
alter sequence protected_bvt_db.s1 increment by 2;
drop sequence protected_bvt_db.s1;
create function protected_bvt_db.f2(a int) returns int language sql as '$1 + 1';
drop function protected_bvt_db.f1(int);
create procedure protected_bvt_db.p2() 'begin select 1; end';
drop procedure protected_bvt_db.p1;
create connector for protected_bvt_db.conn1 with ("type"='kafka', "topic"='t1', "partition"='0', "value"='json', "bootstrap.servers"='127.0.0.1:9092');
insert into protected_bvt_db.t1 values(2, 2);
replace into protected_bvt_db.t1 values(2, 2);
update protected_bvt_db.t1 set a = 3 where a = 1;
delete from protected_bvt_db.t1 where a = 1;
insert into protected_bvt_db.pk_single values(2, 'b');
update protected_bvt_db.pk_compound set v = 'b' where k1 = 1 and k2 = 1;
insert into protected_bvt_db.auto_pk(v) values('b');
insert into protected_bvt_db.uk_table values(2, 'c@d');
insert into protected_bvt_db.part_hash values(2, 'b');
truncate table protected_bvt_db.part_range;
insert into protected_bvt_db.fk_child values(2, 1);
delete from protected_bvt_db.fk_parent where id = 1;
alter table protected_bvt_db.fk_child drop foreign key fk_child_parent;
drop table protected_bvt_db.fk_child;
insert into protected_bvt_db.fk_self values(2, 1);
truncate table protected_bvt_db.t1;
drop table protected_bvt_db.t1;
drop database protected_bvt_db;

-- @session:id=4&user=protected_bvt_acc:protected_bvt_user:protected_bvt_writer&password=111
set enable_privilege_cache = off;
select @@global.protected_databases;
select database();
create table t2(a int);
drop table t1;
create table t_ctas as select * from t1;
-- @session

-- @session:id=2&user=protected_bvt_acc:protected_bvt_user:protected_bvt_writer&password=111
set enable_privilege_cache = off;
insert into protected_bvt_later.t1 values(11);
drop database protected_bvt_later;
create database `CamelDB`;
create database `cameldb`;
drop database if exists `cameldb`;
-- @session

-- @session:id=1&user=protected_bvt_acc:admin&password=111
set enable_privilege_cache = off;
set global lower_case_table_names = 0;
set global protected_databases = 'ProtectedCaseDB';
-- @session

-- @session:id=3&user=protected_bvt_acc:protected_bvt_user:protected_bvt_writer&password=111
set enable_privilege_cache = off;
select @@lower_case_table_names;
create database `protectedcasedb`;
drop database `protectedcasedb`;
create database `ProtectedCaseDB`;
drop database `ProtectedCaseDB`;
-- @session

-- @session:id=1&user=protected_bvt_acc:admin&password=111
set enable_privilege_cache = off;
set global lower_case_table_names = 1;
drop user if exists protected_bvt_user;
drop role if exists protected_bvt_writer;
drop database protected_bvt_db;
drop database protected_bvt_later;
drop database if exists `CamelDB`;
set global protected_databases = 'protected_bvt_unused';
-- @session

set global lower_case_table_names = 1;
drop account protected_bvt_acc;
set global enable_privilege_cache = on;
