set global enable_privilege_cache = off;

drop database if exists protected_bvt_sys;
set global protected_databases = 'protected_bvt_sys';
create database protected_bvt_sys;
create table protected_bvt_sys.t1(a int);
drop table protected_bvt_sys.t1;
drop database protected_bvt_sys;
set global protected_databases = 'protected_bvt_unused';

drop account if exists protected_bvt_acc;
create account protected_bvt_acc ADMIN_NAME 'admin' IDENTIFIED BY '111';

-- @session:id=1&user=protected_bvt_acc:admin&password=111
set global protected_databases = 'protected_bvt_db,protected_bvt_new,protected_bvt_clone,CamelDB';
set global protected_databases = '';
create database protected_bvt_db;
create table protected_bvt_db.t1(a int);
insert into protected_bvt_db.t1 values(1);
create view protected_bvt_db.v1 as select * from protected_bvt_db.t1;
create role protected_bvt_writer;
grant create database,drop database on account * to protected_bvt_writer;
grant all on database protected_bvt_db to protected_bvt_writer;
grant all on table protected_bvt_db.* to protected_bvt_writer;
create user protected_bvt_user identified by '111' default role protected_bvt_writer;
-- @session

-- @session:id=2&user=protected_bvt_acc:protected_bvt_user:protected_bvt_writer&password=111
select * from protected_bvt_db.t1;
create database protected_bvt_normal;
drop database protected_bvt_normal;
create database protected_bvt_new;
create table protected_bvt_db.t2(a int);
create view protected_bvt_db.v2 as select * from protected_bvt_db.t1;
drop view protected_bvt_db.v1;
alter table protected_bvt_db.t1 add column b int;
rename table protected_bvt_db.t1 to protected_bvt_db.t2;
create table protected_bvt_db.t_clone clone protected_bvt_db.t1;
create database protected_bvt_clone clone protected_bvt_db;
insert into protected_bvt_db.t1 values(2);
truncate table protected_bvt_db.t1;
drop table protected_bvt_db.t1;
drop database protected_bvt_db;
create database `cameldb`;
drop database `cameldb`;
-- @session

-- @session:id=1&user=protected_bvt_acc:admin&password=111
drop user if exists protected_bvt_user;
drop role if exists protected_bvt_writer;
drop database protected_bvt_db;
set global protected_databases = 'protected_bvt_unused';
-- @session

drop account protected_bvt_acc;
set global enable_privilege_cache = on;
