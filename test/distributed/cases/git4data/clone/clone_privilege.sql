set global enable_privilege_cache = off;

drop account if exists acc1;
create account acc1 admin_name "root1" identified by "111";

-- @session:id=1&user=acc1:root1&password=111
create database db1 clone mo_catalog;
create database db3 clone system;
create database db4 clone information_schema;

create database db6;
create table db6.t1 clone mo_catalog.mo_tables;
create table db6.t2 clone system.statement_info;

create database db7;
create table db7.t1 (a int primary key);

create table mo_catalog.t1 clone db7.t1;
create table system.t1 clone db7.t1;

create database clone_copy_grants_db;
use clone_copy_grants_db;
create table src(a int primary key, b int);
insert into src values(1, 10), (2, 20);

create role clone_copy_grants_r_select, clone_copy_grants_r_insert;
grant connect on account * to clone_copy_grants_r_select;
grant connect on account * to clone_copy_grants_r_insert;
create user clone_copy_grants_u_select identified by '111' default role clone_copy_grants_r_select;
create user clone_copy_grants_u_insert identified by '111' default role clone_copy_grants_r_insert;

grant select on table clone_copy_grants_db.src to clone_copy_grants_r_select with grant option;
grant select, insert on table clone_copy_grants_db.src to clone_copy_grants_r_insert;

create table dst_plain clone src;
select count(*) from mo_catalog.mo_role_privs
where obj_type = 'table'
  and obj_id = (
      select rel_logical_id
      from mo_catalog.mo_tables
      where reldatabase = 'clone_copy_grants_db' and relname = 'dst_plain'
  )
  and role_name like 'clone_copy_grants_r_%';

create table dst_copy clone src copy grants;
select role_name, privilege_name, privilege_level, with_grant_option
from mo_catalog.mo_role_privs
where obj_type = 'table'
  and obj_id = (
      select rel_logical_id
      from mo_catalog.mo_tables
      where reldatabase = 'clone_copy_grants_db' and relname = 'dst_copy'
  )
  and role_name like 'clone_copy_grants_r_%'
order by role_name, privilege_name;
-- @session
-- @session:id=2&user=acc1:clone_copy_grants_u_select:clone_copy_grants_r_select&password=111
select * from clone_copy_grants_db.src order by a;
select * from clone_copy_grants_db.dst_plain order by a;
select * from clone_copy_grants_db.dst_copy order by a;
-- @session
-- @session:id=3&user=acc1:clone_copy_grants_u_insert:clone_copy_grants_r_insert&password=111
insert into clone_copy_grants_db.dst_copy values(3, 30);
insert into clone_copy_grants_db.dst_plain values(4, 40);
-- @session
-- @session:id=1&user=acc1:root1&password=111
select * from clone_copy_grants_db.dst_copy order by a;
drop user clone_copy_grants_u_select, clone_copy_grants_u_insert;
drop role clone_copy_grants_r_select, clone_copy_grants_r_insert;
drop database clone_copy_grants_db;
-- @session

create database db1 clone mo_catalog;
create database db2 clone system;

create database db3;
create table db3.t1 clone mo_catalog.mo_tables;
create table db3.t2 clone system.statement_info;

create database db4;
create table db4.t1 (a int primary key);
create table mo_catalog.t1 clone db4.t1;
create table system.t1 clone db4.t1;


use mo_catalog;
create database db5 clone db4;

create table db4.copy_grants_to_account_src(a int);
create table db4.copy_grants_to_account_dst clone db4.copy_grants_to_account_src copy grants to account acc1;
drop table db4.copy_grants_to_account_src;

drop account if exists acc1;
drop database if exists db1;
drop database if exists db2;
drop database if exists db3;
drop database if exists db4;
drop database if exists db5;
set global enable_privilege_cache = on;
