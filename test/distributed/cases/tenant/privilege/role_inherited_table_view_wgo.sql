set global enable_privilege_cache = off;
drop user if exists iwgo_u,iwgo_table_u;
drop role if exists iwgo_base,iwgo_mid,iwgo_holder,iwgo_target,iwgo_table_base,iwgo_table_holder,iwgo_table_target;
drop database if exists iwgo_db;
create user iwgo_u identified by '111', iwgo_table_u identified by '111';
create role iwgo_base,iwgo_mid,iwgo_holder,iwgo_target,iwgo_table_base,iwgo_table_holder,iwgo_table_target;
grant connect on account * to iwgo_base;
grant connect on account * to iwgo_table_base;
grant iwgo_base to iwgo_u;
grant iwgo_mid to iwgo_base;
grant iwgo_holder to iwgo_mid;
grant iwgo_table_base to iwgo_table_u;
grant iwgo_table_holder to iwgo_table_base;
create database iwgo_db;
create table iwgo_db.t(a int);
create view iwgo_db.v as select * from iwgo_db.t;
grant select on table iwgo_db.t to iwgo_holder with grant option;
grant select on view iwgo_db.v to iwgo_holder with grant option;
grant select on table iwgo_db.* to iwgo_table_holder with grant option;

-- @session:id=2&user=sys:iwgo_u:iwgo_base&password=111
grant select on table iwgo_db.t to iwgo_target;
grant select on view iwgo_db.v to iwgo_target;
-- @session

-- @session:id=3&user=sys:iwgo_table_u:iwgo_table_base&password=111
grant select on view iwgo_db.v to iwgo_table_target;
-- @session

drop database iwgo_db;
drop user iwgo_u,iwgo_table_u;
drop role iwgo_base,iwgo_mid,iwgo_holder,iwgo_target,iwgo_table_base,iwgo_table_holder,iwgo_table_target;
set global enable_privilege_cache = on;
