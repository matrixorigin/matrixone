-- A restored grant must refer to the target table, not the source table ID.
drop snapshot if exists restore_priv_s1;
drop snapshot if exists restore_priv_s2;
drop account if exists restore_priv_source;
drop account if exists restore_priv_target;
create account restore_priv_source admin_name 'admin' identified by '111';
create account restore_priv_target admin_name 'admin' identified by '111';

-- @session:id=1&user=restore_priv_source:admin&password=111
create database app;
create table app.t (id int primary key);
insert into app.t values (1);
create view app.v as select * from app.t;
create table app.secret (id int);
create role reader;
create user u1 identified by '111' default role reader;
grant connect on account * to reader;
grant select on table app.t to reader with grant option;
grant select on view app.v to reader;
grant insert on table app.* to reader;
grant create table on database app to reader;
grant reader to u1;
-- @session

-- @session:id=2&user=restore_priv_source:u1:reader&password=111
select * from app.t;
select * from app.v;
-- @session

-- @session:id=3&user=restore_priv_target:admin&password=111
create database app;
create table app.t (id int primary key);
insert into app.t values (99);
-- @session

create snapshot restore_priv_s1 for account restore_priv_source;
restore account restore_priv_source {snapshot = 'restore_priv_s1'} to account restore_priv_target;

-- @session:id=4&user=restore_priv_target:admin&password=111
select * from app.t;
select count(*) as bound_view_grants from mo_catalog.mo_role_privs p
join mo_catalog.mo_tables t on p.obj_id = t.rel_logical_id
where p.role_name = 'reader' and p.obj_type = 'view'
and p.privilege_name = 'select' and t.reldatabase = 'app' and t.relname = 'v';
select count(*) as bound_table_grants from mo_catalog.mo_role_privs p
join mo_catalog.mo_tables t on p.obj_id = t.rel_logical_id
where p.role_name = 'reader' and p.obj_type = 'table'
and p.privilege_name = 'select' and t.reldatabase = 'app' and t.relname = 't';
-- @session

-- @session:id=5&user=restore_priv_target:u1:reader&password=111
select * from app.t;
insert into app.t values (2);
create table app.owned (id int);
select * from app.secret;
delete from app.t;
-- @session

-- TRUNCATE changes the physical table ID; privileges follow the logical ID.
-- @session:id=6&user=restore_priv_target:admin&password=111
truncate table app.t;
insert into app.t values (3);
show grants for role reader;
-- @session
create snapshot restore_priv_s2 for account restore_priv_target;
-- @session:id=7&user=restore_priv_target:admin&password=111
grant select on table app.secret to reader;
insert into app.t values (4);
-- @session
restore account restore_priv_target {snapshot = 'restore_priv_s2'};
-- @session:id=8&user=restore_priv_target:u1:reader&password=111
select * from app.t;
select * from app.secret;
delete from app.t;
-- @session
-- @session:id=9&user=restore_priv_target:admin&password=111
show grants for role reader;
select count(*) as bound_table_grants from mo_catalog.mo_role_privs p
join mo_catalog.mo_tables t on p.obj_id = t.rel_logical_id
where p.role_name = 'reader' and p.obj_type = 'table'
and p.privilege_name = 'select' and p.with_grant_option = true
and t.reldatabase = 'app' and t.relname = 't';
-- @session
drop snapshot restore_priv_s2;
drop snapshot restore_priv_s1;
drop account restore_priv_target;
drop account restore_priv_source;
