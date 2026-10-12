-- Equal table names in different databases/accounts are different grant targets.
drop snapshot if exists restore_boundary_s;
drop snapshot if exists restore_boundary_missing;
drop account if exists restore_boundary_source;
drop account if exists restore_boundary_target;
create account restore_boundary_source admin_name 'admin' identified by '111';
create account restore_boundary_target admin_name 'admin' identified by '111';

-- @session:id=1&user=restore_boundary_source:admin&password=111
create database app;
create database other;
create table app.t (id int primary key);
create table other.t (id int primary key);
insert into app.t values (1);
insert into other.t values (2);
create role leaf, reader, receiver;
create user u1 identified by '111' default role reader;
grant connect on account * to reader;
grant select on table app.t to leaf;
grant leaf to reader;
grant reader to u1;
-- @session
-- @session:id=2&user=restore_boundary_target:admin&password=111
create database app;
create table app.t (id int primary key);
insert into app.t values (99);
-- @session

create snapshot restore_boundary_s for account restore_boundary_source;
-- Rejection before any object work must leave the destination untouched.
restore account restore_boundary_source {snapshot = 'restore_boundary_missing'} to account restore_boundary_target;
-- @session:id=3&user=restore_boundary_target:admin&password=111
select * from app.t;
-- @session

restore account restore_boundary_source {snapshot = 'restore_boundary_s'} to account restore_boundary_target;
-- @session:id=4&user=restore_boundary_target:u1:reader&password=111
select * from app.t;
select * from other.t;
insert into app.t values (3);
delete from app.t;
grant select on table app.t to receiver;
restore account restore_boundary_source {snapshot = 'restore_boundary_s'} to account restore_boundary_target;
select * from app.t;
-- @session

-- @session:id=5&user=restore_boundary_target:admin&password=111
select count(*) as correct_grants from mo_catalog.mo_role_privs p
join mo_catalog.mo_tables t on p.obj_id = t.rel_logical_id
where p.role_name = 'leaf' and p.obj_type = 'table' and p.privilege_name = 'select'
and p.with_grant_option = false and t.reldatabase = 'app' and t.relname = 't';
select count(*) as wrong_database_grants from mo_catalog.mo_role_privs p
join mo_catalog.mo_tables t on p.obj_id = t.rel_logical_id
where p.role_name = 'leaf' and t.reldatabase = 'other' and t.relname = 't';
select count(*) as escalated_grants from mo_catalog.mo_role_privs where role_name = 'receiver';
select * from other.t;
update app.t set id = 99;
-- @session

-- Restoring the source must not modify the already restored destination.
-- @session:id=6&user=restore_boundary_source:admin&password=111
drop table app.t;
create table app.t (id int primary key);
insert into app.t values (4);
grant select on table other.t to leaf;
-- @session
restore account restore_boundary_source {snapshot = 'restore_boundary_s'};
-- @session:id=7&user=restore_boundary_source:u1:reader&password=111
select * from app.t;
select * from other.t;
grant select on table app.t to receiver;
-- @session
-- @session:id=8&user=restore_boundary_target:u1:reader&password=111
select * from app.t;
select * from other.t;
-- @session

drop snapshot restore_boundary_s;
drop account restore_boundary_target;
drop account restore_boundary_source;
