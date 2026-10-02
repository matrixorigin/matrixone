drop snapshot if exists auth_owner_s;
drop account if exists auth_restore;
create account auth_restore admin_name 'admin' identified by '111';
-- @session:id=1&user=auth_restore:admin&password=111
create database src;
create database dst;
create database unrelated;
create table src.secret(id int);
insert into src.secret values(42);
create table dst.t(id int);
insert into dst.t values(17);
create view dst.v as select * from dst.t;
create table unrelated.t(id int);
insert into unrelated.t values(99);
create role reader;
create user u1 identified by '111' default role reader;
grant reader to u1;
grant connect on account * to reader;
grant select on table dst.* to reader;
grant select on view dst.v to reader;
-- @session

-- Neither target CREATE nor source SELECT can be bypassed by CLONE.
-- @session:id=2&user=auth_restore:u1:reader&password=111
set enable_privilege_cache=on;
select * from src.secret;
create table dst.copied clone src.secret;
create database copied_db clone src;
-- @session
-- @session:id=1&user=auth_restore:admin&password=111
grant create table on database dst to reader;
grant create database on account * to reader;
-- @session
-- @session:id=2&user=auth_restore:u1:reader&password=111
create table dst.copied clone src.secret;
create database copied_db clone src;
-- @session
-- A rejected database clone must not leave an empty destination behind.
-- @session:id=1&user=auth_restore:admin&password=111
select count(*) from mo_catalog.mo_database where datname='copied_db';
select count(*) from mo_catalog.mo_tables where reldatabase='dst' and relname='copied';
grant select on table src.secret to reader;
-- @session
-- @session:id=2&user=auth_restore:u1:reader&password=111
create table dst.copied clone src.secret;
select * from dst.copied;
create database copied_db clone src;
-- @session

-- Partial restore preserves current grants, not grants at snapshot time.
-- @session:id=1&user=auth_restore:admin&password=111
revoke select on table dst.* from reader;
grant select on table dst.t to reader with grant option;
grant delete on table dst.t to reader;
grant select on table unrelated.t to reader;
create snapshot auth_partial_s for account;
revoke delete on table dst.t from reader;
grant insert on table dst.t to reader;
restore table dst.t {snapshot='auth_partial_s'};
restore table dst.t {snapshot='auth_partial_s'};
-- @session
-- @session:id=2&user=auth_restore:u1:reader&password=111
select * from dst.t;
insert into dst.t values(18);
delete from dst.t;
select * from unrelated.t;
-- @session
-- @session:id=1&user=auth_restore:admin&password=111
select count(*) from mo_catalog.mo_role_privs p join mo_catalog.mo_tables t on p.obj_id=t.rel_logical_id
where p.role_name='reader' and p.privilege_name='select' and p.with_grant_option=true and t.reldatabase='dst' and t.relname='t';
grant select on table dst.* to reader;
restore database dst {snapshot='auth_partial_s'};
restore database dst {snapshot='auth_partial_s'};
-- @session
-- @session:id=2&user=auth_restore:u1:reader&password=111
select * from dst.t;
select * from dst.copied;
delete from dst.t;
select * from unrelated.t;
select * from dst.v;
-- @session

-- SYS account restore preserves owner/creator and invalidates old grants.
-- @session:id=1&user=auth_restore:admin&password=111
create role maker;
create user u2 identified by '111' default role maker;
grant maker to u2;
grant connect, create database on account * to maker;
grant create table on database * to maker;
-- @session
-- @session:id=3&user=auth_restore:u2:maker&password=111
create database owned;
create table owned.t(id int);
-- @session
-- Partial restore must also preserve a non-admin owner's implicit rights.
-- @session:id=1&user=auth_restore:admin&password=111
create snapshot auth_maker_s for account;
restore table owned.t {snapshot='auth_maker_s'};
restore database owned {snapshot='auth_maker_s'};
drop snapshot auth_maker_s;
-- @session
create snapshot auth_owner_s for account auth_restore;
-- @session:id=1&user=auth_restore:admin&password=111
grant delete on table dst.t to reader;
-- @session
-- @session:id=2&user=auth_restore:u1:reader&password=111
delete from dst.t where id=-1;
prepare cached_delete from 'delete from dst.t where id=-1';
execute cached_delete;
-- @session
restore account auth_restore {snapshot='auth_owner_s'};
-- @session:id=2&user=auth_restore:u1:reader&password=111
delete from dst.t where id=-1;
execute cached_delete;
select * from dst.t;
select * from dst.v;
-- @session
-- @session:id=1&user=auth_restore:admin&password=111
select d.owner=r.role_id as correct_owner,d.creator=u.user_id as correct_creator from mo_catalog.mo_database d,mo_catalog.mo_role r,mo_catalog.mo_user u where d.datname='owned' and r.role_name='maker' and u.user_name='u2';
select t.owner=r.role_id as correct_owner,t.creator=u.user_id as correct_creator from mo_catalog.mo_tables t,mo_catalog.mo_role r,mo_catalog.mo_user u where t.reldatabase='owned' and t.relname='t' and r.role_name='maker' and u.user_name='u2';
grant delete on table dst.t to reader;
-- @session
-- @session:id=2&user=auth_restore:u1:reader&password=111
delete from dst.t where id=-1;
-- @session
-- @session:id=1&user=auth_restore:admin&password=111
revoke delete on table dst.t from reader;
-- @session
-- @session:id=2&user=auth_restore:u1:reader&password=111
delete from dst.t where id=-1;
execute cached_delete;
deallocate prepare cached_delete;
-- @session
-- @session:id=3&user=auth_restore:u2:maker&password=111
drop database owned;
-- @session
-- @session:id=1&user=auth_restore:admin&password=111
drop snapshot auth_partial_s;
-- @session
drop snapshot auth_owner_s;
drop account auth_restore;

-- Missing historical owners fail before dropping current data. An account
-- restore can restore those principals, but a partial restore cannot.
create account auth_missing admin_name 'admin' identified by '111';
-- @session:id=10&user=auth_missing:admin&password=111
create role former;
create user builder identified by '111' default role former;
grant former to builder;
grant connect,create database on account * to former;
grant create table on database * to former;
-- @session
-- @session:id=11&user=auth_missing:builder:former&password=111
create database app;
create table app.t(id int);
-- @session
-- @session:id=10&user=auth_missing:admin&password=111
insert into app.t values(1);
create table app.retained(id int);
insert into app.retained values(7);
set @retained_db_id=(select dat_id from mo_catalog.mo_database where datname='app');
create snapshot auth_missing_s for account;
drop user builder;
-- A table owned by a live principal does not recreate the existing database.
update app.retained set id=9;
restore table app.retained {snapshot='auth_missing_s'};
select * from app.retained;
select dat_id=@retained_db_id as retained_database from mo_catalog.mo_database where datname='app';
insert into app.t values(2);
-- The selected table's missing creator must still fail before DROP.
restore table app.t {snapshot='auth_missing_s'};
restore database app {snapshot='auth_missing_s'};
select * from app.t order by id;
drop snapshot auth_missing_s;
-- @session
drop account auth_missing;
