-- Unsupported extensions must reject normally before any file access (#31129).
drop database if exists unsupported_extension;
drop user if exists extension_connect_only;
create database unsupported_extension;
create table unsupported_extension.t(id int primary key);

load some_ext;
select 1 as alive;
create extension pg as myext file '/missing.so';
select 1 as alive;

-- A failed prepare must not publish a new executable handle.
prepare rejected_load from 'load some_ext';
execute rejected_load;
set @extension_sql = 'create extension pg as myext file ''/missing.so''';
prepare rejected_create from @extension_sql;
execute rejected_create;
prepare extension_control from 'select 1 as alive';
execute extension_control;
deallocate prepare extension_control;

-- LOAD DATA is a different, supported operation.
load data inline format='csv', data='1' into table unsupported_extension.t;
select id from unsupported_extension.t;

-- Rejection in either transaction mode must never commit pending writes.
begin;
insert into unsupported_extension.t values (2);
load some_ext;
-- @session:id=1{
select count(*) as committed_rows from unsupported_extension.t;
-- @session}
rollback;
set autocommit=0;
insert into unsupported_extension.t values (3);
create extension pg as myext file '/missing.so';
-- @session:id=1{
select count(*) as committed_rows from unsupported_extension.t;
-- @session}
rollback;
set autocommit=1;
select id from unsupported_extension.t;

-- No grant beyond implicit account-connect is required to reach rejection.
create user extension_connect_only identified by 'extension_pw';
-- @session:id=2&user=sys:extension_connect_only&password=extension_pw{
load some_ext;
select 1 as alive;
create extension pg as myext file '/missing.so';
select 1 as alive;
-- @session}

drop user extension_connect_only;
drop database unsupported_extension;
