-- FLUSH PRIVILEGES is an admin-only no-op and keeps the current transaction.
drop user if exists flush_privileges_user;
drop role if exists flush_privileges_role;
drop database if exists flush_privileges_db;
create database flush_privileges_db;
create table flush_privileges_db.flush (flush int);
create role flush_privileges_role;
create user flush_privileges_user identified by '123456' default role flush_privileges_role;
grant connect on account * to flush_privileges_role;
grant insert, select on table flush_privileges_db.flush to flush_privileges_role;
grant flush_privileges_role to flush_privileges_user;

begin;
insert into flush_privileges_db.flush values (1);
flush privileges;
select count(*) from flush_privileges_db.flush;
rollback;
select count(*) from flush_privileges_db.flush;
set autocommit = 0;
insert into flush_privileges_db.flush values (2);
flush privileges;
select count(*) from flush_privileges_db.flush;
rollback;
select count(*) from flush_privileges_db.flush;
set autocommit = 1;
prepare flush_privileges_existing from 'select count(*) from flush_privileges_db.flush';
flush privileges;
execute flush_privileges_existing;
deallocate prepare flush_privileges_existing;
prepare flush_privileges_unsupported from 'flush privileges';
flush tables;

-- @session:id=1&user=sys:flush_privileges_user:flush_privileges_role&password=123456{
set session enable_privilege_cache = on;
set session mo_rollback_txn_on_error = 0;
begin;
insert into flush_privileges_db.flush values (3);
flush privileges;
select count(*) from flush_privileges_db.flush;
rollback;
select count(*) from flush_privileges_db.flush;
set session enable_privilege_cache = off;
set session mo_rollback_txn_on_error = 0;
begin;
insert into flush_privileges_db.flush values (4);
flush privileges;
select count(*) from flush_privileges_db.flush;
rollback;
select count(*) from flush_privileges_db.flush;
-- A denied statement also preserves pending work in autocommit=0 mode.
set autocommit = 0;
insert into flush_privileges_db.flush values (6);
flush privileges;
select count(*) from flush_privileges_db.flush;
rollback;
select count(*) from flush_privileges_db.flush;
set autocommit = 1;
-- With rollback-on-error enabled, the existing policy rolls back on denial.
set session mo_rollback_txn_on_error = 1;
begin;
insert into flush_privileges_db.flush values (5);
flush privileges;
select count(*) from flush_privileges_db.flush;
rollback;
set session mo_rollback_txn_on_error = 0;
-- @session}
revoke select on table flush_privileges_db.flush from flush_privileges_role;
-- @session:id=1&user=sys:flush_privileges_user:flush_privileges_role&password=123456{
select count(*) from flush_privileges_db.flush;
-- @session}
flush privileges;
grant select on table flush_privileges_db.flush to flush_privileges_role;
-- @session:id=1&user=sys:flush_privileges_user:flush_privileges_role&password=123456{
select count(*) from flush_privileges_db.flush;
-- @session}
-- A different tenant's no-op does not change data in the first tenant.
drop account if exists flush_privileges_tenant_a;
drop account if exists flush_privileges_tenant_b;
create account flush_privileges_tenant_a admin_name 'admin' identified by '123456';
create account flush_privileges_tenant_b admin_name 'admin' identified by '123456';
-- @session:id=2&user=flush_privileges_tenant_a:admin&password=123456{
create database flush_privileges_isolation;
create table flush_privileges_isolation.t(v int);
insert into flush_privileges_isolation.t values (1);
flush privileges;
-- @session}
-- @session:id=3&user=flush_privileges_tenant_b:admin&password=123456{
flush privileges;
-- @session}
-- @session:id=2&user=flush_privileges_tenant_a:admin&password=123456{
select count(*) from flush_privileges_isolation.t;
drop database flush_privileges_isolation;
-- @session}
drop account flush_privileges_tenant_a;
drop account flush_privileges_tenant_b;
drop user if exists flush_privileges_user;
drop role if exists flush_privileges_role;
drop database if exists flush_privileges_db;
