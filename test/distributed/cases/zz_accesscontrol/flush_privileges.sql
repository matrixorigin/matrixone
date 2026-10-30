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
rollback;
select count(*) from flush_privileges_db.flush;
set autocommit = 0;
insert into flush_privileges_db.flush values (2);
flush privileges;
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
begin;
insert into flush_privileges_db.flush values (3);
flush privileges;
rollback;
select count(*) from flush_privileges_db.flush;
set session enable_privilege_cache = off;
begin;
insert into flush_privileges_db.flush values (4);
flush privileges;
rollback;
select count(*) from flush_privileges_db.flush;
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
drop user if exists flush_privileges_user;
drop role if exists flush_privileges_role;
drop database if exists flush_privileges_db;
