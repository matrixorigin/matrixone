-- @suite

-- @case
-- @desc:test ODKU locks the base row resolved by a secondary UNIQUE conflict
-- @label:bvt
drop database if exists odku_resolved_target_lock;
create database odku_resolved_target_lock;
use odku_resolved_target_lock;

-- The conflict resolves to the existing row with id=1. The ODKU must lock
-- that base-row identity, not the unrelated incoming id=2. SELECT FOR UPDATE
-- isolates this lock from the unique-index lock.
create table target_row(id int primary key, u int unique, v int);
insert into target_row values (1, 10, 100);
begin;
select id from target_row where id = 1 for update;
-- @session:id=1{
use odku_resolved_target_lock;
set session lock_wait_timeout = 1;
begin;
-- @pattern
insert into target_row values (2, 10, 200)
  on duplicate key update v = values(v);
rollback;
-- @session}
commit;
select * from target_row;

drop database odku_resolved_target_lock;
