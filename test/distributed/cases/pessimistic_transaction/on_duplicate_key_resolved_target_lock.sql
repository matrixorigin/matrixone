-- @suite

-- @case
-- @desc:test ODKU locks the base row resolved by a secondary UNIQUE conflict
-- @label:bvt
-- Regression: issue #31955.
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

-- Releasing the holder permits the same cross-row UNIQUE conflict to update.
insert into target_row values (2, 10, 200)
  on duplicate key update v = values(v);

-- A snapshot with no target must still lock the incoming PK. The two inputs
-- use different unique keys so only their base-table PK locks can conflict.
begin;
insert into target_row values (3, 20, 300);
-- @session:id=1{
use odku_resolved_target_lock;
set session lock_wait_timeout = 1;
begin;
-- @pattern
insert into target_row values (3, 30, 400)
  on duplicate key update v = values(v);
rollback;
-- @session}
commit;
insert into target_row values (3, 30, 400)
  on duplicate key update v = values(v);

-- One batch may contain an existing target and a new nullable-UNIQUE row.
insert into target_row values (10001, 10, 500), (4, null, 600)
  on duplicate key update v = values(v);
select * from target_row order by id;

drop database odku_resolved_target_lock;
