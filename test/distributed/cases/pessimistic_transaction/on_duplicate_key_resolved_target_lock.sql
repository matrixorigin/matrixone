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

-- Repeated partition references must be fully substituted for ODKU routing
-- and equality reads; ordinary update/delete also consume RowID metadata.
create table repeated_target(id int primary key, u int unique, v int) partition by range(id*id) (partition p0 values less than (0), partition p1 values less than maxvalue);
insert into repeated_target values (1,10,1);
insert into repeated_target values (-1,10,999) on duplicate key update v=v+1;
insert into repeated_target values (-1,10,999) on duplicate key update v=v+1;
select id,u,v from repeated_target;
select id,u,v from repeated_target where id=1;
select id,u,v from repeated_target where u=10;
update repeated_target set v=v+1 where id=1;
select id,u,v from repeated_target where id=1;
delete from repeated_target where id=1;
select count(*) from repeated_target;

drop database odku_resolved_target_lock;
