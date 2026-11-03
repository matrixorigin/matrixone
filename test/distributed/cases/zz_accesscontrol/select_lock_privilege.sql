-- Exclusive SELECT locks require SELECT and UPDATE or DELETE (#31916).
drop database if exists select_lock_priv;
drop user if exists select_lock_reader;
drop role if exists select_lock_role;
drop role if exists select_lock_write_role;
create database select_lock_priv;
create table select_lock_priv.t1(id int primary key, v int);
create table select_lock_priv.t2(id int primary key, v int);
insert into select_lock_priv.t1 values(1, 10);
insert into select_lock_priv.t2 values(1, 20);
set @old_view_security_type = @@view_security_type;
set view_security_type = 'INVOKER';
create view select_lock_priv.v1 as select * from select_lock_priv.t1;
set view_security_type = @old_view_security_type;
create user select_lock_reader identified by '123456';
create role select_lock_role;
create role select_lock_write_role;
grant select_lock_write_role to select_lock_role;
grant select on table select_lock_priv.* to select_lock_role;
grant select on view select_lock_priv.v1 to select_lock_role;
grant select_lock_role to select_lock_reader;

-- @session:id=1&user=sys:select_lock_reader:select_lock_role&password=123456
select id from select_lock_priv.t1;
select id from select_lock_priv.t1 for share;
select id from select_lock_priv.t1 lock in share mode;
begin;
select id from select_lock_priv.t1 for update;
-- @session
-- The rejected statement must not hold a lock, even before its transaction ends.
begin;
select id from select_lock_priv.t1 for update nowait;
rollback;
-- @session:id=1&user=sys:select_lock_reader:select_lock_role&password=123456
rollback;
select id from select_lock_priv.t1 where 1=0 for update;
select id from select_lock_priv.t1 for update nowait;
select id from select_lock_priv.t1 where id in (select id from select_lock_priv.t2 for update);
select id from (select id from select_lock_priv.t1 for update) d;
with d as (select id from select_lock_priv.t1 for update) select id from d;
select id from select_lock_priv.v1 for update;
-- @session

grant update on table select_lock_priv.t1 to select_lock_write_role;
-- @session:id=1&user=sys:select_lock_reader:select_lock_role&password=123456
select id from select_lock_priv.t1 for update;
select id from select_lock_priv.t1 where id in (select id from select_lock_priv.t2) for update;
select id from select_lock_priv.t1 where id in (select id from select_lock_priv.t2 for update);
select t1.id from select_lock_priv.t1 join select_lock_priv.t2 on t1.id=t2.id for update;
prepare lock_stmt from 'select id from select_lock_priv.t1 for update';
execute lock_stmt;
-- @session
revoke update on table select_lock_priv.t1 from select_lock_write_role;
-- @session:id=1&user=sys:select_lock_reader:select_lock_role&password=123456
execute lock_stmt;
-- @session

grant delete on table select_lock_priv.t1 to select_lock_write_role;
grant update on table select_lock_priv.t2 to select_lock_write_role;
-- @session:id=1&user=sys:select_lock_reader:select_lock_role&password=123456
execute lock_stmt;
select t1.id from select_lock_priv.t1 join select_lock_priv.t2 on t1.id=t2.id for update;
select id from select_lock_priv.v1 for update;
-- @session
-- A view and its underlying table can use different alternatives.
grant update on view select_lock_priv.v1 to select_lock_role;
-- @session:id=1&user=sys:select_lock_reader:select_lock_role&password=123456
select id from select_lock_priv.v1 for update;
deallocate prepare lock_stmt;
-- @session

drop user select_lock_reader;
drop role select_lock_role;
drop role select_lock_write_role;
drop database select_lock_priv;
