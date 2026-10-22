-- @suite
-- @case
-- @desc:SHARE SKIP LOCKED filters settled conflicts before LIMIT and OFFSET
-- @label:bvt

drop database if exists locking_read_share_skip_locked_db;
create database locking_read_share_skip_locked_db;
use locking_read_share_skip_locked_db;
create table t(id int primary key, v int);
insert into t values (1, 10), (2, 20), (3, 30);
set @saved_lock_timeout = @@session.lock_wait_timeout;
set session lock_wait_timeout = 1;

begin;
select id from t where id = 1 for update;
-- @session:id=1{
use locking_read_share_skip_locked_db;
set @saved_lock_timeout = @@session.lock_wait_timeout;
set session lock_wait_timeout = 1;
begin;
select id from t order by id limit 2 for share skip locked;
select id from t order by id limit 1 offset 1 for share skip locked;
select id from t where id = 1 for share skip locked;
prepare skip_stmt from 'select id from t where id >= ? order by id limit 2 for share skip locked';
set @first_id = 1;
execute skip_stmt using @first_id;
deallocate prepare skip_stmt;
select cast(id as char character set binary) from t where id = (select (select t.id)) order by id limit 1 offset 1 for share skip locked;
-- @session}
-- Accepted rows retain transaction-owned Shared grants.
select id from t where id = 2 for update nowait;
-- @session:id=1{
rollback;
-- @session}
rollback;

-- Compatible Shared holders are admitted, rather than incorrectly skipped.
begin;
select id from t where id = 1 for share;
-- @session:id=1{
begin;
select id from t where id = 1 for share skip locked;
rollback;
-- @session}
rollback;

-- All-conflict is empty; it must not publish locked rows or wait for release.
begin;
select id from t order by id for update;
-- @session:id=1{
begin;
select id from t order by id limit 2 for share skip locked;
rollback;
-- @session}
rollback;

-- Release is visible to a new transaction on the other session.
-- @session:id=1{
begin;
select id from t order by id for share skip locked;
rollback;
set session lock_wait_timeout = @saved_lock_timeout;
-- @session}
-- The unsupported Exclusive SKIP surface must continue to be rejected.
select id from t for update skip locked;
select sum(v) as original_sum from t;
set session lock_wait_timeout = @saved_lock_timeout;
drop database locking_read_share_skip_locked_db;
