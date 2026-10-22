-- @suite
-- @case
-- @desc:NOWAIT reports settled conflicts without ending the transaction
-- @label:bvt

drop database if exists locking_read_nowait_db;
create database locking_read_nowait_db;
use locking_read_nowait_db;
create table t(id int primary key, v int);
insert into t values (1, 10), (2, 20), (3, 30);
set @saved_lock_timeout = @@session.lock_wait_timeout;
set session lock_wait_timeout = 1;

-- The completed locking read establishes the holder before switching sessions.
begin;
select id from t where id = 1 for update;
-- @session:id=1{
use locking_read_nowait_db;
set @saved_lock_timeout = @@session.lock_wait_timeout;
set session lock_wait_timeout = 1;
begin;
update t set v = 21 where id = 2;
select id from t where id = 3 for share nowait;
select id from t where id = 1 for update nowait;
select id from t where id = 1 for share nowait;
prepare nowait_stmt from 'select id from t where id = ? for update nowait';
set @locked_id = 1;
execute nowait_stmt using @locked_id;
deallocate prepare nowait_stmt;
select id from t where id = 1 and id = (select (select t.id)) for update nowait;
select v from t where id = 2;
-- @session}

-- The waiter retains its Shared grant after the failed NOWAIT statements.
select id from t where id = 3 for update nowait;
-- @session:id=1{
rollback;
set session lock_wait_timeout = @saved_lock_timeout;
-- @session}
-- Waiter rollback releases both its earlier write and successful Shared grant.
select id from t where id > 1 order by id for update nowait;
rollback;

-- Shared admission must remain compatible with another Shared holder.
begin;
select id from t where id = 1 for share;
-- @session:id=1{
begin;
select id from t where id = 1 for share nowait;
rollback;
-- @session}
rollback;

-- After all holders release, ordinary successful NOWAIT returns every row.
begin;
select id from t order by id for update nowait;
rollback;
select sum(v) as original_sum from t;
set session lock_wait_timeout = @saved_lock_timeout;
drop database locking_read_nowait_db;
