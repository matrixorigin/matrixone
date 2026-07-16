-- @suite
-- @case
-- @desc:test lock_wait_timeout changed after BEGIN is honored on remote lock wait path
-- @label:bvt

drop database if exists lock_wait_timeout_remote_db;
create database lock_wait_timeout_remote_db;
use lock_wait_timeout_remote_db;

create table t(a int primary key, b varchar(64));
insert into t values (1, 'ONLY_FULL_GROUP_BY'), (2, 'STRICT_TRANS_TABLES'), (3, 'x');

select @@global.lock_wait_timeout as global_timeout, @@session.lock_wait_timeout as session_timeout;

set @@sql_mode = 'ONLY_FULL_GROUP_BY';

begin;
set session lock_wait_timeout = 1;

-- @session:id=1{
use lock_wait_timeout_remote_db;
set @@sql_mode = 'ONLY_FULL_GROUP_BY';
begin;
select a, b from t where b = @@sql_mode for update;
-- @session}

-- @regex("(?s)Lock wait timeout exceeded; try restarting transaction",true)
select a, b from t where b = @@sql_mode for update;
rollback;

-- @session:id=1{
rollback;
-- @session}

-- A lock wait timeout must roll back the whole explicit transaction. In
-- particular, a following BEGIN must not commit writes completed before the
-- timed-out statement.
create table timeout_rollback(a int primary key, b varchar(64));
insert into timeout_rollback values (1, 'original-1'), (2, 'original-2');
set session lock_wait_timeout = 1;

begin;
update timeout_rollback set b = 'must-be-rolled-back' where a = 1;

-- @session:id=1{
begin;
update timeout_rollback set b = 'holder' where a = 2;
-- @session}

-- @regex("(?s)Lock wait timeout exceeded; try restarting transaction",true)
update timeout_rollback set b = 'waiter' where a = 2;

begin;
commit;

-- @session:id=1{
rollback;
-- @session}

select * from timeout_rollback order by a;

set @@sql_mode = default;
drop database lock_wait_timeout_remote_db;
