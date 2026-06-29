-- @suite
-- @case
-- @desc:test lock_wait_timeout changed after BEGIN is honored on remote lock wait path
-- @label:bvt

drop database if exists lock_wait_timeout_remote_db;
create database lock_wait_timeout_remote_db;
use lock_wait_timeout_remote_db;

create table t(a int primary key, b varchar(64));
insert into t values (1, 'ONLY_FULL_GROUP_BY'), (2, 'STRICT_TRANS_TABLES'), (3, 'x');

set @@sql_mode = 'ONLY_FULL_GROUP_BY';

begin;
set session lock_wait_timeout = 1;

-- @session:id=1{
use lock_wait_timeout_remote_db;
set @@sql_mode = 'ONLY_FULL_GROUP_BY';
begin;
select a, b from t where b = @@sql_mode for update;
-- @session}

-- @regex("(?s)(invalid state lock timeout|lock timeout)",true)
select a, b from t where b = @@sql_mode for update;
rollback;

-- @session:id=1{
rollback;
-- @session}

set @@sql_mode = default;
drop database lock_wait_timeout_remote_db;
