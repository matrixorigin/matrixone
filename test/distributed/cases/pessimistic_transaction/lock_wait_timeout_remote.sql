-- @suite
-- @case
-- @desc:test lock_wait_timeout changed after BEGIN is honored on remote lock wait path
-- @label:bvt

-- Compatibility alias uses native state, bounds, default and account-global inheritance.
select @@session.innodb_lock_wait_timeout = @@session.lock_wait_timeout as same_timeout;
set session INNODB_LOCK_WAIT_TIMEOUT = 2;
select @@session.lock_wait_timeout as native_timeout, @@session.innodb_lock_wait_timeout as alias_timeout;
set session lock_wait_timeout = 3;
show session variables like 'innodb_lock_wait_timeout';
set session innodb_lock_wait_timeout = 0;
set session innodb_lock_wait_timeout = 31536001;
set session innodb_lock_wait_timeout = 'bad';
set session innodb_lock_wait_timeout = NULL;
select @@session.innodb_lock_wait_timeout as preserved_timeout;
set session innodb_lock_wait_timeout = 31536000;
select @@session.lock_wait_timeout as upper_timeout;
set session innodb_lock_wait_timeout = default;
select @@session.innodb_lock_wait_timeout = @@global.lock_wait_timeout as same_default;
set @saved_lock_timeout = @@global.lock_wait_timeout;
set global innodb_lock_wait_timeout = 7;
show global variables like 'innodb_lock_wait_timeout';
select @@session.innodb_lock_wait_timeout = @saved_lock_timeout as existing_session_preserved;
-- @session:id=71{
select @@session.innodb_lock_wait_timeout as inherited_alias, @@session.lock_wait_timeout as inherited_native;
-- @session}
set global lock_wait_timeout = @saved_lock_timeout;
-- @session:id=72{
select @@session.innodb_lock_wait_timeout = @@global.lock_wait_timeout as restored_inheritance;
-- @session}
prepare timeout_alias_stmt from 'set session innodb_lock_wait_timeout = ?';
set @alias_timeout = 4;
execute timeout_alias_stmt using @alias_timeout;
select @@session.lock_wait_timeout as first_bind;
set @alias_timeout = 0;
execute timeout_alias_stmt using @alias_timeout;
select @@session.innodb_lock_wait_timeout as error_preserved;
set @alias_timeout = 5;
execute timeout_alias_stmt using @alias_timeout;
select @@session.lock_wait_timeout as reused_bind;
deallocate prepare timeout_alias_stmt;
set session lock_wait_timeout = default;

drop database if exists lock_wait_timeout_remote_db;
create database lock_wait_timeout_remote_db;
use lock_wait_timeout_remote_db;

create table t(a int primary key, b varchar(64));
insert into t values (1, 'ONLY_FULL_GROUP_BY'), (2, 'STRICT_TRANS_TABLES'), (3, 'x');

select @@global.lock_wait_timeout as global_timeout, @@session.lock_wait_timeout as session_timeout;

set @@sql_mode = 'ONLY_FULL_GROUP_BY';

begin;
set session innodb_lock_wait_timeout = 1;

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
set session innodb_lock_wait_timeout = 1;

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
