-- What a duplicate key does to an open transaction.
--
-- MySQL rolls back only the failing statement and leaves the transaction open,
-- and that is MO's default. mo_rollback_txn_on_duplicate_key = 1 opts a session
-- into treating the violation as fatal to the whole transaction instead.
drop database if exists dupscope;
create database dupscope;
use dupscope;
create table t (a int primary key, b varchar(10));
insert into t values (1, 'one');

-- the default is MySQL's
select @@mo_rollback_txn_on_duplicate_key;

-- a duplicate key reports SQLSTATE 23000, the integrity-constraint class, so a
-- client can route on the SQLSTATE and not only on the error number
insert into t values (1, 'dup');

-- default: the statement is rolled back, the transaction survives, and work
-- done before AND after the failure commits
begin;
insert into t values (20, 'twenty');
insert into t values (1, 'dup');
insert into t values (21, 'twentyone');
commit;
select a from t order by a;

-- opted in: the whole transaction goes, including the row inserted before the
-- failure
delete from t where a <> 1;
set mo_rollback_txn_on_duplicate_key = 1;
select @@mo_rollback_txn_on_duplicate_key;
begin;
insert into t values (30, 'thirty');
insert into t values (1, 'dup');
select a from t order by a;
commit;
select a as after_commit from t order by a;

-- the setting is scoped to the duplicate key: an unrelated statement error
-- still rolls back only the statement
begin;
insert into t values (40, 'forty');
select bad_column from t;
insert into t values (41, 'fortyone');
commit;
select a as unrelated_error_txn_survives from t order by a;

-- and back to MySQL behaviour
set mo_rollback_txn_on_duplicate_key = 0;
delete from t where a <> 1;
begin;
insert into t values (50, 'fifty');
insert into t values (1, 'dup');
commit;
select a as back_to_mysql from t order by a;

-- GLOBAL scope: setting it globally leaves THIS session alone, the way MySQL
-- treats a global assignment, and a session opened afterwards inherits it.
set global mo_rollback_txn_on_duplicate_key = 1;
select @@global.mo_rollback_txn_on_duplicate_key as global_value,
       @@session.mo_rollback_txn_on_duplicate_key as this_session_unchanged;

-- @session:id=1{
use dupscope;
select @@mo_rollback_txn_on_duplicate_key as inherited_by_a_new_session;
delete from t where a <> 1;
begin;
insert into t values (60, 'sixty');
insert into t values (1, 'dup');
commit;
select a as new_session_txn_discarded from t order by a;
-- @session}

set global mo_rollback_txn_on_duplicate_key = 0;
select @@global.mo_rollback_txn_on_duplicate_key as global_restored;

drop database if exists dupscope;
