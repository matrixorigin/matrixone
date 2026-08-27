-- What a failed statement does to an open transaction.
--
-- MySQL rolls back only the failing statement and leaves the transaction open,
-- and that is MO's default for every error but the dozen infrastructure ones
-- (deadlock, lock timeout, a backend that went away) after which a transaction
-- cannot continue anyway. mo_rollback_txn_on_error = 1 opts a session into
-- treating ANY error as fatal to the whole transaction instead.
drop database if exists rbscope;
create database rbscope;
use rbscope;
create table t (a int primary key, b varchar(10));
insert into t values (1, 'one');

-- the default is MySQL's
select @@mo_rollback_txn_on_error;

-- a duplicate key reports SQLSTATE 23000, the integrity-constraint class
insert into t values (1, 'dup');

-- default: the statement is rolled back, the transaction survives, and work
-- done before AND after the failure commits
begin;
insert into t values (20, 'twenty');
insert into t values (1, 'dup');
insert into t values (21, 'twentyone');
commit;
select a from t order by a;

-- and the same for an error that is not a constraint violation at all
delete from t where a <> 1;
begin;
insert into t values (22, 'twentytwo');
select bad_column from t;
insert into t values (23, 'twentythree');
commit;
select a as default_survives_any_error from t order by a;

-- opted in: the whole transaction goes, including the row inserted before the
-- failure
delete from t where a <> 1;
set mo_rollback_txn_on_error = 1;
select @@mo_rollback_txn_on_error;
begin;
insert into t values (30, 'thirty');
insert into t values (1, 'dup');
select a from t order by a;
commit;
select a as after_commit from t order by a;

-- ANY error, not only a duplicate key: an unknown column, a type conversion
-- failure and a missing table each discard the transaction just the same
delete from t where a <> 1;
begin;
insert into t values (40, 'forty');
select bad_column from t;
commit;
select a as unknown_column from t order by a;

delete from t where a <> 1;
begin;
insert into t values (41, 'fortyone');
insert into t values ('notanint', 'x');
commit;
select a as bad_type from t order by a;

delete from t where a <> 1;
begin;
insert into t values (42, 'fortytwo');
select * from no_such_table;
commit;
select a as missing_table from t order by a;

-- Only errors do this. moerr also carries Info and Warning codes, and those
-- must never discard a transaction. That exemption is asserted in Go
-- (TestWarningsNeverRollBackTxn, and TestIsRealError in pkg/common/moerr) and
-- not here on purpose: no SQL statement reaches the frontend with a
-- warning-coded result. An over-long value errors outright in strict mode --
-- "Data truncation: Can't cast ..." is a real error, and IS rolled back with
-- this setting on -- and in non-strict mode it is accepted silently with no
-- error at all, so neither shape exercises the warning path.
delete from t where a <> 1;
begin;
insert into t values (43, 'fortythree');
insert into t values (44, 'this string is longer than ten');
commit;
select a as truncation_is_a_real_error from t order by a;

-- back to MySQL behaviour
set mo_rollback_txn_on_error = 0;
delete from t where a <> 1;
begin;
insert into t values (50, 'fifty');
insert into t values (1, 'dup');
commit;
select a as back_to_mysql from t order by a;

-- GLOBAL scope: setting it globally leaves THIS session alone, the way MySQL
-- treats a global assignment, and a session opened afterwards inherits it.
set global mo_rollback_txn_on_error = 1;
select @@global.mo_rollback_txn_on_error as global_value,
       @@session.mo_rollback_txn_on_error as this_session_unchanged;

-- @session:id=1{
use rbscope;
select @@mo_rollback_txn_on_error as inherited_by_a_new_session;
delete from t where a <> 1;
begin;
insert into t values (60, 'sixty');
insert into t values (1, 'dup');
commit;
select a as new_session_txn_discarded from t order by a;
-- @session}

set global mo_rollback_txn_on_error = 0;
select @@global.mo_rollback_txn_on_error as global_restored;

drop database if exists rbscope;
