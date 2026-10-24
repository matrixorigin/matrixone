-- Regression coverage for MatrixOne issue #30506.
-- A temporary table uses an independent schema transaction.  A database that
-- exists only in the surrounding user transaction must be rejected with the
-- typed 20105 error, rather than exposing the internal ExpectedEOB sentinel.
drop database if exists issue_30506_temp_txn;
drop database if exists issue_30506_temp_committed;

-- The database generation is uncommitted in the parent transaction.
begin;
create database issue_30506_temp_txn;
use issue_30506_temp_txn;
create temporary table tx1 (id int primary key);
-- Expected: not supported: temporary table in an uncommitted database generation
rollback;
drop database if exists issue_30506_temp_txn;

-- A committed database remains a valid temporary-table target.
create database issue_30506_temp_committed;
use issue_30506_temp_committed;
create temporary table tx2 (id int primary key);
insert into tx2 values (1);
select id from tx2 order by id;
drop temporary table tx2;
drop database issue_30506_temp_committed;

-- A database absent from both snapshots keeps the existing typed error.
create temporary table issue_30506_missing.t1 (id int);
-- Expected: Unknown database issue_30506_missing
