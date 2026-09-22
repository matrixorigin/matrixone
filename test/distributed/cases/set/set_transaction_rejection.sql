-- Public-path regression coverage for rejected mixed transaction assignments.
-- The rejected statement must not commit or roll back the transaction that was
-- already active when the request entered the frontend.
drop database if exists issue29185_txn_rejection;
create database issue29185_txn_rejection;
create table issue29185_txn_rejection.t (id int primary key);
insert into issue29185_txn_rejection.t values (1);

-- A committed positive control proves that the observer is querying the same
-- table and that its connection is independent from the writer.
-- @session:id=3{
select count(*) as committed_rows
from issue29185_txn_rejection.t
where id = 1;
-- @session}

set session transaction_read_only = 0;
set session transaction_isolation = 'REPEATABLE-READ';
set @saved_session_sql_mode = @@session.sql_mode;
set session sql_mode = 'ONLY_FULL_GROUP_BY';
begin;
insert into issue29185_txn_rejection.t values (29185);

-- @@transaction_isolation is the NEXT-scope assignment. Static validation
-- must reject the whole list before changing the SESSION read-only value.
set session transaction_read_only = 1,
    @@transaction_isolation = 'READ-COMMITTED';
select @@session.transaction_read_only,
       @@session.tx_read_only,
       @@session.transaction_isolation,
       @@session.tx_isolation;
select count(*) as writer_uncommitted_rows
from issue29185_txn_rejection.t
where id = 29185;

-- An independent autocommit connection must not see the uncommitted row.
-- @session:id=3{
select count(*) as observer_uncommitted_rows
from issue29185_txn_rejection.t
where id = 29185;
-- @session}

-- An unrelated literal assignment must not interrupt static preflight. The
-- rejected NEXT isolation assignment must prevent both earlier SESSION
-- assignments from changing the active connection.
set session transaction_read_only = 1,
    session sql_mode = 'ANSI_QUOTES',
    @@transaction_isolation = 'READ-COMMITTED';
select @@session.transaction_read_only,
       @@session.tx_read_only,
       @@session.sql_mode,
       @@session.transaction_isolation,
       @@session.tx_isolation;
select count(*) as writer_uncommitted_rows_after_static_separator
from issue29185_txn_rejection.t
where id = 29185;

-- @session:id=3{
select count(*) as observer_uncommitted_rows_after_static_separator
from issue29185_txn_rejection.t
where id = 29185;
-- @session}

rollback;
select count(*) as writer_rows_after_rollback
from issue29185_txn_rejection.t
where id = 29185;

-- The rollback must remove the row, and the observer must continue to see no
-- row after the rejected request has been cleaned up.
-- @session:id=3{
select count(*) as observer_rows_after_rollback
from issue29185_txn_rejection.t
where id = 29185;
-- @session}

set session sql_mode = @saved_session_sql_mode;

drop database issue29185_txn_rejection;
