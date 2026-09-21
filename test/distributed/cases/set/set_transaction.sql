set @saved_session_transaction_isolation = @@session.transaction_isolation;
set @saved_session_transaction_read_only = @@session.transaction_read_only;
set @saved_session_tx_read_only = @@session.tx_read_only;
set transaction_isolation = 'REPEATABLE-READ';
set session transaction isolation level read committed;
select @@transaction_isolation;

set transaction isolation level repeatable read;
select @@transaction_isolation;

set @saved_transaction_isolation = @@global.transaction_isolation;
set global transaction isolation level read committed;
select @@global.transaction_isolation;
set global transaction_isolation = @saved_transaction_isolation;

-- SESSION access modes support Connector/J compatibility and synchronize
-- transaction_read_only and tx_read_only; they do not enforce read-only writes.
set session transaction read only;
select @@session.transaction_read_only, @@session.tx_read_only;
set session transaction read write;
select @@session.transaction_read_only, @@session.tx_read_only;
set session transaction isolation level repeatable read, read only;
select @@session.transaction_read_only, @@session.tx_read_only;
select @@transaction_isolation;

-- Duplicate and conflicting characteristics are rejected before state changes.
set session transaction isolation level read committed, isolation level read committed;
set session transaction isolation level read committed, isolation level repeatable read;
set session transaction read only, read only;
set session transaction read only, read write;
select @@transaction_isolation, @@session.transaction_read_only, @@session.tx_read_only;
set session transaction_read_only = @saved_session_transaction_read_only;
set session tx_read_only = @saved_session_tx_read_only;
set session transaction_isolation = @saved_session_transaction_isolation;
