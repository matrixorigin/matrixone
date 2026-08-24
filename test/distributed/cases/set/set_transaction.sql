set @saved_session_transaction_isolation = @@session.transaction_isolation;
set transaction_isolation = 'REPEATABLE-READ';
set session transaction isolation level read committed;
select @@transaction_isolation;

set transaction isolation level repeatable read;
select @@transaction_isolation;

set @saved_transaction_isolation = @@global.transaction_isolation;
set global transaction isolation level read committed;
select @@global.transaction_isolation;
set global transaction_isolation = @saved_transaction_isolation;

-- Access modes fail closed until transaction-level read-only enforcement is implemented.
set session transaction read only;
set session transaction read write;
set session transaction isolation level repeatable read, read only;
select @@transaction_isolation;

-- Duplicate and conflicting characteristics are rejected before state changes.
set session transaction isolation level read committed, isolation level read committed;
set session transaction isolation level read committed, isolation level repeatable read;
set session transaction read only, read only;
set session transaction read only, read write;
set session transaction_isolation = @saved_session_transaction_isolation;
