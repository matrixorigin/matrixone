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

-- Access modes are still accepted for syntax compatibility.
set session transaction isolation level read committed, read write, read only;
select @@transaction_isolation;
set session transaction_isolation = @saved_session_transaction_isolation;
