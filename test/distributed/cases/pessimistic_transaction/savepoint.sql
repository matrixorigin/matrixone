set @@autocommit = 1;


begin;
-- no error
savepoint a;
-- no error
release savepoint a;
-- no error
savepoint b;
-- error
rollback to savepoint b;
-- no error
release savepoint b;
commit;