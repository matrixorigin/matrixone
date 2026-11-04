-- Locking reads must retry an obsolete snapshot before LIMIT stops the input.
drop database if exists locking_read_limit_retry;
create database locking_read_limit_retry;
use locking_read_limit_retry;
create table t(id varchar(8) primary key, c int);
insert into t values ('a',0),('b',0),('c',0);

begin;
update t set c=1 where id='a';
-- @session:id=1{
use locking_read_limit_retry;
set session transaction_isolation='READ-COMMITTED';
begin;
-- @wait:0:commit
select c from t where id='a' limit 1 for update;
commit;
-- @session}

-- Prepared locking reads follow the same retry boundary.
begin;
update t set c=1 where id='b';
-- @session:id=1{
prepare locking_read_stmt from 'select c from t where id=? limit 1 offset 0 for update';
set @locking_read_id='b';
begin;
-- @wait:0:commit
execute locking_read_stmt using @locking_read_id;
commit;
deallocate prepare locking_read_stmt;
-- @session}

-- The session row limit can stop a query without an explicit LIMIT clause.
begin;
update t set c=1 where id='c';
-- @session:id=1{
set @saved_locking_read_limit=@@sql_select_limit;
set session sql_select_limit=1;
begin;
-- @wait:0:commit
select c from t where id='c' for update;
commit;
set session sql_select_limit=cast(@saved_locking_read_limit as unsigned);
set session transaction_isolation=default;
-- @session}
select id,c from t order by id;
drop database locking_read_limit_retry;
