-- Regression coverage for matrixone#25118 (multi-statement error handling).
--
-- The distributed-case runner sends each SQL statement as an independent
-- request.  A statement error must therefore not poison the connection: the
-- following statement is still received and executes deterministically.  The
-- transaction blocks also document the supported all-or-nothing way to avoid
-- partial writes after an error.
drop database if exists mysql_compat_batch_error;
create database mysql_compat_batch_error;
use mysql_compat_batch_error;

create table t (id int primary key, note varchar(20));

-- An error in one request does not prevent the next request from running.
insert into t values (1, 'first');
-- @regex("Duplicate entry",true)
insert into t values (1, 'duplicate');
insert into t values (3, 'after_error');
select id, note from t order by id;

-- Explicit rollback removes successful statements surrounding the error.
truncate table t;
start transaction;
insert into t values (10, 'before_error');
-- @regex("Duplicate entry",true)
insert into t values (10, 'duplicate');
insert into t values (11, 'after_error');
rollback;
select count(*) from t;

-- If the transaction is committed, successful statements are retained.
start transaction;
insert into t values (20, 'before_error');
-- @regex("Duplicate entry",true)
insert into t values (20, 'duplicate');
insert into t values (21, 'after_error');
commit;
select id, note from t order by id;

drop database mysql_compat_batch_error;
