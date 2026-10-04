-- @case
-- @desc: Executable EXPLAIN retains the prepared plan snapshot across executions.
-- @label:bvt

drop database if exists issue_29509_explain;
create database issue_29509_explain;
use issue_29509_explain;
create table t(id bigint primary key);
insert into t values (1), (2);

prepare fixed from 'select count(*) from t where id = 1';
-- @ignore:0
explain analyze force execute fixed;
-- @ignore:0
explain analyze force execute fixed;
deallocate prepare fixed;

prepare parameterized from 'select count(*) from t where id = ?';
set @id = 1;
-- @ignore:0
explain analyze force execute parameterized using @id;
set @id = 2;
-- @ignore:0
explain analyze force execute parameterized using @id;
deallocate prepare parameterized;

drop database issue_29509_explain;
