-- row_count() returns the number of rows affected by the previous statement,
-- following MySQL semantics: affected rows for DML, -1 after a result-set
-- statement or a failed statement, 0 after DDL or when no rows are affected.
drop database if exists row_count_db;
create database row_count_db;
use row_count_db;

create table t(id int primary key, v int);

-- DML reports affected rows
insert into t values (1,10),(2,20);
select row_count();

insert into t values (3,30);
select row_count();

-- insert ignore: a brand-new row is inserted
insert ignore into t values (4,40);
select row_count();

-- insert ignore: duplicate primary key is silently ignored
insert ignore into t values (1,99);
select row_count();

-- update matching rows
update t set v=v+1 where id in (1,2,3);
select row_count();

-- update matching no row
update t set v=0 where id=999;
select row_count();

-- delete
delete from t where id=4;
select row_count();

-- replace that inserts a brand-new row affects 1 row
replace into t values (50,500);
select row_count();

-- after a result-set statement, row_count() is -1
select v from t where id=1;
select row_count();

-- two consecutive selects: the second sees the previous select, so -1
select row_count();

-- DDL reports 0
create table t2(a int);
select row_count();

-- a failed statement (duplicate primary key) makes row_count() return -1
insert into t values (1,12345);
select row_count();

-- prepared statement: row_count() must be evaluated at EXECUTE time, not frozen
-- at PREPARE time
insert into t values (6,60),(7,70);
prepare stmt1 from 'select row_count()';
delete from t where id in (1,2,3);
execute stmt1;
deallocate prepare stmt1;

drop database if exists row_count_db;
