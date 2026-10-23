-- The admission-pressure and FIFO branches are injected deterministically in
-- compile/rscthrottler UT. This case checks the real LOCAL INFILE transaction
-- contract with a prior write, two input rows, and a second observing session.
drop database if exists import_memory_transaction;
create database import_memory_transaction;
use import_memory_transaction;
create table kept (id int primary key, payload varchar(8));
create table incoming (id int primary key);

begin;
insert into kept values (1, 'first');
load data local infile '$resources/load_data/test_columnlist_04.csv' into table incoming fields terminated by ',' lines terminated by '\n' parallel 'false';
select id, payload from kept order by id;
select id from incoming order by id;
-- @session:id=2&user=sys:dump&password=111
select count(*) as n from import_memory_transaction.kept;
select count(*) as n from import_memory_transaction.incoming;
-- @session
commit;
-- @session:id=2&user=sys:dump&password=111
select id, payload from import_memory_transaction.kept order by id;
select id from import_memory_transaction.incoming order by id;
-- @session

delete from incoming;
begin;
insert into kept values (2, 'undo');
load data local infile '$resources/load_data/test_columnlist_04.csv' into table incoming fields terminated by ',' lines terminated by '\n' parallel 'false';
select id, payload from kept order by id;
select id from incoming order by id;
rollback;
select id, payload from kept order by id;
select count(*) as n from incoming;

set autocommit=0;
insert into kept values (3, 'third');
load data local infile '$resources/load_data/test_columnlist_04.csv' into table incoming fields terminated by ',' lines terminated by '\n' parallel 'false';
commit;
set autocommit=1;
select id, payload from kept order by id;
select id from incoming order by id;

delete from incoming;
set autocommit=0;
insert into kept values (4, 'undo');
load data local infile '$resources/load_data/test_columnlist_04.csv' into table incoming fields terminated by ',' lines terminated by '\n' parallel 'false';
rollback;
set autocommit=1;
select id, payload from kept order by id;
select count(*) as n from incoming;

-- The ordinary autocommit path remains usable after both rollback modes.
load data local infile '$resources/load_data/test_columnlist_04.csv' into table incoming fields terminated by ',' lines terminated by '\n' parallel 'false';
select id from incoming order by id;
drop database import_memory_transaction;
