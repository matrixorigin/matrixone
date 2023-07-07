-- @suit

-- @case
-- @desc:Test echo autocommit
-- @label:bvt


SELECT @@session.autocommit;


SET @@session.autocommit=1;
SELECT @@session.autocommit;

SET @@session.autocommit= 0;
SELECT @@session.autocommit;
-- select has started a txn
rollback;

SET @@session.autocommit=OFF;
SELECT @@session.autocommit;
-- select has started a txn
rollback;

SET @@session.autocommit=ON;
SELECT @@session.autocommit;

--error You have an error in your SQL syntax;
SET @@session.autocommit=foo;
SELECT @@session.autocommit;
commit;

SET @@session.autocommit=OFF;
SELECT @@session.autocommit;
commit;

SET @@session.autocommit=ON;
SELECT @@session.autocommit;
commit;

-- convert to the system variable bool type failed
SET @@session.autocommit=foo;
SELECT @@session.autocommit;
commit;


-- @case
-- @desc:Test implicit transaction commit (commit;,rollback;)
-- @label:bvt

set autocommit=0;
select @@autocommit;
commit;

-- Test implicit transaction rollback
drop database if exists db;
create database db;
show databases like 'db';
use db;
create table tab1(a int, b int);
create view view_tab1 as select * from tab1;
insert into tab1 values (2000, 3000);
rollback;
select * from tab1;
commit;


-- Test implicit transaction commit
drop database if exists db;
create database db;
show databases like 'db';
use db;
create table tab1(a int, b int);

-- test table tab1 DML and commit; rollback;
insert into tab1 values (2000, 3000);
insert into tab1 values (10, 10);
commit;
select * from tab1;
update tab1 set a=100000 where b=3000;
select * from tab1;
rollback;
select * from tab1;
update tab1 set a=100000 where b=3000;
commit;
select * from tab1;
delete from tab1 where a=10;
rollback;
select * from tab1;
delete from tab1 where a=10;
commit;
select * from tab1;


-- test view view_tab1 DML and commit; rollback;(view nonsupport insert/delete/update)
create view view_tab1 as select * from tab1;
select * from view_tab1;
commit;

insert into view_tab1 values (200, 300);
commit;

drop database db;

use autocommit_1;
commit;

-- test rollback
drop table if exists t1;
create table t1(col1 varchar(255));
insert into t1 values ('helloworld');
rollback;
-- echo error
select * from t1;
commit;

-- test commit
drop table if exists t2;
create table t2 (a varchar(255));
insert into t2 values ('hello');
commit;
select * from t2;
commit;
drop table t2;


-- @case
-- @desc:Test implicit transaction uncommitted, modify AUTOCOMMIT value, mo throw exception
-- @label:bvt

drop table if exists t3;
create table t3(a int);
insert into t3 values (10),(20),(30);

-- echo mo throw exception
set @@autocommit=ON;
select @@autocommit;

-- echo mo throw exception
set @@autocommit=OFF;
select @@autocommit;

-- echo mo throw exception
set @@autocommit=1;
select @@autocommit;

-- echo mo throw exception
set @@autocommit=0;
select @@autocommit;

rollback;


drop table if exists tab3;
create table tab3 (a int, b varchar(25));
insert into tab3 values (10, 'aa'),(20, 'bb'),(30, 'cc');
-- echo mo throw exception
set @@autocommit=ON;
select @@autocommit;

-- echo mo throw exception
set @@autocommit=OFF;
select @@autocommit;

-- echo mo throw exception
set @@autocommit=1;
select @@autocommit;

-- echo mo throw exception
set @@autocommit=0;
commit;

select * from tab3;
update tab3 set a=1000 where b='aa';
select * from tab3;
rollback;
delete from tab3 where b='cc';
select * from tab3;
commit;
select * from tab3;
commit;

drop table tab3;

-- test An implicit transaction has uncommitted content.
-- Turning on an explicit transaction forces the previously uncommitted content to be committed

drop table if exists t4;
create table t4(a varchar(225), b int);
insert into t4 values ('aa', 1000),('bb', 2000);

begin;
select * from t4;
update t4 set a='xxxx' where b=1000;
select * from t4;
rollback;

select * from t4;
update t4 set a='xxxx' where b=1000;
select * from t4;
commit;
select * from t4;

create view view_t4 as select * from t4;

begin;
select * from view_t4;
delete from t4 where a='bb';
rollback;

select * from t4;
select * from view_t4;
commit;


-- @case
-- @desc:Test explicit transaction commit (commit;rollback;)
-- @label:bvt

set autocommit=1;
select @@autocommit;

drop database if exists test_xx;
begin;
create database test_xx;

-- echo internal error: Uncommitted transaction exists. Please commit or rollback first.
SET @@session.autocommit=1;
SELECT @@session.autocommit;

-- echo internal error: Uncommitted transaction exists. Please commit or rollback first.
SET @@session.autocommit= 0;
SELECT @@session.autocommit;

-- echo internal error: Uncommitted transaction exists. Please commit or rollback first.
SET @@session.autocommit=OFF;
SELECT @@session.autocommit;

-- echo internal error: Uncommitted transaction exists. Please commit or rollback first.
SET @@session.autocommit=ON;
SELECT @@session.autocommit;
commit;

show databases like 'test_xx';
commit;
drop database test_xx;


-- Test explicit transaction rollback;
drop database if exists db;
begin;
create database db;
show databases like 'db';
use db;

begin;
create table table3(a int, b int);
insert into table3 values (2000, 3000);
create view view_table3 as select * from table3;
select * from table3;
select * from view_table3;
rollback;

-- echo error
select * from table3;
select * from view_table3;


-- Test explicit transaction commit;

drop database if exists db;
begin;
create database db;
show databases like 'db';
use db;
create table table3(a int, b int);


-- test table table3 DML and commit; rollback;
insert into table3 values (2000, 3000);
insert into table3 values (10, 10);
commit;
select * from table3;

begin;
update table3 set a=100000 where b=3000;
select * from table3;
rollback;
select * from table3;

begin;
update table3 set a=100000 where b=3000;
commit;
select * from table3;

begin;
delete from table3 where a=10;
rollback;
select * from table3;

begin;
delete from table3 where a=10;
commit;
select * from table3;

-- Test start transaction;rollback;commit;

drop table if exists t3;
start transaction;
create table t3 (b varchar(255));
insert into t3 values ('helloworld');
rollback ;
select * from t3;

drop table if exists t4;
start transaction;
create table t4 (a int);
insert into t4 values (10),(20);
commit;
select * from t4;
drop table t4;


-- Test explicit transactions are nested, Uncommitted content is forced to be submitted
drop table if exists t5;
start transaction;

create table t5(a int);
insert into t5 values(10),(20),(30);
-- execute success
drop table t5;

start transaction;
-- t5 is dropped. error and rollback.
insert into t5 values(100),(2000),(3000);
-- execute success due to last txn rollback.
set @autocommit=0;
begin;
-- error. t5 is dropped. txn rollback.
select * from t5;

insert into t5 values(1),(2),(3);
rollback;

-- error. t5 is dropped. txn rollback.
select * from t5;

begin;
-- error. t5 is dropped. txn rollback.
select * from t5;
insert into t5 values(100),(2000),(3000);
delete from t5;

begin;
select * from t5;
rollback;

select * from t5;

drop table t5



-- Test explicit transactions  include set command;
    start transaction;
-- execute error
set @@a=0;
rollback;

set @@b=0;
-- execute error
commit;
-- execute error
select @@b;


-- Test AUTOCOMMIT=1 Each DML statement is a separate transaction

drop database if exists db;
create database db;
show databases like 'db';
use db;
create table t6(a int, b int);

-- test table t6 DML and commit; rollback;
insert into t6 values (2000, 3000);
insert into t6 values (10, 10);

select * from t6;
update t6 set a=100000 where b=3000;
select * from t6;
delete from t6 where a=10;

select * from t6;


-- test view view_t6 DML and commit; rollback;
create view view_t6 as select * from t6;
select * from view_t6;

insert into view_t6 values (200, 300);
insert into view_t6 values (10, 10);


select * from view_t6;
update view_t6 set a=100000 where b=3000;
select * from view_t6;
delete from view_t6 where a=10;
select * from view_t6;

drop database db;

use autocommit_1;



-- @case
-- @desc:Test Nested explicit transactions within implicit transactions
-- @label:bvt

set @@autocommit=0;
select @@autocommit;

create table t7(a int);
insert into t7 values (500);
commit;


begin;
insert into t7 values (1000);
commit;
insert into t7 values (2000);
rollback;
select * from t7;
drop table t7;
commit;
drop table t7;

create table t8(a int);
insert into t8 values (500);
rollback;


begin;
insert into t8 values (1000);
create table t9 (a char(25));
commit;

insert into t9 values ('hello');
rollback;
select * from t9;
commit;
drop table t9;
rollback;
set @@autocommit=on;

