drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set @@autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;

-- @session:id=1{
use autocommit_isolation;
select * from test_11;
-- @session}
commit;
set @@autocommit=1;

select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set @@autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;

-- @session:id=1{
select * from test_11;
-- @session}

delete from test_11 where c =1;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

update test_11 set d = c +1 where c > 2;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

commit;
set @@autocommit=1;

select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;

set @@autocommit=0;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

commit;
set @@autocommit=1;

select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
-- @bvt:issue#6949
set @@autocommit=0;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
delete from test_11 where c = 1;
select * from test_11;
-- @session}
-- @pattern
Insert into test_11 values(1,1);
select * from test_11;

commit;
set @@autocommit=1;

select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}
-- @bvt:issue

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set @@autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;
-- @session:id=1{
select * from test_11;
Insert into test_11 values(5,4);
select * from test_11;
-- @session}
select * from test_11;
Insert into test_11 values(50,50);
-- @session:id=1{
-- @wait:0:commit
-- @pattern
Insert into test_11 values(50,50);
select * from test_11;
-- @session}
select * from test_11;
commit;
set @@autocommit=1;

-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set @@autocommit=0;
select * from test_11;
-- @session:id=1{
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session}
-- @pattern
Insert into test_11 values(50,50);
select * from test_11;
commit;
set @@autocommit=1;
-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set @@autocommit=0;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @wait:0:commit
-- @pattern
Insert into test_11 values(50,50);
select * from test_11;
-- @session}
select * from test_11;

-- @session:id=1{
select * from test_11;
delete from test_11 where c = 50;
select * from test_11;
-- @session}
select * from test_11;

commit;
set @@autocommit=1;

-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set @@autocommit=0;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @wait:0:commit
-- @pattern
Insert into test_11 values(50,50);
select * from test_11;
-- @session}
select * from test_11;

-- @session:id=1{
select * from test_11;
update test_11 set c = 100 where d = 50;
select * from test_11;
-- @session}
select * from test_11;
-- @pattern
Insert into test_11 values(100,50);

commit;
set @@autocommit=1;

-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set @@autocommit=0;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @wait:0:commit
-- @pattern
Insert into test_11 values(50,50);
select * from test_11;
-- @session}
select * from test_11;

-- @session:id=1{
select * from test_11;
update test_11 set c = 100 where d = 50;
select * from test_11;
-- @session}
select * from test_11;
update test_11 set c = 101 where c = 50;

commit;
set @@autocommit=1;

-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set @@autocommit=0;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @wait:0:commit
-- @pattern
Insert into test_11 values(50,50);
select * from test_11;
-- @session}
select * from test_11;

-- @session:id=1{
select * from test_11;
update test_11 set c = 100 where d = 50;
select * from test_11;
-- @session}
select * from test_11;
update test_11 set c = 100 where d = 50;

commit;
set @@autocommit=1;
-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
set @@autocommit=0;

create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

commit;
set @@autocommit=1;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

set @@autocommit=0;
drop table test_11;
-- test_11 does not exist. the txn will not be rollback.
-- so the test_11 will be deleted after the txn is committed.
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

commit;
set @@autocommit=1;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
set @@autocommit=0;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- drop table test_11;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}
commit;
set @@autocommit=1;

select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

drop table if exists test_11;
create database test_drop_db;
set autocommit=0;
use test_drop_db;
drop database test_drop_db;
show databases like '%test_drop_db%';
set autocommit=1;
