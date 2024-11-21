drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;

-- @session:id=1{
use isolation;
select * from test_11;
-- @session}
commit;

select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
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
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

commit;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
-- @bvt:issue#6949
drop table if exists test_11;
begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
delete from test_11 where c = 1;
select * from test_11;
-- @session}
Insert into test_11 values(1,1);
select * from test_11;

commit;
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
begin;
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
Insert into test_11 values(50,50);
select * from test_11;
-- @session}
select * from test_11;
commit;
-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
select * from test_11;
-- @session:id=1{
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session}

Insert into test_11 values(50,50);
select * from test_11;
commit;
-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @wait:0:commit
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
-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @wait:0:commit
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
Insert into test_11 values(100,50);

commit;
-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @wait:0:commit
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
-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @wait:0:commit
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
-- @session:id=1{
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

commit;
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

begin;
drop table test_11;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

commit;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
begin;
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

select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

drop table if exists test_11;

drop table if exists t1;
create table t1 (a int not null, b int);
insert into t1 values (1, 1);
begin;
select * from t1;
update t1 set a=null where b=1;
select * from t1;
commit;
drop table if exists t1;

-- -------------------------------------------------------
drop table if exists rename01;
create table rename01 (c int primary key,d int);
insert into rename01 values(1,1);
insert into rename01 values(2,2);
begin;
insert into rename01 values(3,1);
insert into rename01 values(4,2);
alter table rename01 rename column c to `newCCCC`;
select * from rename01;
-- @session:id=1{
use isolation;
-- @wait:0:commit
insert into rename01 (c, d) values (5,7);
insert into rename01 (newCCCC, d) values (5,7);
select * from rename01;
-- @session}
select * from rename01;
drop table rename01;

drop table if exists t1;
create table t1 (a int primary key, b int);
begin;
delete from t1 where a = 1;
-- @session:id=1{
use isolation;
-- @wait:0:commit
delete from t1 where a = 1;
-- @session}
commit;

begin;
delete from t1 where a in (1,2,3);
-- @session:id=1{
use isolation;
-- @wait:0:commit
delete from t1 where a = 3;
-- @session}
commit;

begin;
update t1 set b = 10 where a = 1;
-- @session:id=1{
use isolation;
-- @wait:0:commit
delete from t1 where a = 1;
-- @session}
commit;

begin;
select * from t1 where a = 1 for update;
-- @session:id=1{
use isolation;
-- @wait:0:commit
delete from t1 where a = 1;
-- @session}
commit;
drop table if exists t1;
