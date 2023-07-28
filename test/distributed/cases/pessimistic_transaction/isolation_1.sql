drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;

-- @session:id=1{
use isolation_1;
begin;
select * from test_11;
-- @session}
commit;

select * from test_11;
-- @session:id=1{
commit;
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
begin;
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
commit;
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
begin;
select * from test_11;
-- @session}

commit;
select * from test_11;

-- @session:id=1{
commit;
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
begin;
select * from test_11;
Insert into test_11 values(5,4);
select * from test_11;
-- @session}

select * from test_11;
Insert into test_11 values(50,50);

-- @session:id=1{
Insert into test_11 values(51,50);
select * from test_11;
-- @session}

select * from test_11;
commit;

-- @session:id=1{
commit;
select * from test_11;
-- @session}

select * from test_11;

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
begin;
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
commit;
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
begin;
select * from test_11;
-- @wait:0:commit
Insert into test_11 values(50,50);
select * from test_11;
-- @session}
select * from test_11;

-- @session:id=1{
delete from test_11 where c = 50;
select * from test_11;
-- @session}
select * from test_11;

commit;
-- @session:id=1{
commit;
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
-- @bvt:issue#10585
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
begin;
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
commit;
select * from test_11;
-- @session}
select * from test_11;
-- @bvt:issue
-- -------------------------------------------------------
-- @bvt:issue#10585
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
begin;
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
commit;
select * from test_11;
-- @session}
select * from test_11;
-- @bvt:issue
-- -------------------------------------------------------
-- @bvt:issue#10585
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
begin;
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
commit;
select * from test_11;
-- @session}
select * from test_11;
-- @bvt:issue

-- -------------------------------------------------------
drop table if exists test_11;
begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
begin;
select * from test_11;
-- @session}

commit;
select * from test_11;
-- @session:id=1{
commit;
select * from test_11;
-- @session}

-- -------------------------------------------------------
-- @bvt:issue#10701
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
begin;
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
commit;
select * from test_11;
-- @session}
-- @bvt:issue
-- -------------------------------------------------------
drop table if exists test_11;
begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
begin;
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
commit;
select * from test_11;
-- @session}

drop table if exists test_11;




