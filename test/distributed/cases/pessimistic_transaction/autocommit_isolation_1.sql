drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);

set autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;

-- @session:id=1{
use autocommit_isolation_1;
set autocommit=0;
select * from test_11;
-- @session}
commit;
set autocommit=1;

select * from test_11;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;

-- @session:id=1{
set autocommit=0;
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
set autocommit=1;
select * from test_11;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
set autocommit=0;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
set autocommit=0;
select * from test_11;
-- @session}

commit;
set autocommit=1;
select * from test_11;

-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;

-- @session:id=1{
set autocommit=0;
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
set autocommit=1;

-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}

select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
select * from test_11;

-- @session:id=1{
set autocommit=0;
select * from test_11;
Insert into test_11 values(5,4);
select * from test_11;
-- @session}

select * from test_11;
Insert into test_11 values(50,50);

-- @session:id=1{
-- @wait:0:commit
-- duplicate 50 will not rollback the txn
Insert into test_11 values(50,50);
-- txn does not rollback. record (5,4) also exists.
select * from test_11;
-- @session}

select * from test_11;
commit;
set autocommit=1;

-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}

select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set autocommit=0;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
set autocommit=0;
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
set autocommit=1;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set autocommit=0;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
set autocommit=0;
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
-- @wait:1:commit
Insert into test_11 values(100,50);

commit;
set autocommit=1;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set autocommit=0;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
set autocommit=0;
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
-- @wait:1:commit
update test_11 set c = 101 where c = 50;

commit;
set autocommit=1;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}
select * from test_11;

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set autocommit=0;
select * from test_11;
Insert into test_11 values(50,50);
select * from test_11;
-- @session:id=1{
set autocommit=0;
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
-- @wait:1:commit
update test_11 set c = 100 where d = 50;

commit;
set autocommit=1;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}
select * from test_11;


-- -------------------------------------------------------
drop table if exists test_11;
set autocommit=0;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
set autocommit=0;
select * from test_11;
-- @session}

commit;
set autocommit=1;
select * from test_11;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
set autocommit=0;
select * from test_11;
-- @session}

set autocommit=0;
drop table test_11;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}

commit;
set autocommit=1;
select * from test_11;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}

-- -------------------------------------------------------
drop table if exists test_11;
set autocommit=0;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
select * from test_11;
-- @session:id=1{
set autocommit=0;
select * from test_11;
-- @session}

-- drop table test_11;
select * from test_11;
-- @session:id=1{
select * from test_11;
-- @session}
commit;
set autocommit=1;

select * from test_11;
-- @session:id=1{
commit;
set autocommit=1;
select * from test_11;
-- @session}

drop table if exists test_11;





