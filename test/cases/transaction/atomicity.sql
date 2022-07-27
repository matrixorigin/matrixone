drop table if exists test_11;
create table test_11 (c int primary key,d int);

begin;
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Rollback;
select * from test_11 ;

begin;
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
commit;
select * from test_11 ;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
begin;
delete from test_11 where c < 3;
update test_11 set d = c + 1 where c >= 3;
rollback;
select * from test_11 ;

begin;
delete from test_11 where c <3;
update test_11 set d = c + 1 where c >= 3;
commit;
select * from test_11 ;

drop table if exists test_11;
begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
rollback;
select * from test_11 ;

begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
delete from test_11 where c <3;
update test_11 set d = c + 1 where c >= 3;
commit;
select * from test_11;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
rollback;
select * from test_11;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
commit;
drop table if exists test_11;
select * from test_11 ;


