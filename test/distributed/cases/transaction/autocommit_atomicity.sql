drop table if exists test_11;
create table test_11 (c int primary key,d int);

set @@autocommit=0;
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Rollback;
set @@autocommit=1;
select * from test_11 ;

set @@autocommit=0;
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
commit;
set @@autocommit=1;
select * from test_11 ;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
set @@autocommit=0;
delete from test_11 where c < 3;
update test_11 set d = c + 1 where c >= 3;
rollback;
set @@autocommit=1;
select * from test_11 ;

set @@autocommit=0;
delete from test_11 where c <3;
update test_11 set d = c + 1 where c >= 3;
commit;
set @@autocommit=1;
select * from test_11 ;

drop table if exists test_11;
set @@autocommit=0;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
rollback;
set @@autocommit=1;
select * from test_11 ;

set @@autocommit=0;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
delete from test_11 where c <3;
update test_11 set d = c + 1 where c >= 3;
commit;
set @@autocommit=1;
select * from test_11;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set @@autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
drop table if exists test_11;
rollback;
set @@autocommit=1;
select * from test_11;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
set @@autocommit=0;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
drop table if exists test_11;
commit;
set @@autocommit=1;
select * from test_11 ;



