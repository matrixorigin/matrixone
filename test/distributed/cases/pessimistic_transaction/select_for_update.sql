create table su_01(c1 int not null,c2 varchar(25),c3 int,primary key(c1),unique index u1(c2));
insert into su_01 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- pk , one rows lock, lock and unlock row update/delete/insert/truncate/alter/drop
begin;
select * from su_01 where c1=1 for update;
-- @session:id=1{
use select_for_update;
select * from su_01 where c1=1;
update su_01 set c2='loo' where c1=2;
select * from su_01;
-- @session}
commit;

truncate table su_01;
insert into su_01 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_01 where c1=1 for update;
-- @session:id=1{
use select_for_update;
select * from su_01 where c1=1;
insert into su_01 values(6,'polly',70);
select * from su_01;
-- @session}
commit;

truncate table su_01;
insert into su_01 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_01 where c1=1 for update;
-- @session:id=1{
use select_for_update;
select * from su_01 where c1=1;
-- @wait:0:commit
delete from su_01 where c1=1;
select * from su_01;
-- @session}
commit;

insert into su_01 values(1,'results',20);

begin;
select * from su_01 where c1=1 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
truncate table su_01 ;
select * from su_01;
-- @session}
commit;

insert into su_01 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_01 where c1=1 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
alter table su_01 drop index u1;
select * from su_01;
show create table su_01;
-- @session}
commit;

begin;
select * from su_01 where c1=1 for update;
update su_01 set c2='desc' where c1=1;
update su_01 set c3=c3-1 where c1=1;
select * from su_01 where c1=1;
insert into su_01 values(5,'polly',80);
commit;
select * from su_01;

truncate table su_01;
insert into su_01 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_01 where c1=1 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
drop table su_01;
select * from su_01;
-- @session}
commit;

create table su_01(c1 int not null,c2 varchar(25),c3 int,primary key(c1),unique index u1(c2));
insert into su_01 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- select pk not return data
begin;
select * from su_01 where c1=7 for update;
-- @session:id=1{
use select_for_update;
select * from su_01;
delete from su_01 where c1=7;
-- @session}
commit;

begin;
select * from su_01 where c1=7 for update;
-- @session:id=1{
use select_for_update;
select * from su_01;
update su_01 set c3=c3*10 where c1=1;
-- @session}
commit;
drop table su_01;

create table su_01_1(c1 int not null,c2 varchar(25),c3 int,primary key(c1));
insert into su_01_1 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- pk , more rows lock,where pk and non pk
start transaction ;
select * from su_01_1 where c1>1 and c3<70 for update;
-- @session:id=1{
use select_for_update;
update su_01_1 set c3=c3+10 where c3=70;
select * from su_01_1;
-- @session}
commit;
select * from su_01_1;
start transaction;
select * from su_01_1 where c1>1 and c3<70 for update;
-- @session:id=1{
use select_for_update;
update su_01_1 set c3=c3-1 where c1=4;
select * from su_01_1;
-- @session}
rollback;

start transaction;
select * from su_01_1 where c1>1 and c3<70 for update;
-- @session:id=1{
begin;
use select_for_update;
-- @wait:0:rollback
update su_01_1 set c3=101 where c1=3;
select * from su_01_1;
rollback ;
-- @session}
rollback;

start transaction ;
select * from su_01_1 where c1>1 and c3<70 for update;
-- @session:id=1{
begin;
use select_for_update;
delete from su_01_1 where c1=1;
select * from su_01_1;
rollback ;
-- @session}
commit;

start transaction ;
select * from su_01_1 where c1>1 and c3<70 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:rollback
delete from su_01_1 where c2="plo";
select * from su_01_1;
-- @session}
rollback;

start transaction;
select * from su_01_1 where c1>1 and c3<70 for update;
-- @session:id=1{
use select_for_update;
insert into su_01_1 values (10,'full',100);
select * from su_01_1;
-- @session}
rollback;

start transaction;
select * from su_01_1 where c1>1 and c3<70 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:rollback
truncate table su_01_1;
select * from su_01_1;
-- @session}
rollback;

insert into su_01_1 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

start transaction;
select * from su_01_1 where c1>1 and c3<70 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:rollback
drop table su_01_1;
select * from su_01_1;
-- @session}
rollback;

create table su_02(c1 int not null,c2 varchar(25),c3 int,unique index u1(c3));
insert into su_02 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- unique index , more rows lock,lock and unlock row update/delete/insert/truncate/alter/drop
begin;
select * from su_02 where c3>35 for update;
-- @session:id=1{
use select_for_update;
select * from su_02 where c3>35;
insert into su_02 values (8,'results',100);
select * from su_02;
-- @session}
commit;

begin;
select * from su_02 where c3>35 for update;
-- @session:id=1{
use select_for_update;
select * from su_02 where c3>35;
-- @wait:0:commit
delete from su_02 where c3<60;
select * from su_02;
-- @session}
commit;

begin;
select * from su_02 where c3>35 for update;
-- @session:id=1{
use select_for_update;
select * from su_02 where c3>35;
-- @wait:0:commit
update su_02 set c2='kitty' where c3=70;
select * from su_02;
-- @session}
commit;

begin;
select * from su_02 where c3>35 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
alter table su_02 drop index u1;
show create table su_02;
-- @session}
commit;

begin;
select * from su_02 where c3>35 for update;
update su_02 set c3=c3-1 where c3=60;
insert into su_02 values(5,'polly',80);
select * from su_02;
commit;
select * from su_02;
truncate table su_02;
select * from su_02;

insert into su_02 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_02 where c3>35 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
drop table su_02;
-- @session}
commit;

drop table if exists su_02;

create table su_02_1(c1 int not null,c2 varchar(25),c3 int,unique index u1(c3));
insert into su_02_1 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- unique index more rows lock
start transaction;
select * from su_02_1 where c3 between 25 and 85 and c2 !='kelly' for update;
-- @session:id=1{
use select_for_update;
update su_02_1 set c2='non' where c3=60;
select * from su_02_1;
-- @session}
commit;

start transaction;
select * from su_02_1 where c3 between 25 and 85 and c2 !='kelly' for update;
-- @session:id=1{
use select_for_update;
update su_02_1 set c3=c3+100 where c2='results';
select * from su_02_1;
-- @session}
commit;

start transaction;
select * from su_02_1 where c3 between 25 and 85 and c2 !='kelly' for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
delete from su_02_1 where c3=50;
select * from su_02_1;
-- @session}
commit;

start transaction;
select * from su_02_1 where c3 between 25 and 85 and c2 !='kelly' for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
alter table su_02_1 drop index u1;
show create table su_02_1;
-- @session}
commit;

start transaction;
select * from su_02_1 where c3 between 25 and 85 and c2 !='kelly' for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
truncate table su_02_1;
select * from su_02_1;
-- @session}
commit;

insert into su_02_1 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

start transaction;
select * from su_02_1 where c3 between 25 and 85 and c2 !='kelly' for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
drop table su_02_1;
select * from su_02_1;
-- @session}
commit;

create table su_02_2(c1 int not null,c2 varchar(25),c3 int,unique index u1(c3));
insert into su_02_2 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- unique index one rows lock
begin;
select * from su_02_2 where c3>55 and c3<65 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
update su_02_2 set c1=c1+500 where c3=60;
select * from su_02_2;
-- @session}
commit;

begin;
select * from su_02_2 where c3>55 and c3<65 for update;
-- @session:id=1{
use select_for_update;
update su_02_2 set c1=30 where c2 like 'yell%';
select * from su_02_2;
-- @session}
commit;

create table su_03(c1 int not null,c2 varchar(25),c3 int,primary key(c1),key u1(c3));
insert into su_03 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- secondary index , more rows lock, lock and unlock row update/delete/insert/truncate/alter/drop
begin;
select * from su_03 where c3 in(50,70) for update;
-- @session:id=1{
use select_for_update;
select * from su_03 where c3>35;
insert into su_03 values (8,'results',100);
select * from su_03 ;
-- @session}
commit;

begin;
select * from su_03 where c3 in(50,70) for update;
-- @session:id=1{
use select_for_update;
select * from su_03 where c3>35;
-- @wait:0:commit
update su_03 set c2='kitty' where c3=70;
select * from su_03;
-- @session}
commit;

begin;
select * from su_03 where c3 in(50,70) for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
alter table su_03 drop index u1;
show create table su_03;
-- @session}
commit;

begin;
select * from su_03 where c3 in(50,70) for update;
-- @session:id=1{
use select_for_update;
select * from su_03 where c3>35;
-- @wait:0:commit
delete from su_03 where c3 between 10 and 70;
select * from su_03;
-- @session}
commit;

truncate table su_03;
insert into su_03 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_03 where c3 in(50,70) for update;
update su_03 set c3=c3-1 where c3=70;
insert into su_03 values(5,'polly',80);
delete from su_03 where c3=50;
select * from su_03;
commit;
update su_03 set c3=c3-1 where c3=70;
select * from su_03;

truncate table su_03;
insert into su_03 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_03 where c3 in(50,70) for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
drop table su_03;
-- @session}
commit;
drop table if exists su_03;

-- @bvt:issue#11015
-- select 0 rows, alter table drop index if wait lock
create table su_03(c1 int not null,c2 varchar(25),c3 int,primary key(c1),key u1(c3));
insert into su_03 values(1,'results',20);
begin;
select * from su_03 where c3 in(50,70) for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
alter table su_03 drop index u1;
show create table su_03;
-- @session}
commit;
drop table if exists su_03;
-- @bvt:issue

create table su_03_1(c1 int not null,c2 varchar(25),c3 int,primary key(c1),key u1(c3));
insert into su_03_1 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- secondary index , one row lock
start transaction ;
select c1 from su_03_1 where c3 between 10 and 30 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
update su_03_1 set c2='high' where c2 like "res%";
select * from su_03_1;
-- @session}
rollback;

start transaction ;
select c1 from su_03_1 where c3 between 10 and 30 for update;
-- @session:id=1{
use select_for_update;
insert into su_03_1 values(10,'boo',120);
select * from su_03_1;
-- @session}
rollback;

start transaction ;
select c1 from su_03_1 where c3 between 10 and 30 for update;
-- @session:id=1{
begin;
use select_for_update;
update su_03_1 set c2='high' where c3>=120;
select * from su_03_1;
rollback;
-- @session}
select * from su_03_1;
commit;
drop table  su_03_1;

create table su_04(c1 int not null,c2 varchar(25),c3 int);
insert into su_04 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- unconstrained column, one rows lock
begin;
select * from su_04 where c2='kelly' for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
update su_04 set c3=c3-1 where c2='kelly';
select * from su_04;
-- @session}
update su_04 set c3=c3-1 where c2='kelly';
insert into su_04 values (10,'mini',90);
select * from su_04;
commit;

begin;
select * from su_04 where c2 in ('kelly') for update;
-- @session:id=1{
start transaction;
use select_for_update;
select * from su_04 where c2='kelly';
-- @wait:0:commit
delete from su_04 where c2='kelly';
select * from su_04;
commit;
-- @session}
select * from su_04;
commit;
-- add truncate case
begin;
select * from su_04 where c2='kelly' for update;
-- @session:id=1{
use select_for_update;
delete from su_04 where c1=4;
select * from su_04;
-- @session}
update su_04 set c3=c3-1 where c2='kelly';
select * from su_04;
commit;

begin;
select * from su_04 where c2='results' for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
alter table su_04 add unique index a1(c1);
show create table su_04;
-- @session}
commit;

begin;
select * from su_04 where c2='results' for update;
insert into su_04 values(10,'tell',96);
update su_04 set c2='wed';
delete from su_04 where c1=2;
commit;

truncate table su_04;
insert into su_04 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_04 where c2='kelly' for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
drop table su_04;
-- @session}
commit;
select * from su_04;

drop table if exists su_04;

create table su_05(c1 int not null primary key,c2 varchar(25),c3 decimal(6,2))partition by key(c1)partitions 4;;
insert into  su_05 values (1,'mod',78.9),(2,'proto',0.34),(3,'mod',6.5),(4,'mode',9.0),(5,'make',662.9),(6,'io',88.92);
select * from `%!%p0%!%su_05`;
select * from `%!%p1%!%su_05`;
select * from `%!%p2%!%su_05`;
select * from `%!%p3%!%su_05`;
-- ddl partition, pk ,more rows lock
begin;
select * from su_05 where c1>3 for update;
-- @session:id=1{
use select_for_update;
select * from su_05;
-- @wait:0:commit
update su_05 set c3=c3-1 where c1>2;
select * from su_05;
-- @session}
commit;
update su_05 set c3=c3-1 where c1>2;
select * from su_05;

begin;
select * from su_05 where c1 in(1,3,6) and c2 !='io' for update;
-- @session:id=1{
use select_for_update;
select * from su_05;
-- @wait:0:commit
delete from su_05 where c1=1;
select * from su_05;
-- @session}
commit;
begin;
select * from su_05 where c1 in(1,3,6) and c2 !='io' for update;
-- @session:id=1{
use select_for_update;
select * from su_05;
-- @wait:0:commit
truncate table su_05;
select * from su_05;
-- @session}
commit;

insert into  su_05 values (1,'mod',78.9),(2,'proto',0.34),(3,'mod',6.5),(4,'mode',9.0),(5,'make',662.9),(6,'io',88.92);

begin;
select * from su_05 where c1 in(1,3,6) and c2 !='io' for update;
-- @session:id=1{
use select_for_update;
select * from su_05;
insert into su_05 values(9,'kol',89.01);
select * from su_05;
-- @session}
commit;

begin;
select * from su_05 where c1 in(1,3,6) and c2 !='io' for update;
-- @session:id=1{
use select_for_update;
select * from su_05;
update su_05 set c2='polly' where c1=9;
select * from su_05;
-- @session}
commit;
drop table su_05;
-- @bvt:issue#11009
create table su_05_1(c1 int auto_increment primary key,c2 varchar(25),c3 decimal(6,2))partition by key(c1)partitions 4;;
insert into  su_05_1(c2,c3) values ('mod',78.9),('proto',0.34),('mod',6.5),('mode',9.0),('make',662.9),('io',88.92);
select * from `%!%p0%!%su_05_1`;
select * from `%!%p1%!%su_05_1`;
select * from `%!%p2%!%su_05_1`;
select * from `%!%p3%!%su_05_1`;
-- ddl partition, pk ,one rows lock
set autocommit=0;
select * from su_05_1 where c1=4 for update;
-- @session:id=1{
use select_for_update;
update su_05_1 set c2='polly' where c3=9.0;
select * from su_05_1;
-- @session}
commit;

select * from su_05_1 where c1=4 for update;
-- @session:id=1{
use select_for_update;
update su_05_1 set c3=c3-0.09 where c1=4;
select * from su_05_1;
-- @session}
commit;

select * from su_05_1 where c1=4 for update;
-- @session:id=1{
use select_for_update;
insert into su_05_1(c2,c3) values('xin',8.90);
select * from su_05_1;
-- @session}
commit;

select * from su_05_1 where c1=3 for update;
-- @session:id=1{
use select_for_update;
alter table su_05_1 add unique index s1(c3);
show create table su_05_1;
-- @session}
commit;

select * from su_05_1 where c1=3 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
truncate table su_05_1;
select * from su_05_1;
-- @session}
commit;

insert into  su_05_1(c2,c3) values ('mod',78.9),('proto',0.34),('mod',6.5),('mode',9.0),('make',662.9),('io',88.92);

select * from su_05_1 where c1=3 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
drop table su_05_1;
select * from su_05_1;
-- @session}
commit;
-- @bvt:issue
set autocommit=1;

create table su_06(c1 int not null,c2 varchar(25),c3 int,primary key(c1),key u1(c3));
insert into su_06 values(1,'results',20),(2,'plo',50),(3,'kelly',60),(4,'yellow',70);
start transaction;
select * from su_06 where c1>=2 for update;
-- @session:id=1{
use select_for_update;
prepare stmt1 from 'update su_06 set c3=c3+1.09 where c1=?';
set @var = 2;
-- @wait:0:commit
execute stmt1 using @var;
select * from su_06;
-- @session}
commit;

start transaction;
select * from su_06 where c1>=2 for update;
-- @session:id=1{
use select_for_update;
prepare stmt1 from 'update su_06 set c3=c3+1.09 where c1=?';
set @var = 1;
execute stmt1 using @var;
select * from su_06;
-- @session}
commit;

start transaction;
select * from su_06 where c1>=2 for update;
-- @session:id=1{
use select_for_update;
prepare stmt1 from 'delete from su_06 where c3 in (?)';
set @var = 3;
execute stmt1 using @var;
select * from su_06;
-- @session}
commit;

create table su_07(c1 int not null,c2 varchar(25),c3 int,primary key(c1),unique index u1(c2));
insert into su_07 values(7,'results',20),(1,'plo',50),(3,'kelly',60),(4,'yellow',70);
-- pk ,more rows lock, expression
begin;
select * from su_07 where c1+2>=5 for update;
-- @session:id=1{
use select_for_update;
select * from su_07 where c1=1;
-- @wait:0:commit
update su_07 set c2='loo' where c1=4;
select * from su_07;
-- @session}
commit;

begin;
select * from su_07 where c1+2>=5 for update;
-- @session:id=1{
use select_for_update;
select * from su_07 where c1=1;
update su_07 set c2='cool' where c1=1;
select * from su_07;
-- @session}
commit;

begin;
select * from su_07 where c3-c1>20 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
delete from su_07 where c1=1;
select * from su_07;
-- @session}
commit;

truncate table su_07;
insert into su_07 values(7,'results',20),(1,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_07 where c3/c1=20 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
update su_07 set c3=c3/10 where c1=3;
select * from su_07;
-- @session}
commit;

begin;
select * from su_07 where c3<c1*10 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
alter table su_07 drop index u1;
select * from su_07;
-- @session}
commit;

begin;
select * from su_07 where c3<c1*10 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
truncate table su_07;
select * from su_07;
-- @session}
commit;

insert into su_07 values(7,'results',20),(1,'plo',50),(3,'kelly',60),(4,'yellow',70);

begin;
select * from su_07 where c3>c1 for update;
-- @session:id=1{
use select_for_update;
-- @wait:0:commit
drop table su_07;
select * from su_07;
-- @session}
commit;

drop database if exists test;
create database test;
use test;
drop table if exists t1;
create table t1(a int primary key, b int unique key);
insert into t1 select result, result from generate_series(1,10000)g;
-- @separator:table
select mo_ctl('dn','flush','test.t1');
select * from t1 where b in (1,2) for update;
drop table t1;