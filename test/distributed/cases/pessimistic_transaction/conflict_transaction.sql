-- primary key conflict/insert into values
create table ct_01(a int primary key,b varchar(25));
-- @pattern
insert into ct_01 values(1,'bell'),(2,'app'),(1,'com');
insert into ct_01 values(1,'bell'),(2,'app');
begin;
insert into ct_01 values(3,'oppo'),(3,'zow');
-- @session:id=1{
use conflict_transaction;
start transaction ;
-- @pattern
insert into ct_01 values(2,'yooo');
commit;
-- @session}
commit;
select * from ct_01;

--primary key conflict/update
create table ct_02(a int primary key,b varchar(25));
insert into ct_02 values(1,'bell'),(2,'app'),(3,'com');
start transaction ;
update ct_02 set a=5 where b='app';
-- @session:id=1{
begin;
-- @wait:0:commit
-- @pattern
update ct_02 set a=5 where b='bell';
commit;
-- @session}
commit;
begin;
-- @pattern
update ct_02 set a=3 where b='bell';
commit;
select * from ct_02;

-- primary key conflict/delete
create table ct_03(a int primary key,b varchar(25));
insert into ct_03 values(1,'bell'),(2,'app'),(3,'com');
begin;
delete from ct_03 where a=1;
select * from ct_03;
-- @session:id=1{
begin;
-- @wait:0:commit
update ct_03 set b='urea' where a=1;
select * from ct_03;
commit;
-- @session}
commit;
select * from ct_03;

-- primary key conflict/insert into select
create table ct_04_temp(a int,b varchar(25));
insert into ct_04_temp values (1,'bell'),(2,'app'),(1,'com');
create table ct_04(a int primary key,b varchar(25));
begin;
-- @pattern
insert into ct_04 select * from ct_04_temp;
commit;
select * from ct_04;

-- primary key conflict/insert infile.
-- @bvt:issue#3433
create table ct_05(a int,b varchar(25) primary key);
begin;
load data infile '$resources/load_data/ct_file.csv' into table ct_05 fields terminated by ',';
commit;
select * from ct_05;
-- @bvt:issue

--unique index and secondary index conflict
create table ct_06(a bigint,b varchar(25),c int, d varchar(25),primary key(a),unique index c(c),key b(b),key d(d));
start transaction ;
insert into ct_06 select 5678,'high',487,'comment test';
-- @bvt:issue#6949
insert into ct_06 select 5679,'lower',487,'define';
-- @bvt:issue
insert into ct_06 values (897,'number',908,'run tools'),(898,'string',908,'ffff'),(899,'string',918,'while'),(900,'string',948,'word file'),(901,'string',902,'oooo'),(902,'string',87,'dddd'),(903,'string',87,'eeee');
select * from ct_06;
commit;
select * from ct_06;

begin;
insert into ct_06 values (500,'number1',908,'ooooffff');
-- @session:id=1{
start transaction ;
-- @wait:0:commit
insert into ct_06 values (501,'number2',908,'zzzztttt');
commit;
select * from ct_06;
-- @session}
commit;
select * from ct_06;

--comprimary key conflict
create table ct_07(a int,b varchar(25),c date, d double,primary key(a,c));
insert into ct_07 values (1,'901','2011-09-29',0.01),(2,'187','2011-09-29',1.31),(3,'90','2111-02-09',10.01);
begin;
insert into ct_07 values (3,'90','2111-02-09',10.01);
-- @pattern
insert into ct_07 values (4,'11','2011-09-29',7.00),(2,'567','2011-09-29',1.31),(4,'90','2011-09-29',89.3);
select * from ct_07;
commit;
select * from ct_07;