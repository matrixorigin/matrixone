drop database if exists special;
create database special;
use special;

-- case 1
create table ct_07(a int,b varchar(25),c date, d double,primary key(a,c));
insert into ct_07 values (1,'901','2011-09-29',0.01),(2,'187','2011-09-29',1.31),(3,'90','2111-02-09',10.01);
begin;
insert into ct_07 values (3,'90','2111-02-09',10.01);
-- @pattern
insert into ct_07 values (4,'11','2011-09-29',7.00),(2,'567','2011-09-29',1.31),(4,'90','2011-09-29',89.3);
select * from ct_07;
commit;
select * from ct_07;

-- case 2
create table dis_table_02(a int not null auto_increment,b varchar(25) not null,c datetime,primary key(a),key bstr (b),key cdate (c) );
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
create table dis_table_03(b varchar(25) primary key,c datetime);
begin ;
insert into dis_table_03 select b,c from dis_table_02;
select * from dis_table_03;
-- @session:id=1{
use special;
insert into dis_table_03 select 'bbb','2012-09-30';
update dis_table_03 set b='aaa';
select * from dis_table_03;
-- @session}
-- @session:id=2{
use special;
select * from dis_table_03;
truncate table dis_table_03;
-- @session}
insert into dis_table_03 select 'bbb','2012-09-30';
select * from dis_table_03;
commit;
select * from dis_table_03;

-- case 3
create database if not exists iso_db_02;
start transaction ;
use special;
show tables;
-- @session:id=1{
begin ;
use special;
create table iso_table_0001(a int);
-- @session}
insert into iso_table_0001 values (2);
-- @session:id=2{
use special;
create table iso_table_0001(a int);
drop database iso_db_02;
-- @session}
-- @session:id=1{
commit;
-- @session}
create table iso_table_0001(a int);
commit;
use special;
select * from iso_table_0001;

drop database if exists special;