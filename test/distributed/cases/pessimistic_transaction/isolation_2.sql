--env prepare statement
drop table if exists dis_table_01;
drop table if exists dis_table_02;
drop table if exists dis_table_03;
drop table if exists dis_table_04;
drop table if exists dis_table_05;
drop table if exists dis_table_06;
drop table if exists dis_table_07;
drop table if exists dis_view_01;
drop table if exists dis_view_02;
drop table if exists dis_temp_01;
drop table if exists iso_table_0001;

create table dis_table_01(a int,b varchar(25));
insert into dis_table_01 select 20,'apple';
insert into dis_table_01 select 21,'orange';
start transaction;
create view dis_view_01 as select * from dis_table_01;
-- @session:id=1{
use isolation_2;
begin;
insert into dis_table_01 values (22,'pear');
select * from dis_table_01;
update dis_table_01 set b='bens' where a=20;
select * from dis_table_01;
rollback ;
-- @session}
select * from dis_view_01;
-- @session:id=2{
use isolation_2;
select * from dis_table_01;
update dis_table_01 set a=19 where b='apple';
select * from dis_table_01;
-- @session}
commit;
select * from dis_view_01;
select * from dis_table_01;

-------------------------
create table dis_table_02(a int not null auto_increment,b varchar(25) not null,c datetime,primary key(a),key bstr (b),key cdate (c) );
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
create table dis_table_03(b varchar(25) primary key,c datetime);
begin ;
insert into dis_table_03 select b,c from dis_table_02;
select * from dis_table_03;
-- @session:id=1{
insert into dis_table_03 select 'bbb','2012-09-30';
update dis_table_03 set b='aaa';
select * from dis_table_03;
-- @session}
-- @session:id=2{
select * from dis_table_03;
truncate table dis_table_03;
-- @session}
insert into dis_table_03 select 'bbb','2012-09-30';
select * from dis_table_03;
commit;
select * from dis_table_03;

begin ;
insert into dis_table_02 values (null,'ccc',null);
select * from dis_table_02;
-- @session:id=1{
start transaction ;
insert into dis_table_02 values (5,null,'1345-09-23');
select * from dis_table_02;
commit;
-- @session}
update dis_table_02 set a=90;
commit;
select * from dis_table_02;

---------------------------------------
start transaction ;
create database dis_db_01;
use dis_db_01;
begin;
create table dis_table_04(a int);
insert into dis_table_04 values (4);
-- @session:id=1{
create table dis_table_04(a int);
insert into dis_table_04 values (4);
drop database dis_db_01;
-- @session}
delete from dis_table_04 where a=4;
select * from dis_table_04;
rollback ;
select * from dis_db_01.dis_table_04;
drop database dis_db_01;
drop table isolation_2.dis_table_04;
---------------------------------------
begin;
use isolation_2;
create external table ex_table_dis(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_number.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select num_col1,num_col2 from ex_table_dis;
-- @session:id=1{
select * from ex_table_dis;
-- @session}
update ex_table_dis set num_col1=1000;
select num_col1,num_col2 from ex_table_dis;
commit;
select num_col1,num_col2 from ex_table_dis;
-- @session:id=1{
insert into dis_table_01 select num_col1,'fffff' from ex_table_dis;
select * from dis_table_01;
select num_col1,num_col2 from ex_table_dis;
drop table ex_table_dis;
-- @session}
select * from dis_table_01;

begin;
create view  aaa as select * from dis_table_02;
show create table aaa ;
-- @session:id=1{
insert into  dis_table_02(b,c) values ('vvv','2000-09-08');
-- @session}
-- @session:id=2{
begin ;
select b, c from dis_table_02;
delete from dis_table_02 where a=1;
rollback ;
-- @session}
commit ;
select b, c from aaa;
-- @session:id=1{
select b, c from aaa;
-- @session}
drop view aaa ;

start transaction ;
insert into dis_table_02(b,c) values ('','1999-06-04');
-- @session:id=1{
prepare stmt1 from "update dis_table_02 set c='2222-07-12' where a=2";
execute stmt1;
select b, c from dis_table_02;
-- @session}
update dis_table_02 set c='2000-09-02' where a=2;
select b, c from dis_table_02;
-- @session:id=2{
begin ;
create database dis_db_02;
rollback ;
-- @session}
commit;
select b, c from dis_table_02;

begin ;
prepare stmt1 from "insert into dis_table_02(b,c) values('oppo','1009-11-11')";
execute stmt1;
select b, c from dis_table_02;
-- @session:id=1{
select b, c from dis_table_02;
-- @session}
prepare stmt2 from "update dis_table_02 set a=null";
execute stmt2;
commit;
select b,c from dis_table_02;
use dis_db_02;
select b,c from dis_table_02;
insert into dis_table_02(b,c) values ('','1999-06-04');

------------------------------
-- @bvt:issue#9124
create temporary table dis_temp_01(a int,b varchar(100),primary key(a));
begin ;
insert into dis_temp_01 values (233,'uuuu');
-- @session:id=1{
select * from dis_temp_01;
-- @session}
select * from dis_temp_01;
-- @session:id=1{
truncate table dis_temp_01;
-- @session}
rollback ;
select * from dis_temp_01;
drop table dis_temp_01;
-- @bvt:issue

start transaction;
load data infile '$resources/external_table_file/isolation_01.csv' into table dis_table_02;
-- @session:id=1{
update dis_table_02 set b='pppp';
select b, c from dis_table_02;
-- @session}
select b, c from dis_table_02;
-- @session:id=2{
begin ;
create view dis_view_02 as select * from dis_table_02;
insert into dis_table_02 values (2,'oooo','1802-03-20');
select b, c from dis_table_02;
-- @session}
-- @session:id=1{
use isolation_2;
select * from dis_view_02;
-- @session}
select * from dis_view_02;
-- @session:id=2{
insert into dis_table_02 values (2,'oooo','1802-03-20');
-- @session}
commit;
-- @session:id=1{
select b, c from dis_table_02;
-- @session}
select * from dis_view_02;
drop table dis_view_02;

begin ;
select * from dis_table_01;
-- @session:id=1{
truncate table dis_table_01;
-- @session}
-- @bvt:issue#9095
insert into dis_table_01 select 9999,'abcdefg';
-- @bvt:issue
-- @session:id=1{
select * from dis_table_01;
-- @session}
explain select * from dis_table_01;
commit ;
-- @session:id=1{
select * from dis_table_01;
-- @session}

begin ;
delete from dis_table_02 where a>1;
select b, c from dis_table_02;
-- @session:id=1{
select b, c from dis_table_02;
-- @wait:0:commit
update dis_table_02 set b='tittttt' where a>1;
select b, c from dis_table_02;
-- @session}
select b, c from dis_table_02;
-- @session:id=2{
rollback;
start transaction ;
update dis_table_02 set b='catttteee' where a>1;
select b, c from dis_table_02;
commit;
-- @session}
commit;
select b, c from dis_table_02;
-- @session:id=1{
select b, c from dis_table_02;
-- @session}

--------------------------------
create database if not exists iso_db_02;
start transaction ;
use iso_db_02;
show tables;
-- @session:id=1{
begin ;
use iso_db_02;
create table iso_table_0001(a int);
-- @session}
insert into iso_table_0001 values (2);
-- @session:id=2{
use iso_db_02;
create table iso_table_0001(a int);
drop database iso_db_02;
-- @session}
-- @session:id=1{
commit;
-- @session}
create table iso_table_0001(a int);
commit;
use iso_db_02;
select * from iso_table_0001;

use isolation_2;
create table dis_table_04(a int,b varchar(25) not null,c datetime,primary key(a),unique key bstr (b),key cdate (c));
insert into dis_table_04 values (6666,'kkkk','2010-11-25');
insert into dis_table_04 values (879,'oopp','2011-11-26');
insert into dis_table_01 select 20,'apple';
insert into dis_table_01 select 21,'orange';
select * from dis_table_01;
start transaction ;
use isolation_2;
update dis_table_04 set b=(select 'ccccool' from dis_table_01 limit 1)  where a=879;
select * from dis_table_04 ;
-- @session:id=1{
begin ;
use isolation_2;
-- @wait:0:commit
update dis_table_04 set b='uuyyy' where a=879;
select * from dis_table_04;
commit;
-- @session}
commit;
update dis_table_04 set b=(select 'kkkk')  where a=879;
-- @session:id=1{
select * from dis_table_04;
-- @session}
----------------------------
-- @bvt:issue#9124
begin ;
use isolation_2;
create temporary table dis_table_05(a int,b varchar(25) not null,c datetime,primary key(a),unique key bstr (b),key cdate (c));
load data infile 'fff.csv' to dis_table_05;
-- @session:id=1{
use isolation_2;
select * from dis_table_05;
-- @session}
insert into dis_table_05 values (8900,'kkkk77','1772-04-20');
commit;
select * from dis_table_05;
-- @session:id=1{
select * from dis_table_05;
-- @session}
drop table dis_table_05;
-- @bvt:issue

-- auto_increment 主键冲突
use isolation_2;
create table dis_table_06(a int auto_increment primary key,b varchar(25),c double default 0.0);
insert into dis_table_06(a,b) values(2,'moon');
insert into dis_table_06(b) values('sun');
begin;
use isolation_2;
insert into dis_table_06(a,b) values (3,'llllp');
select * from dis_table_06;
-- @session:id=1{
use isolation_2;
-- @wait:0:commit
insert into dis_table_06 values (3,'uuubbb',12.02);
select * from dis_table_06;
-- @session}
insert into dis_table_06(a,b) values (4,'cookie');
commit;
select * from dis_table_06;

begin;
use isolation_2;
insert into dis_table_06(a,b) values (5,'leetio');
select * from dis_table_06;
-- @session:id=1{
update dis_table_06 set a=5 where b='sun';
select * from dis_table_06;
-- @session}
commit;
select * from dis_table_06;
drop table dis_table_06;

--compk 冲突
create table dis_table_07(a int,b varchar(25),c double,d datetime,primary key(a,b,d));
insert into dis_table_07 values (1,'yellow',20.09,'2020-09-27');
begin;
insert into dis_table_07 values (2,'blue',10.00,'2021-01-20');
-- @session:id=1{
use isolation_2;
-- @wait:0:commit
insert into dis_table_07 values (2,'blue',11.00,'2021-01-20');
select * from dis_table_07;
-- @session}
select * from dis_table_07;
commit;
select * from dis_table_07;
-- @session:id=1{
insert into dis_table_07 values (2,'blue',12.00,'2024-01-20');
-- @session}
begin;
update dis_table_07 set d='2024-01-20' where a=2 and b='blue';
-- @session:id=1{
select * from dis_table_07;
-- @session}
select * from dis_table_07;
commit;
select * from dis_table_07;
drop table dis_table_07;
