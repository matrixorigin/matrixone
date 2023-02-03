--Bvt case only covers syntax and semantics, and this function needs to be verified by performance test
--sort key bool type
create table sort_key_01(col1 int not null,col2 Bool)cluster by col2;
insert into sort_key_01 values (10,TRUE);
insert into sort_key_01 values (4,FALSE);
insert into sort_key_01 values (20,NULL);
insert into sort_key_01 values (6,FALSE);
select * from sort_key_01;
select col2 from sort_key_01;
drop table sort_key_01;

--sort key char type and insert select sort table
create table sort_key_02(col1 int not null,col2 char(20))cluster by  col2;
insert into sort_key_02 values (10,'zacc'),(4,'del'),(20,'中国'),(2,'134'),(6,NULL),(3,''),(1,'a few mo');
select col2 from sort_key_02;
select * from sort_key_02;
create table sort_key_03(col2 char(255));
insert into sort_key_03 select col2 from sort_key_02;
select * from sort_key_03;

--sort key varchar type and insert select
create table sort_key_04(col1 int,col2 varchar(255))cluster by  col2;
insert into sort_key_04 select * from sort_key_02;
select col2 from sort_key_04;
drop table sort_key_04;

--sort key text type
create external table ex_table_sort(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 char(255) ,col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 blob,col20 json)infile{"filepath"='$resouces/load_data/sort_key_1.csv'};
create table sort_key_05(col1 tinyint,col2 text)cluster by  col2;
insert into sort_key_05 select col1,col17 from ex_table_sort;
select col2 from sort_key_05;

--sort key tinyint type and auto_increment
create table sort_key_06(col1 int,col2 tinyint)cluster by  col2;
insert into sort_key_06 select col3,col1 from ex_table_sort;
select col2 from sort_key_06;
drop table sort_key_06;
create table sort_key_06(col1 int,col2 tinyint auto_increment)cluster by  col2;
insert into sort_key_06 select col3,col1 from ex_table_sort;
select col2 from sort_key_06;

--sort key tinyint unsigned type
create table sort_key_07(col1 int,col2 tinyint unsigned)cluster by  col2;
insert into sort_key_07 select col3,col5 from ex_table_sort;
select col2 from sort_key_07;
drop table sort_key_07;
create table sort_key_07(col1 int,col2 tinyint unsigned default 240)cluster by  col2;
insert into sort_key_07 select col3,col5 from ex_table_sort;
select col2 from sort_key_07;
drop table sort_key_07;

--sort key smallint type and order by sort column
create table sort_key_08(col1 int,col2 smallint)cluster by  col2;
insert into sort_key_08 select col3,col2 from ex_table_sort;
select * from sort_key_08;
select col2 from sort_key_08 order by col2;
drop table sort_key_08;

--sort key smallint unsigned type
create table sort_key_09(col1 char,col2 smallint unsigned)cluster by col2;
insert into sort_key_09 select col11,col6 from ex_table_sort;
select col2 from sort_key_09;
drop table sort_key_09;

--sort key int type and create view/alter view
create table sort_key_10(col1 char,col2 int)cluster by col2;
insert into sort_key_10 select col11,col3 from ex_table_sort;
select col2 from sort_key_10;
create view sort_key_view as select * from sort_key_10;
select * from sort_key_view;
alter view sort_key_view as select '2',3 ;
select * from sort_key_view;
drop table sort_key_10;
drop view sort_key_view;

--sort key int unsigned type
create table sort_key_11(col1 char,col2 int)cluster by col2;
insert into sort_key_11 select col11,col7 from ex_table_sort;
select * from sort_key_11;
drop table sort_key_11;

--sort key bigint type
create table sort_key_12(col1 tinyint auto_increment,col2 bigint not null)cluster by col2;
insert into sort_key_12 select col1,col4 from ex_table_sort;
select col2 from sort_key_12;
drop table sort_key_12;

--sort key bigint unsigned type
create table sort_key_13(col1 tinyint default 1,col2 bigint unsigned)cluster by col2;
insert into sort_key_13 select col1,col8 from ex_table_sort;
select col2 from sort_key_13;
drop table sort_key_13;

--sort key decimal type
create table sort_key_14(col1 tinyint,col2 decimal(15,6))cluster by col2;
insert into sort_key_14 select col1,col16 from ex_table_sort;
select col2 from sort_key_14;
drop table sort_key_14;

--sort key float type
create table sort_key_15(col1 tinyint,col2 float)cluster by col2;
insert into sort_key_15 select col1,col9 from ex_table_sort;
select col2 from sort_key_15;
drop table sort_key_15;

--sort key double type
create table sort_key_16(col1 tinyint,col2 double)cluster by col2;
insert into sort_key_16 select col1,col10 from ex_table_sort;
select col2 from sort_key_16;
drop table sort_key_16;

--sort key date type
create table sort_key_17(col1 tinyint,col2 date)cluster by col2;
insert into sort_key_17 select col1,col12 from ex_table_sort;
select col2 from sort_key_17;
drop table sort_key_17;

--sort key datetime type
create table sort_key_18(col1 tinyint,col2 datetime)cluster by col2;
insert into sort_key_18 select col1,col13 from ex_table_sort;
select col2 from sort_key_18;
drop table sort_key_18;

--sort key timestamp type
create table sort_key_19(col1 tinyint,col2 timestamp)cluster by col2;
insert into sort_key_19 select col1,col14 from ex_table_sort;
select col2 from sort_key_19;
drop table sort_key_19;

--sort key blob type
create table sort_key_20(col1 tinyint,col2 blob)cluster by col2;
insert into sort_key_20 select col1,col19 from ex_table_sort;
select col2 from sort_key_20;
drop table sort_key_20;

--sort key json type
create table sort_key_21(col1 tinyint,col2 json)cluster by col2;
insert into sort_key_21 select col1,col20 from ex_table_sort;
select col2 from sort_key_21;
drop table sort_key_21;

--create temporary table sort key
create temporary table sort_key_10(col1 char,col2 int)cluster by col2;
insert into sort_key_10 select col11,col3 from ex_table_sort;
select col2 from sort_key_10;
drop table sort_key_10;

--unique index and secondary index
create table sort_key_11(col1 int,col2 double not null,col3 date,col4 char(25) default 'abc',unique key col1 (col1),key col2(col2))cluster by col1;
insert into sort_key_11 values (563,23.01,'2022-10-01','');
insert into sort_key_11 values (777,0.01,'2022-12-08','90');
select * from sort_key_11;
drop table sort_key_11;
create table sort_key_11(col1 int,col2 double not null,col3 date,col4 char(25) default 'abc')cluster by col1;
create unique index index1 on sort_key_11(col1) comment'test';
create unique index index2 on sort_key_11(col1,col2) comment'test';
show create table sort_key_11;
insert into sort_key_11 values (563,23.01,'2022-10-01','');
drop index index1;
drop index index2;

--update/delete/trancate
create table sort_key_15(col1 tinyint,col2 float)cluster by col2;
insert into sort_key_15 select col1,col9 from ex_table_sort;
select col2 from sort_key_15;
update sort_key_15 set col2=9.01 where col1 is null;
select col2 from sort_key_15;
delete from sort_key_15 where col1=70;
select col2 from sort_key_15;
select * from sort_key_15;
truncate table sort_key_15;
select col2 from sort_key_15;
drop table sort_key_15;

--Abnormal test
create table sort_key_22(col1 char,col2 int primary key)cluster by col2;
create table sort_key_23(col1 int,col2 date,col3 char(20),primary key(col1,col3))cluster by col2;
create table sort_key_24(col1 int,col2 date,col3 char(20))order by col4;
create table sort_key_24(col1 int,col2 date,col3 char(20)) partition by hash(col1)cluster by col2;