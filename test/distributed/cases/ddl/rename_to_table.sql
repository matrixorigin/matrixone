--rename table name
create table rename_table_01(a int primary key,b varchar(10));
insert into rename_table_01 values(10,'pear'),(11,'yeap'),(23,'sss');
rename table rename_table_01 to `123456`;
select * from `123456`;
select * from rename_table_01;
drop table `123456`;
create table rename_table_02(a int not null,b varchar(10));
rename table rename_table_02 to _rename02;
drop table _rename02;
create table rename_table_03(a int ,b varchar(10),UNIQUE KEY(a));
insert into rename_table_03 values(20,'ste'),(22,'ip'),(24,'opt');
rename table rename_table_03 to test@123456;
rename table rename_table_03 to `test@123456`;
select * from `test@123456`;
drop table `test@123456`;
create table rename_table_04(a int,b decimal(5,3));
insert into rename_table_04 values(20,3.56),(22,52.02),(24,59.1);
rename table rename_table_04 to `中文`;
select * from `中文`;
drop table `中文`;
create table rename_table_05(a int auto_increment primary key,b varchar(10));
insert into rename_table_05(b) values('ui'),('we'),('mn');
rename table rename_table_05 to `ff@#$%^&*()!`;
select * from `ff@#$%^&*()!`;
drop table `ff@#$%^&*()!`;
create table rename_table_06(a int not null,b varchar(10));
insert into rename_table_06 values(20,'ste'),(22,'ip'),(24,'opt');
rename table rename_table_06 to `Fast`;
select * from `fast`;
select * from `Fast`;
drop table `fast`;
create table rename_table_07(a int,b varchar(10));
rename table rename_table_07 to ``;
drop table rename_table_07;
create table rename_table_08(a int,b varchar(10));
rename table rename_table_08 to `rename`;
drop table `rename`;

--rename to exists table name
create table rename_table_09(a int,b varchar(10));
create table rename_table_10(a int,b varchar(10));
insert into rename_table_10 values(20,'ste'),(22,'ip'),(24,'opt');
-- @bvt:issue#18424
rename table rename_table_10 to rename_table_09;
-- @bvt:issue
drop table rename_table_09;
drop table rename_table_10;
--rename after rename
create table rename_table_11(a int primary key auto_increment,b varchar(10));
insert into rename_table_11(b) values ('key'),('aabb'),('ccdd');
rename table rename_table_11 to rename_table_12;
select * from rename_table_12;
rename table rename_table_12 to rename_table_11;
select * from rename_table_11;
drop table rename_table_11;

--after rename load ,update,delete,truncate
create table rename_table_20(a int primary key auto_increment,b varchar(10));
insert into rename_table_20(b) values ('key'),('tab'),('rows');
rename table rename_table_20 to rf0123;
insert into rf0123(b) values ('key1'),('tab1'),('rows1');
select * from rf0123;
update rf0123 set a=10 where a=1;
select * from rf0123;
delete from rf0123 where a=3;
select * from rf0123;
truncate table rf0123;
select * from rf0123;
drop table rf0123;

--create view
create table rename_table_05(a int primary key auto_increment,b varchar(10));
insert into rename_table_05(b) values ('key'),('tab'),('rows');
create view rename_view_05 as select * from rename_table_05;
rename table rename_table_05 to rt_05;
select * from rt_05;
select * from rename_view_05;
drop table rt_05;
drop table rename_view_05;

--rename table include primary key,multi primary key
create table rename_table_21(a bigint primary key,b decimal(5,3));
insert into rename_table_21 values(1258,0.762),(157,21.25),(958,44.85);
rename table rename_table_21 to w_01;
show create table w_01;
alter table w_01 drop primary key;
show create table w_01;
drop table w_01;

create table rename_table_22(a bigint,b decimal(5,3),c varchar(10),primary key(a,c));
insert into rename_table_22 values(1,0.762,'0102934'),(2,21.25,'0103084'),(1,44.85,'0104552');
rename table rename_table_22 to w_02;
show create table w_02;
alter table w_02 drop primary key;
show create table w_02;
drop table w_02;

--rename table include unique index key
create table rename_table_23(a bigint,b decimal(5,3),unique key idx(a));
insert into rename_table_23 values(1002,0.762),(743,21.25),(958,44.85);
rename table rename_table_23 to w_03;
alter table w_03 drop index idx;
show create table w_03;
alter table w_03 add unique index idx1(a);
show create table w_03;
drop table w_03;

--rename table include foreign key
create table rename_fk_01(col1 varchar(30) not null primary key,col2 int);
create table rename_fk_02(col1 int,col2 varchar(25),col3 tinyint,constraint ck foreign key(col2) REFERENCES rename_fk_01(col1));
show create table rename_fk_02;
insert into rename_fk_01 values ('90',5983),('100',734),('190',50);
insert into rename_fk_02(col2,col3) values ('90',5),('90',4),('100',0),(NULL,80);
select * from rename_fk_01;
select * from rename_fk_02;
insert into rename_fk_02(col2,col3) values ('200',5);
rename table rename_fk_02 to w_04;
show create table w_04;
insert into rename_fk_01 values ('200',983),('300',45),('400',23);
insert into w_04(col2,col3) values ('400',5),('500',4),('300',0);
select * from w_04;
alter table w_04 drop foreign key ck;
show create table w_04;
drop table w_04;

--rename table include auto_increment,not null,default
create table rename_table_25(a int primary key auto_increment,b varchar(10) not null,c decimal(5,3) default 0.05);
insert into rename_table_25(b,c) values('nnnn',0.5),('jeep',23.43),('moon',2.3);
rename table rename_table_25 to w_05;
show create table w_05;
drop table w_05;

--after rename table alter table
create table rename_table_26(a int primary key auto_increment,b varchar(10));
insert into rename_table_26(b) values ('kimi'),('red'),('lipo');
rename table rename_table_26 to w_06;
alter table w_06 add column c decimal(4,2) default 0.05;
alter table w_06 modify b varchar(20);
show create table w_06;
alter table w_06 drop column c;
show create table w_06;
drop table w_06;

--more rename to
create table rename_table_01(a int primary key auto_increment,b varchar(10));
create table rename_table_02(a int primary key auto_increment,b varchar(10));
create table rename_table_03(a int primary key auto_increment,b varchar(10));
create table rename_table_04(a int primary key auto_increment,b varchar(10));
create table rename_table_05(a int primary key auto_increment,b varchar(10));
insert into rename_table_01(b) values ('key');
insert into rename_table_02(b) values ('key');
insert into rename_table_03(b) values ('key');
insert into rename_table_04(b) values ('key');
insert into rename_table_05(b) values ('key');
-- @bvt:issue#18425
rename table rename_table_01 to rename01,rename_table_02 to rename02,rename_table_03 to rename03,rename_table_04 to rename04,rename_table_05 to rename05;
select * from rename03;
-- @bvt:issue
