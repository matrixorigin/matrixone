drop database if exists test;
create database test;
use test;

-- single column primary key
drop table if exists pri01;
create table pri01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into pri01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into pri01 values (2, 3, 'b', '32r32r', 'database', 1111111);
insert into pri01 values (3, null, null, null, null, null);
drop table if exists pri02;
create table pri02 like pri01;
show create table pri01;
show create table pri02;
desc pri01;
desc pri02;
select * from pri01;
select * from pri02;
drop table pri01;
drop table pri02;

-- multi column primary key
drop table if exists pri03;
create table pri03(col1 int unsigned, col2 char, col3 binary(10), col4 decimal(20,0));
alter table pri03 add primary key (col1, col3);
insert into pri03 values (1, '3', '324', 31.31231);
insert into pri03 values (2, 'v', '321', 28390);
drop table if exists pri04;
create table pri04 like pri03;
select * from pri03;
select * from pri04;
show create table pri03;
show create table pri04;
desc pri03;
desc pri04;
drop table pri03;
drop table pri04;

-- partition by
drop table if exists test03;
create table test03 (
      emp_no      int             not null,
      birth_date  date            not null,
      first_name  varchar(14)     not null,
      last_name   varchar(16)     not null,
      gender      varchar(5)      not null,
      hire_date   date            not null,
      primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into test03 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                          (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20');
drop table if exists test04;
create table test04 like test03;
show create table test03;
show create table test04;
desc test03;
desc test04;
select * from test03;
select * from test04;
drop table test03;
drop table test04;

-- unique key
drop table if exists test07;
create table test07 (col1 int unique key, col2 varchar(20));
insert into test07 (col1, col2) values (133, 'database');
drop table if exists test08;
create table test08 like test07;
show create table test07;
show create table test08;
desc test07;
desc test08;
select * from test07;
select * from test08;
drop table test07;
drop table test08;

-- @bvt:issue#15296
drop table if exists test07;
create temporary table test07(col1 int unique key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double, unique index(col1, col2));
insert into test07 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into test07 values (2, 3, 'b', '32r32r', 'database', 1111111);
insert into test07 values (3, null, null, null, null, null);
drop table if exists test08;
create table test08 like test07;
show create table test07;
show create table test08;
desc test07;
desc test08;
select * from test07;
select * from test08;
drop table test07;
drop table test08;
-- @bvt:issue

-- table with foreign key, then create table like
drop table if exists foreign01;
drop table if exists foreign02;
create table foreign01 (a int primary key, b varchar(5) unique key);
create table foreign02 (a int ,b varchar(5), c int, foreign key(c) references foreign01(a));
insert into foreign01 values (101,'abc'),(102,'def');
insert into foreign02 values (1,'zs1',101),(2,'zs2',102);
drop table if exists foreign03;
drop table if exists foreign04;
create table foreign03 like foreign01;
create table foreign04 like foreign02;
desc foreign01;
desc foreign02;
desc foreign03;
desc foreign04;
select * from foreign01;
select * from foreign02;
select * from foreign03;
select * from foreign04;
drop table foreign02;
drop table foreign01;
drop table foreign04;
drop table foreign03;

-- auto_increment
drop table if exists null01;
create table null01(col1 int auto_increment primary key, col2 char, col3 varchar(20));
insert into null01 values (1, '2', 'database');
insert into null01 values (2, 'a', 'table');
drop table if exists null02;
create table null02 like null01;
show create table null01;
show create table null02;
desc null01;
desc null02;
select * from null01;
select * from null02;
drop table null01;
drop table null02;

-- create table like in prepare statement
drop table if exists prepare01;
create table prepare01(col1 int primary key , col2 char);
insert into prepare01 values (1,'a'),(2,'b'),(3,'c');
drop table if exists prepare02;
prepare s1 from 'create table prepare02 like prepare01';
execute s1;
show create table prepare01;
show create table prepare02;
desc prepare01;
desc prepare02;
select * from prepare01;
select * from prepare02;
drop table prepare01;
drop table prepare02;

-- create table like view
drop table if exists table10;
create table table10 (id int, name varchar(50));
show create table table10;
insert into table10 values(1,'ashley'),(2,'ben'),(3,'cindy');
select * from table10;
drop view if exists view01;
create view view01 as select * from table10;
drop table if exists table11;
create table table11 like view01;
show create view view01;
select * from view01;
drop view view01;
drop table table10;

drop database test;