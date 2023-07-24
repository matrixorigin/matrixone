-- @suit
-- @case
-- @desc:auto_increment
-- @label:bvt

-- auto_increment = 0
drop table if exists auto_increment01;
create table auto_increment01(col1 int auto_increment primary key)auto_increment = 0;
select * from auto_increment01;
Insert into auto_increment01 values();
select last_insert_id();
Select * from auto_increment01;
Insert into auto_increment01 values(1);
Select * from auto_increment01;
drop table auto_increment01;


-- auto_increment > 0
-- @bvt:issue#10836
Drop table if exists auto_increment02;
Create table auto_increment02(col1 int auto_increment unique key)auto_increment = 10;
Insert into auto_increment02 values();
Select * from auto_increment02;
Insert into auto_increment02 values(10);
insert into auto_increment02 values(100);
select last_insert_id();
Select * from auto_increment02;
Drop table auto_increment02;
-- @bvt:issue


-- auto_increment > 0 and have duplicate value
Drop table if exists auto_increment03;
create table auto_increment03(col1 int auto_increment primary key) auto_increment = 10000;
Insert into auto_increment03 values();
select last_insert_id();
Insert into auto_increment03 values(10000);
Insert into auto_increment03 values(10000);
Select * from auto_increment03;
Drop table auto_increment03;


-- auto_increment > 0 and col is primary key: check for duplicate primary keys
Drop table if exists auto_increment04;
Create table auto_increment04(col1 int primary key auto_increment) auto_increment = 10;
insert into auto_increment04 values();
Select * from auto_increment04;
Insert into auto_increment04 values();
select last_insert_id();
Insert into auto_increment04 values(100);
Insert into auto_increment04 values(200);
Insert into auto_increment04 values(10);
Insert into auto_increment04 values(11);
Select * from auto_increment04;
Drop table auto_increment04;


-- auto_increment > 0 and column constraint unique index
Drop table if exists auto_increment05;
Create table auto_increment05(col1 int unique key auto_increment) auto_increment = 10000;
Insert into auto_increment05 values();
Insert into auto_increment05 values();
Insert into auto_increment05 values();
select last_insert_id();
Select * from auto_increment05;
Insert into auto_increment05 values(10001);
Insert into auto_increment05 values(10002);
Select * from auto_increment05;
Drop table auto_increment05;


-- auto_increment > 0 and test the threshold value of int
Drop table if exists auto_increment06;
Create table auto_increment06(col1 int auto_increment primary key) auto_increment = 2147483646;
Insert into auto_increment06 values();
Insert into auto_increment06 values();
Insert into auto_increment06 values();
select last_insert_id();
Select * from auto_increment06;
Insert into auto_increment06 values(10001);
Insert into auto_increment06 values(10002);
Select * from auto_increment06;
Drop table auto_increment06;


-- auto_increment > 0 and test the threshold value of smallint
Drop table if exists auto_increment07;
Create table auto_increment07(col1 smallint auto_increment primary key) auto_increment = 32766;
Insert into auto_increment07 values();
Insert into auto_increment07 values();
Insert into auto_increment07 values();
select last_insert_id();
Select * from auto_increment07;
Drop table auto_increment07;


-- auto_increment > 0 and test the threshold value of bigint
Drop table if exists auto_increment08;
Create table auto_increment08(col1 bigint auto_increment primary key) auto_increment = 9223372036854775806;
Insert into auto_increment08 values();
Insert into auto_increment08 values();
Insert into auto_increment08 values();
select last_insert_id();
Select * from auto_increment08;
Drop table auto_increment08;


-- auto_increment > 0 and test the threshold value of tinyint unsigned
Drop table if exists auto_increment09;
Create table auto_increment09(col1 tinyint unsigned auto_increment primary key) auto_increment = 254;
Insert into auto_increment09 values();
Insert into auto_increment09 values();
Insert into auto_increment09 values();
select last_insert_id();
Select * from auto_increment09;
Drop table auto_increment09;


-- auto_increment > 0 and the column constraint unique index
-- @bvt:issue#10834
Drop table if exists auto_increment10;
Create table auto_increment10(col1 int auto_increment, col2 int, unique index(col1)) auto_increment = 254;
Insert into auto_increment10(col2) values(100);
Insert into auto_increment10(col2) values(200);
insert into auto_increment10(col2) values(100);
select last_insert_id();
Select * from auto_increment10;
Drop table auto_increment10;
-- @bvt:issue


-- auto_increment > 0 and update/delete
Drop table if exists auto_increment11;
Create table auto_increment11(col1 int auto_increment primary key) auto_increment = 100;
insert into auto_increment11 values();
Insert into auto_increment11 values();
Insert into auto_increment11 values();
select last_insert_id();
Select * from auto_increment11;
Delete from auto_increment11 where col1 = 100;
Update auto_increment11 set col1 = 200 where col1 = 101;
Select * from auto_increment11;
Drop table auto_increment11;


-- auto_increment > 0 and insert into table non-int type
-- @bvt:issue#10842
Drop table if exists auto_increment12;
create table auto_increment12(col1 int auto_increment primary key)auto_increment = 10;
Insert into auto_increment12 values();
Insert into auto_increment12 values();
Select * from auto_increment12;
Insert into auto_increment12 values(16.898291);
insert into auto_increment12 values(124312.4321424324);
insert into auto_increment12 values();
select last_insert_id();
Select * from auto_increment12;
Drop table auto_increment12;
-- @bvt:issue


-- auto_increment > 0 and truncate table
Drop table if exists auto_increment10;
Create table auto_increment13(col1 int auto_increment primary key)auto_increment = 30000;
Insert into auto_increment13 values();
Insert into auto_increment13 values();
select * from auto_increment13;
Truncate table auto_increment13;
Insert into auto_increment13 values();
select last_insert_id();
Insert into auto_increment13 values(10000);
Select * from auto_increment13;
Drop table auto_increment13;


-- auto_increment > 0, order by
Drop table if exists auto_increment14;
Create table auto_increment14(col1 int primary key auto_increment, col2 varchar(10))auto_increment = 100;
insert into auto_increment14 values (-2147483648, 'aaa');
select * from auto_increment14 order by c;
insert into auto_increment14 values (-2147483649, 'aaa');
Insert into auto_increment14 values();
Insert into auto_increment14 values();
Select last_insert_id();
insert into auto_increment14(col2) values ('22222');
select * from auto_increment14 order by col1;
select * from auto_increment14 order by col1 desc;
Drop table auto_increment14;


-- test one table more auto_increment columns
drop table if exists auto_increment15;
create table auto_increment15(
                                 a int primary key auto_increment,
                                 b bigint auto_increment,
                                 c int auto_increment,
                                 d int auto_increment,
                                 e bigint auto_increment
);
show create table auto_increment15;
insert into auto_increment15 values (),(),(),();
select * from auto_increment15 order by a;
insert into auto_increment15 values (NULL, NULL, NULL, NULL, NULL);
select * from auto_increment15 order by a;
insert into auto_increment15(b,c,d) values (NULL,NULL,NULL);
select * from auto_increment15 order by a;
insert into auto_increment15(a,b) values (100, 400);
select * from auto_increment15 order by a;
insert into auto_increment15(c,d,e) values (200, 200, 200);
select * from auto_increment15;
insert into auto_increment15(c,d,e) values (200, 400, 600);
select * from auto_increment15;
Drop table auto_increment15;


-- abnormal test:auto_increment < 0
Drop table if exists auto_increment16;
Create table auto_increment16(col1 int auto_increment)auto_increment < 0;
Drop table auto_increment16;


-- temporary table: auto_incerment = 0
drop table if exists auto_increment01;
create temporary table auto_increment01(col1 int auto_increment primary key)auto_increment = 0;
select * from auto_increment01;
Insert into auto_increment01 values();
select last_insert_id();
Select * from auto_increment01;
Insert into auto_increment01 values(1);
Select * from auto_increment01;
drop table auto_increment01;


-- temporary table:auto_increment > 0
-- @bvt:issue#10836
Drop table if exists auto_increment02;
Create temporary table auto_increment02(col1 int auto_increment unique key)auto_increment = 10;
Insert into auto_increment02 values();
select last_insert_id();
Select * from auto_increment02;
Insert into auto_increment02 values(10);
insert into auto_increment02 values(100);
Select * from auto_increment02;
Drop table auto_increment02;
-- @bvt:issue


-- temporary table:auto_increment > 0 and have duplicate value
Drop table if exists auto_increment03;
create temporary table auto_increment03(col1 int auto_increment primary key) auto_increment = 10000;
Insert into auto_increment03 values();
Insert into auto_increment03 values(10000);
Insert into auto_increment03 values(10000);
Insert into auto_increment03 values();
select last_insert_id();
Select * from auto_increment03;
Drop table auto_increment03;


-- temporary table:auto_increment > 0 and col is primary key: check for duplicate primary keys
Drop table if exists auto_increment04;
Create temporary table auto_increment04(col1 int primary key auto_increment) auto_increment = 10;
insert into auto_increment04 values();
Select * from auto_increment04;
Insert into auto_increment04 values();
select last_insert_id();
Insert into auto_increment04 values(100);
Insert into auto_increment04 values(200);
Insert into auto_increment04 values(10);
Insert into auto_increment04 values(11);
Select * from auto_increment04;
Drop table auto_increment04;


-- temporary table:auto_increment > 0 and column constraint unique index
-- @bvt:issue#10834
Drop table if exists auto_increment05;
Create temporary table auto_increment05(col1 int unique key auto_increment) auto_increment = 10000;
Insert into auto_increment05 values();
Insert into auto_increment05 values();
Insert into auto_increment05 values();
select last_insert_id();
Select * from auto_increment05;
Insert into auto_increment05 values(10001);
Insert into auto_increment05 values(10002);
Select * from auto_increment05;
Drop table auto_increment05;
-- @bvt:issue#10834


-- temporary table:auto_increment > 0 and test the threshold value of int unsigned
Drop table if exists auto_increment06;
Create temporary table auto_increment06(col1 int unsigned auto_increment primary key) auto_increment = 2147483646;
Insert into auto_increment06 values();
Insert into auto_increment06 values();
Insert into auto_increment06 values();
select last_insert_id();
Select * from auto_increment06;
Insert into auto_increment06 values(10001);
Insert into auto_increment06 values(10002);
Select * from auto_increment06;
Drop table auto_increment06;


-- auto_increment > 0 and test the threshold value of smallint unsigned
Drop table if exists auto_increment07;
Create table auto_increment07(col1 smallint unsigned auto_increment primary key) auto_increment = 65534;
Insert into auto_increment07 values();
Insert into auto_increment07 values();
Insert into auto_increment07 values();
Insert into auto_increment07 values();
select last_insert_id();
Select * from auto_increment07;
Drop table auto_increment07;


-- auto_increment > 0 and test the threshold value of bigint unsigned
Drop table if exists auto_increment08;
Create table auto_increment08(col1 bigint unsigned auto_increment primary key) auto_increment = 9223372036854775806;
Insert into auto_increment08 values();
Insert into auto_increment08 values();
Insert into auto_increment08 values();
select last_insert_id();
Select * from auto_increment08;
Drop table auto_increment08;


-- auto_increment > 0 and test the threshold value of tinyint
Drop table if exists auto_increment09;
Create table auto_increment09(col1 tinyint auto_increment primary key) auto_increment = 254;
Insert into auto_increment09 values();
Insert into auto_increment09 values();
Insert into auto_increment09 values();
select last_insert_id();
Select * from auto_increment09;
Drop table auto_increment09;


-- temporary table:auto_increment > 0 and column constraint unique index
-- @bvt:issue#10834
Drop table if exists auto_increment10;
Create temporary table auto_increment10(col1 int auto_increment, col2 int, unique index(col1)) auto_increment = 3267183;
Insert into auto_increment10(col2) values(100);
Insert into auto_increment10(col2) values(200);
insert into auto_increment10(col2) values(100);
select last_insert_id();
Select * from auto_increment10;
Drop table auto_increment10;
-- @bvt:issue


-- temporary table:auto_increment > 0 and update/delete
Drop table if exists auto_increment11;
Create temporary table auto_increment11(col1 int auto_increment primary key) auto_increment = 100;
insert into auto_increment11 values();
Insert into auto_increment11 values();
Insert into auto_increment11 values();
select last_insert_id();
Select * from auto_increment11;
Delete from auto_increment11 where col1 = 100;
-- @bvt:issue#10834
Update auto_increment11 set col1 = 200 where col1 = 101;
-- @bvt:issue
Select * from auto_increment11;
Drop table auto_increment11;


-- temporary table:auto_increment > 0 and insert into table non-int type
-- @bvt:issue#10842
Drop table if exists auto_increment12;
create temporary table auto_increment12(col1 int auto_increment primary key)auto_increment = 10;
Insert into auto_increment12 values();
Insert into auto_increment12 values();
Select * from auto_increment12;
Insert into auto_increment12 values(16.898291);
insert into auto_increment12 values();
select last_insert_id();
Select * from auto_increment12;
Drop table auto_increment12;
-- @bvt:issue


-- temporary:auto_increment > 0 and truncate table, auto_increment columns whether it will be cleared.
Drop table if exists auto_increment13;
Create table auto_increment13(col1 int auto_increment primary key)auto_increment = 30000;
Insert into auto_increment13 values();
Insert into auto_increment13 values();
select * from auto_increment13;
Truncate table auto_increment13;
Insert into auto_increment13 values();
select last_insert_id();
Insert into auto_increment13 values(10000);
Select * from auto_increment13;
Drop table auto_increment13;


-- temporary: auto_increment > 0, order by
Drop table if exists auto_increment14;
Create table auto_increment14(col1 int primary key auto_increment, col2 varchar(10))auto_increment = 100;
insert into auto_increment14 values (-2147483648, 'aaa');
select * from auto_increment14 order by c;
insert into auto_increment14 values (-2147483649, 'aaa');
Insert into auto_increment14 values();
Insert into auto_increment14 values();
Select last_insert_id();
insert into auto_increment14(col2) values ('22222');
select * from auto_increment14 order by col1;
select * from auto_increment14 order by col1 desc;
Drop table auto_increment14;


-- temporary: test one table more auto_increment columns
drop table if exists auto_increment15;
create temporary table auto_increment15(
a int primary key auto_increment,
b bigint auto_increment,
c int auto_increment,
d int auto_increment,
e bigint auto_increment
)auto_increment = 100;
show create table auto_increment15;
insert into auto_increment15 values (),(),(),();
select * from auto_increment15 order by a;
insert into auto_increment15 values (NULL, NULL, NULL, NULL, NULL);
select * from auto_increment15 order by a;
insert into auto_increment15(b,c,d) values (NULL,NULL,NULL);
select * from auto_increment15 order by a;
insert into auto_increment15(a,b) values (100, 400);
select * from auto_increment15 order by a;
insert into auto_increment15(c,d,e) values (200, 200, 200);
select * from auto_increment15;
insert into auto_increment15(c,d,e) values (200, 400, 600);
select * from auto_increment15;
Drop table auto_increment15;


-- temporary table:abnormal test:auto_increment < 0
Drop table if exists auto_increment16;
Create temporary table auto_increment16(col1 int auto_increment)auto_increment < 0;
Drop table auto_increment16;