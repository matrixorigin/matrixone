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

-- auto_increment insert 0 should allocate new id when NO_AUTO_VALUE_ON_ZERO is not set
drop table if exists auto_increment_zero;
create table auto_increment_zero(id int auto_increment primary key, val int);
insert into auto_increment_zero values (0,1),(0,2);
select * from auto_increment_zero order by id;
drop table auto_increment_zero;


-- auto_increment > 0
Drop table if exists auto_increment02;
Create table auto_increment02(col1 int auto_increment unique key)auto_increment = 10;
Insert into auto_increment02 values();
Select * from auto_increment02;
-- @pattern
Insert into auto_increment02 values(10);
insert into auto_increment02 values(100);
select last_insert_id();
Select * from auto_increment02;
Drop table auto_increment02;



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
-- @pattern
Insert into auto_increment05 values(10001);
-- @pattern
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
Drop table if exists auto_increment10;
Create table auto_increment10(col1 int auto_increment, col2 int, unique index(col1)) auto_increment = 254;
Insert into auto_increment10(col2) values(100);
Insert into auto_increment10(col2) values(200);
insert into auto_increment10(col2) values(100);
-- @ignore:0
select last_insert_id();
Select * from auto_increment10;
Drop table auto_increment10;


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


-- LAST_INSERT_ID() for a multi-row insert is the first generated value.
drop table if exists auto_increment_first_generated;
create table auto_increment_first_generated(
    id int auto_increment primary key,
    v int
) auto_increment = 100;
insert into auto_increment_first_generated(v) values (1), (2), (3);
select last_insert_id();
select * from auto_increment_first_generated order by id;
drop table auto_increment_first_generated;

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
Drop table if exists auto_increment05;
Create temporary table auto_increment05(col1 int unique key auto_increment) auto_increment = 10000;
Insert into auto_increment05 values();
Insert into auto_increment05 values();
Insert into auto_increment05 values();
select last_insert_id();
Select * from auto_increment05;
-- @regex("Duplicate entry",true)
Insert into auto_increment05 values(10001);
-- @regex("Duplicate entry",true)
Insert into auto_increment05 values(10002);
Select * from auto_increment05;
Drop table auto_increment05;


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
Drop table if exists auto_increment10;
Create temporary table auto_increment10(col1 int auto_increment, col2 int, unique index(col1)) auto_increment = 3267183;
Insert into auto_increment10(col2) values(100);
Insert into auto_increment10(col2) values(200);
insert into auto_increment10(col2) values(100);
select last_insert_id();
Select * from auto_increment10;
Drop table auto_increment10;


-- temporary table:auto_increment > 0 and update/delete
Drop table if exists auto_increment11;
Create temporary table auto_increment11(col1 int auto_increment primary key) auto_increment = 100;
insert into auto_increment11 values();
Insert into auto_increment11 values();
Insert into auto_increment11 values();
select last_insert_id();
Select * from auto_increment11;
Delete from auto_increment11 where col1 = 100;
Update auto_increment11 set col1 = 200 where col1 = 101;
Select * from auto_increment11;
Drop table auto_increment11;


-- temporary table:auto_increment > 0 and insert into table non-int type
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

-- system variable: auto_increment_increment
drop table if exists auto_increment17;
set auto_increment_offset = 10;
create table auto_increment17(col1 int auto_increment);
insert into auto_increment17 values();
select * from auto_increment17;
drop table auto_increment17;
create table auto_increment17(col1 int auto_increment) auto_increment = 0;
insert into auto_increment17 values();
select * from auto_increment17;
drop table auto_increment17;
set auto_increment_offset = 100;
create table auto_increment17(col1 int auto_increment);
insert into auto_increment17 values();
select * from auto_increment17;
drop table auto_increment17;
# reset to 1
set auto_increment_offset = 1;

-- ALTER TABLE AUTO_INCREMENT through the production SQL path.
drop table if exists auto_increment_alter;
create table auto_increment_alter(col1 int auto_increment primary key, col2 int);
insert into auto_increment_alter values(),();
alter table auto_increment_alter auto_increment = 100;
insert into auto_increment_alter values();
select * from auto_increment_alter order by col1;
drop table auto_increment_alter;

-- A request below the stored maximum must allocate MAX(id) + 1.
drop table if exists auto_increment_alter_max;
create table auto_increment_alter_max(col1 int auto_increment primary key, col2 int);
insert into auto_increment_alter_max values (1, 1), (200, 200);
alter table auto_increment_alter_max auto_increment = 100;
insert into auto_increment_alter_max(col2) values (201);
select * from auto_increment_alter_max order by col1;
drop table auto_increment_alter_max;

-- Quoted AUTO_INCREMENT column names must remain safe in the MAX query.
drop table if exists auto_increment_alter_quoted;
create table auto_increment_alter_quoted(`1id` int auto_increment primary key, col2 int);
insert into auto_increment_alter_quoted(col2) values (1);
alter table auto_increment_alter_quoted auto_increment = 10;
insert into auto_increment_alter_quoted(col2) values (10);
select * from auto_increment_alter_quoted order by `1id`;
drop table auto_increment_alter_quoted;

-- COPY reconciles the explicit request, copied maximum, and source allocator.
drop table if exists auto_increment_alter_copy;
create table auto_increment_alter_copy(id bigint primary key auto_increment, v int) auto_increment = 10;
insert into auto_increment_alter_copy(v) values (1);
delete from auto_increment_alter_copy;
alter table auto_increment_alter_copy auto_increment = 100, algorithm = copy;
insert into auto_increment_alter_copy(v) values (2);
insert into auto_increment_alter_copy(id, v) values (500, 3);
alter table auto_increment_alter_copy add column extra int, auto_increment = 100, algorithm = copy;
insert into auto_increment_alter_copy(v) values (4);
select id from auto_increment_alter_copy order by id;
drop table auto_increment_alter_copy;

-- COPY must preserve the session-initialized allocator for a newly added
-- AUTO_INCREMENT column when the source table is empty.
drop table if exists auto_increment_alter_add_empty;
set auto_increment_offset = 10;
create table auto_increment_alter_add_empty(v int);
alter table auto_increment_alter_add_empty add column id bigint auto_increment, algorithm = copy;
insert into auto_increment_alter_add_empty(v) values (1);
select * from auto_increment_alter_add_empty;
drop table auto_increment_alter_add_empty;
set auto_increment_offset = 1;

-- INPLACE rename must not orphan the allocator row used by a later reset.
-- This canonical case also runs through the proxy/multi-CN BVT suite.
drop table if exists auto_increment_alter_rename;
create table auto_increment_alter_rename(id bigint primary key auto_increment, v int);
insert into auto_increment_alter_rename(v) values (1), (2);
alter table auto_increment_alter_rename algorithm = instant, rename column id to new_id;
alter table auto_increment_alter_rename auto_increment = 100;
insert into auto_increment_alter_rename(v) values (3);
select * from auto_increment_alter_rename order by new_id;
drop table auto_increment_alter_rename;

-- A partitioned ALTER owns logical and physical allocator resets as one SQL
-- statement and publishes the same next value through the public table.
drop table if exists auto_increment_alter_partitioned;
create table auto_increment_alter_partitioned(id bigint primary key auto_increment, v int) partition by key(id) partitions 2;
insert into auto_increment_alter_partitioned(v) values (1), (2);
alter table auto_increment_alter_partitioned auto_increment = 100;
insert into auto_increment_alter_partitioned(v) values (3);
select * from auto_increment_alter_partitioned order by id;
drop table auto_increment_alter_partitioned;

-- A same-statement rename must read the old column and publish the final cache key.
drop table if exists auto_increment_alter_rename_combined;
create table auto_increment_alter_rename_combined(id bigint primary key auto_increment, v int);
insert into auto_increment_alter_rename_combined(v) values (1), (2);
alter table auto_increment_alter_rename_combined rename column id to new_id, auto_increment = 100;
insert into auto_increment_alter_rename_combined(v) values (3);
select * from auto_increment_alter_rename_combined order by new_id;
drop table auto_increment_alter_rename_combined;

-- CREATE, combined rename/reset, and implicit allocation must share the final name and post-ALTER epoch.
drop table if exists auto_increment_alter_create_txn;
begin;
create table auto_increment_alter_create_txn(id bigint primary key auto_increment, v int);
alter table auto_increment_alter_create_txn rename column id to new_id, auto_increment = 100;
insert into auto_increment_alter_create_txn(v) values (1);
select * from auto_increment_alter_create_txn order by new_id;
commit;
drop table auto_increment_alter_create_txn;

-- LAST_INSERT_ID reports the first generated value even when one INSERT ...
-- SELECT is split into multiple execution batches.
drop table if exists auto_increment_multi_batch;
create table auto_increment_multi_batch(id bigint auto_increment primary key, v bigint);
insert into auto_increment_multi_batch(v)
select result from generate_series(1, 20000) g;
select last_insert_id();
select min(id), max(id), count(*) from auto_increment_multi_batch;
drop table auto_increment_multi_batch;

-- An all-manual INSERT reports zero in its OK packet but must not change the
-- session value observed by LAST_INSERT_ID().
drop table if exists auto_increment_manual_result;
create table auto_increment_manual_result(id bigint auto_increment primary key, v int);
insert into auto_increment_manual_result(v) values (1);
select last_insert_id();
insert into auto_increment_manual_result(id, v) values (100, 2);
select last_insert_id();
drop table auto_increment_manual_result;
