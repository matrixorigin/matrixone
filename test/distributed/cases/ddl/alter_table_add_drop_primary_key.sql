-- @suit
-- @case
-- @desc: alter table add/drop column
-- @label:bvt

-- normal table adds primary key
drop table if exists pri01;
create table pri01 (col1 int, col2 decimal);
insert into pri01 (col1, col2) values (1,2378.328839842);
insert into pri01 values (234, -3923.2342342);
select * from pri01;
show create table pri01;
alter table pri01 add constraint primary key(col1);
insert into pri01 values (23423, 32432543.3242);
insert into pri01 values (234, -3923.2342342);
show columns from pri01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri01' and COLUMN_NAME not like '__mo%';
drop table pri01;


-- failed to add a single primary key, duplicate values existed in the table
drop table if exists pri02;
create table pri02 (col1 char, col2 bigint unsigned);
insert into pri02 (col1, col2) values ('a', 327349284903284032);
insert into pri02 values ('*', 3289323423);
insert into pri02 values ('*', 328932342342424);
select * from pri02;
alter table pri02 add constraint primary key (col1);
show create table pri02;
show columns from pri02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri02' and COLUMN_NAME not like '__mo%';
drop table pri02;


-- failed to add a single primary key, duplicate values existed in the table
drop table if exists pri03;
create table pri03 (col1 char, col2 bigint unsigned);
insert into pri03 (col1, col2) values ('a', 327349284903284032);
insert into pri03 values ('*', 3289323423);
select * from pri03;
alter table pri03 add constraint primary key (col1);
show create table pri03;
show columns from pri03;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri03' and COLUMN_NAME not like '__mo%';
drop table pri03;


-- failed to add a single primary key, null fields existed in the table
drop table if exists pri04;
create table pri04 (col1 varchar(100), col2 float);
insert into pri04 (col1, col2) values ('databaseDATABASE 数据库数据库系统', -32734928490.3284032);
insert into pri04 values ('3782973804u2databasejnwfhui34数据库endfcioc', 3289323423);
insert into pri04 values (null, 378270389824324);
select * from pri04;
alter table pri04 add constraint primary key (col1);
show create table pri04;
show columns from pri04;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri04' and COLUMN_NAME not like '__mo%';
drop table pri04;


-- failed to add a single primary key, null fields does not existed in the table
drop table if exists pri05;
create table pri05 (col1 date, col2 double);
insert into pri05 (col1, col2) values ('1997-01-13', -32734928490.3284032);
insert into pri05 values ('2023-08-18', 3289323423);
select * from pri05;
alter table pri05 add constraint primary key (col1);
show create table pri05;
show columns from pri05;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri05' and COLUMN_NAME not like '__mo%';
drop table pri05;


--  the column constraint in the table is 'default null'
drop table if exists pri06;
create table pri06 (col1 smallint default null, col2 double);
insert into pri06 (col1, col2) values (100, -32734928490.3284032);
insert into pri06 values (200, 3289323423);
select * from pri06;
alter table pri06 add constraint primary key (col1);
show create table pri06;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri06' and COLUMN_NAME not like '__mo%';
show columns from pri06;
drop table pri06;


--  modify the value of the column after add primary key
drop table if exists pri07;
create table pri07 (col1 decimal, col2 double);
insert into pri07 (col1, col2) values (12.213231000021312, -32734928490.3284032);
insert into pri07 values (32784234.4234243243243242, 3289323423);
select * from pri07;
alter table pri07 add constraint primary key (col1);
show create table pri07;
show columns from pri07;
update pri07 set col1 = 1000000 where col2 = 3289323423;
update pri07 set col1 = 12.213231000021312 where col2 = 3289323423;
delete from pri07 where col1 = 12.213231000021312;
select * from pri07;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri07' and COLUMN_NAME not like '__mo%';
drop table pri07;


-- abnormal test：Add more than one primary key to the same table
drop table if exists pri08;
create table pri08 (col1 binary, col2 int unsigned);
insert into pri08 values ('ewfijew', 372984324);
insert into pri08 values ('ew8u3ejkfcwev', 2147483647);
select * from pri08;
alter table pri08 add constraint primary key (col1);
show create table pri08;
show columns from pri08;
alter table pri08 add constraint primary key (col2);
show create table pri08;
show columns from pri08;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri08' and COLUMN_NAME not like '__mo%';
drop table pri08;


-- add multiple columns of primary keys. The primary key column cannot be empty
drop table if exists pri09;
create table pri09 (col1 binary, col2 int unsigned);
insert into pri09 values ('a', 372893243);
insert into pri09 values (null, 2147483647);
select * from pri09;
alter table pri09 add constraint primary key (col1, col2);
show create table pri09;
show columns from pri09;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri09' and COLUMN_NAME not like '__mo%';
drop table pri09;


-- add multiple primary key columns to a common table
drop table if exists pri10;
create table pri10 (col1 int, col2 char(1));
insert into pri10 (col1, col2) values (1, 'a');
insert into pri10 values (-2, '*');
select * from pri10;
alter table pri10 add constraint primary key (col1, col2);
show create table pri10;
show columns from pri10;
insert into pri10 (col1, col2) values (1, null);
insert into pri10 values (-2, 'p');
-- @pattern
insert into pri10 (col1, col2) values (1, 'a');
select * from pri10;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri10' and COLUMN_NAME not like '__mo%';
drop table pri10;


-- abnormal test: change a single primary key column to multiple primary key columns
drop table if exists pri11;
create table pri11 (col1 int primary key , col2 decimal, col3 char);
insert into pri11 (col1, col2, col3) values (1, 3289034.3232, 'q');
insert into pri11 values (2, 3829.3232, 'a');
alter table pri11 add constraint primary key (col1, col2);
show create table pri11;
show columns from pri11;
select * from pri11;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri11' and COLUMN_NAME not like '__mo%';
drop table pri11;


-- abnormal test：Add/drop primary keys for temporary tables
drop table if exists temp01;
create temporary table temp01 (col1 datetime, col2 blob);
insert into temp01 values ('1997-01-13 00:00:00', '342ewfyuehcdeiuopwu4jo3lekwdfhiu48woi3jrdnefrbwui34f');
insert into temp01 values ('2012-01-13 23:23:59', '63298ufh3jcweuiv4h32jhf432ouy4hu3enjwfnwje4n3bj24f34573');
select * from temp01;
alter table temp01 add constraint primary key (col2);
select * from temp01;
show create table temp01;
show columns from temp01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'temp01' and COLUMN_NAME not like '__mo%';
drop table temp01;


-- abnormal test：external table add/drop primary key
drop table if exists ex_table_2_1;
create external table ex_table_2_1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_1.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
alter table ex_table_2_1 add constraint primary key (num_col1, num_col2);
show create table ex_table_2_1;
select * from ex_table_2_1;


-- primary key deletion
drop table if exists droppri01;
create table droppri01 (col1 int primary key , col2 decimal);
insert into droppri01 (col1, col2) values (1, 234324234.234242);
insert into droppri01 values (32894324,4234294023.4324324234);
alter table droppri01 drop primary key;
show create table droppri01;
show columns from droppri01;
insert into droppri01 (col1, col2) values (1, 3489372843);
truncate table droppri01;
alter table droppri01 add constraint primary key (col2);
show create table droppri01;
alter table droppri01 drop primary key;
show create table droppri01;
show columns from droppri01;
select * from droppri01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'droppri01' and COLUMN_NAME not like '__mo%';
drop table droppri01;


-- Run the show create table error command to add or delete a primary key in one column and add a primary key in the other column
drop table if exists pri01;
create table pri01(col1 int, col2 decimal);
alter table pri01 add constraint primary key(col1);
show create table pri01;
alter table pri01 drop primary key;
show create table pri01;
alter table pri01 add constraint primary key(col2);
show create table pri01;
show columns from pri01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'pri01' and COLUMN_NAME not like '__mo%';
drop table pri01;


-- multi-column primary key deletion
drop table if exists droppri02;
create table droppri02 (col1 int auto_increment, col2 decimal, col3 char, col4 varchar not null, col5 float, primary key (col1, col2, col3));
show create table droppri02;
show columns from droppri02;
alter table droppri02 drop primary key;
show create table droppri02;
show columns from droppri02;
drop table droppri02;


-- prepare
drop table if exists prepare01;
create table prepare01(col1 int primary key , col2 char);
insert into prepare01 values (1,'a'),(2,'b'),(3,'c');
show create table prepare01;
show columns from prepare01;
prepare s1 from 'alter table prepare01 drop primary key';
execute s1;
show create table prepare01;
show columns from prepare01;
prepare s2 from 'alter table prepare01 add constraint primary key(col2) ';
execute s2;
show create table prepare01;
show columns from prepare01;
drop table prepare01;


-- permission
drop role if exists role_r1;
drop user if exists role_u1;
drop table if exists test01;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
create table test01(col1 int, col2 varchar(10));
insert into test01 values(1, 'ewuijernf');
insert into test01 values(2, 'abscde');
grant create database on account * to role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant select on table * to role_r1;
grant show tables on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use alter_table_add_drop_primary_key;
alter table test01 add constraint primary key(col1);
-- @session
grant alter table on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use alter_table_add_drop_primary_key;
alter table test01 add constraint primary key(col1);
show create table test01;
show columns from test01;
alter table test01 drop primary key;
show create table test01;
show columns from test01;
-- @session
show create table test01;
show columns from test01;
drop table test01;
drop role role_r1;
drop user role_u1;


-- mixed situation
-- modify change rename column, add/drop primary key
drop table if exists mix01;
create table mix01 (col1 int, col2 decimal, col3 char, col4 varchar(100));
insert into mix01 (col1, col2, col3, col4) values (1, 2, 'a', 'w3uir34jn2k48ujf4');
insert into mix01 (col1, col2, col3, col4) values (2, 3, 'd', '3289u3ji2dff43');
alter table mix01 modify col1 float after col3, change column col2 col2New double, rename column col3 to newCol3, add constraint primary key(col1);
insert into mix01 (col1, col2, col3, col4) values (3, 'w', 37283.323, 'dswhjkfrewr');
alter table mix01 add column col5 int after col1, rename column col2new to newnewCol2;
select * from mix01;
alter table mix01 rename column col2new to newnewCol2, drop primary key;
show create table mix01;
delete from mix01 where newnewcol2 = 2;
update mix01 set newnewcol2 = 8290432.324 where newcol3 = 'd';
select * from mix01;
show create table mix01;
show columns from mix01;
drop table mix01;

-- begin, alter table add/drop primary key column, commit, then select
drop table if exists table01;
begin;
create table table01(col1 int, col2 decimal);
insert into table01 values(100,200);
insert into table01 values(200,300);
alter table table01 add constraint primary key (col2);
commit;
select * from table01;
select col1 from table01;
drop table table01;

drop table if exists table01;
begin;
create table table01(col1 int primary key, col2 decimal);
insert into table01 values(100,200);
insert into table01 values(200,300);
alter table table01 drop primary key;
commit;
select * from table01;
select col1 from table01;
drop table table01;

drop table if exists t1;
create table t1(a int primary key, b int);
insert into t1 select result ,result from generate_series(1, 20000000) g;
alter table t1 drop primary key;
begin;
create table t2(a int, b int);
insert into t2 select a,b from t1;
select count(*) from t2;
drop table t2;
commit;