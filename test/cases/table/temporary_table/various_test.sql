drop database if exists test_temporary;
create database test_temporary;
use test_temporary;
drop table if exists t1;
drop table if exists t2;
create temporary table t1(a int primary key,b int);
create temporary table t2(c int);
insert into t1 values(1,1),(2,2),(4,5);
insert into t2 values(2),(3),(8),(4);
drop table if exists t3;
create table t3(a int);
show tables;
insert into t3 values(1),(7),(8),(2);
select * from t1;
select * from t2;
-- test join between temporary table and normal table
select * from t1,t2,t3 where t1.a = t2.c and t1.b > t3.a;
drop table t2;
truncate table t1;
insert into t1 values(1,1),(1,2);
drop table if exists t4;
create temporary table t4(a int auto_increment,b varchar(20));
insert into t4 values(1,"abc");
insert into t4(b) values("def");
select * from t4;

-- test subquery
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create temporary table t1 (a int);
create temporary table t2 (a int, b int);
create temporary table t3 (a int);
create temporary table t4 (a int not null, b int not null);
insert into t1 values (2);
insert into t2 values (1,7),(2,7);
insert into t4 values (4,8),(3,8),(5,9);
insert into t2 values (100, 5);
select * from t3 where a in (select b from t2);
select * from t3 where a in (select b from t2 where b > 7);
select * from t3 where a not in (select b from t2);

-- test load data
drop table if exists t1;
create temporary table t1(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned
);

-- import data
import data infile '$resources/load_data/integer_numbers_1.csv' into table t1;
select * from t1;

-- into outfile
select * from t1 into outfile '$resources/into_outfile_2/outfile_integer_numbers_1.csv';
delete from t1;

-- import data
import data infile '$resources/into_outfile_2/outfile_integer_numbers_1.csv' into table t1 ignore 1 lines;
select * from t1;
delete from t1;

import data infile '$resources/load_data/integer_numbers_2.csv' into table t1 fields terminated by'*';
select * from t1;
delete from t1;

drop table t1;