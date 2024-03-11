-- noinspection SqlDialectInspectionForFile

-- noinspection SqlNoDataSourceInspectionForFile

drop database if exists special;
create database special;
use special;
set @@autocommit = 1;

-- case 1
# 2.primary key test
create table t(a int primary key);
insert into t select * from generate_series(1,200000) g;
select count(*) from t;
# abort,duplicate key
-- @pattern
insert into t select * from t;
# transaction test
begin;
-- @pattern
insert into t select * from t;
select count(*) from t;
commit;
select count(*) from t;

-- case 2
begin;
use special;
create external table ex_table_dis(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_number.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select num_col1,num_col2 from ex_table_dis;
-- @session:id=1{
use special;
select * from ex_table_dis;
-- @session}
update ex_table_dis set num_col1=1000;
select num_col1,num_col2 from ex_table_dis;
commit;
select num_col1,num_col2 from ex_table_dis;

drop database if exists special;