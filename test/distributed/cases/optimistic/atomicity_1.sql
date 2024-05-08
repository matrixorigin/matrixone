drop table if exists atomic_table_1;
drop table if exists atomic_table_2;
drop table if exists atomic_table_3;
drop table if exists atomic_view_1;
drop table if exists atomic_view_2;
drop table if exists t_code_rule_2;
create table atomic_table_2(a bigint ,b varchar(200),c double);

-- insert into select from table
begin;
create table atomic_table_1(a bigint not null,b varchar(200),c double,primary key(a,b));
insert into atomic_table_1 select 1,"gooooooge",8092.9;
insert into atomic_table_1 select 2,"caaaaaate",92.09;
commit;
select * from atomic_table_1;

start transaction ;
insert into atomic_table_2 select * from atomic_table_1;
select * from atomic_table_2;
rollback ;
select * from atomic_table_2;
begin;
insert into atomic_table_2 select * from atomic_table_1;
commit;
select * from atomic_table_2;

-- create view and abnormal
begin;
create view atomic_view_1 as select * from atomic_table_1;
insert into atomic_table_1 select 10,"eeeeee",20.3;
commit;
select * from atomic_view_1;

start transaction ;
insert into atomic_table_1 select 10,"eeeeee",20.3;
insert into atomic_table_1 select 11,"ffff",2.3;
commit;
select * from atomic_table_1;
select * from atomic_view_1;

begin;
create view atomic_view_2 as select * from atomic_table_2;
rollback ;
select * from atomic_view_2;
show create table atomic_view_2;

begin;
drop view atomic_view_2;
commit ;
drop view atomic_view_2;
-- load data
create table atomic_table_3a(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col1))partition by hash(col1)partitions 4;
-- @bvt:issue#5941
begin;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  atomic_table_3a fields terminated by ',';
select col1,col2 from atomic_table_3a;
update atomic_table_3a set col1=400;
rollback;
select col1 from atomic_table_3a;
-- @bvt:issue

-- @bvt:issue#5941
start transaction ;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  atomic_table_3a fields terminated by ',';
select count(*) from atomic_table_3a;
update atomic_table_3a set col1=100;
commit;
select col1 from atomic_table_3a;
-- @bvt:issue

create table atomic_table_3(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col1))partition by hash(col1)partitions 4;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  atomic_table_3 fields terminated by ',';
start transaction ;
update  atomic_table_3 set col2=20;
select  col1,col2 from atomic_table_3;
show create table atomic_table_3;
rollback ;
select  col1,col2 from atomic_table_3;

-- create external/TEMPORARY table
begin;
create external table atomic_ex_table(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_number.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select num_col1  from  atomic_ex_table;
rollback ;
select num_col1  from  atomic_ex_table;
show create table atomic_ex_table;
desc atomic_ex_table;

-- @bvt:issue#9124
create TEMPORARY TABLE atomic_temp(a int);
begin;
insert into atomic_temp values (5);
rollback ;
select * from atomic_temp;
drop table atomic_temp;

start transaction ;
create TEMPORARY TABLE atomic_temp(a int);
insert into atomic_temp values (5);
select * from atomic_temp;
rollback ;
select * from atomic_temp;
show create table atomic_temp;

start transaction ;
create TEMPORARY TABLE atomic_temp(a int);
insert into atomic_temp values (5);
commit ;
select * from atomic_temp;
-- @bvt:issue

CREATE TABLE `t_code_rule` (
  `code_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `code_no` varchar(50) NOT NULL,
  `org_no` varchar(50) NOT NULL,
  `org_name` varchar(50) NOT NULL,
  `code_type` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`code_id`),
  UNIQUE KEY `code_type` (`code_type`),
  KEY `code_no` (`code_no`),
  KEY `org_no` (`org_no`)
);
-- @bvt:issue#6949
start transaction ;
insert into t_code_rule values (18373453,'aaaaaa','fffff','ccccc',10);
insert into t_code_rule values (18373453,'aaaaaa','fffff','ccccc',20);
commit ;
select * from t_code_rule;
-- @bvt:issue

begin;
-- @bvt:issue#7133
insert into t_code_rule values (18373453,'aaaaaa','fffff','ccccc',5);
delete from t_code_rule where code_id=18373453;
select * from t_code_rule;
-- @bvt:issue
rollback ;
select * from t_code_rule;

insert into t_code_rule values (18373453,'aaaaaa','fffff','ccccc',5);
begin ;
-- @bvt:issue#7133
delete from t_code_rule where code_id=18373453;
commit ;

begin;
insert into t_code_rule(code_no,org_no,org_name,code_type) values ('',null,'ccccc',5);
commit ;
select * from t_code_rule;

insert into t_code_rule values (18373453,'aaaaaa','fffff','ccccc',5);
begin;
update t_code_rule set org_name=NULL where code_id=18373453;
commit ;
select * from t_code_rule;
-- @bvt:issue

--anormal transaction sql
begin ;
create account aname admin_name 'admin' identified by '111';
create role role1,role2;
grant role1 to role2;
grant create table ,drop table on database * to role1;
truncate table  t_code_rule;
drop table t_code_rule;
drop database atomic_1;
drop role role1,role2;
commit;












