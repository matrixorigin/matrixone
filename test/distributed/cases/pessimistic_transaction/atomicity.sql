drop table if exists test_11;
create table test_11 (c int primary key,d int);

begin;
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Rollback;
select * from test_11 ;

begin;
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
commit;
select * from test_11 ;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
begin;
delete from test_11 where c < 3;
update test_11 set d = c + 1 where c >= 3;
rollback;
select * from test_11 ;

begin;
delete from test_11 where c <3;
update test_11 set d = c + 1 where c >= 3;
commit;
select * from test_11 ;

drop table if exists test_11;
begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
rollback;
select * from test_11 ;

begin;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
delete from test_11 where c <3;
update test_11 set d = c + 1 where c >= 3;
commit;
select * from test_11;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
rollback;
select * from test_11;

drop table if exists test_11;
create table test_11 (c int primary key,d int);
Insert into test_11 values(1,1);
Insert into test_11 values(2,2);
begin;
Insert into test_11 values(3,1);
Insert into test_11 values(4,2);
commit;
drop table if exists test_11;
select * from test_11 ;

begin;
create table test_12(col1 int primary key,col2 varchar(25));
create unique index id_01 on test_12(col2);
select * from test_12;
show create table test_12;
-- @session:id=1{
show create table test_12;
-- @session}
rollback ;
show create table test_12;
select * from test_12;

start transaction;
create table test_12(col1 int primary key,col2 varchar(25));
insert into test_12 values(1,'a'),(2,'b');
-- @session:id=1{
use atomicity;
select * from test_12;
-- @wait:0:commit
create table test_12(col1 int,col2 varchar(25));
insert into test_12 values (90,'tt');
-- @session}
select * from test_12;
show create table test_12;
commit;
show create table test_12;
select * from test_12;
drop table test_12;

start transaction;
create table test_12(col1 int primary key auto_increment,col2 varchar(25));
insert into test_12(col2) values('c'),('d'),('e');
create index id_01 on test_12(col2);
select * from test_12;
show create table test_12;
commit;
show create table test_12;
select * from test_12;

create database s_db_1;
begin;
use s_db_1;
create table test_13(col1 int primary key,col2 varchar(25));
rollback;
drop database s_db_1;
use s_db_1;
select * from test_13;

create database s_db_1;
start transaction ;
use s_db_1;
create table test_13(col1 int primary key,col2 varchar(25));
-- @session:id=1{

create database s_db_1;
-- @session}
commit;
drop database s_db_1;

begin;
use atomicity;
create table test_14(col1 int primary key,col2 varchar(25), unique key col2(col2));
insert into test_14 values(1,'a'),(2,'b');
create view test_view_1 as select * from test_14;
-- @session:id=1{
use atomicity;
select * from test_view_1;
-- @session}
show create table test_14;
select  * from test_view_1;
rollback ;
select * from test_14;
select  * from test_view_1;
show create table test_14;

start transaction ;
use atomicity;
create temporary table test_15(col1 int,col2 float);
insert into test_15 values(1,20.98),(2,30.34);
-- @session:id=1{
use atomicity;
select * from test_15;
-- @session}
select * from test_15;
rollback ;
select * from test_15;

start transaction ;
use atomicity;
create external table test_ex_table_1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_number.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select num_col1 ,num_col2 from test_ex_table_1;
create table test_16(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19));
insert into test_16 select * from test_ex_table_1;
rollback ;
select num_col1 ,num_col2  from test_ex_table_1;
select num_col1 ,num_col2  from test_16;

begin;
use atomicity;
create external table test_ex_table_1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_number.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select num_col1 ,num_col2 from test_ex_table_1;
create table test_16(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19));
insert into test_16 select * from test_ex_table_1;
-- @session:id=1{
use atomicity;
select num_col1 ,num_col2 from test_ex_table_1;
-- @session}
commit;
select num_col1 ,num_col2 from test_ex_table_1;
select num_col1 ,num_col2 from test_16;

------------------------------------------------------------
drop table if exists alter01;
create table alter01 (col1 int, col2 decimal);
show create table alter01;
insert into alter01 values(1, 3412.324);
insert into alter01 values (-10, 323943.2343);

begin;
alter table alter01 change col1 col1New float;
rollback;
show create table alter01;
select * from alter01;
drop table alter01;

------------------------------------------------------------
drop table if exists alter01;
create table alter01 (col1 int primary key, col2 decimal);
show create table alter01;
insert into alter01 values(1, 3412.324);
insert into alter01 values (-10, 323943.2343);

begin;
alter table alter01 modify col1 float not null;
rollback;
show create table alter01;
select * from alter01;
drop table alter01;

------------------------------------------------------------
drop table if exists alter01;
create table alter01 (col1 int primary key, col2 decimal);
show create table alter01;
insert into alter01 values(1, 3412.324);
insert into alter01 values (-10, 323943.2343);

begin;
alter table alter01 change col1 col1New float not null;
rollback;
show create table alter01;
select * from alter01;
drop table alter01;

--------------------------------------------------------
drop table if exists rename01;
create table rename01(c int primary key,d int);
begin;
insert into rename01 values(1,1);
insert into rename01 values(2,2);
alter table rename01 rename column c to `euwhbnfew`;
rollback;
select * from rename01;
show create table rename01;

drop table rename01;

---------------------------------------------------------
drop table if exists pri01;
create table pri01(col1 int ,col2 int);
begin;
insert into pri01 values(1,1);
insert into pri01 values(2,2);
alter table pri01 add constraint primary key(col1);
show create table pri01;
rollback;
select * from pri01;
show create table pri01;

drop table pri01;

--insert duplicate data to null table
CREATE TABLE IF NOT EXISTS indup_07(
    col1 INT primary key,
    col2 VARCHAR(20) NOT NULL,
    col3 VARCHAR(30) NOT NULL,
    col4 BIGINT default 30
);
insert into indup_07 values(22,'11','33',1), (23,'22','55',2),(24,'66','77',1),(25,'99','88',1),(22,'11','33',1) on duplicate key update col1=col1+col2;
select * from indup_07;

--update out of date range
insert into indup_07 values(24,'1','1',100) on duplicate key update col1=2147483649;

--transaction
begin;
insert into indup_07 values(22,'11','33',1), (23,'22','55',2),(33,'66','77',1) on duplicate key update col1=col1+1,col2='888';
select * from indup_07;
rollback ;
select * from indup_07;
start transaction ;
insert into indup_07 values(22,'11','33',1), (23,'22','55',2),(33,'66','77',1) on duplicate key update col1=col1+1,col2='888';
select * from indup_07;
commit;
select * from indup_07;
