-- Test explicit transactions are nested, Uncommitted content is forced to be submitted
drop table if exists t5;
start transaction;

create table t5(a int);
insert into t5 values(10),(20),(30);
-- execute success
drop table t5;

start transaction;
show tables;
-- t5 is dropped. error and rollback.
insert into t5 values(100),(2000),(3000);

rollback;
set @@autocommit=on;

--env prepare statement
drop table if exists t5;
drop table if exists dis_table_02;
drop table if exists dis_table_03;

create table dis_table_02(a int not null auto_increment,b varchar(25) not null,c datetime,primary key(a),key bstr (b),key cdate (c) );
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
insert into dis_table_02(b,c) values ('aaaa','2020-09-08');
create table dis_table_03(b varchar(25) primary key,c datetime);
begin ;
insert into dis_table_03 select b,c from dis_table_02;
select * from dis_table_03;
-- @session:id=1{
use drop_table_truncate;
select * from dis_table_03;
truncate table dis_table_03;
-- @session}
insert into dis_table_03 select 'bbb','2012-09-30';
select * from dis_table_03;
commit;

drop table if exists dis_table_02;
drop table if exists dis_table_03;

begin;
create table t1(a int);
show tables;
insert into t1 values (1);
drop table t1;
show tables;
commit;

-- @bvt:issue#10316
create table t1(a int);
begin;
insert into t1 values (1);
select * from t1;
-- @session:id=1{
use drop_table_truncate;
-- @wait:0:commit
truncate table t1;
select * from t1;
-- @session}
select * from t1;
commit;
select * from t1;

begin;
create table t2(a int);
show tables;
insert into t2 values (1);
truncate table t2;
insert into t2 values (2);
select * from t2;
drop table t2;
show tables;
commit;
-- @bvt:issue