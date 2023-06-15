-- @suit
-- @case
-- @desc:statement_info
-- @label:bvt

-- ddl test
drop database if exists test;
create database test;
use test;
create table ddl01(col1 int,col2 char primary key);
insert into ddl01 values(1,'m');
insert into ddl01 values(2,'f');
insert into ddl01 values(3,'a');
create view view01 as select * from ddl01;
alter table ddl01 add column col3 double;
select * from ddl01;
alter table ddl01 drop column col1;
select * from ddl01;
drop view view01;
select sleep(10);
truncate table ddl01;
drop table ddl01;
select statement from statement_info where query_type = 'DDL' and sql_source_type != 'internal_sql' order by response_at desc limit 5;



-- dql test
drop table if exists dql01;
create table dql01(col1 decimal(38,20),col2 binary not null,col3 varchar(20));
insert into dql01 values(12.2421132124,'2','database');
insert into dql01 values(0,'*','database');
insert into dql01 values(-23142.3214,'0','数据库');
insert into dql01 values(-23999999.23432132143254323,'A','matrixone');
insert into dql01 values(329342.32420,'&',null);
insert into dql01 values(3289342.34242,'.','matrixone');
insert into dql01 values(32783.4,4,null);
select * from dql01;

select * from dql01 where col1 > 0;
select sum(col1),col3 from dql01 group by col3;
select avg(col1) as avg from dql01 group by col3 order by avg desc;
select count(*) from dql01 group by col3;
select sleep(10);

-- @bvt:issue#10043
select statement from system.statement_info where query_type = 'DQL' and sql_source_type != 'internal_sql' order by response_at desc limit 5;
-- @bvt:issue
drop table dql01;

drop table if exists dql02;
create table dql02(col1 int,col2 varchar);
create view test_view as select * from dql02;
show create table dql02;
show create view test_view;
show triggers;
show procedure status;
show config;
show events;
show plugins;
show profiles;
show privileges;
show processlist;
show tables;
show collation;
show collation like '%';
show index from dql02;
values row(1,1), row(2,2), row(3,3) order by column_0 desc;
with cte1 as (select 1),cte2 as (select 2) select * from cte1 join cte2;
select sleep(10);
-- @bvt:issue#10043
select statement from system.statement_info where query_type = 'DQL' and sql_source_type != 'internal_sql' order by response_at desc limit 5;
-- @bvt:issue#10043
drop table dql02;



-- dml test
drop table if exists statement_info03;
create table statement_info03(col1 varchar(20),col2 double);
insert into statement_info03 values('testcase',123456.122);
insert into statement_info03 values('casetest',4865132.0);
insert into statement_info03 values('hcdwkjqq38923',-1002903.324);
insert into statement_info03 values('management',0392324);
insert into statement_info03 values(null,null);
select * from statement_info03;
delete from statement_info03 where col1 = 'testcase';
delete from statement_info03 where col2 = null;
select * from statement_info03;
select group_concat(col1,col2) from statement_info03;
update statement_info03 set col1 = null where col1 != null;
select sleep(10);

select statement from system.statement_info where query_type = 'DML' and sql_source_type != 'internal_sql' order by response_at desc limit 5;
drop table statement_info03;



-- dcl test
drop account if exists test_account;
create account test_account admin_name = 'test_name' identified by '111' open comment 'tenant_test';
create role test_role;
create user user_name identified by 'password';
create database if not exists db1;
grant create table,drop table on database *.* to test_role;
revoke test_role from user_name;
drop user user_name;
drop role test_role;
drop account test_account;
select sleep(10);

select statement from system.statement_info where query_type = 'DCL' and sql_source_type != 'internal_sql' order by response_at desc limit 5;



-- tcl test
begin;
commit;
start transaction;
rollback;
select sleep(10);
select statement from system.statement_info where query_type = 'TCL' and sql_source_type != 'internal_sql' order by response_at desc limit 5;



-- multiple spaces appear in the sql statement
drop table if exists multispace01;
create    table multispace01(col1 int);
insert into    multispace01 values(1);
insert into multispace01 values (10);
select sleep(10);

-- @bvt:issue#10043
select statement from system.statement_info where query_type = 'DQL' and sql_source_type != 'internal_sql' order by response_at desc limit 5;
-- @bvt:issue
select statement from system.statement_info where query_type = 'DML' and sql_source_type != 'internal_sql' order by response_at desc limit 5;
drop table multispace01;



-- uppercase and lowercase
Drop table if exists case01;
cReate table case01(col1 text default 'matrixone');
inSERT into case01 valueS('Database test');
insert into case01 values('uppERCASE and uppercase');
insert into case01 Values(NulL);
insert INTO case01 values('**&&*nUll');
insert into case01 values();
drop table cASE01;
select * FROM caseE01 where col1 is null;
create account Account_user01 admin_name = 'Account_test01' identified by '111' open comment 'tenant_test';
creatE role role_Role;
create user User_test identified by '123456';
create DATABASE iF noT exists db1;
grant create table,drop table on database *.* to test_role;
drop database db1;
drop user user_test;
drop account account_user01;
select sleep(10);
select statement from system.statement_info where query_type = 'DDL' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
select statement from system.statement_info where query_type = 'DML' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
-- @bvt:issue#10043
select statement from system.statement_info where query_type = 'DQL' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
-- @bvt:issue
select statement from system.statement_info where query_type = 'DCL' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;



-- line feed in sql
drop table if exists line01;
create table line01(col int, col2 decimal);
insert into line01 values(1,100000);
insert into line01 values(2,93232432);
drop table line01;
select sleep(10);
select statement from system.statement_info where query_type = 'DDL' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;



-- paser error
aaa;
selct sleep(10);
prepare s1 from select * from test_table where col1=?;
select statement from system.statement_info where query_type = 'Other' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
create table 123(col1 int);


-- test other type
select * from unnest('{"a":1}') as f;
create table test_table(col1 int);
prepare s1 from select * from test_table where col1=?;
set @a=2;
execute s1 using @a;
deallocate prepare s1;
select sleep(10);
select statement from system.statement_info where query_type = 'Other' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;



-- create a new tenant and connect to the database and test the same
create account if not exists `bvt_query_type` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- ddl
-- @session:id=1&user=bvt_query_type:admin:accountadmin&password=123456
drop database if exists newAccount;
create database newAccount;
use newAccount;
create table dl01(col1 int,col2 binary(20));
truncate table dl01;
drop table dl01;

-- dml
create table dml01(col1 float, col2 double);
insert into dml01 values(12.234214,138219432432);
insert into dml01 values(3281043242,-3232142.32421421);
delete from dml01 where col1 = 12.234214;
update dml01 set col1 = null where col2 = -3232142.32421421;


-- dql
select * from dml01 where col1 = 12.234214;
select count(*) from dml01 where col2 = -3232142.32421421;
select * from dml01 order by col1 desc;


-- tcl
begin;
commit;
start transaction;
rollback;


-- multiple spaces appear in the sql statement
drop table if exists multispace02;
create    table multispace02(col1 int);
insert into    multispace02 values(1);
insert into multispace02 values (10);
drop table multispace02;


-- uppercase and lowercase
Drop table if exists case02;
cReate table case02(col1 text default 'matrixone');
inSERT into case02 valueS('Database test');
insert into CASE02 values('uppERCASE and uppercase');
insert into case02 Values(NulL);
insert INTO case02 values('**&&*nUll');
insert into case02 values();
select * FROM caSE02 where col1 is null;
drop table case02;


-- there is a line feed in sql
drop table if exists line02;
create table line02(col int,col2 decimal);
insert into line02 values(1,100000);
insert into line02 values(2,93232432);
drop table line02;


-- paser error
aaa;
SElct sleep(10);
prepare s2 from select * from test_table where col1=?;


-- test other type
select * from unnest('{"a":1}') as f;
create table test_table(col1 int);
prepare s1 from select * from test_table where col1=?;
set @a=2;
execute s1 using @a;
deallocate prepare s1;

drop database newAccount;
-- @session

-- ddl,dml,dql,tcl Other
select sleep(20);
select statement from system.statement_info where query_type = 'DDL' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
select statement from system.statement_info where query_type = 'DML' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
select statement from system.statement_info where query_type = 'TCL' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
select statement from system.statement_info where query_type = 'Other' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
-- @bvt:issue#10043
select statement from system.statement_info where query_type = 'DQL' and sql_source_type != 'internal_sql'  order by response_at desc limit 5;
select statement from system.statement_info where sql_source_type != 'internal_sql' order by response_at desc limit 5;
-- @bvt:issue
