-- prepare
drop account if exists bvt_query_type;
create account if not exists `bvt_query_type` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- CASE: part 1
-- @session:id=1&user=bvt_query_type:admin:accountadmin&password=123456
create database statement_query_type;

-- test TCL sql
begin;
commit;
start transaction;
rollback;

-- test DQL sql
drop database if exists test_db;
create database test_db;
use test_db;
drop table if exists test_table;
create table test_table(
col1 int,
col2 varchar
);
create view test_view as select * from test_table;
show create database test_db;
show create table test_table;
show create view test_view;
show triggers;
show procedure status;
show config;
show events;
show plugins;
show profiles;
show privileges;
show tables;
show collation;
show collation like '%';
show index from test_table;
values row(1,1), row(2,2), row(3,3) order by column_0 desc;
WITH cte1 AS (SELECT 1),cte2 AS (SELECT 2) SELECT * FROM cte1 join cte2;


-- test DML sql
insert into test_table values (1,'a'),(2,'b'),(3,'c');
update test_table set col2='xxx' where col1=1;
delete from test_table where col1=3;


-- test DCL sql
create account test_account admin_name = 'test_name' identified by '111' open comment 'tenant_test';
create role test_role;
create user user_name identified by 'password';
create database if not exists db1;
grant create table,drop table on database *.* to test_role;
revoke test_role from user_name;
drop user user_name;
drop role test_role;
drop account test_account;
drop database db1;


-- test DDL type
create database db2;
create table table_2(
col1 int,
col2 varchar
);
create view view_2 as select * from table_2;
create index index_table_2 on table_2(col1);

drop index index_table_2 on table_2;
drop view view_2;
drop table table_2;
drop database db2;


-- test other type
select * from unnest('{"a":1}') as f;
prepare s1 from select * from test_table where col1=?;
set @a=2;
execute s1 using @a;
deallocate prepare s1;


-- test DDL and DQL
drop table if exists test_01;
create table test_01(a int, b varchar);
show create table test_01;
insert into test_01 values (1,'a');
insert into test_01 values (2,'b');
update test_01 set a=100 where b='b';
select * from test_01;
explain select * from test_01;
delete from test_01 where a=1;
truncate table test_01;
drop table test_01;
drop database test_db;


-- test sql executes in a transaction
use statement_query_type;
create database test_db;
begin;
use test_db;
create table test_table(
col1 int,
col2 varchar
);
create view test_view as select * from test_table;
show create database test_db;
show create table test_table;
show triggers;
show procedure status;
show config;
show events;
show plugins;
show profiles;
show privileges;
show tables;
show index from test_table;
values row(1,1), row(2,2), row(3,3) order by column_0 desc;
WITH cte1 AS (SELECT 1),cte2 AS (SELECT 2) SELECT * FROM cte1 join cte2;

insert into test_table values (1,'a'),(2,'b'),(3,'c');

create account test_account admin_name = 'test_name' identified by '111' open comment 'tenant_test';
create role test_role;
create user user_name identified by 'password';
create database if not exists db1;
grant create table,drop table on database *.* to test_role;
revoke test_role from user_name;
drop user user_name;
drop role test_role;
drop account test_account;
drop database db1;

create database db2;
create table table_2(
col1 int,
col2 varchar
);
create view view_2 as select * from table_2;
create index index_table_2 on table_2(col1);

drop index index_table_2 on table_2;
drop view view_2;
drop table table_2;
drop database db2;

select * from unnest('{"a":1}') as f;
prepare s1 from select * from test_table where col1=?;
set @a=2;
execute s1 using @a;
deallocate prepare s1;
rollback;

use system;
select sleep(1);
-- @session

-- RESULT CHECK: part 1
select sleep(15);
select statement,query_type,sql_source_type from  system.statement_info where account="bvt_query_type" and sql_source_type="external_sql" and status != "Running" and statement not like '%mo_ctl%' and aggr_count <1 order by request_at desc limit 96;
-- @bvt:issue

-- CASE: part 2
-- test cloud_user_sql type
/* cloud_user */ use statement_query_type;
/* cloud_user */ begin;
/* cloud_user */ commit;
/* cloud_user */ start transaction;
/* cloud_user */ rollback;

/* cloud_user */ drop database if exists test_db;
/* cloud_user */ create database test_db;
/* cloud_user */ use test_db;
/* cloud_user */ drop table if exists test_table;
/* cloud_user */ create table test_table(col1 int,col2 varchar);
/* cloud_user */ create view test_view as select * from test_table;
/* cloud_user */ show create database test_db;
/* cloud_user */ show create table test_table;
/* cloud_user */ show create view test_view;
/* cloud_user */ show triggers;
/* cloud_user */ show procedure status;
/* cloud_user */ show config;
/* cloud_user */ show events;
/* cloud_user */ show plugins;
/* cloud_user */ show profiles;
/* cloud_user */ show privileges;
/* cloud_user */ show tables;
/* cloud_user */ show collation;
/* cloud_user */ show collation like '%';
/* cloud_user */ show index from test_table;
/* cloud_user */ values row(1,1), row(2,2), row(3,3) order by column_0 desc;
/* cloud_user */ WITH cte1 AS (SELECT 1),cte2 AS (SELECT 2) SELECT * FROM cte1 join cte2;

/* cloud_user */ insert into test_table values (1,'a'),(2,'b'),(3,'c');
/* cloud_user */ update test_table set col2='xxx' where col1=1;
/* cloud_user */ delete from test_table where col1=3;

/* cloud_user */ create account test_account admin_name = 'test_name' identified by '111' open comment 'tenant_test';
/* cloud_user */ create role test_role;
/* cloud_user */ create user user_name identified by 'password';
/* cloud_user */ create database if not exists db1;
/* cloud_user */ grant create table,drop table on database *.* to test_role;
/* cloud_user */ revoke test_role from user_name;
/* cloud_user */ drop user user_name;
/* cloud_user */ drop role test_role;
/* cloud_user */ drop account test_account;
/* cloud_user */ drop database db1;

/* cloud_user */ create database db2;
/* cloud_user */ create table table_2(col1 int,col2 varchar);
/* cloud_user */ create view view_2 as select * from table_2;
/* cloud_user */ create index index_table_2 on table_2(col1);

/* cloud_user */ drop index index_table_2 on table_2;
/* cloud_user */ drop view view_2;
/* cloud_user */ drop table table_2;
/* cloud_user */ drop database db2;

/* cloud_user */ select * from unnest('{"a":1}') as f;
/* cloud_user */ prepare s1 from select * from test_table where col1=?;
/* cloud_user */ set @a=2;
/* cloud_user */ execute s1 using @a;
/* cloud_user */ deallocate prepare s1;

/* cloud_user */ drop table if exists test_01;
/* cloud_user */ create table test_01(a int, b varchar);
/* cloud_user */ show create table test_01;
/* cloud_user */ insert into test_01 values (1,'a');
/* cloud_user */ insert into test_01 values (2,'b');
/* cloud_user */ update test_01 set a=100 where b='b';
/* cloud_user */ select * from test_01;
/* cloud_user */ explain select * from test_01;
/* cloud_user */ delete from test_01 where a=1;
/* cloud_user */ truncate table test_01;
/* cloud_user */ drop table test_01;
/* cloud_user */ use system;
/* cloud_user */ drop database test_db;

/* cloud_user */ select sleep(1);

-- RESULT CHECK: part 2
-- @session:id=1&user=bvt_query_type:admin:accountadmin&password=123456
select sleep(15);
-- @session
/* cloud_user */ select statement,query_type,sql_source_type from  system.statement_info where user="dump" and sql_source_type="cloud_user_sql" and status != "Running" and statement not like '%mo_ctl%' order by request_at desc limit 67;

-- CASE: part 3
-- test cloud_no_user_sql type
/* cloud_nonuser */ use statement_query_type;
/* cloud_nonuser */ begin;
/* cloud_nonuser */ commit;
/* cloud_nonuser */ start transaction;
/* cloud_nonuser */ rollback;

/* cloud_nonuser */ drop database if exists test_db;
/* cloud_nonuser */ create database test_db;
/* cloud_nonuser */ use test_db;
/* cloud_nonuser */ drop table if exists test_table;
/* cloud_nonuser */ create table test_table(col1 int,col2 varchar);
/* cloud_nonuser */ create view test_view as select * from test_table;
/* cloud_nonuser */ show create database test_db;
/* cloud_nonuser */ show create table test_table;
/* cloud_nonuser */ show create view test_view;
/* cloud_nonuser */ show triggers;
/* cloud_nonuser */ show procedure status;
/* cloud_nonuser */ show config;
/* cloud_nonuser */ show events;
/* cloud_nonuser */ show plugins;
/* cloud_nonuser */ show profiles;
/* cloud_nonuser */ show privileges;
/* cloud_nonuser */ show tables;
/* cloud_nonuser */ show collation;
/* cloud_nonuser */ show collation like '%';
/* cloud_nonuser */ show index from test_table;
/* cloud_nonuser */ values row(1,1), row(2,2), row(3,3) order by column_0 desc;
/* cloud_nonuser */ WITH cte1 AS (SELECT 1),cte2 AS (SELECT 2) SELECT * FROM cte1 join cte2;

/* cloud_nonuser */ insert into test_table values (1,'a'),(2,'b'),(3,'c');
/* cloud_nonuser */ update test_table set col2='xxx' where col1=1;
/* cloud_nonuser */ delete from test_table where col1=3;

/* cloud_nonuser */ create account test_account admin_name = 'test_name' identified by '111' open comment 'tenant_test';
/* cloud_nonuser */ create role test_role;
/* cloud_nonuser */ create user user_name identified by 'password';
/* cloud_nonuser */ create database if not exists db1;
/* cloud_nonuser */ grant create table,drop table on database *.* to test_role;
/* cloud_nonuser */ revoke test_role from user_name;
/* cloud_nonuser */ drop user user_name;
/* cloud_nonuser */ drop role test_role;
/* cloud_nonuser */ drop account test_account;
/* cloud_nonuser */ drop database db1;

/* cloud_nonuser */ create database db2;
/* cloud_nonuser */ create table table_2(col1 int,col2 varchar);
/* cloud_nonuser */ create view view_2 as select * from table_2;
/* cloud_nonuser */ create index index_table_2 on table_2(col1);

/* cloud_nonuser */ drop index index_table_2 on table_2;
/* cloud_nonuser */ drop view view_2;
/* cloud_nonuser */ drop table table_2;
/* cloud_nonuser */ drop database db2;

/* cloud_nonuser */ select * from unnest('{"a":1}') as f;
/* cloud_nonuser */ prepare s1 from select * from test_table where col1=?;
/* cloud_nonuser */ set @a=2;
/* cloud_nonuser */ execute s1 using @a;
/* cloud_nonuser */ deallocate prepare s1;

/* cloud_nonuser */ drop table if exists test_01;
/* cloud_nonuser */ create table test_01(a int, b varchar);
/* cloud_nonuser */ show create table test_01;
/* cloud_nonuser */ insert into test_01 values (1,'a');
/* cloud_nonuser */ insert into test_01 values (2,'b');
/* cloud_nonuser */ update test_01 set a=100 where b='b';
/* cloud_nonuser */ select * from test_01;
/* cloud_nonuser */ explain select * from test_01;
/* cloud_nonuser */ delete from test_01 where a=1;
/* cloud_nonuser */ truncate table test_01;
/* cloud_nonuser */ drop table test_01;
/* cloud_nonuser */ use system;
/* cloud_nonuser */ drop database test_db;
/* cloud_nonuser */ select sleep(1);

-- RESULT CHECK: part 3
-- @session:id=1&user=bvt_query_type:admin:accountadmin&password=123456
select sleep(15);
-- @session
/* cloud_nonuser */ select statement,query_type,sql_source_type from  system.statement_info where user="dump" and sql_source_type="cloud_nonuser_sql" and status != "Running" and statement not like '%mo_ctl%' and aggr_count = 0 order by request_at desc limit 67;

-- CASE: last
begin;
use statement_query_type;
create table test_table(col1 int,col2 varchar);
create view test_view as select * from test_table;
show create view test_view;
show collation;
show collation like '%';
load data infile '$resources/load_data/test.csv' into table test_table;
insert into test_table values (1,'a'),(2,'b'),(3,'c');
update test_table set col2='xxx' where col1=1;
delete from test_table where col1=3;
rollback ;

-- cleanup
drop account if exists bvt_query_type;
