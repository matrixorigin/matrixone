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
show processlist;
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
begin;
create database test_db;
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
show processlist;
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
select sleep(15);
select statement,query_type,sql_source_type from  statement_info where user="dump" and sql_source_type="external_sql" order by request_at desc limit 114;


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
/* cloud_user */ show processlist;
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

-- @bvt:issue#7778
/* cloud_user */ select sleep(15);
/* cloud_user */ select statement,query_type,sql_source_type from  statement_info where user="dump" and sql_source_type="cloud_user_sql" order by request_at desc limit 68;
-- @bvt:issue

-- test cloud_no_user_sql type
/* cloud_nouser */ use statement_query_type;
/* cloud_nouser */ begin;
/* cloud_nouser */ commit;
/* cloud_nouser */ start transaction;
/* cloud_nouser */ rollback;

/* cloud_nouser */ drop database if exists test_db;
/* cloud_nouser */ create database test_db;
/* cloud_nouser */ use test_db;
/* cloud_nouser */ drop table if exists test_table;
/* cloud_nouser */ create table test_table(col1 int,col2 varchar);
/* cloud_nouser */ create view test_view as select * from test_table;
/* cloud_nouser */ show create database test_db;
/* cloud_nouser */ show create table test_table;
/* cloud_nouser */ show create view test_view;
/* cloud_nouser */ show triggers;
/* cloud_nouser */ show procedure status;
/* cloud_nouser */ show config;
/* cloud_nouser */ show events;
/* cloud_nouser */ show plugins;
/* cloud_nouser */ show profiles;
/* cloud_nouser */ show privileges;
/* cloud_nouser */ show processlist;
/* cloud_nouser */ show tables;
/* cloud_nouser */ show collation;
/* cloud_nouser */ show collation like '%';
/* cloud_nouser */ show index from test_table;
/* cloud_nouser */ values row(1,1), row(2,2), row(3,3) order by column_0 desc;
/* cloud_nouser */ WITH cte1 AS (SELECT 1),cte2 AS (SELECT 2) SELECT * FROM cte1 join cte2;

/* cloud_nouser */ insert into test_table values (1,'a'),(2,'b'),(3,'c');
/* cloud_nouser */ update test_table set col2='xxx' where col1=1;
/* cloud_nouser */ delete from test_table where col1=3;

/* cloud_nouser */ create account test_account admin_name = 'test_name' identified by '111' open comment 'tenant_test';
/* cloud_nouser */ create role test_role;
/* cloud_nouser */ create user user_name identified by 'password';
/* cloud_nouser */ create database if not exists db1;
/* cloud_nouser */ grant create table,drop table on database *.* to test_role;
/* cloud_nouser */ revoke test_role from user_name;
/* cloud_nouser */ drop user user_name;
/* cloud_nouser */ drop role test_role;
/* cloud_nouser */ drop account test_account;
/* cloud_nouser */ drop database db1;

/* cloud_nouser */ create database db2;
/* cloud_nouser */ create table table_2(col1 int,col2 varchar);
/* cloud_nouser */ create view view_2 as select * from table_2;
/* cloud_nouser */ create index index_table_2 on table_2(col1);

/* cloud_nouser */ drop index index_table_2 on table_2;
/* cloud_nouser */ drop view view_2;
/* cloud_nouser */ drop table table_2;
/* cloud_nouser */ drop database db2;

/* cloud_nouser */ select * from unnest('{"a":1}') as f;
/* cloud_nouser */ prepare s1 from select * from test_table where col1=?;
/* cloud_nouser */ set @a=2;
/* cloud_nouser */ execute s1 using @a;
/* cloud_nouser */ deallocate prepare s1;

/* cloud_nouser */ drop table if exists test_01;
/* cloud_nouser */ create table test_01(a int, b varchar);
/* cloud_nouser */ show create table test_01;
/* cloud_nouser */ insert into test_01 values (1,'a');
/* cloud_nouser */ insert into test_01 values (2,'b');
/* cloud_nouser */ update test_01 set a=100 where b='b';
/* cloud_nouser */ select * from test_01;
/* cloud_nouser */ explain select * from test_01;
/* cloud_nouser */ delete from test_01 where a=1;
/* cloud_nouser */ truncate table test_01;
/* cloud_nouser */ drop table test_01;
/* cloud_nouser */ use system;
/* cloud_nouser */ drop database test_db;
/* cloud_nouser */ select sleep(15);
/* cloud_nouser */ select statement,query_type,sql_source_type from  statement_info where user="dump" order by request_at desc limit 68;
-- @bvt:issue#7789
show create view test_view;
show collation;
show collation like '%';
-- @bvt:issue

begin;
use statement_query_type;
create table test_table(col1 int,col2 varchar);
load data infile '$resources/load_data/test.csv' into table test_table;
insert into test_table values (1,'a'),(2,'b'),(3,'c');
-- @bvt:issue#7772
update test_table set col2='xxx' where col1=1;
delete from test_table where col1=3;
-- @bvt:issue
rollback ;
