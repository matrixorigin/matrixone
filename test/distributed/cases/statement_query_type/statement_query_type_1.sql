-- prepare
drop account if exists bvt_query_type_part1;
create account if not exists `bvt_query_type_part1` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- Login bvt_query_type_part1 account
-- @session:id=1&user=bvt_query_type_part1:admin:accountadmin&password=123456

-- CASE: part 1
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
show collation like 'utf8mb4_general_ci';
show collation like 'utf8mb4_general_ci%';
show index from test_table;
values row(1,1), row(2,2), row(3,3) order by column_0 desc;
WITH cte1 AS (SELECT 1),cte2 AS (SELECT 2) SELECT * FROM cte1 join cte2;
select * from unnest('{"a":1}') as f;


-- test DML sql
insert into test_table values (1,'a'),(2,'b'),(3,'c');
update test_table set col2='xxx' where col1=1;


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

-- test DDL and DQL
drop table if exists test_01;
create table test_01(a int, b varchar);
show create table test_01;
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
select * from unnest('{"a":1}') as f;

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

prepare s1 from select * from test_table where col1=?;
set @a=2;
execute s1 using @a;
deallocate prepare s1;
rollback;

use system;
-- END part 1
-- --+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

-- @session
-- END

--
-- TIPs: DO NOT run this case multiple times in 15s
--

-- cleanup
drop account if exists bvt_query_type_part1;