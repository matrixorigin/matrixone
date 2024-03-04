-- prepare
drop account if exists bvt_query_type;
create account if not exists `bvt_query_type` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- Login bvt_query_type account
-- @session:id=1&user=bvt_query_type:admin:accountadmin&password=123456

create database statement_query_type;
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
/* cloud_user */ show collation like 'utf8mb4_general_ci';
/* cloud_user */ show collation like 'utf8mb4_general_ci%';
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

-- END part 2
-- --+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

-- @session
-- END

--
-- TIPs: DO NOT run this case multiple times in 15s
--

-- cleanup
drop account if exists bvt_query_type;
