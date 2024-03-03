-- prepare
drop account if exists bvt_query_type;
create account if not exists `bvt_query_type` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- Login bvt_query_type account
-- @session:id=1&user=bvt_query_type:admin:accountadmin&password=123456

create database statement_query_type;
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
/* cloud_nonuser */ show create table test_table;
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
/* cloud_nonuser */ show collation like 'utf8mb4_general_ci';
/* cloud_nonuser */ show collation like 'utf8mb4_general_ci%';
/* cloud_nonuser */ show index from test_table;
/* cloud_nonuser */ values row(1,1), row(2,2), row(3,3) order by column_0 desc;
/* cloud_nonuser */ WITH cte1 AS (SELECT sleep(1)),cte2 AS (SELECT 2) SELECT * FROM cte1 join cte2;

/* cloud_nonuser */ insert into test_table values (1,'a'),(2,'b'),(3,'c');
/* cloud_nonuser */ update test_table set col2='xxx' where col1=1;
/* cloud_nonuser */ delete from test_table where col1=3;
/* cloud_nonuser */ explain select * from test_table;

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

/* cloud_nonuser */ prepare s1 from select * from test_table where col1=?;
/* cloud_nonuser */ set @a=2;
/* cloud_nonuser */ execute s1 using @a;
/* cloud_nonuser */ deallocate prepare s1;
/* cloud_nonuser */ truncate table test_table;
/* cloud_nonuser */ drop table test_table;

/* cloud_nonuser */ select sleep(1), * from unnest('{"a":1}') as f;
/* cloud_nonuser */ use system;
/* cloud_nonuser */ drop database test_db;

-- END part 3
-- --+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

-- @session
-- END ALL bvt_query_type query

--
-- TIPs: DO NOT run this case multiple times in 15s
--
select sleep(15);

-- cleanup
drop account if exists bvt_query_type;
