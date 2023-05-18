-- transaction sql
begin;
rollback;
commit;
start transaction;
commit;

-- create db/table/view (insert,delete,update,select)
create database db1;
use db1;
create table t1(a int, b varchar);
insert into t1 values (1, 'a'),(1, 'b'),(3, 'c'),(4,'d'),(5,'e');
update t1 set b='xx' where a=5;
update t1 set b='yy' where a=1;
select * from t1;
select * from t1 limit 3;
delete from t1 where a=5;
delete from t1 where a=1;

-- test create view
create view v1 as select * from t1;
create view v2 as select * from t1 limit 1;

-- test prepare
set @a=1;
prepare s1 from "select * from t1 where a>?";
prepare s2 from "select * from t1 where a=?";

execute s1 using @a;
execute s2 using @a;

deallocate prepare s2;
deallocate prepare s2;

-- test show
show databases like 'mysql';
show tables;
show create database db1;
show create view v1;
show create table t1;
show columns from t1;

-- test drop table/database/view
drop view v1;
drop table t1;
drop view v2;
drop database db1;

-- test create/drop account
create account test_tenant_1 admin_name 'test_account' identified by '111';
drop account test_tenant_1;

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

-- test transaction (insert/delete/update/select)
create database db2;
use db2;
create table t2(a int, b varchar);
begin;
insert into t2 values (1, 'a'),(1, 'b'),(3, 'c'),(4,'d'),(5,'e');
update t2 set b='xx' where a=5;
update t2 set b='yy' where a=1;
select * from t2;
select * from t2 limit 3;
delete from t2 where a=5;
delete from t2 where a=1;
commit;
drop database db2;

-- test cloud user execute
/* cloud_user */ create database db2;
/* cloud_user */ use db2;
/* cloud_user */ create table t2(a int, b varchar);
/* cloud_user */ insert into t2 values (1, 'a'),(1, 'b'),(3, 'c'),(4,'d'),(5,'e');
/* cloud_user */ update t2 set b='xx' where a=5;
/* cloud_user */ update t2 set b='yy' where a=1;
/* cloud_user */ select * from t2;
/* cloud_user */ select * from t2 limit 3;
/* cloud_user */ delete from t2 where a=5;
/* cloud_user */ delete from t2 where a=1;
/* cloud_user */ drop database db2;

-- test other
select 1;
select 1 union select 2;
values row(1,1), row(2,2), row(3,3) order by column_0 desc;
WITH cte1 AS (SELECT 1),cte2 AS (SELECT 2) SELECT * FROM cte1 join cte2;
select * from unnest('{"a":1}') as f;
use system;

-- test_tenant_1 wait 15s
create account test_tenant_1 admin_name 'test_account' identified by '111';
-- @session:id=2&user=test_tenant_1:test_account&password=111
select sleep(16);
-- @session

select statement, result_count from statement_info where user="dump" and statement not like '%mo_ctl%' order by request_at desc limit 76;
drop account test_tenant_1;

