-- test: subscription database privilege check for non-admin user
-- issue: grant privilege on subscription db to a role, user with that role should be able to query

set global enable_privilege_cache = off;

-- sys tenant: prepare data and publication
create database pub_db_priv_test;
use pub_db_priv_test;
create table t1(a int, b varchar(50));
insert into t1 values(1, 'hello'),(2, 'world'),(3, 'test');
create publication pub_priv_test database pub_db_priv_test account all;

-- create tenant
create account acc_priv_test admin_name 'root' identified by '111';

-- acc_priv_test admin: subscribe and setup role/user
-- @session:id=1&user=acc_priv_test:root&password=111
create database sub_db from sys publication pub_priv_test;

-- admin can query subscription db
select * from sub_db.t1 order by a;

-- create role and user
create role sub_reader_role;
create user if not exists sub_reader identified by '111' default role sub_reader_role;

-- grant privileges to role
grant connect on account * to sub_reader_role;
-- @session

-- sub_reader user: subscription metadata is hidden without database/table privileges
-- @session:id=2&user=acc_priv_test:sub_reader:sub_reader_role&password=111
select count(*) from information_schema.tables where table_schema = 'sub_db';
select count(*) from information_schema.columns where table_schema = 'sub_db';
-- @session

-- grant metadata and data privileges through the reader's active role
-- @session:id=1&user=acc_priv_test:root&password=111
grant show databases on account * to sub_reader_role;
grant show tables on database sub_db to sub_reader_role;
grant select on table sub_db.* to sub_reader_role with grant option;
-- @session

-- sub_reader user: query subscription db with granted privilege
-- @session:id=2&user=acc_priv_test:sub_reader:sub_reader_role&password=111
select * from sub_db.t1 order by a;
use sub_db;
select * from t1 order by a;
show tables;
select table_schema, table_name from information_schema.tables
where table_schema = 'sub_db' order by table_name;
select table_schema, table_name, column_name, ordinal_position from information_schema.columns
where table_schema = 'sub_db' order by table_name, ordinal_position;
-- @session

-- cleanup: acc_priv_test admin
-- @session:id=1&user=acc_priv_test:root&password=111
drop user sub_reader;
drop role sub_reader_role;
drop database sub_db;
-- @session

-- cleanup: sys tenant
drop account acc_priv_test;
drop publication pub_priv_test;
drop database pub_db_priv_test;

set global enable_privilege_cache = on;
