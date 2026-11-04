-- test: subscription database privilege check for non-admin user
-- issue: grant privilege on subscription db to a role, user with that role should be able to query

set global enable_privilege_cache = off;

-- sys tenant: prepare data and publication
create database pub_db_priv_test;
use pub_db_priv_test;
create table t1(a int, b varchar(50));
insert into t1 values(1, 'hello'),(2, 'world'),(3, 'test');
create table hidden_publisher_table(a int);
create view private_view as select a from t1;
create publication pub_priv_test database pub_db_priv_test table t1, private_view account all;
create cluster table mo_catalog.metadata_show_cluster(a int);

-- create tenant
create account acc_priv_test admin_name 'root' identified by '111';

-- acc_priv_test admin: subscribe and setup role/user
-- @session:id=1&user=acc_priv_test:root&password=111
create database sub_db from sys publication pub_priv_test;
show tables from mo_catalog like 'mo_tables';
show tables from mo_catalog like 'metadata_show_cluster';
-- A local same-name table in another tenant is never made visible by the publisher.
create database pub_db_priv_test;
create table pub_db_priv_test.t1(a int);
create view pub_db_priv_test.private_view as select 1 as a;

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
show tables from sub_db;
show columns from sub_db.t1;
show create table sub_db.t1;
-- @session

-- grant metadata and data privileges through the reader's active role
-- @session:id=1&user=acc_priv_test:root&password=111
grant show databases on account * to sub_reader_role;
grant select on table sub_db.* to sub_reader_role with grant option;
grant select on table pub_db_priv_test.private_view to sub_reader_role;
-- @session

-- sub_reader user: query subscription db with granted privilege
-- @session:id=2&user=acc_priv_test:sub_reader:sub_reader_role&password=111
select * from sub_db.t1 order by a;
use sub_db;
select * from t1 order by a;
show tables;
show full tables from sub_db;
show columns from sub_db.t1;
show create table sub_db.t1;
-- Table privileges expose columns, but never authorize a view definition.
show columns from sub_db.private_view;
show create table sub_db.private_view;
show create view sub_db.private_view;
show create table pub_db_priv_test.private_view;
show create table private_view;
prepare subscription_view_denied from 'show create table private_view';
-- @session

-- Prepare with the original database gate, then revoke it before EXECUTE.
-- @session:id=1&user=acc_priv_test:root&password=111
grant show tables on database sub_db to sub_reader_role;
-- @session
-- @session:id=2&user=acc_priv_test:sub_reader:sub_reader_role&password=111
prepare subscription_view_definition from 'show create table private_view';
-- @session
-- @session:id=1&user=acc_priv_test:root&password=111
revoke show tables on database sub_db from sub_reader_role;
-- @session
-- @session:id=2&user=acc_priv_test:sub_reader:sub_reader_role&password=111
execute subscription_view_definition;
deallocate prepare subscription_view_definition;
show tables from sub_db like 'hidden%';
show columns from sub_db.hidden_publisher_table;
show columns from pub_db_priv_test.t1;
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
drop database pub_db_priv_test;
-- @session

-- cleanup: sys tenant
drop account acc_priv_test;
drop publication pub_priv_test;
drop table mo_catalog.metadata_show_cluster;
drop database pub_db_priv_test;

set global enable_privilege_cache = on;
