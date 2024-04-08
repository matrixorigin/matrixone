set global enable_privilege_cache = off;
create database db1;
create publication pubname1 database db1 account test_tenant_1 comment 'publish db1 database';
create publication pubname2 database db1 account test_tenant_1 comment 'publish db1 database';
set global syspublications = "pubname1,pubname2";
create account test_tenant_2 admin_name 'test_account' identified by '111';
-- @session:id=1&user=test_tenant_2:test_account&password=111
show subscriptions;
show databases;
-- @session
set global syspublications = default;
drop account test_tenant_2;
drop publication pubname1;
drop publication pubname2;
drop database db1;

set global enable_privilege_cache = on;


create account `0b6d35cc_11ab_4da5_a5c5_c4cc09917c11` admin_name 'admin' identified by '111';
create account `62915dd9_d454_4b02_be16_0741d94b62cc` admin_name 'admin' identified by '111';
-- @session:id=3&user=0b6d35cc_11ab_4da5_a5c5_c4cc09917c11:admin&password=111
create database test;
use test;
create table tt(a int);
insert into tt values(1);
create publication mo_test_pub database test account `62915dd9_d454_4b02_be16_0741d94b62cc`;
-- @session
-- @session:id=5&user=62915dd9_d454_4b02_be16_0741d94b62cc:admin&password=111
create database mo_test_sub from `0b6d35cc_11ab_4da5_a5c5_c4cc09917c11` publication mo_test_pub;
show subscriptions;
-- @session
-- @session:id=7&user=0b6d35cc_11ab_4da5_a5c5_c4cc09917c11:admin&password=111
drop publication mo_test_pub;
-- @session
drop account `0b6d35cc_11ab_4da5_a5c5_c4cc09917c11`;
drop account `62915dd9_d454_4b02_be16_0741d94b62cc`;