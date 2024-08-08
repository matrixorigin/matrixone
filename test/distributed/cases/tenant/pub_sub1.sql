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
-- @ignore:5,7
show subscriptions;
-- @session
-- @session:id=7&user=0b6d35cc_11ab_4da5_a5c5_c4cc09917c11:admin&password=111
drop publication mo_test_pub;
-- @session
drop account `0b6d35cc_11ab_4da5_a5c5_c4cc09917c11`;
drop account `62915dd9_d454_4b02_be16_0741d94b62cc`;