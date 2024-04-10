set global keep_user_target_list_in_result = 0;
select @@keep_user_target_list_in_result;
set global keep_user_target_list_in_result = 1;

drop account if exists default_1;
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=1&user=default_1:admin&password=111111
select @@lower_case_table_names;
select @@keep_user_target_list_in_result;
create database if not exists test;
use test;
drop table if exists t1;
create table t1(aa int, bb int, cc int, AbC varchar(25), A_BC_d double);
insert into t1 values (1,2,3,'A',10.9);
select * from t1;
select Aa from t1;
select BB from t1;
select aA, bB, CC, abc, a_Bc_D from t1;
drop table t1;
drop database test;
-- @session
