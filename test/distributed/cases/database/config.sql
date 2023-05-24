drop database if exists test;
create database test;
select `variable_value` from mo_catalog.mo_mysql_compatibility_mode where dat_name ="test";
alter database test set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.7.0';
select `variable_value` from mo_catalog.mo_mysql_compatibility_mode where dat_name ="test";
drop database test;

drop database if exists test;
create database test;
use test;
select version();
alter database test set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.7.0';
select version();
drop database test;
drop account if exists abc;
create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=abc:admin&password=123456
drop database if exists test;
drop database if exists test1;
create database test;
create database test1;
use test;
select version();
alter database test set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.7.0';
select version();
use test1;
select version();
alter account config abc set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.8.0';
select version();
drop database test;
drop database test1;
-- @session
drop account abc;