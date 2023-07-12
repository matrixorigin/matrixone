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

show global variables like 'sql_mode';
create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=5&user=abc:admin&password=123456
show global variables like 'sql_mode';
-- @session
set global sql_mode = "NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES";
show global variables like 'sql_mode';
-- @session:id=6&user=abc:admin&password=123456
show global variables like 'sql_mode';
set global sql_mode = "NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE";
show global variables like 'sql_mode';
-- @session
show global variables like 'sql_mode';
set global sql_mode = "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES";
drop account abc;