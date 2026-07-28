create account acc1 ADMIN_NAME 'admin1' IDENTIFIED BY '111';

drop database if exists db1;
create database db1;
-- pub an empty database
create publication pub_all database db1 account all;

-- @session:id=1&user=acc1:admin1&password=111
create database syssub1 from sys publication pub_all;
-- @ignore:3,4,5,7,9,10,11,12
show table status from syssub1;
-- @session

drop publication pub_all;
drop database db1;
drop account acc1;
