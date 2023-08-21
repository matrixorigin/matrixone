-- alter set mysql_compatibility_mode:sys account moadmin user
create database alter_db;
select version();
use alter_db;
alter database alter_db set mysql_compatibility_mode = '1.0.0';
select version();
alter database alter_db set mysql_compatibility_mode = '0.8.0';
select version();
drop database alter_db;

-- alter set mysql_compatibility_mode:sys account non moadmin user
create user alter_user1 identified by '123';
create role role1;
grant create database on account * to role1;
grant role1 to alter_user1;
create user alter_user2 identified by '123';
grant role1 to alter_user2;
-- @session:id=6&user=sys:alter_user1:role1&password=123
create database alter_db1;
alter database alter_db1 set mysql_compatibility_mode = '0.9.0';
select version();
-- @session
-- @session:id=7&user=sys:alter_user2:role1&password=123
alter database alter_db1 set mysql_compatibility_mode = '0.9.1';
select version();
-- @session
drop role role1;
drop user alter_user1,alter_user2;

-- alter set mysql_compatibility_mode: non sys account moadmin user
create account alter_account ADMIN_NAME admin IDENTIFIED BY '12345';
-- @session:id=8&user=alter_account:admin&password=12345
select version();
create database alter_db2;
alter database alter_db2 set mysql_compatibility_mode = '1.0.0';
select version();
drop database alter_db2;
-- @session

-- alter set mysql_compatibility_mode:non sys account non moadmin user
-- @session:id=8&user=alter_account:admin&password=12345
create user alter_user3 identified by '123';
create role role1;
grant create database on account * to role1;
grant role1 to alter_user3;
create user alter_user4 identified by '123';
grant role1 to alter_user4;
-- @session
-- @session:id=9&user=alter_account:alter_user3:role1&password=123
create database alter_db3;
select version();
alter database alter_db3 set mysql_compatibility_mode = '1.1.0';
select version();
-- @session
-- @session:id=10&user=alter_account:alter_user4:role1&password=123
select version();
alter database alter_db3 set mysql_compatibility_mode = '1.0.1';
select version();
-- @session
drop account alter_account;
