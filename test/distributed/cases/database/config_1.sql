
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=1&user=default_1:admin&password=111111
create role role1;
grant create database on account * to role1;
create user user1 identified by '123456' default role role1;
create role role2;
grant create database on account * to role2;
create user user2 identified by '123456' default role role2;
-- @session
-- @session:id=2&user=default_1:user1:role1&password=123456
create database db1;
alter database db1 set mysql_compatibility_mode = '0.7.0';
-- @session
-- @session:id=3&user=default_1:user2:role2&password=123456
alter database db1 set mysql_compatibility_mode = '0.8.0';
-- @session
drop account default_1;

drop role if exists role1,role2;
drop user if exists alter_user1,alter_user2,alter_user3,alter_user4;
-- alter set mysql_compatibility_mode:sys account moadmin user
create database alter_db;
use alter_db;
alter database alter_db set mysql_compatibility_mode = '1.0.0';
alter database alter_db set mysql_compatibility_mode = '0.8.0';
drop database alter_db;

-- alter set mysql_compatibility_mode:sys account non moadmin user
create user alter_user1 identified by '123';
create role role1;
create role role2;
grant all on account * to role1;
grant all on account * to role2;
grant role1 to alter_user1;
create user alter_user2 identified by '123';
grant role2 to alter_user2;
-- @session:id=4&user=sys:alter_user1:role1&password=123
create database alter_db1;
use alter_db1;
alter database alter_db1 set mysql_compatibility_mode = '0.9.0';
-- @session
-- @session:id=5&user=sys:alter_user2:role2&password=123
use alter_db1;
alter database alter_db1 set mysql_compatibility_mode = '0.9.1';
-- @session
drop role role1,role2;
drop user alter_user1,alter_user2;
drop database alter_db1;

-- alter set mysql_compatibility_mode: non sys account moadmin user
create account alter_account ADMIN_NAME admin IDENTIFIED BY '12345';
-- @session:id=6&user=alter_account:admin&password=12345
create database alter_db2;
use alter_db2;
alter database alter_db2 set mysql_compatibility_mode = '1.0.0';
drop database alter_db2;
-- @session

-- alter set mysql_compatibility_mode:non sys account non moadmin user
-- @session:id=6&user=alter_account:admin&password=12345
create user alter_user3 identified by '123';
create role role1;
create role role2;
grant all on account * to role1;
grant all on account * to role2;
grant role1 to alter_user3;
create user alter_user4 identified by '123';
grant role2 to alter_user4;
-- @session
-- @session:id=7&user=alter_account:alter_user3:role1&password=123
create database alter_db3;
use alter_db3;
alter database alter_db3 set mysql_compatibility_mode = '1.1.0';
-- @session
-- @session:id=8&user=alter_account:alter_user4:role2&password=123
use alter_db3;
alter database alter_db3 set mysql_compatibility_mode = '1.0.1';
-- @session
drop account alter_account;
