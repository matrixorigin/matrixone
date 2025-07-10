set global connection_control_failed_connections_threshold = 3;
set global connection_control_max_connection_delay = 300000000;

drop user if exists user1;
create user user1 identified by '123456';
select login_attempts from mo_catalog.mo_user where user_name = 'user1';
alter user user1 lock;
alter user user1 unlock;
-- @session:id=1&user=sys:user1&password=123456
alter user user1 unlock;
-- @session
select login_attempts from mo_catalog.mo_user where user_name = 'user1';
drop user user1;

drop user if exists alter_user1;
create user alter_user1 identified by '123';
create role role1;
grant all on account * to role1;
grant role1 to alter_user1;
-- @session:id=2&user=sys:alter_user1:role1&password=123
alter user alter_user1 unlock;
-- @session

drop user if exists alter_user1;
drop role if exists role1;


create user alter_user1 identified by '123';
create role role1;
create role role2;
grant all on account * to role1;
grant all on account * to role2;
grant role1 to alter_user1;
create user alter_user2 identified by '123';
grant role2 to alter_user2;

-- @session:id=3&user=sys:alter_user2:role2&password=123
alter user alter_user1 unlock;
alter user alter_user1 identified by '1234';
-- @session

-- @session:id=4&user=sys:alter_user1:role1&password=1234
alter user alter_user1 unlock;
alter user alter_user1 identified by '1234';
-- @session

-- @session:id=5&user=sys:alter_user1:role1&password=1234
alter user alter_user1 unlock;
alter user alter_user1 identified by random password;
-- @session

drop user if exists alter_user1,alter_user2;
drop role if exists role1,role2;

set global connection_control_failed_connections_threshold = default;
set global connection_control_max_connection_delay = default;
