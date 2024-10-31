set global connection_control_failed_connections_threshold = 3;
set global connection_control_max_connection_delay = 300000000;

drop user if exists user1;
create user user1 identified by '123456';
-- @session:id=1&user=sys:user1&password=123456
select * from mo_catalog.mo_user where user_name = 'user1';
-- @session
select login_attempts from mo_catalog.mo_user where user_name = 'user1';
drop user user1;
set global connection_control_failed_connections_threshold = default;
set global connection_control_max_connection_delay = default;
