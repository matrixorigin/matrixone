create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';

# scope global
-- @session:id=1
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';
-- @session

-- @session:id=2&user=acc_idx:root&password=123456
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';
-- @session

-- @session:id=1
set global max_connections = 152;
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';
-- @session

-- @session:id=2&user=acc_idx:root&password=123456
# do not affect existing session of other account
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';
-- @session

-- @session:id=3&user=acc_idx:root&password=123456
# do not affect new session of other account
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';
-- @session

-- @session:id=1
# reset
set global max_connections = 151;
-- @session

drop account acc_idx;
