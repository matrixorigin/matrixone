create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';

# scope session
-- @session:id=1&user=acc_idx:root&password=123456
select @@rand_seed1;
select @@global.rand_seed1;
show variables like 'rand_seed1';
show global variables like 'rand_seed1';
-- @session

-- @session:id=2&user=acc_idx:root&password=123456
select @@rand_seed1;
select @@global.rand_seed1;
show variables like 'rand_seed1';
show global variables like 'rand_seed1';
-- @session

-- @session:id=1&user=acc_idx:root&password=123456
set rand_seed1 = 1;
select @@rand_seed1;
show variables like 'rand_seed1';
-- @session

-- @session:id=2&user=acc_idx:root&password=123456
select @@rand_seed1;
show variables like 'rand_seed1';
-- @session

-- @session:id=1&user=acc_idx:root&password=123456
# error
set global rand_seed1 = 1;
-- @session


# scope global
-- @session:id=3&user=acc_idx:root&password=123456
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';
-- @session

-- @session:id=4&user=acc_idx:root&password=123456
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';
-- @session

-- @session:id=3&user=acc_idx:root&password=123456
# error
set max_connections = 152;
-- @session

-- @session:id=3&user=acc_idx:root&password=123456
set global max_connections = 152;
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';
-- @session

-- @session:id=4&user=acc_idx:root&password=123456
select @@max_connections;
-- @ignore:0
select @@global.max_connections;
show variables like 'max_connections';
-- @ignore:1
show global variables like 'max_connections';
-- @session

-- @session:id=5&user=acc_idx:root&password=123456
select @@max_connections;
select @@global.max_connections;
show variables like 'max_connections';
show global variables like 'max_connections';

# reset
set global max_connections = 151;
-- @session


# scope both
-- @session:id=6&user=acc_idx:root&password=123456
select @@autocommit;
select @@global.autocommit;
show variables like 'autocommit';
show global variables like 'autocommit';
-- @session

-- @session:id=7&user=acc_idx:root&password=123456
select @@autocommit;
select @@global.autocommit;
show variables like 'autocommit';
show global variables like 'autocommit';
-- @session

-- @session:id=6&user=acc_idx:root&password=123456
set autocommit = 0;
select @@autocommit;
select @@global.autocommit;
show variables like 'autocommit';
show global variables like 'autocommit';
-- @session

-- @session:id=7&user=acc_idx:root&password=123456
select @@autocommit;
select @@global.autocommit;
show variables like 'autocommit';
show global variables like 'autocommit';
-- @session

-- @session:id=6&user=acc_idx:root&password=123456
set global autocommit = 0;
select @@autocommit;
select @@global.autocommit;
show variables like 'autocommit';
show global variables like 'autocommit';
-- @session

-- @session:id=7&user=acc_idx:root&password=123456
select @@autocommit;
-- @ignore:0
select @@global.autocommit;
show variables like 'autocommit';
-- @ignore:1
show global variables like 'autocommit';
-- @session

-- @session:id=8&user=acc_idx:root&password=123456
select @@autocommit;
select @@global.autocommit;
show variables like 'autocommit';
show global variables like 'autocommit';

# reset
set global autocommit = 1;
-- @session

drop account acc_idx;