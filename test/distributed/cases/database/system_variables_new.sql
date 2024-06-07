-- prepare account1 account2 account3
create account acc1 ADMIN_NAME 'admin' IDENTIFIED BY '111';
create account acc2 ADMIN_NAME 'admin' IDENTIFIED BY '111';
create account acc3 ADMIN_NAME 'admin' IDENTIFIED BY '111';

-- acc1 set global variables,sys,acc2,acc3 confirm env isolation
-- @session:id=1&user=acc1:admin&password=111
set global sql_mode = "NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE";
show global variables like 'sql_mode';
SELECT @@GLOBAL.sql_mode;
SET global TIME_ZONE='+08:00';
select @@TIME_ZONE;
SET @@global.autocommit=off;
SELECT @@global.autocommit;
set global enable_privilege_cache = off;
show global variables like 'enable_privilege_cache';
set global save_query_result=on;
SHOW GLOBAL VARIABLES LIKE 'save_query_result';
set global max_connections=65536;
select @@max_connections;
set global lower_case_table_names=0;
select @@global.lower_case_table_names;
set global keep_user_target_list_in_result = 0;
select @@global.keep_user_target_list_in_result;
set global interactive_timeout = 20;
show global variables like 'interactive_timeout';
set global net_write_timeout = 10;
show global variables like 'net_write_timeout';
set global wait_timeout = 60;
show global variables like 'wait_timeout';
set global sql_select_limit = 20;
show global variables like 'sql_select_limit';
set global max_allowed_packet = 23564;
show global variables like 'max_allowed_packet';
set global tx_isolation = 1;
show global variables like 'tx_isolation';
-- @session
-- @session:id=2&user=acc2:admin&password=111
show global variables like 'sql_mode';
SELECT @@GLOBAL.sql_mode;
select @@TIME_ZONE;
SELECT @@global.autocommit;
show global variables like 'autocommit';
show global variables like 'enable_privilege_cache';
SHOW GLOBAL VARIABLES LIKE 'save_query_result';
select @@GLOBAL.max_connections;
select @@global.lower_case_table_names;
select @@global.keep_user_target_list_in_result;
show global variables like 'interactive_timeout';
show global variables like 'net_write_timeout';
show global variables like 'wait_timeout';
show global variables like 'sql_select_limit';
show global variables like 'max_allowed_packet';
show global variables like 'tx_isolation';
-- @session
-- sys confirm
show global variables like 'sql_mode';
SELECT @@GLOBAL.sql_mode;
select @@TIME_ZONE;
SELECT @@global.autocommit;
show global variables like 'autocommit';
show global variables like 'enable_privilege_cache';
select @@global.lower_case_table_names;
show global variables like 'net_write_timeout';
show global variables like 'wait_timeout';
show global variables like 'sql_select_limit';
show global variables like 'max_allowed_packet';
show global variables like 'tx_isolation';

-- acc2 set session variables
-- @session:id=2&user=acc2:admin&password=111
SET @@session.autocommit=off;
SELECT @@session.autocommit;
set enable_privilege_cache = off;
show variables like 'enable_privilege_cache';
set save_query_result=on;
SHOW VARIABLES LIKE 'save_query_result';
set max_connections=1000;
select @@max_connections;
set lower_case_table_names=0;
select @@lower_case_table_names;
set keep_user_target_list_in_result = 0;
select @@keep_user_target_list_in_result;
set interactive_timeout = 100;
SELECT @@interactive_timeout;
set net_write_timeout = 15;
SELECT @@net_write_timeout;
set wait_timeout = 19;
SELECT @@wait_timeout;
set sql_select_limit = 200;
SELECT @@sql_select_limit;
set max_allowed_packet = 83745;
SELECT @@max_allowed_packet;
set tx_isolation = 1;
SELECT @@tx_isolation;
SET @@SESSION.SQL_LOG_BIN= 0;
select @@SESSION.SQL_LOG_BIN;
SET @@session.autocommit=ON;
SELECT @@session.autocommit;
SET TIME_ZONE='+08:00';
select @@TIME_ZONE;
-- @session

-- acc2 other session confirm
-- @session:id=3&user=acc2:admin&password=111
SELECT @@session.autocommit;
select @@max_connections;
show variables like 'enable_privilege_cache';
SHOW VARIABLES LIKE 'save_query_result';
select @@lower_case_table_names;
select @@keep_user_target_list_in_result;
SELECT @@interactive_timeout;
SELECT @@net_write_timeout;
SELECT @@wait_timeout;
SELECT @@sql_select_limit;
SELECT @@max_allowed_packet;
SELECT @@tx_isolation;
select @@SESSION.SQL_LOG_BIN;
SELECT @@session.autocommit;
select @@TIME_ZONE;
-- @session
--acc3 confirm
-- @session:id=4&user=acc3:admin&password=111
SELECT @@session.autocommit;
select @@max_connections;
show variables like 'enable_privilege_cache';
SHOW VARIABLES LIKE 'save_query_result';
select @@lower_case_table_names;
select @@keep_user_target_list_in_result;
SELECT @@interactive_timeout;
SELECT @@net_write_timeout;
SELECT @@wait_timeout;
SELECT @@sql_select_limit;
SELECT @@max_allowed_packet;
SELECT @@tx_isolation;
select @@SESSION.SQL_LOG_BIN;
SELECT @@session.autocommit;
select @@TIME_ZONE;
-- @session
-- sys confirm
SELECT @@session.autocommit;
select @@max_connections;
show variables like 'enable_privilege_cache';
SHOW VARIABLES LIKE 'save_query_result';
select @@lower_case_table_names;
select @@keep_user_target_list_in_result;
SELECT @@net_write_timeout;
SELECT @@wait_timeout;
SELECT @@sql_select_limit;
SELECT @@max_allowed_packet;
SELECT @@tx_isolation;
select @@SESSION.SQL_LOG_BIN;
SELECT @@session.autocommit;
select @@TIME_ZONE;

drop account acc1;
drop account acc2;
drop account acc3;
