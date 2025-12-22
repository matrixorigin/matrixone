-- @session:id=0&user=dump&password=111
-- Test cases for Group 0
create user user1_group0 identified by '1234';
-- Clean up Group 0
drop user if exists user1_group0;

set global validate_password = 'ON';
set global validate_password.check_user_name = 'ON';
set global validate_password.changed_characters_percentage = 0;
set global validate_password.length = 5;
set global validate_password.mixed_case_count = 0;
set global validate_password.number_count = 0;
set global validate_password.special_char_count = 0;
set global validate_password.policy = '0';
-- @session

-- ========================
-- 1. Test Group 1
-- ========================
-- @session:id=1&user=dump&password=111
show variables like "%validate_password%";

-- test
create user user1_group1 identified by '1234'; -- fail
create user user2_group1 identified by 'abc'; -- fail
create user user3_group1 identified by '12345'; -- success
create user user4_group1 identified by 'abcde'; -- success

-- verify
select user_name from mo_catalog.mo_user where user_name in ('user3_group1', 'user4_group1');
-- Clean up Group 1
drop user if exists user1_group1, user2_group1, user3_group1, user4_group1;

set global validate_password = 'ON';
set global validate_password.check_user_name = 'ON';
set global validate_password.changed_characters_percentage = 60;
set global validate_password.length = 8;
set global validate_password.mixed_case_count = 2;
set global validate_password.number_count = 2;
set global validate_password.special_char_count = 0;
set global validate_password.policy = '1';
-- @session

-- ========================
-- 2. Test Group 2
-- ========================
-- @session:id=2&user=dump&password=111
show variables like "%validate_password%";

-- Test cases
create user user1_group2 identified by '12345678'; -- Expected failure
create user user2_group2 identified by 'abcdefgH'; -- Expected failure
-- @bvt:issue#4511
create user useR32Go identified by 'oG23Resu'; -- Expected failure
-- @bvt:issue
create user user4_group2 identified by 'AbCLq56%'; -- Expected success

-- Verify results
select user_name from mo_catalog.mo_user where user_name in ('user3_group2', 'user4_group2');

-- Clean up Group 2
drop user if exists user1_group2, user2_group2, useR32Go, user4_group2;

-- Set parameters for Group 3 password complexity
set global validate_password = 'ON';
set global validate_password.check_user_name = 'OFF';
set global validate_password.changed_characters_percentage = 50;
set global validate_password.length = 30;
set global validate_password.mixed_case_count = 5;
set global validate_password.number_count = 8;
set global validate_password.special_char_count = 2;
set global validate_password.policy = '1';
-- @session

-- ========================
-- 3. Test Group 3
-- ========================
-- @session:id=3&user=dump&password=111
-- Test cases
create user user1_group3 identified by 'abcdefgH'; -- Expected failure
create user user2_group3 identified by '1234abcd'; -- Expected failure
create user user3_group3 identified by 'Abc123KLedjrg563O28d'; -- Expected failure
create user user4_group3 identified by 'Xyz78pLPAbc123JHedjrg563Ukkd_%'; -- Expected success

-- Verify results
select user_name from mo_catalog.mo_user where user_name in ('user4_group3');

-- Clean up Group 3
drop user if exists user1_group3, user2_group3, user3_group3, user4_group3;

-- Set parameters for Group 4 password complexity
set global validate_password = 'ON';
set global validate_password.check_user_name = 'ON';
set global validate_password.changed_characters_percentage = 40;
set global validate_password.length = 15;
set global validate_password.mixed_case_count = 3;
set global validate_password.number_count = 5;
set global validate_password.special_char_count = 3;
set global validate_password.policy = '1';
-- @session

-- ========================
-- 4. Test Group 4
-- ========================
-- @session:id=4&user=dump&password=111
-- Test cases
create user user1_group4 identified by '123456789abcde'; -- Expected failure
create user user2_group4 identified by 'Abcdefg1234567'; -- Expected failure
create user user3_group4 identified by 'Abc123%_@RMXy78'; -- Expected success

-- Verify results
select user_name from mo_catalog.mo_user where user_name in ('user3_group4', 'user4_group4');

-- Clean up Group 4
drop user if exists user1_group4, user2_group4, user3_group4, user4_group4;

-- ========================
-- Invalid test
-- ========================
set global validate_password = 'HELLO';
set global validate_password.check_user_name = 'IN';
set global validate_password.changed_characters_percentage = 101;
set global validate_password.length = inv;
set global validate_password.mixed_case_count = abc;
set global validate_password.number_count = -3;
set global validate_password.special_char_count = -1;
set global validate_password.policy = 'HIGH';

-- ========================
-- Reset default configurations after all tests
-- ========================
set global validate_password = 'OFF';
set global validate_password.check_user_name = 'ON';
set global validate_password.changed_characters_percentage = 0;
set global validate_password.length = 8;
set global validate_password.mixed_case_count = 1;
set global validate_password.number_count = 1;
set global validate_password.special_char_count = 1;
set global validate_password.policy = '0';
-- @session

-- ========================
-- 1. Set global parameters for password lifecycle and failed connection threshold
-- ========================
-- @session:id=5&user=dump&password=111
-- Create dump user and grant privileges to modify other users

-- Show current variables before setting new values
SHOW VARIABLES LIKE "%connection_control_failed_connections_threshold%";
SHOW VARIABLES LIKE "%connection_control_max_connection_delay%";
SHOW VARIABLES LIKE "%password_history%";
SHOW VARIABLES LIKE "%password_reuse_interval%";

-- Set parameters for Group 1 password lifecycle
SET GLOBAL connection_control_failed_connections_threshold = 3;
SET GLOBAL connection_control_max_connection_delay = 5000;
SET GLOBAL password_history = 0;
SET GLOBAL password_reuse_interval = 0;
-- @session

-- ========================
-- 2. Verify parameters for Group 1 in a new session
-- ========================

-- @session:id=6&user=dump&password=111
-- Show the current parameter settings after change
SHOW VARIABLES LIKE "%connection_control_failed_connections_threshold%";
SHOW VARIABLES LIKE "%connection_control_max_connection_delay%";
SHOW VARIABLES LIKE "%password_history%";
SHOW VARIABLES LIKE "%password_reuse_interval%";

-- Modify test users' passwords using administrator
CREATE USER user1_group1 IDENTIFIED BY 'oldpassword';
CREATE USER user2_group1 IDENTIFIED BY 'oldpassword2';

-- Change user passwords
ALTER USER user1_group1 IDENTIFIED BY 'newpassword1';
SELECT SLEEP(1);
ALTER USER user2_group1 IDENTIFIED BY 'newpassword2';
SELECT SLEEP(1);

-- Verify users' password change status
SELECT user_name FROM mo_catalog.mo_user WHERE user_name IN ('user1_group1', 'user2_group1');

-- Clean up Group 1
DROP USER IF EXISTS user1_group1, user2_group1;

-- ========================
-- 3. Test Group 2: Password history and reuse interval
-- ========================

-- Set parameters for Group 2 password lifecycle
SET GLOBAL connection_control_failed_connections_threshold = 5;
SET GLOBAL connection_control_max_connection_delay = 1000;
SET GLOBAL password_history = 6;
SET GLOBAL password_reuse_interval = 10;
-- @session

-- @session:id=7&user=dump&password=111
-- Show the current parameter settings before test
SHOW VARIABLES LIKE "%connection_control_failed_connections_threshold%";
SHOW VARIABLES LIKE "%connection_control_max_connection_delay%";
SHOW VARIABLES LIKE "%password_history%";
SHOW VARIABLES LIKE "%password_reuse_interval%";

-- Modify test users' passwords using administrator
CREATE USER user1_group2 IDENTIFIED BY 'oldpassword1';
CREATE USER user2_group2 IDENTIFIED BY 'oldpassword2';

-- Change user passwords
ALTER USER user1_group2 IDENTIFIED BY 'newpassword1!'; 
SELECT SLEEP(1);
ALTER USER user2_group2 IDENTIFIED BY 'newpassword2!';
SELECT SLEEP(1);

-- Attempt to reuse old passwords (expected failure)
ALTER USER user1_group2 IDENTIFIED BY 'oldpassword1'; -- Expected failure: Old password cannot be reused
SELECT SLEEP(1);
ALTER USER user2_group2 IDENTIFIED BY 'oldpassword2'; -- Expected failure: Old password cannot be reused
SELECT SLEEP(1);

-- Verify users' password change status
SELECT user_name FROM mo_catalog.mo_user WHERE user_name IN ('user1_group2', 'user2_group2');

-- Clean up Group 2
DROP USER IF EXISTS user1_group2, user2_group2;

-- ========================
-- 4. Test Group 3: Failed connection attempts and lock time
-- ========================

-- Set parameters for Group 3 password lifecycle
SET GLOBAL connection_control_failed_connections_threshold = 10;
SET GLOBAL connection_control_max_connection_delay = 2147483647;
SET GLOBAL password_history = 10;
SET GLOBAL password_reuse_interval = 30;
-- @session

-- @session:id=8&user=dump&password=111
-- Show the current parameter settings before test
SHOW VARIABLES LIKE "%connection_control_failed_connections_threshold%";
SHOW VARIABLES LIKE "%connection_control_max_connection_delay%";
SHOW VARIABLES LIKE "%password_history%";
SHOW VARIABLES LIKE "%password_reuse_interval%";

-- Modify test users' passwords using administrator
CREATE USER user1_group3 IDENTIFIED BY 'password123';
CREATE USER user2_group3 IDENTIFIED BY 'password456';

-- Simulate failed connection attempts (requires client-side simulation)
-- Once failed attempts exceed threshold, verify if user gets locked

-- Clean up Group 3
DROP USER IF EXISTS user1_group3, user2_group3;

-- ========================
-- 5. Test Group 4: Test advanced settings (lock time and password history)
-- ========================

-- Set parameters for Group 4 password lifecycle
SET GLOBAL connection_control_failed_connections_threshold = 0;
SET GLOBAL connection_control_max_connection_delay = 1000;
SET GLOBAL password_history = 1;
SET GLOBAL password_reuse_interval = 7;
-- @session

-- @session:id=9&user=dump&password=111
-- Show the current parameter settings before test
SHOW VARIABLES LIKE "%connection_control_failed_connections_threshold%";
SHOW VARIABLES LIKE "%connection_control_max_connection_delay%";
SHOW VARIABLES LIKE "%password_history%";
SHOW VARIABLES LIKE "%password_reuse_interval%";

-- Modify test users' passwords using administrator
CREATE USER user1_group4 IDENTIFIED BY 'oldpassword123';
CREATE USER user2_group4 IDENTIFIED BY 'oldpassword456';

-- Change passwords
ALTER USER user1_group4 IDENTIFIED BY 'newpassword123';
ALTER USER user2_group4 IDENTIFIED BY 'newpassword456';

-- Attempt to reuse old passwords (expected failure)
ALTER USER user1_group4 IDENTIFIED BY 'oldpassword123'; -- Expected failure: Old password cannot be reused
ALTER USER user2_group4 IDENTIFIED BY 'oldpassword456'; -- Expected failure: Old password cannot be reused

-- Verify users' password change status
SELECT user_name FROM mo_catalog.mo_user WHERE user_name IN ('user1_group4', 'user2_group4');

-- Clean up Group 4
DROP USER IF EXISTS user1_group4, user2_group4;
-- ========================
-- 6. Test Group 5: Invalid values for lifecycle parameters
-- ========================

SET GLOBAL connection_control_failed_connections_threshold = -1; -- Invalid: cannot be negative
SET GLOBAL connection_control_max_connection_delay = -100; -- Invalid: cannot be negative
SET GLOBAL password_history = -2; -- Invalid: cannot be negative
SET GLOBAL password_reuse_interval = -5; -- Invalid: cannot be negative
-- ========================
-- Reset default configurations after all tests
-- ========================
SET GLOBAL connection_control_failed_connections_threshold = 3;
SET GLOBAL connection_control_max_connection_delay = 0;
SET GLOBAL password_history = 0;
SET GLOBAL password_reuse_interval = 0;
-- @session

