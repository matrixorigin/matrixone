set global enable_privilege_cache = on;
drop user if exists u1;
drop role if exists r1;

-- setup
create user u1 identified by '111';
create role r1;
--r1 just have CONNECT privilege. r1 does not have the SHOW DATABASES privilege.
grant r1 to u1;

-----------------------------------------
--TEST 1: the privilege cache is enabled.
-----------------------------------------

-- @session:id=2&user=sys:u1:r1&password=111
show variables like 'enable_privilege_cache';
-- fail. has cache. the SHOW DATABASES privilege is not granted to r1.
show databases;
-- @session

-- give the SHOW DATABASES privilege to r1
grant show databases on account * to r1;

-- @session:id=2&user=sys:u1:r1&password=111
-- success. has cache. the SHOW DATABASES privilege is granted to r1.
show databases;
-- @session

-- revoke the SHOW DATABASES privilege from r1
revoke show databases on account * from r1;

-- @session:id=2&user=sys:u1:r1&password=111
show variables like 'enable_privilege_cache';
-- success. has cache. the SHOW DATABASES privilege is in cache.
show databases;
show databases;

-- clear the cache.
set clear_privilege_cache = on;
show variables like 'clear_privilege_cache';
show variables like 'enable_privilege_cache';
-- fail. has cache. the SHOW DATABASES privilege is not granted to r1.
show databases;
-- fail too. has cache. we do not cache fail.
show databases;
-- @session

-- give the SHOW DATABASES privilege to r1
grant show databases on account * to r1;

-- @session:id=2&user=sys:u1:r1&password=111
-- success. has cache. the SHOW DATABASES privilege is granted to r1.
show databases;
-- success. has cache. the SHOW DATABASES privilege is in cache.
show databases;
-- @session

------------------------------------------
--TEST 2: the privilege cache is disabled.
------------------------------------------
revoke show databases on account * from r1;

-- @session:id=2&user=sys:u1:r1&password=111
set enable_privilege_cache = off;
show variables like 'clear_privilege_cache';
show variables like 'enable_privilege_cache';
-- fail. no cache. the logic of the privilege check judges
-- the SHOW DATABASES privilege is not granted to r1.
show databases;
-- fail. no cache.
show databases;
-- @session

-- give the SHOW DATABASES privilege to r1
grant show databases on account * to r1;

-- @session:id=2&user=sys:u1:r1&password=111
-- success. no cache. the logic of the privilege check judges
-- the SHOW DATABASES privilege is granted to r1.
show databases;
-- @session

revoke show databases on account * from r1;

-- @session:id=2&user=sys:u1:r1&password=111
-- fail. no cache. the logic of the privilege check judges
-- the SHOW DATABASES privilege is not granted to r1.
show databases;
show databases;
-- @session

drop user if exists u1;
drop user if exists r1;
set clear_privilege_cache = on;
set global enable_privilege_cache = on;