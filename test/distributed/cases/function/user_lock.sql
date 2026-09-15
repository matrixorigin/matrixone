-- @suite
-- @case
-- @desc:test mysql-compatible user-level lock functions
-- @label:bvt

drop database if exists user_lock_bvt_db;
create database user_lock_bvt_db;
use user_lock_bvt_db;

create table user_lock_bvt_holder (conn_id bigint unsigned);

select is_free_lock('user_lock_bvt_lock');
select is_used_lock('user_lock_bvt_lock') is null;
select release_lock('user_lock_bvt_lock') is null;

-- @session:id=1{
use user_lock_bvt_db;
insert into user_lock_bvt_holder values (connection_id());
select get_lock('user_lock_bvt_lock', 0);
select get_lock('user_lock_bvt_lock', 0);
-- @session}

select is_free_lock('user_lock_bvt_lock');
select get_lock('user_lock_bvt_lock', 0);
select is_used_lock('user_lock_bvt_lock') = (select conn_id from user_lock_bvt_holder);
select release_lock('user_lock_bvt_lock');

-- @session:id=2{
use user_lock_bvt_db;
select is_used_lock('user_lock_bvt_lock') = (select conn_id from user_lock_bvt_holder);
select release_all_locks();
-- @session}

-- @session:id=1{
use user_lock_bvt_db;
select is_free_lock('user_lock_bvt_lock');
select release_lock('user_lock_bvt_lock');
select is_free_lock('user_lock_bvt_lock');
select release_all_locks();
-- @session}

select is_free_lock('user_lock_bvt_lock');
select is_used_lock('user_lock_bvt_lock') is null;
select get_lock('user_lock_bvt_lock', 0);
select release_all_locks();
select release_all_locks();

select get_lock('user_lock_bvt_multi_a', 0);
select get_lock('user_lock_bvt_multi_a', 0);
select get_lock('user_lock_bvt_multi_a', 0);
select get_lock('user_lock_bvt_multi_b', 0);
select release_all_locks();
select is_free_lock('user_lock_bvt_multi_a');
select is_free_lock('user_lock_bvt_multi_b');

-- User-level lock names are case-insensitive.
select get_lock('User_Lock_Bvt_Case', 0);
select is_free_lock('user_lock_bvt_case');
select release_lock('USER_LOCK_BVT_CASE');
select is_free_lock('user_lock_bvt_case');
select release_lock('User_Lock_Bvt_Case');

-- These free-lock probes cover public SQL timeout types. The contended results
-- below cannot distinguish the exact rounding policies: both calls return 0
-- while the other session retains the lock. The DECIMAL WAIT/deadline versus
-- DOUBLE FAST_FAIL distinction is asserted by
-- TestGetLockDecimalTimeoutUsesDecimalRounding in pkg/sql/plan/function.
select get_lock('user_lock_bvt_timeout_zero_double', cast(0 as double));
select release_lock('user_lock_bvt_timeout_zero_double');
select get_lock('user_lock_bvt_timeout_decimal_free', 0.5);
select release_lock('user_lock_bvt_timeout_decimal_free');
select get_lock('user_lock_bvt_timeout_double_free', cast(0.5 as double));
select release_lock('user_lock_bvt_timeout_double_free');

-- NULL lock names must use MySQL's user-lock-name error, not SQL NULL propagation.
-- error ER_USER_LOCK_WRONG_NAME
select get_lock(NULL, NULL);
-- error ER_USER_LOCK_WRONG_NAME
select release_lock(NULL);
-- error ER_USER_LOCK_WRONG_NAME
select is_free_lock(NULL);
-- error ER_USER_LOCK_WRONG_NAME
select is_used_lock(NULL);

-- A NULL timeout is a zero-second FastFail: acquire when free and re-enter for
-- the same session, but do not wait when another session owns the lock.
select get_lock('user_lock_bvt_null_timeout_free', NULL);
select get_lock('user_lock_bvt_null_timeout_free', NULL);
select release_lock('user_lock_bvt_null_timeout_free');
select release_lock('user_lock_bvt_null_timeout_free');
select is_free_lock('user_lock_bvt_null_timeout_free');

-- @session:id=1{
select get_lock('user_lock_bvt_null_timeout_busy', 0);
-- @session}
-- @session:id=2{
select get_lock('user_lock_bvt_null_timeout_busy', NULL);
-- @session}
-- @session:id=1{
select release_lock('user_lock_bvt_null_timeout_busy');
-- @session}
select is_free_lock('user_lock_bvt_null_timeout_busy');

-- Both contended calls return 0 because the holder remains locked until after
-- each call. These SQL checks cover the public contended path; the precise
-- DECIMAL WAIT/deadline versus DOUBLE FAST_FAIL distinction is asserted by
-- TestGetLockDecimalTimeoutUsesDecimalRounding in the function unit tests.
-- @session:id=1{
select get_lock('user_lock_bvt_decimal_timeout_busy', 0);
-- @session}
-- @session:id=2{
select get_lock('user_lock_bvt_decimal_timeout_busy', 0.5);
-- @session}
-- @session:id=1{
select release_lock('user_lock_bvt_decimal_timeout_busy');
-- @session}
-- @session:id=1{
select get_lock('user_lock_bvt_double_timeout_busy', 0);
-- @session}
-- @session:id=2{
select get_lock('user_lock_bvt_double_timeout_busy', CAST(0.5 AS DOUBLE));
-- @session}
-- @session:id=1{
select release_lock('user_lock_bvt_double_timeout_busy');
-- @session}

drop table user_lock_bvt_holder;
drop database user_lock_bvt_db;
