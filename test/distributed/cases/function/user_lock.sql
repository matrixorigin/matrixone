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

select get_lock('User_Lock_Bvt_Case', 0);
select is_free_lock('user_lock_bvt_case');
select release_lock('USER_LOCK_BVT_CASE');
select is_free_lock('user_lock_bvt_case');

drop table user_lock_bvt_holder;
drop database user_lock_bvt_db;
