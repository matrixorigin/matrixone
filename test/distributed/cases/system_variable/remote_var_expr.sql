-- @suit

-- @case
-- @desc:test session variable expressions folded before remote run
-- @label:bvt

drop database if exists remote_var_expr_db;
create database remote_var_expr_db;
use remote_var_expr_db;

create table t(a int primary key, b varchar(64));
insert into t values (1, 'ONLY_FULL_GROUP_BY'), (2, 'STRICT_TRANS_TABLES'), (3, 'x');

set @@sql_mode = 'ONLY_FULL_GROUP_BY';
select @@sql_mode as mode;
select count(*) as matched from t where b = @@sql_mode;
-- @session:id=1{
use remote_var_expr_db;
begin;
select a, b from t where b = 'ONLY_FULL_GROUP_BY' for update;
-- @wait:0:rollback
commit;
-- @session}
select sleep(0.2);
begin;
set session lock_wait_timeout = 1;
select a, b from t where b = @@sql_mode for update;
rollback;
begin;
select a, b from t where b = @@sql_mode for update;
commit;
select sum(length(@@sql_mode)) as mode_len_sum from t;
prepare stmt_remote_var from 'select count(*) as matched from t where b = @@sql_mode';
execute stmt_remote_var;

set @@sql_mode = 'STRICT_TRANS_TABLES';
select @@sql_mode as mode;
select count(*) as matched from t where b = @@sql_mode;
select sum(length(@@sql_mode)) as mode_len_sum from t;
execute stmt_remote_var;

deallocate prepare stmt_remote_var;
set @@sql_mode = default;
drop database remote_var_expr_db;
