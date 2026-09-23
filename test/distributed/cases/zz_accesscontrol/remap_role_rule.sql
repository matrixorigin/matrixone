-- A role rule keyed by the remap source database must follow remapdb to the
-- destination relation. Otherwise the planner misses the rule after the
-- table reference is remapped and exposes rows that the role must not see.

set @rr_saved_privilege_cache_29222 = @@global.enable_privilege_cache;
set global enable_privilege_cache = off;

drop user if exists rr_user_29222;
drop role if exists rr_role_29222;
drop database if exists rr_src_29222;
drop database if exists rr_dst_29222;
drop database if exists rr_collision_a_29222;
drop database if exists rr_collision_b_29222;
drop database if exists rr_collision_dst_29222;

create database rr_src_29222;
create database rr_dst_29222;
create table rr_src_29222.t(id int, tenant int);
create table rr_dst_29222.t(id int, tenant int);
insert into rr_src_29222.t values (90, 1);
insert into rr_dst_29222.t values (1, 1), (2, 2);

create database rr_collision_a_29222;
create database rr_collision_b_29222;
create database rr_collision_dst_29222;
create table rr_collision_a_29222.t(id int, v int);
create table rr_collision_b_29222.t(id int, v int);
create table rr_collision_dst_29222.t(id int primary key, v int);
insert into rr_collision_dst_29222.t values (1, 10);

create role rr_role_29222;
alter role rr_role_29222 add rule "select * from rr_src_29222.t where tenant = 1" on table rr_src_29222.t;
create user rr_user_29222 identified by '123456' default role rr_role_29222;
grant connect on account * to rr_role_29222;
grant select, insert on table *.* to rr_role_29222;

-- The role rule is source-keyed, while the query resolves against the target.
-- The target has one allowed and one denied row; a missed rewrite returns both.
-- @session:id=1&user=sys:rr_user_29222:rr_role_29222&password=123456
set enable_remap_hint = 1;
set remap_rewrites = '{"remapdb":{"rr_src_29222":"rr_dst_29222"}}';
select id, tenant from rr_src_29222.t order by id;

-- Remapped DML must write to the target as well. The later root query proves
-- that the row was inserted into the remapped target, not the source table.
insert into rr_src_29222.t values (3, 3);
-- @session
select id, tenant from rr_dst_29222.t order by id;

-- A collision must fail before an INSERT target is changed. INSERT ... SELECT
-- carries the rewrite option on its read source, so this exercises the public
-- parser, remap walk, error propagation, and DML execution path together.
-- @session:id=2&user=sys:rr_user_29222:rr_role_29222&password=123456
set enable_remap_hint = 1;
set remap_rewrites = '{"remapdb":{"rr_collision_a_29222":"rr_collision_dst_29222","rr_collision_b_29222":"rr_collision_dst_29222"},"rewrites":{"rr_collision_a_29222.t":"select id, v from rr_collision_a_29222.t","rr_collision_b_29222.t":"select id, v from rr_collision_b_29222.t"}}';
-- @regex("remapdb rewrite key collision",true)
insert into rr_collision_dst_29222.t select id, v from rr_collision_a_29222.t;

-- Clear the invalid policy before checking the target and issuing the valid
-- same-session request. The first result must still contain only the seed row.
set remap_rewrites = '';
select id, v from rr_collision_dst_29222.t order by id;
insert into rr_collision_dst_29222.t values (2, 20);
select id, v from rr_collision_dst_29222.t order by id;
set enable_remap_hint = 0;
-- @session

drop user if exists rr_user_29222;
alter role rr_role_29222 drop rule on table rr_src_29222.t;
drop role if exists rr_role_29222;
drop database if exists rr_src_29222;
drop database if exists rr_dst_29222;
drop database if exists rr_collision_a_29222;
drop database if exists rr_collision_b_29222;
drop database if exists rr_collision_dst_29222;
set global enable_privilege_cache = @rr_saved_privilege_cache_29222;
