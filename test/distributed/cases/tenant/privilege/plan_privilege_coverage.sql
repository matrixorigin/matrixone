-- A covering grant on one plan object must not authorize another object.
drop account if exists plan_privilege_coverage;
create account plan_privilege_coverage admin_name 'admin' identified by '111';

-- @session:id=1&user=plan_privilege_coverage:admin&password=111
create database plan_auth_src;
create database plan_auth_dst;
create table plan_auth_src.m(period int, amount int);
insert into plan_auth_src.m values (1,10),(1,20),(2,60),(2,null);
create view plan_auth_src.v as select period,amount from plan_auth_src.m;
create table plan_auth_dst.t(v bigint);
create table plan_auth_dst.r(id int primary key, v int);
insert into plan_auth_dst.r values (1,1);
create role plan_auth_writer;
grant connect on account * to plan_auth_writer;
grant select on table plan_auth_src.* to plan_auth_writer;
grant select on view plan_auth_src.v to plan_auth_writer;
grant all on table plan_auth_dst.* to plan_auth_writer;
create user plan_auth_user identified by '111' default role plan_auth_writer;
-- @session

-- @session:id=2&user=plan_privilege_coverage:plan_auth_user:plan_auth_writer&password=111
set enable_privilege_cache = on;
prepare shared_write from 'insert into plan_auth_dst.t select u.v from (with grouped as (select period,sum(amount) total from plan_auth_src.m group by period), totals as (select max(total) mx,min(total) mn from grouped) select mx v from totals union all select mn v from totals) u limit 2';
execute shared_write;
select count(*),sum(v) from plan_auth_dst.t;
-- @session

-- @session:id=1&user=plan_privilege_coverage:admin&password=111
revoke select on table plan_auth_src.* from plan_auth_writer;
-- @session

-- @session:id=2&user=plan_privilege_coverage:plan_auth_user:plan_auth_writer&password=111
select count(*) from plan_auth_src.m;
-- A granted view keeps its definer semantics but cannot cover an independent
-- ungranted base-table read in the same plan.
select count(*) from plan_auth_src.v;
select count(*) from plan_auth_src.v v join plan_auth_src.m m on v.period=m.period;
execute shared_write;
insert into plan_auth_dst.t select amount from plan_auth_src.m;
select count(*),sum(v) from plan_auth_dst.t;
set enable_privilege_cache = off;
select count(*) from plan_auth_src.v;
select count(*) from plan_auth_src.v v join plan_auth_src.m m on v.period=m.period;
execute shared_write;
select count(*),sum(v) from plan_auth_dst.t;
-- @session

-- @session:id=1&user=plan_privilege_coverage:admin&password=111
grant select on table plan_auth_src.* to plan_auth_writer;
revoke select on view plan_auth_src.v from plan_auth_writer;
-- @session

-- @session:id=2&user=plan_privilege_coverage:plan_auth_user:plan_auth_writer&password=111
execute shared_write;
select count(*) from plan_auth_src.v;
select count(*),sum(v) from plan_auth_dst.t;
-- @session

-- An ALL grant on the source must not cover missing destination INSERT.
-- @session:id=1&user=plan_privilege_coverage:admin&password=111
revoke select on table plan_auth_src.* from plan_auth_writer;
grant all on table plan_auth_src.* to plan_auth_writer;
revoke all on table plan_auth_dst.* from plan_auth_writer;
-- @session

-- @session:id=2&user=plan_privilege_coverage:plan_auth_user:plan_auth_writer&password=111
-- TABLE ALL must not imply a separate VIEW grant.
select count(*) from plan_auth_src.v;
execute shared_write;
-- @session

-- @session:id=1&user=plan_privilege_coverage:admin&password=111
select count(*),sum(v) from plan_auth_dst.t;
grant insert on table plan_auth_dst.* to plan_auth_writer;
grant select on table plan_auth_dst.* to plan_auth_writer;
-- @session

-- @session:id=2&user=plan_privilege_coverage:plan_auth_user:plan_auth_writer&password=111
execute shared_write;
-- REPLACE needs both INSERT and DELETE on the same target.
replace into plan_auth_dst.r values (1,2);
-- @session

-- @session:id=1&user=plan_privilege_coverage:admin&password=111
select count(*),sum(v) from plan_auth_dst.t;
select id,v from plan_auth_dst.r;
grant delete on table plan_auth_dst.* to plan_auth_writer;
-- @session

-- @session:id=2&user=plan_privilege_coverage:plan_auth_user:plan_auth_writer&password=111
replace into plan_auth_dst.r values (1,2);
deallocate prepare shared_write;
-- @session

-- @session:id=1&user=plan_privilege_coverage:admin&password=111
select id,v from plan_auth_dst.r;
drop user plan_auth_user;
drop role plan_auth_writer;
drop database plan_auth_src;
drop database plan_auth_dst;
-- @session

drop account plan_privilege_coverage;
