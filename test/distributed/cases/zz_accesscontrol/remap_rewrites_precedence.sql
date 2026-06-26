-- Precedence of table-rewrite sources: role rule < remap_rewrites session
-- variable < inline /*+ ... */ hint. The inline hint is effective for a single
-- query only. Also verifies the session variable can be changed and cleared.

set global enable_privilege_cache = off;

drop user if exists rmp_user;
drop role if exists rmp_role;
drop database if exists rmp_db;

create database rmp_db;
create table rmp_db.t1(a int, age int);
insert into rmp_db.t1 values (1,1),(2,2),(100,30),(200,55);

create role rmp_role;
-- role rule: keep rows with age > 28
alter role rmp_role add rule "select * from rmp_db.t1 where age > 28" on table rmp_db.t1;
create user rmp_user identified by '123456' default role rmp_role;
grant connect on account * to rmp_role;
grant select on table *.* to rmp_role;

-- @session:id=1&user=sys:rmp_user:rmp_role&password=123456
set enable_remap_hint = 1;

-- only the role rule applies: age > 28 -> (100,30),(200,55)
select * from rmp_db.t1 order by a;

-- session variable overrides the role rule for the same table: age > 50 -> (200,55)
set remap_rewrites = '{"rmp_db.t1": "select * from rmp_db.t1 where age > 50"}';
select * from rmp_db.t1 order by a;

-- inline hint overrides the session variable (and role) for THIS query only: a = 1 -> (1,1)
/*+ {"rewrites": {"rmp_db.t1": "select * from rmp_db.t1 where a = 1"}} */ select * from rmp_db.t1 order by a;

-- the next query (no inline hint) is back to the session variable: age > 50 -> (200,55)
select * from rmp_db.t1 order by a;

-- changing the session variable takes effect immediately: a < 100 -> (1,1),(2,2)
set remap_rewrites = '{"rmp_db.t1": "select * from rmp_db.t1 where a < 100"}';
select * from rmp_db.t1 order by a;

-- clearing the session variable reverts to the role rule: age > 28 -> (100,30),(200,55)
set remap_rewrites = '';
select * from rmp_db.t1 order by a;
set enable_remap_hint = 0;
-- @session

drop user if exists rmp_user;
drop role if exists rmp_role;
drop database if exists rmp_db;
