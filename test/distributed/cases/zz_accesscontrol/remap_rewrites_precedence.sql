-- Table-rewrite layering: role rule, then the remap_rewrites session variable,
-- then the inline /*+ ... */ hint are applied as STACKED VIEWS (each layer is a
-- view over the previous one), NOT as overrides. A session/inline rule sees the
-- role-rewritten relation, so it can only narrow/transform further, never undo
-- what the role rule did. The inline layer is effective for one query only.
-- Also verifies the session variable can be changed and cleared.

set global enable_privilege_cache = off;

drop user if exists rmp_user;
drop role if exists rmp_role;
drop database if exists rmp_db;

create database rmp_db;
create table rmp_db.t(id int, v int);
insert into rmp_db.t values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60);

create role rmp_role;
-- role rule keeps id <= 4
alter role rmp_role add rule "select * from rmp_db.t where id <= 4" on table rmp_db.t;
create user rmp_user identified by '123456' default role rmp_role;
grant connect on account * to rmp_role;
grant select on table *.* to rmp_role;

-- @session:id=1&user=sys:rmp_user:rmp_role&password=123456
set enable_remap_hint = 1;

-- role rule only: id <= 4 -> 1,2,3,4
select id from rmp_db.t order by id;

-- session layered on top of the role rule: id <= 4 AND id >= 2 -> 2,3,4.
-- It does NOT overwrite the role rule: 5 and 6 stay hidden even though the
-- session rule alone would have kept them.
set remap_rewrites = '{"rmp_db.t": "select * from rmp_db.t where id >= 2"}';
select id from rmp_db.t order by id;

-- inline layered on top of role+session, for THIS query only:
-- id <= 4 AND id >= 2 AND id <= 3 -> 2,3
/*+ {"rewrites": {"rmp_db.t": "select * from rmp_db.t where id <= 3"}} */ select id from rmp_db.t order by id;

-- next query (no inline) is back to role+session -> 2,3,4
select id from rmp_db.t order by id;

-- changing the session variable re-layers immediately: id <= 4 AND id <= 2 -> 1,2
set remap_rewrites = '{"rmp_db.t": "select * from rmp_db.t where id <= 2"}';
select id from rmp_db.t order by id;

-- clearing the session variable leaves only the role rule: id <= 4 -> 1,2,3,4
set remap_rewrites = '';
select id from rmp_db.t order by id;
set enable_remap_hint = 0;
-- @session

drop user if exists rmp_user;
drop role if exists rmp_role;
drop database if exists rmp_db;
