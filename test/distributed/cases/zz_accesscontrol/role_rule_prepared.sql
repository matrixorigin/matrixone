set enable_privilege_cache = off;

drop user if exists issue29148_rule_user;
drop role if exists issue29148_rule_role;
drop database if exists issue29148_rule_db;

create database issue29148_rule_db;
create table issue29148_rule_db.t(id int, amount int, tenant int);
insert into issue29148_rule_db.t values (1,100,1),(2,200,2);

create role issue29148_rule_role;
create user issue29148_rule_user identified by '123456' default role issue29148_rule_role;
grant connect on account * to issue29148_rule_role;
grant select on table issue29148_rule_db.t to issue29148_rule_role;
grant create view on database issue29148_rule_db to issue29148_rule_role;
create procedure issue29148_rule_db.make_handle() 'begin prepare issue29148_call from ''select id, amount from issue29148_rule_db.t order by id''; end';

-- A procedure-produced handle must capture an empty policy, preserve the
-- empty-to-empty refresh, and reject execution after mandatory rules appear.
-- @session:id=1&user=sys:issue29148_rule_user:issue29148_rule_role&password=123456
use issue29148_rule_db;
set enable_remap_hint = 0;
call issue29148_rule_db.make_handle();
execute issue29148_call;
set role issue29148_missing_role;
execute issue29148_call;
-- @session

alter role issue29148_rule_role add rule "select id, amount from issue29148_rule_db.t where tenant = 1" on table issue29148_rule_db.t;

-- @session:id=1&user=sys:issue29148_rule_user:issue29148_rule_role&password=123456
set role issue29148_missing_role;
select id, amount from issue29148_rule_db.t order by id;
execute issue29148_call;
deallocate prepare issue29148_call;
call issue29148_rule_db.make_handle();
execute issue29148_call;
deallocate prepare issue29148_call;
-- @session

-- @session:id=1&user=sys:issue29148_rule_user:issue29148_rule_role&password=123456
set enable_remap_hint = 1;
set enable_privilege_cache = off;
prepare issue29148_stale from 'select id, amount from issue29148_rule_db.t order by id';
execute issue29148_stale;
-- @session

-- Refresh the same role's rule in the root session. The user session must
-- explicitly refresh its role before the prepared handle can be considered.
alter role issue29148_rule_role add rule "select id, amount from issue29148_rule_db.t where tenant = 2" on table issue29148_rule_db.t;

-- @session:id=1&user=sys:issue29148_rule_user:issue29148_rule_role&password=123456
set role issue29148_rule_role;
select id, amount from issue29148_rule_db.t order by id;
execute issue29148_stale;
prepare issue29148_fresh from 'select id, amount from issue29148_rule_db.t order by id';
execute issue29148_fresh;
deallocate prepare issue29148_fresh;
-- @session

drop user if exists issue29148_rule_user;
drop role if exists issue29148_rule_role;
drop database if exists issue29148_rule_db;
