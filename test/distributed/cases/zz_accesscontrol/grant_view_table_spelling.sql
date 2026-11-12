-- A privilege stored on a view through the TABLE spelling ("grant ... on
-- table v") and the bare MySQL spelling ("grant ... on v") is enforced when
-- the view is queried (issue #32444). Table-class wildcard grants stay
-- distinct from view access. The case snapshots the shared global cache
-- setting first and restores the genuine original value at the end.
set @saved_enable_privilege_cache = @@global.enable_privilege_cache;
select @saved_enable_privilege_cache;
set global enable_privilege_cache = off;

-- cleanup
drop account if exists acc_vt;

-- setup: create the tenant under test
create account acc_vt admin_name = 'admin' identified by '111';

-- @session:id=1&user=acc_vt:admin&password=111
create role r_vw;
create user u_vw identified by '111' default role r_vw;
grant r_vw to u_vw;
create database db_vt;
use db_vt;
create table t1 (a int);
insert into t1 values (1);
create view vw1 as select * from t1;
grant connect on account * to r_vw;
-- @session

-- Reproduce the issue core: the grant is written with the TABLE spelling on
-- a view. The row is stored under the table object class against the view's
-- identity, never under the view object class.
-- @session:id=1&user=acc_vt:admin&password=111
grant select on table db_vt.vw1 to r_vw;
select count(*) as table_class_grant from mo_catalog.mo_role_privs rp join mo_catalog.mo_role r on rp.role_id = r.role_id where r.role_name = 'r_vw' and rp.obj_type = 'table' and rp.privilege_name = 'select';
select count(*) as view_class_grant from mo_catalog.mo_role_privs rp join mo_catalog.mo_role r on rp.role_id = r.role_id where r.role_name = 'r_vw' and rp.obj_type = 'view' and rp.privilege_name = 'select';
-- @session

-- The grant must be honored at query time in a fresh session
-- @session:id=2&user=acc_vt:u_vw&password=111
select * from db_vt.vw1;
-- @session

-- Revoke through the same spelling must take the privilege away
-- @session:id=1&user=acc_vt:admin&password=111
revoke select on table db_vt.vw1 from r_vw;
-- @session

-- verify revoked in a NEW session
-- @session:id=3&user=acc_vt:u_vw&password=111
select * from db_vt.vw1;
-- @session

-- The bare MySQL spelling ("on db.vw", no object keyword) is the same
-- legacy-table storage path and must be enforced too.
-- @session:id=1&user=acc_vt:admin&password=111
grant select on db_vt.vw1 to r_vw;
-- @session

-- @session:id=4&user=acc_vt:u_vw&password=111
select * from db_vt.vw1;
-- @session

-- A WITH GRANT OPTION grant written with the same spelling can be passed on
-- by its non-admin holder in the same spelling.
-- @session:id=1&user=acc_vt:admin&password=111
revoke select on db_vt.vw1 from r_vw;
grant select on db_vt.vw1 to r_vw with grant option;
create user u2 identified by '111' default role r_vw;
grant r_vw to u2;
create role r_dst;
create user u3 identified by '111' default role r_dst;
grant r_dst to u3;
-- @session

-- @session:id=5&user=acc_vt:u2&password=111
grant select on db_vt.vw1 to r_dst with grant option;
-- @session

-- The onward grant is enforced for the downstream role holder
-- @session:id=6&user=acc_vt:u3&password=111
select * from db_vt.vw1;
-- @session

-- Table-class wildcard grants stay distinct from view access: db.* on the
-- table object class covers the base table but not the view, while the view
-- spelling authorizes the view.
-- @session:id=1&user=acc_vt:admin&password=111
revoke select on db_vt.vw1 from r_dst;
grant select on table db_vt.* to r_dst;
-- @session

-- @session:id=7&user=acc_vt:u3&password=111
select * from db_vt.t1;
select * from db_vt.vw1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
grant select on view db_vt.vw1 to r_dst;
-- @session

-- @session:id=8&user=acc_vt:u3&password=111
select * from db_vt.vw1;
-- @session

-- The VIEW spelling keeps rejecting a base table loudly
-- @session:id=1&user=acc_vt:admin&password=111
grant select on view db_vt.t1 to r_vw;
-- @session

-- A base-table exact grant does not authorize an unrelated view
-- @session:id=1&user=acc_vt:admin&password=111
use db_vt;
create view vw2 as select a + 1 as a from t1;
create role r_other;
create user u_other identified by '111' default role r_other;
grant r_other to u_other;
grant select on table db_vt.t1 to r_other;
-- @session

-- @session:id=9&user=acc_vt:u_other&password=111
select * from db_vt.t1;
select * from db_vt.vw2;
-- @session

-- ALL PRIVILEGES through the TABLE spelling covers the view read
-- @session:id=1&user=acc_vt:admin&password=111
grant all on table db_vt.vw2 to r_other;
-- @session

-- @session:id=10&user=acc_vt:u_other&password=111
select * from db_vt.vw2;
-- @session

-- revoking ALL through the same spelling removes it again
-- @session:id=1&user=acc_vt:admin&password=111
revoke all on table db_vt.vw2 from r_other;
-- @session

-- @session:id=11&user=acc_vt:u_other&password=111
select * from db_vt.vw2;
-- @session

-- Challenge regression (cache_lifecycle / cache_switch_probe): REVOKE ALL on
-- a relation must clear every privilege row of the exact object, including a
-- separately granted SELECT, and must remove the rows of both stored
-- spellings (ON TABLE and ON VIEW classes). Otherwise the relation stays
-- authorized through the surviving row.
-- @session:id=1&user=acc_vt:admin&password=111
create role r_play;
create user u_play identified by '111' default role r_play;
grant r_play to u_play;
grant connect on account * to r_play;
grant select on table db_vt.vw1 to r_play;
grant all on table db_vt.vw1 to r_play;
-- @session

-- @session:id=12&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
revoke all on table db_vt.vw1 from r_play;
-- @session

-- @session:id=13&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- a view-spelling grant on the same view is removed by the same revoke too
-- @session:id=1&user=acc_vt:admin&password=111
grant select on view db_vt.vw1 to r_play;
-- @session

-- @session:id=14&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
revoke all on table db_vt.vw1 from r_play;
-- @session

-- @session:id=15&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- the same semantics on a base table: a leftover SELECT row must not keep
-- the table authorized after REVOKE ALL
-- @session:id=1&user=acc_vt:admin&password=111
grant select on table db_vt.t1 to r_play;
grant all privileges on table db_vt.t1 to r_play;
-- @session

-- @session:id=16&user=acc_vt:u_play&password=111
select * from db_vt.t1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
revoke all privileges on table db_vt.t1 from r_play;
-- @session

-- @session:id=17&user=acc_vt:u_play&password=111
select * from db_vt.t1;
-- @session

-- Review regression (qualification symmetry): an exact-relation grant and
-- revoke written qualified (db.v) or unqualified (v inside the database
-- context) resolve to the same relation identity, so a revoke in either
-- spelling removes the row stored by the other spelling.
-- @session:id=1&user=acc_vt:admin&password=111
use db_vt;
grant select on table vw1 to r_play;
-- @session

-- the unqualified TABLE spelling authorizes the view in a fresh session
-- @session:id=18&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- a revoke written with the qualified spelling removes the unqualified grant
-- @session:id=1&user=acc_vt:admin&password=111
revoke all on table db_vt.vw1 from r_play;
-- @session

-- @session:id=19&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- the other direction: a qualified grant is removed by an unqualified revoke
-- @session:id=1&user=acc_vt:admin&password=111
grant select on table db_vt.vw1 to r_play;
-- @session

-- @session:id=20&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
use db_vt;
revoke select on table vw1 from r_play;
-- @session

-- @session:id=21&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- cross-wording symmetry for the VIEW grant spelling: an unqualified VIEW
-- grant is removed by a qualified TABLE-wording REVOKE ALL
-- @session:id=1&user=acc_vt:admin&password=111
use db_vt;
grant select on view vw1 to r_play;
-- @session

-- @session:id=22&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
revoke all on table db_vt.vw1 from r_play;
-- @session

-- @session:id=23&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- and a qualified VIEW grant is removed by the unqualified TABLE-wording
-- REVOKE ALL
-- @session:id=1&user=acc_vt:admin&password=111
grant select on view db_vt.vw1 to r_play;
-- @session

-- @session:id=24&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
use db_vt;
revoke all on table vw1 from r_play;
-- @session

-- @session:id=25&user=acc_vt:u_play&password=111
select * from db_vt.vw1;
-- @session

-- Review regression (wildcard scoping): the revoke of a wildcard scope
-- (db.*) only touches grants stored under its own object class. A
-- TABLE-wording wildcard revoke must not remove an independent VIEW-class
-- wildcard grant, and the reciprocal holds too; only the same-class
-- wording takes the grant away.
-- @session:id=1&user=acc_vt:admin&password=111
create role r_wild;
create user u_wild identified by '111' default role r_wild;
grant r_wild to u_wild;
grant connect on account * to r_wild;
grant select on view db_vt.* to r_wild;
-- @session

-- the VIEW-class wildcard grant authorizes the view
-- @session:id=26&user=acc_vt:u_wild&password=111
select * from db_vt.vw1;
-- @session

-- TABLE-wording wildcard revokes leave the VIEW-class wildcard grant intact
-- @session:id=1&user=acc_vt:admin&password=111
revoke select on table db_vt.* from r_wild;
revoke all on table db_vt.* from r_wild;
-- @session

-- @session:id=27&user=acc_vt:u_wild&password=111
select * from db_vt.vw1;
-- @session

-- the same-class wording removes it
-- @session:id=1&user=acc_vt:admin&password=111
revoke select on view db_vt.* from r_wild;
-- @session

-- @session:id=28&user=acc_vt:u_wild&password=111
select * from db_vt.vw1;
-- @session

-- reciprocal control: the TABLE-wording wildcard grant survives
-- VIEW-wording revokes
-- @session:id=1&user=acc_vt:admin&password=111
grant select on table db_vt.* to r_wild;
-- @session

-- @session:id=29&user=acc_vt:u_wild&password=111
select * from db_vt.t1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
revoke select on view db_vt.* from r_wild;
revoke all on view db_vt.* from r_wild;
-- @session

-- @session:id=30&user=acc_vt:u_wild&password=111
select * from db_vt.t1;
-- @session

-- @session:id=1&user=acc_vt:admin&password=111
revoke select on table db_vt.* from r_wild;
-- @session

-- @session:id=31&user=acc_vt:u_wild&password=111
select * from db_vt.t1;
-- @session

-- cleanup
-- @session:id=1&user=acc_vt:admin&password=111
drop database db_vt;
-- @session

drop account acc_vt;
-- restore the genuine original global value captured at case start;
-- the stable oracle below must show the same value as at case start
set global enable_privilege_cache = @saved_enable_privilege_cache;
select @@global.enable_privilege_cache;
