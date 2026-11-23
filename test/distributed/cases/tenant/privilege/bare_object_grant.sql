-- #31804: object_type is optional in GRANT/REVOKE; the omitted form defaults to
-- TABLE and touches the same actual grant as the `ON TABLE` keyword form.
-- Real-user oracle: bare grant allows reads on the granted object only, bare
-- revoke denies on the same live session, the session keeps working
-- afterwards, and explicit ON TABLE revoke removes grants given via the bare
-- form. The case snapshots the shared global cache setting first and restores
-- the genuine original value at the end; the runner finally does the same on
-- any exit so no other case can be affected.
set @saved_enable_privilege_cache = @@global.enable_privilege_cache;
select @saved_enable_privilege_cache;
set global enable_privilege_cache = off;
drop database if exists bare_grant_bvt;
create database bare_grant_bvt;
use bare_grant_bvt;
create table tgt (a int);
insert into tgt values (1), (2), (3);
create table other (b int);
insert into other values (7);
drop role if exists r_bare;
create role r_bare;
drop user if exists u_bare;
create user u_bare identified by '111' default role r_bare;
grant connect on account * to r_bare;
grant show databases on account * to r_bare;
grant show tables on database bare_grant_bvt to r_bare;

-- bare db.table grant
grant select on bare_grant_bvt.tgt to r_bare;
-- @session:id=101&user=sys:u_bare:r_bare&password=111
use bare_grant_bvt;
select a from tgt order by a;
-- no partial grant: the other table in the same database stays denied
-- @ignore:0
select * from other;
-- the same session keeps working after the rejection
select 1;
-- @session

-- bare revoke denies the same live named session, which then continues
revoke select on bare_grant_bvt.tgt from r_bare;
-- @session:id=101&user=sys:u_bare:r_bare&password=111
use bare_grant_bvt;
-- @ignore:0
select a from tgt;
select 1;
-- @session
-- a fresh connection also works and is equally denied
-- @session:id=102&user=sys:u_bare:r_bare&password=111
use bare_grant_bvt;
-- @ignore:0
select a from tgt;
select 1;
-- @session

-- bare db.* level grant and revoke
grant select on bare_grant_bvt.* to r_bare;
-- @session:id=101&user=sys:u_bare:r_bare&password=111
use bare_grant_bvt;
select a from tgt order by a;
select * from other;
-- @session
revoke select on bare_grant_bvt.* from r_bare;
-- @session:id=101&user=sys:u_bare:r_bare&password=111
use bare_grant_bvt;
-- @ignore:0
select a from tgt;
select 1;
-- @session

-- multi-privilege bare grant with WITH GRANT OPTION, revoked through the
-- explicit ON TABLE keyword form: both forms address the same actual grant
grant select, insert, update on bare_grant_bvt.tgt to r_bare with grant option;
-- @session:id=101&user=sys:u_bare:r_bare&password=111
use bare_grant_bvt;
insert into tgt values (4);
select a from tgt order by a;
-- @session
revoke select, insert, update on table bare_grant_bvt.tgt from r_bare;
-- @session:id=101&user=sys:u_bare:r_bare&password=111
use bare_grant_bvt;
-- @ignore:0
select a from tgt;
select 1;
-- @session

-- error control: a bad grantee must not partially authorize anything
-- @ignore:0
grant select on bare_grant_bvt.tgt to no_such_role;
-- @session:id=101&user=sys:u_bare:r_bare&password=111
use bare_grant_bvt;
-- @ignore:0
select a from tgt;
select 1;
-- @session
select count(*) from tgt;
select * from other;

drop user if exists u_bare;
drop role if exists r_bare;
drop database if exists bare_grant_bvt;
-- restore the genuine original global value captured at case start;
-- the stable oracle below must show the same value as at case start
set global enable_privilege_cache = @saved_enable_privilege_cache;
select @@global.enable_privilege_cache;
