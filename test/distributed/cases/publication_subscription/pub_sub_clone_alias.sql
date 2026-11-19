-- Issue #31194: CREATE TABLE ... CLONE of a subscribed table resolves the
-- subscription alias to the publisher's physical database while planning.
-- The planner must not leave the physical name in the statement, otherwise a
-- re-plan of the same statement (RC "def changed" retry) resolves the
-- publisher database directly and fails for the subscriber. The re-plan
-- itself is covered by TestBuildCloneTableSubscriptionReplanKeepsAlias; this
-- file guards the subscriber-visible CLONE paths around it.

drop account if exists clone_alias_pub;
drop account if exists clone_alias_sub;
create account clone_alias_pub admin_name = 'test_account' identified by '111';
create account clone_alias_sub admin_name = 'test_account' identified by '111';

-- @session:id=1&user=clone_alias_pub:test_account&password=111
drop database if exists ca_pubdb;
create database ca_pubdb;
use ca_pubdb;
create table src(a int primary key, b varchar(10));
insert into src values (1,'a'),(2,'b'),(3,'c');
create publication ca_pub database ca_pubdb account clone_alias_sub;
-- @session

-- @session:id=2&user=clone_alias_sub:test_account&password=111
drop database if exists ca_sub;
drop database if exists ca_local;
create database ca_sub from clone_alias_pub publication ca_pub;
create database ca_local;
create table ca_local.c1 clone ca_sub.src;
select * from ca_local.c1 order by a;
drop table ca_local.c1;
create table ca_local.c1 clone ca_sub.src;
select count(*) from ca_local.c1;
use ca_sub;
create table ca_local.c2 clone src;
select count(*) from ca_local.c2;
use ca_local;
prepare s1 from 'create table ca_local.c3 clone ca_sub.src';
execute s1;
select count(*) from ca_local.c3;
drop table ca_local.c3;
execute s1;
select count(*) from ca_local.c3;
deallocate prepare s1;
data branch create table ca_local.c4 from ca_sub.src;
select count(*) from ca_local.c4;
-- the publisher's physical database is not visible to the subscriber
create table ca_local.c5 clone ca_pubdb.src;
drop database ca_local;
drop database ca_sub;
-- @session

-- @session:id=1&user=clone_alias_pub:test_account&password=111
drop publication ca_pub;
drop database ca_pubdb;
-- @session

drop account clone_alias_pub;
drop account clone_alias_sub;
