-- A foreground TRUNCATE must wait for a competing lifecycle owner, even when
-- its ordinary table has no snapshot, PITR, or branch history.
drop database if exists truncate_lifecycle_wait_a;
drop database if exists truncate_lifecycle_wait_b;
create database truncate_lifecycle_wait_a;
create database truncate_lifecycle_wait_b;
create table truncate_lifecycle_wait_a.t (id int primary key);
create table truncate_lifecycle_wait_b.t (id int primary key);
insert into truncate_lifecycle_wait_a.t values (1);
insert into truncate_lifecycle_wait_b.t values (2);

begin;
select feature_code from mo_catalog.mo_feature_registry where feature_code = 'SNAPSHOT' for update;
-- @session:id=1{
-- @wait:0:rollback
truncate table truncate_lifecycle_wait_a.t;
select count(*) as remaining from truncate_lifecycle_wait_a.t;
-- @session}
select count(*) as remaining from truncate_lifecycle_wait_b.t;
truncate table truncate_lifecycle_wait_b.t;
select count(*) as remaining from truncate_lifecycle_wait_b.t;
drop database truncate_lifecycle_wait_a;
drop database truncate_lifecycle_wait_b;
