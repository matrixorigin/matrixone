drop snapshot if exists issue_27055_bvt_snapshot;
drop database if exists issue_27055_bvt_rollback_target;
drop database if exists issue_27055_bvt_snapshot_target;
drop database if exists issue_27055_bvt_autocommit_target;
drop database if exists issue_27055_bvt_direct_target;
drop database if exists issue_27055_bvt_new_target;
drop database if exists issue_27055_bvt_new_source;
drop database if exists issue_27055_bvt_target;
drop database if exists issue_27055_bvt_source;

create database issue_27055_bvt_source;
create table issue_27055_bvt_source.t(id int primary key, v int);
insert into issue_27055_bvt_source.t values (1, 1), (2, 2), (3, 3);

-- The issue reproduction: an INSERT on an existing source must be cloned.
begin;
insert into issue_27055_bvt_source.t values (4, 4);
create database issue_27055_bvt_target clone issue_27055_bvt_source;
select count(*), sum(id), sum(v) from issue_27055_bvt_source.t;
select count(*), sum(id), sum(v) from issue_27055_bvt_target.t;
commit;
select count(*), sum(id), sum(v) from issue_27055_bvt_target.t;

-- A clone immediately after BEGIN must still use the shared transaction.
begin;
create database issue_27055_bvt_direct_target clone issue_27055_bvt_source;
select count(*), sum(id), sum(v) from issue_27055_bvt_direct_target.t;
commit;

-- Source database and table metadata created in the transaction must be enumerable.
begin;
create database issue_27055_bvt_new_source;
create table issue_27055_bvt_new_source.t(id int primary key, v int);
insert into issue_27055_bvt_new_source.t values (7, 70);
create database issue_27055_bvt_new_target clone issue_27055_bvt_new_source;
show tables from issue_27055_bvt_new_target;
select count(*), sum(id), sum(v) from issue_27055_bvt_new_target.t;
commit;
select count(*), sum(id), sum(v) from issue_27055_bvt_new_target.t;

-- Clone and all transaction-local changes must disappear together on rollback.
begin;
insert into issue_27055_bvt_source.t values (5, 5);
create database issue_27055_bvt_rollback_target clone issue_27055_bvt_source;
select count(*), sum(id), sum(v) from issue_27055_bvt_rollback_target.t;
rollback;
select count(*), sum(id), sum(v) from issue_27055_bvt_source.t;
select count(*) from mo_catalog.mo_database
where account_id = 0 and datname = 'issue_27055_bvt_rollback_target';
select 1;

-- Existing autocommit and named-snapshot behavior remains unchanged.
create database issue_27055_bvt_autocommit_target clone issue_27055_bvt_source;
select count(*), sum(id), sum(v) from issue_27055_bvt_autocommit_target.t;
create snapshot issue_27055_bvt_snapshot for database issue_27055_bvt_source;
insert into issue_27055_bvt_source.t values (6, 6);
create database issue_27055_bvt_snapshot_target clone issue_27055_bvt_source {snapshot = "issue_27055_bvt_snapshot"};
select count(*), sum(id), sum(v) from issue_27055_bvt_snapshot_target.t;

drop snapshot issue_27055_bvt_snapshot;
drop database issue_27055_bvt_snapshot_target;
drop database issue_27055_bvt_autocommit_target;
drop database issue_27055_bvt_new_target;
drop database issue_27055_bvt_new_source;
drop database issue_27055_bvt_direct_target;
drop database issue_27055_bvt_target;
drop database issue_27055_bvt_source;
