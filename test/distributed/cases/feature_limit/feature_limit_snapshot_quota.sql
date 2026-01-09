-- Feature limit BVT: snapshot quota by scope (table/database/account).
-- This case verifies:
-- 1) quota is enforced per snapshot level (table/database/account)
-- 2) dropping snapshots frees quota (count is based on existing snapshots)
-- 3) limits are configured by sys through mo_feature_registry_upsert/mo_feature_limit_upsert

drop account if exists fl_snap_acc;
create account fl_snap_acc admin_name = 'admin' identified by '111';

-- Ensure registry exists and allows snapshot scopes used by CREATE SNAPSHOT.
-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["account","database","table"]}',
  true
);

set @fl_snap_id = (select account_id from mo_catalog.mo_account where account_name = 'fl_snap_acc');

-- Configure quotas for this tenant.
-- @ignore:0
select mo_feature_limit_upsert(@fl_snap_id, 'snapshot', 'table', 1);
-- @ignore:0
select mo_feature_limit_upsert(@fl_snap_id, 'snapshot', 'database', 1);
-- @ignore:0
select mo_feature_limit_upsert(@fl_snap_id, 'snapshot', 'account', 2);

-- @session:id=1&user=fl_snap_acc:admin&password=111
drop database if exists fl_snap_db;
create database fl_snap_db;
use fl_snap_db;
drop table if exists t;
create table t(a int primary key, b varchar(10));
insert into t values (1, 'a');

-- Table level quota: 1
drop snapshot if exists spt1;
drop snapshot if exists spt2;
drop snapshot if exists spt3;
create snapshot spt1 for table fl_snap_db t;
create snapshot spt2 for table fl_snap_db t;
drop snapshot spt1;
create snapshot spt3 for table fl_snap_db t;
drop snapshot spt3;

-- Database level quota: 1
drop snapshot if exists spd1;
drop snapshot if exists spd2;
drop snapshot if exists spd3;
create snapshot spd1 for database fl_snap_db;
create snapshot spd2 for database fl_snap_db;
drop snapshot spd1;
create snapshot spd3 for database fl_snap_db;
drop snapshot spd3;

-- Account level quota: 2
drop snapshot if exists spa1;
drop snapshot if exists spa2;
drop snapshot if exists spa3;
drop snapshot if exists spa4;
create snapshot spa1 for account;
create snapshot spa2 for account;
create snapshot spa3 for account;
drop snapshot spa1;
create snapshot spa4 for account;
drop snapshot spa2;
drop snapshot spa4;

drop table t;
drop database fl_snap_db;

-- @session
drop account fl_snap_acc;
