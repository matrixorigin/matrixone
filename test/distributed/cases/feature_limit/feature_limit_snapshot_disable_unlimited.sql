-- Feature limit BVT: snapshot disable/unlimited and dynamic update.
-- This case verifies:
-- 1) quota = 0 disables snapshot creation for a scope
-- 2) quota = -1 allows unlimited snapshots for a scope
-- 3) updating quota takes effect immediately

drop account if exists fl_snap_acc2;
create account fl_snap_acc2 admin_name = 'admin' identified by '111';

-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["account","database","table"]}',
  true
);

set @fl_snap2_id = (select account_id from mo_catalog.mo_account where account_name = 'fl_snap_acc2');

-- Disable table snapshots, allow unlimited database snapshots.
-- @ignore:0
select mo_feature_limit_upsert(@fl_snap2_id, 'snapshot', 'table', 0);
-- @ignore:0
select mo_feature_limit_upsert(@fl_snap2_id, 'snapshot', 'database', -1);

-- @session:id=1&user=fl_snap_acc2:admin&password=111
drop database if exists fl_snap_db2;
create database fl_snap_db2;
use fl_snap_db2;
drop table if exists t;
create table t(a int primary key, b varchar(10));
insert into t values (1, 'a');

-- Table snapshot should fail (disabled).
drop snapshot if exists spt_disabled;
create snapshot spt_disabled for table fl_snap_db2 t;

-- Database snapshots should succeed (unlimited).
drop snapshot if exists spd_u1;
drop snapshot if exists spd_u2;
create snapshot spd_u1 for database fl_snap_db2;
create snapshot spd_u2 for database fl_snap_db2;
drop snapshot spd_u1;
drop snapshot spd_u2;

-- @session
-- Enable table snapshots with quota = 2.
-- @ignore:0
select mo_feature_limit_upsert(@fl_snap2_id, 'snapshot', 'table', 2);

-- @session:id=1&user=fl_snap_acc2:admin&password=111
drop snapshot if exists spt1;
drop snapshot if exists spt2;
drop snapshot if exists spt3;
create snapshot spt1 for table fl_snap_db2 t;
create snapshot spt2 for table fl_snap_db2 t;
create snapshot spt3 for table fl_snap_db2 t;
drop snapshot spt1;
drop snapshot spt2;

drop table t;
drop database fl_snap_db2;

-- @session
drop account fl_snap_acc2;
