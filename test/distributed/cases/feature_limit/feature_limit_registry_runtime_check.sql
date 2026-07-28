-- Feature limit BVT: runtime registry enforcement.
-- This case verifies:
-- 1) disabling a feature in mo_feature_registry blocks runtime usage (quota treated as 0)
-- 2) scope_spec.allowed_scope is enforced at runtime (invalid scope rejected before auto-init limit row)
-- 3) re-enabling a feature restores previous limit behavior

drop account if exists fl_reg_acc;
create account fl_reg_acc admin_name = 'admin' identified by '111';

set @fl_reg_id = (select account_id from mo_catalog.mo_account where account_name = 'fl_reg_acc');

-- Ensure SNAPSHOT feature is registered and supports all snapshot scopes.
-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["account","database","table"]}',
  true
);

-- Set unlimited quota for table snapshots, then disable registry.
-- @ignore:0
select mo_feature_limit_upsert(@fl_reg_id, 'snapshot', 'table', -1);
-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["account","database","table"]}',
  false
);

-- @session:id=1&user=fl_reg_acc:admin&password=111
drop database if exists fl_reg_db;
create database fl_reg_db;
use fl_reg_db;
drop table if exists t;
create table t(a int primary key, b varchar(10));
insert into t values (1, 'a');

-- Registry disabled: table snapshot should fail even if quota is -1.
drop snapshot if exists spt_disabled_by_registry;
create snapshot spt_disabled_by_registry for table fl_reg_db t;

-- @session
-- Re-enable registry: table snapshot should succeed (quota = -1).
-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["account","database","table"]}',
  true
);

-- @session:id=1&user=fl_reg_acc:admin&password=111
drop snapshot if exists spt_ok_after_enable;
create snapshot spt_ok_after_enable for table fl_reg_db t;
drop snapshot spt_ok_after_enable;

-- @session
-- Restrict allowed scopes to database only.
-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["database"]}',
  true
);

-- @session:id=1&user=fl_reg_acc:admin&password=111
-- Scope rejected: table snapshot should fail due to registry scope_spec.
drop snapshot if exists spt_scope_rejected;
create snapshot spt_scope_rejected for table fl_reg_db t;

-- Allowed scope: database snapshot should succeed with default limit initialization.
drop snapshot if exists spd_ok_default;
create snapshot spd_ok_default for database fl_reg_db;
drop snapshot spd_ok_default;

drop table t;
drop database fl_reg_db;

-- @session
drop account fl_reg_acc;

