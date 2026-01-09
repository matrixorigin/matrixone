-- Feature limit BVT: data branch quota.
-- This case verifies:
-- 1) branch quota is enforced for DATA BRANCH CREATE TABLE
-- 2) DATA BRANCH DELETE TABLE reduces the active branch count (table_deleted = true)
-- 3) quota can be disabled (0) and set to unlimited (-1)
-- 4) DATA BRANCH CREATE DATABASE consumes quota by number of cloned tables

drop account if exists fl_branch_acc;
create account fl_branch_acc admin_name = 'admin' identified by '111';

-- @ignore:0
select mo_feature_registry_upsert(
  'branch',
  'Branch feature',
  '{"allowed_scope":[]}',
  true
);

-- Keep snapshot registry aligned for the branch test input snapshots.
-- @ignore:0
select mo_feature_registry_upsert(
  'snapshot',
  'Snapshot feature',
  '{"allowed_scope":["account","database","table"]}',
  true
);

set @fl_branch_id = (select account_id from mo_catalog.mo_account where account_name = 'fl_branch_acc');

-- Allow enough snapshots to create inputs for data branch operations.
-- @ignore:0
select mo_feature_limit_upsert(@fl_branch_id, 'snapshot', 'table', -1);

-- Branch quota: only 1 active branch table.
-- @ignore:0
select mo_feature_limit_upsert(@fl_branch_id, 'branch', '', 1);

-- @session:id=1&user=fl_branch_acc:admin&password=111
drop database if exists fl_branch_db;
create database fl_branch_db;
use fl_branch_db;
drop table if exists base;
create table base(a int primary key, b varchar(10));
insert into base values (1, 'a'), (2, 'b');

drop snapshot if exists sp_base;
create snapshot sp_base for table fl_branch_db base;

-- Create 1 branch table, the second should fail with quota = 1.
drop table if exists b1;
drop table if exists b2;
data branch create table b1 from base{snapshot='sp_base'};
data branch create table b2 from base{snapshot='sp_base'};

-- Delete b1 so active branch count goes back to 0, then b2 should succeed.
data branch delete table fl_branch_db.b1;
data branch create table b2 from base{snapshot='sp_base'};

-- Disable branch feature, creation should fail.
-- @session
-- @ignore:0
select mo_feature_limit_upsert(@fl_branch_id, 'branch', '', 0);
-- @session:id=1&user=fl_branch_acc:admin&password=111
drop table if exists b3;
data branch create table b3 from base{snapshot='sp_base'};

-- Unlimited branch feature, multiple creates should succeed.
-- @session
-- @ignore:0
select mo_feature_limit_upsert(@fl_branch_id, 'branch', '', -1);
-- @session:id=1&user=fl_branch_acc:admin&password=111
drop table if exists b3;
drop table if exists b4;
data branch create table b3 from base{snapshot='sp_base'};
data branch create table b4 from base{snapshot='sp_base'};

drop snapshot sp_base;
drop table b2;
drop table b3;
drop table b4;
drop table base;
drop database fl_branch_db;

-- Branch CREATE DATABASE consumes quota by number of cloned tables.
drop database if exists src_db;
create database src_db;
use src_db;
drop table if exists t1;
drop table if exists t2;
create table t1(a int primary key);
create table t2(a int primary key);
insert into t1 values (1);
insert into t2 values (2);

-- Set branch quota to 1, but cloning src_db requires 2.
-- @session
-- @ignore:0
select mo_feature_limit_upsert(@fl_branch_id, 'branch', '', 1);
-- @session:id=1&user=fl_branch_acc:admin&password=111
drop database if exists dst_db;
data branch create database dst_db from src_db;

-- Increase quota and retry.
-- @session
-- @ignore:0
select mo_feature_limit_upsert(@fl_branch_id, 'branch', '', 10);
-- @session:id=1&user=fl_branch_acc:admin&password=111
data branch create database dst_db from src_db;
drop database dst_db;
drop database src_db;

-- @session
drop account fl_branch_acc;
