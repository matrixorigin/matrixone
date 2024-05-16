-- create snapshot success
create snapshot snapshot_01 for cluster;
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
create snapshot snapshot_02 for account default_1;
select sleep(1);
create snapshot snapshot_03 for account default_1;
select sleep(1);
create snapshot snapshot_04 for account default_1;
-- @ignore:1
show snapshots;
-- @ignore:1
show snapshots where SNAPSHOT_NAME = 'snapshot_01';
-- @ignore:1
show snapshots where SNAPSHOT_LEVEL = 'cluster';
-- @ignore:1
show snapshots where ACCOUNT_NAME = 'default_1';
DROP SNAPSHOT snapshot_01;
DROP SNAPSHOT snapshot_02;
DROP SNAPSHOT snapshot_03;
DROP SNAPSHOT snapshot_04;

-- @session:id=1&user=default_1:admin&password=111111
create snapshot snapshot_05 for account default_1;
select sleep(1);
create snapshot snapshot_06 for account default_1;
select sleep(1);
create snapshot snapshot_07 for account default_1;
-- @ignore:1
show snapshots;
-- @ignore:1
show snapshots where SNAPSHOT_NAME = 'snapshot_07';
-- @ignore:1
show snapshots where SNAPSHOT_LEVEL = 'account';
-- @ignore:1
show snapshots where ACCOUNT_NAME = 'default_1';
-- @session
drop account default_1;

-- create snapshot failed
create snapshot snapshot_08 for account default_1;
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
create snapshot snapshot_09 for account default_1;
select sleep(1);
create snapshot snapshot_09 for account default_1;
create account default_2 ADMIN_NAME admin IDENTIFIED BY '111111';
create snapshot snapshot_10 for account default_2;
-- @ignore:1
show snapshots;

-- @session:id=2&user=default_1:admin&password=111111
create snapshot snapshot_11 for account default_1;
create snapshot snapshot_12 for account default_2;
create snapshot snapshot_13 for cluster;
-- @ignore:1
show snapshots;
create user  efg identified by '111';
-- @session

-- @session:id=4&user=default_1:efg&password=111
create snapshot snapshot_14 for account default_1;
create snapshot snapshot_15 for account default_2;
-- @session
drop snapshot if exists snapshot_09;
drop snapshot if exists snapshot_10;
drop account default_1;
drop account default_2;
