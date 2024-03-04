-- create snapshot success
create snapshot snapshot_01 for cluster;
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
create snapshot snapshot_02 for account default_1;
select sleep(1);
create snapshot snapshot_03 for account default_1;
select sleep(1);
create snapshot snapshot_04 for account default_1;
show snapshots;
show snapshots where sname = 'snapshot_01';

-- @session:id=1&user=default_1:admin&password=111111
create snapshot snapshot_05 for account default_1;
select sleep(1);
create snapshot snapshot_06 for account default_1;
select sleep(1);
create snapshot snapshot_07 for account default_1;
show snapshots;
-- @session
drop account default_1;