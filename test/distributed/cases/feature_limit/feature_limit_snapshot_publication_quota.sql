-- A publication snapshot is stored by the publisher and charged to its quota.
drop snapshot if exists fl_snap_pub_sys_second;
drop account if exists fl_snap_pub_sub;
drop account if exists fl_snap_pub_other;
drop account if exists fl_snap_pub_owner;
create account fl_snap_pub_owner admin_name = 'admin' identified by '111';
create account fl_snap_pub_sub admin_name = 'admin' identified by '111';
create account fl_snap_pub_other admin_name = 'admin' identified by '111';
set @fl_snap_pub_id = (select account_id from mo_catalog.mo_account where account_name = 'fl_snap_pub_owner');

-- @session:id=1&user=fl_snap_pub_owner:admin&password=111
create database fl_snap_pub_db;
create publication fl_snap_pub database fl_snap_pub_db account fl_snap_pub_sub;

-- @session
-- @ignore:0
select mo_feature_limit_upsert(@fl_snap_pub_id, 'snapshot', 'account', 0);

-- @session:id=1&user=fl_snap_pub_owner:admin&password=111
alter publication fl_snap_pub account fl_snap_pub_other;

-- @session:id=2&user=fl_snap_pub_sub:admin&password=111
create snapshot fl_snap_pub_revoked for account from fl_snap_pub_owner publication fl_snap_pub;

-- @session:id=1&user=fl_snap_pub_owner:admin&password=111
select count(*) from mo_catalog.mo_snapshots where sname = 'fl_snap_pub_revoked';
alter publication fl_snap_pub account fl_snap_pub_sub;

-- @session:id=2&user=fl_snap_pub_sub:admin&password=111
create snapshot fl_snap_pub_disabled for account from fl_snap_pub_owner publication fl_snap_pub;

-- @session:id=1&user=fl_snap_pub_owner:admin&password=111
select count(*) from mo_catalog.mo_snapshots where sname = 'fl_snap_pub_disabled';

-- @session
-- @ignore:0
select mo_feature_limit_upsert(@fl_snap_pub_id, 'snapshot', 'account', 1);

-- @session:id=2&user=fl_snap_pub_sub:admin&password=111
create snapshot fl_snap_pub_first for account from fl_snap_pub_owner publication fl_snap_pub;

-- @session:id=1&user=fl_snap_pub_owner:admin&password=111
select obj_id = current_account_id() from mo_catalog.mo_snapshots where sname = 'fl_snap_pub_first';

-- @session
create snapshot fl_snap_pub_sys_second for account fl_snap_pub_owner;
select count(*) from mo_catalog.mo_snapshots where sname = 'fl_snap_pub_sys_second';

-- @session:id=1&user=fl_snap_pub_owner:admin&password=111
drop snapshot fl_snap_pub_first;
drop publication fl_snap_pub;
drop database fl_snap_pub_db;

-- @session
drop account fl_snap_pub_sub;
drop account fl_snap_pub_other;
drop account fl_snap_pub_owner;
