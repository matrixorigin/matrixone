drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '1111' comment 'test_comment';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '11111';
drop account if exists acc04;
create account acc04 admin_name = 'test_account' identified by '111111'  comment 'test_comment';

drop snapshot if exists snapshot_01;
create snapshot snapshot_01 for cluster;

drop account if exists acc01;
drop account if exists acc02;
drop account if exists acc03;
drop account if exists acc04;

restore cluster from snapshot snapshot_01;

-- @ignore:2,6,7
show accounts;

drop account if exists acc01;
drop account if exists acc02;
drop account if exists acc03;
drop account if exists acc04;

drop snapshot if exists snapshot_01;
