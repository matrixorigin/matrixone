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

drop account if exists acc05;
create account acc05 admin_name = 'test_account' identified by '1111111'  comment 'test_comment';

drop account if exists acc06;
create account acc06 admin_name = 'test_account' identified by '11111111'  comment 'test_comment';

drop account if exists acc07;
create account acc07 admin_name = 'test_account' identified by '111111111'  comment 'test_comment';

-- @ignore:2,5,6,7
show accounts;


restore cluster from snapshot snapshot_01;

-- @ignore:2,5,6,7
show accounts;

drop account if exists acc01;
drop account if exists acc02;
drop account if exists acc03;
drop account if exists acc04;
drop account if exists acc05;
drop account if exists acc06;
drop account if exists acc07;

drop snapshot if exists snapshot_01;
