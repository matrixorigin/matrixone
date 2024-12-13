drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @ignore:2,5,6,7
show accounts;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db01;
create database db01;
use db01;
create table t01 (a int);
insert into t01 values (1);
-- @session

drop snapshot if exists snapshot_acc01_dropped;
create snapshot snapshot_acc01_dropped for account acc01;

drop account if exists acc01;

select * from db01.t01{snapshot='snapshot_acc01_dropped'};

restore account acc01 from snapshot snapshot_acc01_dropped;

-- @ignore:2,5,6,7
show accounts;

drop account if exists acc01;
drop snapshot if exists snapshot_acc01_dropped;


drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @ignore:2,5,6,7
show accounts;

drop snapshot if exists snapshot_acc01_dropped_created;
create snapshot snapshot_acc01_dropped_created for account acc01;

drop account if exists acc01;

create account acc01 admin_name = 'test_account' identified by '1111';

restore account acc01 from snapshot snapshot_acc01_dropped_created;

-- @ignore:2,5,6,7
show accounts;

drop account if exists acc01;
drop snapshot if exists snapshot_acc01_dropped_created;
