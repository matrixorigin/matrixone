drop snapshot if exists sn1;
create snapshot sn1 for database db1;
-- @ignore:1
show snapshots;

drop database if exists db1;
create database if not exists db1;
create snapshot sn2 for database db1;
-- @ignore:1
show snapshots;

drop database if exists db1;
drop snapshot if exists sn2;
drop snapshot if exists sn1;
-- @ignore:1
show snapshots;

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
drop snapshot if exists sn1;
create snapshot sn1 for database db1;
-- @ignore:1
show snapshots;

drop database if exists db1;
create database if not exists db1;
create snapshot sn2 for database db1;
-- @ignore:1
show snapshots;

drop database if exists db1;
drop snapshot if exists sn2;
drop snapshot if exists sn1;
-- @session

drop account if exists acc1;

drop snapshot if exists sn1;
create snapshot sn1 for table db1 tbl1;
-- @ignore:1
show snapshots;

drop database if exists db1;
create database if not exists db1;
create table db1.tbl1 (a int);
insert into db1.tbl1 values (1), (2), (3);
create snapshot sn2 for table db1 tbl1;
-- @ignore:1
show snapshots;

drop database if exists db1;
drop snapshot if exists sn2;
drop snapshot if exists sn1;
-- @ignore:1
show snapshots;

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=2&user=acc01:test_account&password=111
drop snapshot if exists sn1;
create snapshot sn1 for table db1 tbl1;
-- @ignore:1
show snapshots;

drop database if exists db1;
create database if not exists db1;
create table db1.tbl1 (a int);
insert into db1.tbl1 values (1), (2), (3);
create snapshot sn2 for table db1 tbl1;
-- @ignore:1
show snapshots;

drop database if exists db1;
drop snapshot if exists sn2;
drop snapshot if exists sn1;
-- @session

drop account if exists acc1;
