drop database if exists db1;
drop database if exists db2;
drop database if exists db3;
drop account if exists acc1;

create account acc1 admin_name "root1" identified by "111";

-- @session:id=1&user=acc1:root1&password=111
drop database if exists test1;
create database test1;
create database test2;
create table test1.t1 (a int);
create table test2.t1 (a int);

drop snapshot if exists acc_sp0;
drop snapshot if exists acc_sp1;
drop snapshot if exists acc_sp2;
drop pitr if exists acc_pitr0;
drop pitr if exists acc_pitr1;

create snapshot acc_sp0 for account acc1;
create snapshot acc_sp1 for database test1;
create snapshot acc_sp2 for table test1 t1;
create pitr acc_pitr0 for database test1 range 2 'h';
create pitr acc_pitr1 for table test1 t1 range 3 'h';

-- @ignore:4
show recovery_window for account acc1;
-- @ignore:4
show recovery_window for database test1;
-- @ignore:4
show recovery_window for table test2 t1;

drop table test1.t1;
-- @ignore:4
show recovery_window for table test1 t1;

drop database test1;
-- @ignore:4
show recovery_window for database test1;
-- @ignore:4
show recovery_window for account acc1;

drop snapshot acc_sp0;
drop snapshot acc_sp1;
drop pitr acc_pitr0;

-- @ignore:4
show recovery_window for table test1 t1;

drop snapshot acc_sp2;
drop pitr acc_pitr1;

show recovery_window for table test1 t1;
-- @session

create database db1;
create table db1.t1 (a int);

create database db2;
create table db2.t1 (a int);

drop snapshot if exists sys_sp0;
drop snapshot if exists sys_sp1;
drop snapshot if exists sys_sp2;
drop snapshot if exists sys_sp3;
drop snapshot if exists sys_sp4;
drop snapshot if exists sys_sp5;

drop pitr if exists sys_pitr0;
drop pitr if exists sys_pitr1;
drop pitr if exists sys_pitr2;
drop pitr if exists sys_pitr3;
drop pitr if exists sys_pitr4;
drop pitr if exists sys_pitr5;

create snapshot sys_sp0 for account acc1;
create snapshot sys_sp1 for account sys;
create snapshot sys_sp2 for database db1;
create snapshot sys_sp3 for database db2;
create snapshot sys_sp4 for table db1 t1;
create snapshot sys_sp5 for table db2 t1;
create pitr sys_pitr0 for account acc1 range 1 'h';
create pitr sys_pitr1 for account sys  range 1 'h';
create pitr sys_pitr2 for database db1 range 1 'h';
create pitr sys_pitr3 for database db2 range 1 'h';
create pitr sys_pitr4 for table db1 t1 range 1 'h';
create pitr sys_pitr5 for table db2 t1 range 1 'h';

-- @ignore:4
show recovery_window for account sys;
-- @ignore:4
show recovery_window for account acc1;
-- @ignore:4
show recovery_window for table db1 t1;
-- @ignore:4
show recovery_window for table db2 t1;

drop account acc1;
-- @ignore:4
show recovery_window for account acc1;

drop database db1;
-- @ignore:4
show recovery_window for table db1 t1;

drop database db2;
-- @ignore:4
show recovery_window for table db2 t1;

drop snapshot sys_sp0;
drop snapshot sys_sp1;
drop snapshot sys_sp2;
drop snapshot sys_sp3;
drop snapshot sys_sp4;
drop pitr sys_pitr0;
drop pitr sys_pitr1;
drop pitr sys_pitr2;
drop pitr sys_pitr3;
drop pitr sys_pitr4;

-- @ignore:4
show recovery_window for table db1 t1;
-- @ignore:4
show recovery_window for table db2 t1;

drop snapshot sys_sp5;
drop pitr sys_pitr5;

-- @ignore:4
show recovery_window for table db2 t1;

create database db3;
use db3;

create table t1(a int);
create snapshot sp0 for table db3 t1;
create pitr pitr0 for table db3 t1 range 1 'h';

drop table t1;
create table t1(a int);
create snapshot sp1 for table db3 t1;
create pitr pitr1 for table db3 t1 range 1 'h';

drop table t1;
create table t1(a int);
create snapshot sp2 for table db3 t1;
create pitr pitr2 for table db3 t1 range 1 'h';

-- @ignore:4
show recovery_window for table db3 t1;

drop database db3;
drop pitr pitr0;
drop pitr pitr1;
drop pitr pitr2;
drop snapshot sp0;
drop snapshot sp1;
drop snapshot sp2;