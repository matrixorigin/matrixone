drop pitr if exists pitr01;
create pitr pitr01 for account range 1 'h';
drop snapshot if exists sp01;
create snapshot sp01 for account;
drop database if exists test;
create database test;
use test;
create table t1(col int, col2 int);
insert into t1 values(1,1);
create table t2(col int, col2 int);
insert into t2 values(2,2);
create table t3(col int, col2 int);
insert into t3 values(3,3);

drop pitr if exists pitr02;
create pitr pitr02 for database test range 1 'h';
drop snapshot if exists sp02;
create snapshot sp02 for database test;

drop pitr if exists pitr03;
create pitr pitr03 for table test t1 range 1 'h';
drop snapshot if exists sp03;
create snapshot sp03 for table test t1;

drop pitr if exists pitr04;
create pitr pitr04 for table test t2 range 1 'h';
drop snapshot if exists sp04;
create snapshot sp04 for table test t2;

drop pitr if exists pitr05;
create pitr pitr05 for table test t3 range 1 'h';
drop snapshot if exists sp05;
create snapshot sp05 for table test t3;

-- @ignore:0,1,2,3,4
show recovery_window for account;
-- @ignore:0,1,2,3,4
show recovery_window for database test;
-- @ignore:0,1,2,3,4
show recovery_window for table test t1;
-- @ignore:0,1,2,3,4
show recovery_window for table test t2;
-- @ignore:0,1,2,3,4
show recovery_window for table test t3;

drop database test;
drop snapshot sp01;
drop snapshot sp02;
drop snapshot sp03;
drop snapshot sp04;
drop snapshot sp05;
drop pitr pitr01;
drop pitr pitr02;
drop pitr pitr03;
drop pitr pitr04;
drop pitr pitr05;


drop pitr if exists pitr01;
create pitr pitr01 for account range 1 'h';
drop snapshot if exists sp01;
create snapshot sp01 for account;
drop database if exists test;
create database test;
use test;
create table t1(col int, col2 int);
insert into t1 values(1,1);
create table t2(col int, col2 int);
insert into t2 values(2,2);
create table t3(col int, col2 int);
insert into t3 values(3,3);

drop pitr if exists pitr02;
create pitr pitr02 for database test range 1 'h';
drop snapshot if exists sp02;
create snapshot sp02 for database test;

drop pitr if exists pitr03;
create pitr pitr03 for table test t1 range 1 'h';
drop snapshot if exists sp03;
create snapshot sp03 for table test t1;

drop pitr if exists pitr04;
create pitr pitr04 for table test t2 range 1 'h';
drop snapshot if exists sp04;
create snapshot sp04 for table test t2;

drop pitr if exists pitr05;
create pitr pitr05 for table test t3 range 1 'h';
drop snapshot if exists sp05;
create snapshot sp05 for table test t3;

-- @ignore:0,1,2,3,4
show recovery_window for account;
-- @ignore:0,1,2,3,4
show recovery_window for database test;
-- @ignore:0,1,2,3,4
show recovery_window for table test t1;
-- @ignore:0,1,2,3,4
show recovery_window for table test t2;
-- @ignore:0,1,2,3,4
show recovery_window for table test t3;

drop database test;
create database test;
use test;
create table t1(col int, col2 int);
insert into t1 values(1,1);
create table t2(col int, col2 int);
insert into t2 values(2,2);
create table t3(col int, col2 int);
insert into t3 values(3,3);
drop pitr if exists pitr01;
create pitr pitr06 for account range 1 'h';
drop snapshot if exists sp01;
create snapshot sp06 for account;
drop pitr if exists pitr07;
create pitr pitr07 for database test range 1 'h';
drop snapshot if exists sp07;
create snapshot sp07 for database test;
drop pitr if exists pitr08;
create pitr pitr08 for table test t1 range 1 'h';
drop snapshot if exists sp08;
create snapshot sp08 for table test t1;
drop pitr if exists pitr09;
create pitr pitr09 for table test t2 range 1 'h';
drop snapshot if exists sp09;
create snapshot sp09 for table test t2;
drop pitr if exists pitr10;
create pitr pitr10 for table test t3 range 1 'h';
drop snapshot if exists sp10;
create snapshot sp10 for table test t3;

-- @ignore:0,1,2,3,4
show recovery_window for account;
-- @ignore:0,1,2,3,4
show recovery_window for database test;
-- @ignore:0,1,2,3,4
show recovery_window for table test t1;
-- @ignore:0,1,2,3,4
show recovery_window for table test t2;
-- @ignore:0,1,2,3,4
show recovery_window for table test t3;

drop database test;
drop snapshot sp01;
drop snapshot sp02;
drop snapshot sp03;
drop snapshot sp04;
drop snapshot sp05;
drop snapshot sp06;
drop snapshot sp07;
drop snapshot sp08;
drop snapshot sp09;
drop snapshot sp10;
drop pitr pitr01;
drop pitr pitr02;
drop pitr pitr03;
drop pitr pitr04;
drop pitr pitr05;
drop pitr pitr06;
drop pitr pitr07;
drop pitr pitr08;
drop pitr pitr09;
drop pitr pitr10;

show recovery_window for account acc01;
show recovery_window for database mo_catalog;
show recovery_window for table mo_catalog mo_pitr;

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc01:test_account&password=111
show recovery_window for account sys;
drop pitr if exists pitr01;
create pitr pitr01 for account range 1 'h';
drop snapshot if exists sp01;
create snapshot sp01 for account;
drop database if exists test;
create database test;
use test;
create table t1(col int, col2 int);
insert into t1 values(1,1);
create table t2(col int, col2 int);
insert into t2 values(2,2);
create table t3(col int, col2 int);
insert into t3 values(3,3);

drop pitr if exists pitr02;
create pitr pitr02 for database test range 1 'h';
drop snapshot if exists sp02;
create snapshot sp02 for database test;

drop pitr if exists pitr03;
create pitr pitr03 for table test t1 range 1 'h';
drop snapshot if exists sp03;
create snapshot sp03 for table test t1;

drop pitr if exists pitr04;
create pitr pitr04 for table test t2 range 1 'h';
drop snapshot if exists sp04;
create snapshot sp04 for table test t2;

drop pitr if exists pitr05;
create pitr pitr05 for table test t3 range 1 'h';
drop snapshot if exists sp05;
create snapshot sp05 for table test t3;

-- @ignore:0,1,2,3,4
show recovery_window for account;
-- @ignore:0,1,2,3,4
show recovery_window for database test;
-- @ignore:0,1,2,3,4
show recovery_window for table test t1;
-- @ignore:0,1,2,3,4
show recovery_window for table test t2;
-- @ignore:0,1,2,3,4
show recovery_window for table test t3;

drop database test;
drop snapshot sp01;
drop snapshot sp02;
drop snapshot sp03;
drop snapshot sp04;
drop snapshot sp05;
drop pitr pitr01;
drop pitr pitr02;
drop pitr pitr03;
drop pitr pitr04;
drop pitr pitr05;
-- @session

drop account if exists acc01;
