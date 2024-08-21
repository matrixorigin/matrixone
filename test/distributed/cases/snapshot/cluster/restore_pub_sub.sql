drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

drop database if exists test02;
create database test02;
use test02;
drop table if exists rs02;
create table rs02 (col1 int primary key , col2 datetime);
insert into rs02 values (1, '2020-10-13 10:10:10');
insert into rs02 values (2, null);
insert into rs02 values (3, '2021-10-10 00:00:00');
insert into rs02 values (4, '2023-01-01 12:12:12');
insert into rs02 values (5, null);
insert into rs02 values (6, null);
insert into rs02 values (7, '2023-11-27 01:02:03');
select * from rs02;
drop table if exists rs03;
create table rs03 (col1 int, col2 float, col3 decimal, col4 enum('1','2','3','4'));
insert into rs03 values (1, 12.21, 32324.32131, 1);
insert into rs03 values (2, null, null, 2);
insert into rs03 values (2, -12.1, 34738, null);
insert into rs03 values (1, 90.2314, null, 4);
insert into rs03 values (1, 43425.4325, -7483.432, 2);

drop publication if exists pub02;
create publication pub02 database test02 table rs02 account acc01;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub02;
create database sub02 from sys publication pub02;
-- @ignore:5,7
show subscriptions;

drop snapshot if exists sp02;
create snapshot sp02 for account acc01;
-- @ignore:1
show snapshots;
drop database sub02;
-- @session

drop publication pub02;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
restore account acc01 from snapshot sp02;
-- @ignore:5,7
show subscriptions;
show databases;
-- @session

drop account acc01;
drop database if exists test02;
