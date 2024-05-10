drop database if exists database01;
drop database if exists test02;
drop database if exists test03;
drop database if exists procedure_test;
-- create tenant
drop account if exists test_tenant_1;
create account test_tenant_1 admin_name 'test_account' identified by '111';

-- test sys tenant database publication to test_tenant_1 tenant
show databases;
create database database01;
use database01;
create table table01(col1 int, col2 decimal);
insert into table01 values (1, 2);
insert into table01 values (234, 2413242);
select * from table01;

-- publish to tenant test_tenant_1
create publication publication01 database database01 account test_tenant_1 comment 'publish database to account01';
-- @ignore:2,3
show publications;

-- subscribe database01
-- @session:id=2&user=test_tenant_1:test_account&password=111
create database sub_database01 from sys publication publication01;
show databases;
use sub_database01;
show tables;
select * from table01;
truncate table table01;
delete from table01 where col1 = 1;
update table01 set col1 = 100 where col2 = 2413242;
-- @ignore:10,11,12
show table status;
drop table table01;
-- @ignore:3,5
show subscriptions;
-- @session

drop publication publication01;
drop database database01;

-- sys、acc1、acc2
drop database if exists database01;
create database database01;
use database01;
create table t1(a int, b int);
insert into t1 values (1, 1), (2, 2), (3, 3);
create publication publication01 database database01;
-- @ignore:2,3
show publications;

drop account if exists test_tenant_1;
drop account if exists test_tenant_2;
create account test_tenant_1 admin_name 'test_account' identified by '111';
create account test_tenant_2 admin_name 'test_account' identified by '111';

-- @session:id=3&user=test_tenant_1:test_account&password=111
drop database if exists sub_database01;
create database sub_database01 from sys publication publication01;
-- @ignore:3,5
show subscriptions;
use sub_database01;
show tables;
select * from t1;
-- @session

drop table if exists t2;
create table t2(col1 int primary key );
insert into t2 values (1),(2),(3);

-- @session:id=4&user=test_tenant_1:test_account&password=111
use sub_database01;
show tables;
select * from t2;
-- @session

-- @session:id=5&user=test_tenant_2:test_account&password=111
drop database if exists sub_database01;
create database sub_database01 from sys publication publication01;
-- @ignore:3,5
show subscriptions;
use sub_database01;
show tables;
select * from t1;
select * from t2;
-- @session
drop publication publication01;

drop database if exists database02;
create database database02;
use database02;
create table table03(col1 char, col2 varchar(100));
insert into table03 values ('1', 'database');
insert into table03 values ('a', 'data warehouse');
create publication publication02 database database02;
-- @ignore:2,3
show publications;

-- @session:id=4&user=test_tenant_1:test_account&password=111
drop database if exists sub_database02;
create database sub_database02 from sys publication publication02;
-- @ignore:3,5
show subscriptions all;
use sub_database02;
show tables;
select * from table03;
-- @session

-- @session:id=7&user=test_tenant_2:test_account&password=111
drop database if exists sub_database02;
create database sub_database02 from sys publication publication02;
-- @ignore:3,5
show subscriptions all;
use sub_database02;
show tables;
select * from table03;
-- @ignore:10,11,12
show table status;
-- @session
drop publication publication02;

drop database if exists database03;
create database database03;
use database03;
drop table if exists table01;
create table table01(col1 int);
insert into table01 values (-1),(1),(2);
create publication publication03 database database03 account test_tenant_1;
-- @ignore:2,3
show publications;

-- @session:id=8&user=test_tenant_1:test_account&password=111
drop database if exists sub_database03;
create database sub_database03 from sys publication publication03;
-- @ignore:3,5
show subscriptions all;
use sub_database03;
show tables;
select * from table01;
desc table01;
-- @ignore:10,11,12
show table status;
-- @session

-- @session:id=9&user=test_tenant_2:test_account&password=111
-- @ignore:3,5
show subscriptions all;
-- @session

-- sys modify sub to all tanant
alter publication publication03 account all;
-- @ignore:2,3
show publications;

-- @session:id=10&user=test_tenant_2:test_account&password=111
-- @ignore:3,4,5
show subscriptions all;
create database sub_database03 from sys publication publication03;
use sub_database03;
show tables;
show columns from table01;
desc table01;
select * from table01;
-- @session

drop publication publication03;
-- @ignore:2,3
show publications;

-- @session:id=11&user=test_tenant_1:test_account&password=111
use sub_database01;
drop database sub_database01;
drop database sub_database02;
drop database sub_database03;
-- @session

-- @session:id=12&user=test_tenant_2:test_account&password=111
drop database sub_database01;
drop database sub_database02;
-- @session

drop database database01;
drop database database02;
drop database database03;
drop account test_tenant_1;
drop account test_tenant_2;