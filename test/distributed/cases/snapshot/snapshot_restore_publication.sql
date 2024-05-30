-- drop, and then restore
drop database if exists db1;
create database db1;
use db1;
create table t1(a int);
insert into t1 values (1),(2),(3);

-- create account test_tenant_1
create account test_tenant_1 admin_name 'test_account' identified by '111';

-- @setup
-- publish to tenant test_tenant_1
create publication pubname1 database db1 account test_tenant_1 comment 'publish db1 database';

-- @ignore:2,3
show publications;
show databases like 'db1';

-- @session:id=2&user=test_tenant_1:test_account&password=111
create database sub_db1 from sys publication pubname1;
show databases;
use sub_db1;
show tables;
select * from t1;
-- @session

create snapshot snapshot2 for account sys;

drop publication pubname1;
drop database db1;
-- @ignore:2,3
show publications;
show databases like 'db1';

-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;
-- @session

restore account sys from snapshot snapshot2;
-- @ignore:2,3
show publications;
show databases like 'db1';

-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;
-- @session

drop snapshot snapshot2;
drop account test_tenant_1;
drop publication if exists pubname1;
drop database if exists db1;
-- @ignore:2,3
show publications;
show databases like 'db1';


-- create and then restore
create database db1;
use db1;
create table t1(a int);
insert into t1 values (1),(2),(3);

-- create account test_tenant_1
create account test_tenant_1 admin_name 'test_account' identified by '111';

-- @setup
-- publish to tenant test_tenant_1
create publication pubname1 database db1 account test_tenant_1 comment 'publish db1 database';

create database db2;
use db2;
create table t2(a int);
insert into t2 values (1),(2),(3);

-- create account test_tenant_2
create account test_tenant_2 admin_name 'test_account' identified by '111';

-- @setup
-- publish to tenant test_tenant_1 and test_tenant_2
create publication pubname2 database db2 account all comment 'publish db2 database';

-- @ignore:2,3
show publications;
show databases like 'db%';

-- @session:id=3&user=test_tenant_1:test_account&password=111
create database sub_db1 from sys publication pubname1;
use sub_db1;
show tables;
select * from t1;

create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;

-- @ignore:3,5
show subscriptions;
-- @session

-- @session:id=4&user=test_tenant_2:test_account&password=111
create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;
-- @ignore:3,5
show subscriptions;
-- @session

create snapshot snapshot3 for account sys;

drop publication pubname1;
drop publication pubname2;
drop database db1;
drop database db2;
-- @ignore:2,3
show publications;
show databases like 'db%';

-- @session:id=3&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:3,5
show subscriptions;
-- @session

-- @session:id=4&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:3,5
show subscriptions;
-- @session

restore account sys from snapshot snapshot3;

-- @ignore:2,3
show publications;
show databases like 'db%';

-- @session:id=3&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:3,5
show subscriptions;
-- @session

-- @session:id=4&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:3,5
show subscriptions;
-- @session

drop snapshot snapshot3;
drop account test_tenant_1;
drop account test_tenant_2;
drop publication if exists pubname1;
drop publication if exists pubname2;
drop database if exists db1;
drop database if exists db2;
-- @ignore:2,3
show publications;
show databases like 'db%';



-- create and then restore
create snapshot snapshot4 for account sys;
create database db1;
use db1;
create table t1(a int);
insert into t1 values (1),(2),(3);

-- create account test_tenant_1
create account test_tenant_1 admin_name 'test_account' identified by '111';

-- @setup
-- publish to tenant test_tenant_1
create publication pubname1 database db1 account test_tenant_1 comment 'publish db1 database';

create database db2;
use db2;
create table t2(a int);
insert into t2 values (1),(2),(3);

-- create account test_tenant_2
create account test_tenant_2 admin_name 'test_account' identified by '111';

-- @setup
-- publish to tenant test_tenant_1 and test_tenant_2
create publication pubname2 database db2 account all comment 'publish db2 database';

-- @ignore:2,3
show publications;
show databases like 'db%';

-- @session:id=5&user=test_tenant_1:test_account&password=111
create database sub_db1 from sys publication pubname1;
use sub_db1;
show tables;
select * from t1;

create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;

-- @ignore:3,5
show subscriptions;
-- @session

-- @session:id=6&user=test_tenant_2:test_account&password=111
create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;
-- @ignore:3,5
show subscriptions;
-- @session

restore account sys from snapshot snapshot4;

-- @ignore:2,3
show publications;
show databases like 'db%';

-- @session:id=5&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:3,5
show subscriptions;
-- @session

-- @session:id=6&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:3,5
show subscriptions;
-- @session

drop snapshot snapshot4;
drop account test_tenant_1;
drop account test_tenant_2;
drop publication if exists pubname1;
drop publication if exists pubname2;
drop database if exists db1;
drop database if exists db2;
-- @ignore:2,3
show publications;
show databases like 'db%';
-- @ignore:1
show snapshots;

-- create, drop and create, then restore
create snapshot snapshot5 for account sys;
create database db1;
use db1;
create table t1(a int);

-- create account test_tenant_1
create account test_tenant_1 admin_name 'test_account' identified by '111';

-- @setup
-- publish to tenant test_tenant_1
create publication pubname1 database db1 account test_tenant_1 comment 'publish db1 database';

-- @ignore:2,3
show publications;
show databases like 'db%';

-- @session:id=7&user=test_tenant_1:test_account&password=111
create database sub_db1 from sys publication pubname1;
use sub_db1;
show tables;

-- @ignore:3,5
show subscriptions;
-- @session


create snapshot snapshot6 for account sys;
drop publication pubname1;
drop database db1;

create database db2;
use db2;
create table t2(a int);
insert into t2 values (1),(2),(3);

-- create account test_tenant_2
create account test_tenant_2 admin_name 'test_account' identified by '111';

-- @setup
-- publish to tenant test_tenant_1 and test_tenant_2
create publication pubname2 database db2 account all comment 'publish db2 database';

-- @ignore:2,3
show publications;
show databases like 'db%';

-- @session:id=7&user=test_tenant_1:test_account&password=111
create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;
-- @ignore:3,5
show subscriptions;
-- @session

-- @session:id=8&user=test_tenant_2:test_account&password=111
create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;
-- @ignore:3,5
show subscriptions;
-- @session

restore account sys from snapshot snapshot6;

-- @ignore:2,3
show publications;
show databases like 'db%';

-- @session:id=7&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:3,5
show subscriptions;
-- @session

-- @session:id=8&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;

-- @ignore:3,5
show subscriptions;
-- @session

restore account sys from snapshot snapshot5;
-- @session:id=7&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:3,5
show subscriptions;
-- @session

-- @session:id=8&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;

-- @ignore:3,5
show subscriptions;
-- @session

drop snapshot snapshot5;
drop snapshot snapshot6;
drop account test_tenant_1;
drop account test_tenant_2;
drop publication if exists pubname1;
drop publication if exists pubname2;
drop database if exists db1;
drop database if exists db2;
-- @ignore:2,3
show publications;
show databases like 'db%';
-- @ignore:1
show snapshots;
