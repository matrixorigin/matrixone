drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';
drop account if exists acc04;
create account acc04 admin_name = 'test_account' identified by '111';

drop database if exists db1;
create database db1;
use db1;
create table t1(a int);
insert into t1 values (1),(2),(3);

create publication pubname1 database db1 account acc01 comment 'publish db1 database';
-- @ignore:5,6
show publications;
show databases like 'db1';

-- @session:id=1&user=acc01:test_account&password=111
create database sub_db1 from sys publication pubname1;
show databases like 'sub_db1';
use sub_db1;
show tables;
select * from t1;

drop database if exists db2;
create database if not exists db2;
use db2;
create table t2(a int);
insert into t2 values (1),(2),(3);

create publication pubname2 database db2 account acc02 comment 'publish db2 database';
-- @ignore:5,6
show publications;
show databases like 'db2';
-- @session

-- @session:id=2&user=acc02:test_account&password=111
create database sub_db2 from acc01 publication pubname2;
show databases like 'sub_db2';
use sub_db2;
show tables;
select * from t2;

drop database if exists db3;
create database if not exists db3;
use db3;
create table t3(a int);
insert into t3 values (1),(2),(3);

create publication pubname3 database db3 account acc03 comment 'publish db3 database';
-- @ignore:5,6
show publications;
show databases like 'db3';
-- @session

-- @session:id=3&user=acc03:test_account&password=111
create database sub_db3 from acc02 publication pubname3;
show databases like 'sub_db3';
use sub_db3;
show tables;
select * from t3;

drop database if exists db4;
create database if not exists db4;
use db4;
create table t4(a int);
insert into t4 values (1),(2),(3);

create publication pubname4 database db4 account acc04 comment 'publish db4 database';
-- @ignore:5,6
show publications;
show databases like 'db4';
-- @session

-- @session:id=4&user=acc04:test_account&password=111
create database sub_db4 from acc03 publication pubname4;
show databases like 'sub_db4';
use sub_db4;
show tables;
select * from t4;

drop database if exists db5;
create database if not exists db5;
use db5;
create table t5(a int);
insert into t5 values (1),(2),(3);

create publication pubname5 database db5 account sys comment 'publish db5 database';
-- @ignore:5,6
show publications;
show databases like 'db5';
-- @session

create database sub_db5 from acc04 publication pubname5;
show databases like 'sub_db5';
use sub_db5;
show tables;
select * from t5;

-- @cleanup
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;
create snapshot cluster_sp for cluster;
-- @ignore:1
show snapshots;

drop publication if exists pubname1;
drop database if exists db1;
-- @ignore:5,6
show publications;
show databases like 'db1';

drop database if exists sub_db5;
show databases like 'sub_db5';

-- @session:id=1&user=acc01:test_account&password=111
drop publication if exists pubname2;
drop database if exists db2;
-- @ignore:5,6
show publications;
show databases like 'db2';

drop database if exists sub_db1;
show databases like 'sub_db1';
-- @session


-- @session:id=2&user=acc02:test_account&password=111
drop publication if exists pubname3;
drop database if exists db3;
-- @ignore:5,6
show publications;
show databases like 'db3';

drop database if exists sub_db2;
show databases like 'sub_db2';
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop publication if exists pubname4;
drop database if exists db4;
-- @ignore:5,6
show publications;
show databases like 'db4';

drop database if exists sub_db3;
show databases like 'sub_db3';
-- @session

-- @session:id=4&user=acc04:test_account&password=111
drop publication if exists pubname5;
drop database if exists db5;
-- @ignore:5,6
show publications;
show databases like 'db5';

drop database if exists sub_db4;
show databases like 'sub_db4';
-- @session

restore cluster from snapshot cluster_sp;

-- @ignore:5,6
show publications;
show databases like 'db1';
show databases like 'sub_db5';
select * from sub_db5.t5;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db2';
show databases like 'sub_db1';
select * from sub_db1.t1;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db3';
show databases like 'sub_db2';
select * from sub_db2.t2;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db4';
show databases like 'sub_db3';
select * from sub_db3.t3;
-- @session

-- @session:id=4&user=acc04:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db5';
show databases like 'sub_db4';
select * from sub_db4.t4;
-- @session

-- @cleanup
drop snapshot if exists cluster_sp;
-- @ignore:1
show snapshots;

drop database if exists sub_db5;
drop publication if exists pubname1;
-- @ignore:5,6
show publications;
drop database if exists db1;

-- @session:id=1&user=acc01:test_account&password=111
drop publication if exists pubname2;
-- @ignore:5,6
show publications;
drop database if exists db2;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop publication if exists pubname3;
-- @ignore:5,6
show publications;
drop database if exists db3;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop publication if exists pubname4;
-- @ignore:5,6
show publications;
drop database if exists db4;
-- @session

-- @session:id=4&user=acc04:test_account&password=111
drop publication if exists pubname5;
-- @ignore:5,6
show publications;
drop database if exists db5;
-- @session

drop account if exists acc01;
drop account if exists acc02;
drop account if exists acc03;
drop account if exists acc04;
