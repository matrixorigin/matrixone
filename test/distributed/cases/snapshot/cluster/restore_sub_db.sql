drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

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

create publication pubname2 database db2 account sys comment 'publish db2 database';
-- @ignore:5,6
show publications;
show databases like 'db2';

drop snapshot if exists snap1;
create snapshot snap1 for account acc01;
-- @ignore:1
show snapshots;


drop database if exists sub_db1;
show databases like 'sub_db1';
drop publication if exists pubname2;
-- @ignore:5,6
show publications;
drop database if exists db2;
show databases like 'db2';

restore account acc01 from snapshot snap1;

show databases like 'sub_db1';
use sub_db1;
show tables;
select * from t1;
show databases like 'db2';
-- @ignore:5,6
show publications;

drop snapshot if exists snap1;
-- @ignore:1
show snapshots;
drop publication if exists pubname2;
-- @ignore:5,6
show publications;
-- @session
drop publication if exists pubname1;
drop database if exists db1;
drop account if exists acc01;



drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

drop database if exists db1;
create database db1;
use db1;
create table t1(a int);
insert into t1 values (1),(2),(3);

create publication pubname1 database db1 account acc01 comment 'publish db1 database';
-- @ignore:5,6
show publications;
show databases like 'db1';


-- @session:id=2&user=acc01:test_account&password=111
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

create publication pubname2 database db2 account sys comment 'publish db2 database';
-- @ignore:5,6
show publications;
show databases like 'db2';
-- @session

drop snapshot if exists snap2;
create snapshot snap2 for account acc01;
-- @ignore:1
show snapshots;

-- @session:id=2&user=acc01:test_account&password=111
drop database if exists sub_db1;
show databases like 'sub_db1';
drop publication if exists pubname2;
-- @ignore:5,6
show publications;
drop database if exists db2;
show databases like 'db2';
-- @session

restore account acc01 from snapshot snap2;

-- @session:id=2&user=acc01:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;
select * from t1;
show databases like 'db2';
-- @ignore:5,6
show publications;

drop publication if exists pubname2;
-- @ignore:5,6
show publications;
-- @session

drop snapshot if exists snap2;
drop account if exists acc01;

drop publication if exists pubname1;
drop database if exists db1;
