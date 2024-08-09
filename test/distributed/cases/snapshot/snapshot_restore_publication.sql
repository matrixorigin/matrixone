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

-- @ignore:5,6
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
-- @ignore:5,6
show publications;
show databases like 'db1';

-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;
-- @session

restore account sys from snapshot snapshot2;
select user_id,user_name,creator,owner,default_role from mo_catalog.mo_user;
-- @ignore:5,6
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
-- @ignore:5,6
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

-- @ignore:5,6
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

-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=4&user=test_tenant_2:test_account&password=111
create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;
-- @ignore:5,7
show subscriptions;
-- @session

create snapshot snapshot3 for account sys;

drop publication pubname1;
drop publication pubname2;
drop database db1;
drop database db2;
-- @ignore:5,6
show publications;
show databases like 'db%';

-- @session:id=3&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=4&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

restore account sys from snapshot snapshot3;
select user_id,user_name,creator,owner,default_role from mo_catalog.mo_user;

-- @ignore:5,6
show publications;
show databases like 'db%';

-- @session:id=3&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=4&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

drop snapshot snapshot3;
drop account test_tenant_1;
drop account test_tenant_2;
drop publication if exists pubname1;
drop publication if exists pubname2;
drop database if exists db1;
drop database if exists db2;
-- @ignore:5,6
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

-- @ignore:5,6
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

-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=6&user=test_tenant_2:test_account&password=111
create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;
-- @ignore:5,7
show subscriptions;
-- @session

restore account sys from snapshot snapshot4;
select user_id,user_name,creator,owner,default_role from mo_catalog.mo_user;

-- @ignore:5,6
show publications;
show databases like 'db%';

-- @session:id=5&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=6&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

drop snapshot snapshot4;
drop account test_tenant_1;
drop account test_tenant_2;
drop publication if exists pubname1;
drop publication if exists pubname2;
drop database if exists db1;
drop database if exists db2;
-- @ignore:5,6
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

-- @ignore:5,6
show publications;
show databases like 'db%';

-- @session:id=7&user=test_tenant_1:test_account&password=111
create database sub_db1 from sys publication pubname1;
use sub_db1;
show tables;

-- @ignore:5,7
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

-- @ignore:5,6
show publications;
show databases like 'db%';

-- @session:id=7&user=test_tenant_1:test_account&password=111
create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=8&user=test_tenant_2:test_account&password=111
create database sub_db2 from sys publication pubname2;
use sub_db2;
show tables;
select * from t2;
-- @ignore:5,7
show subscriptions;
-- @session

restore account sys from snapshot snapshot6;
select user_id,user_name,creator,owner,default_role from mo_catalog.mo_user;

-- @ignore:5,6
show publications;
show databases like 'db%';

-- @session:id=7&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=8&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;

-- @ignore:5,7
show subscriptions;
-- @session

restore account sys from snapshot snapshot5;
select user_id,user_name,creator,owner,default_role from mo_catalog.mo_user;
-- @session:id=7&user=test_tenant_1:test_account&password=111
show databases like 'sub_db1';
use sub_db1;
show tables;

show databases like 'sub_db2';
use sub_db2;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=8&user=test_tenant_2:test_account&password=111
show databases like 'sub_db2';
use sub_db2;
show tables;

-- @ignore:5,7
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
-- @ignore:5,6
show publications;
show databases like 'db%';
-- @ignore:1
show snapshots;

drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=9&user=acc01:test_account&password=111
drop database if exists db09;
create database db09;
use db09;
drop table if exists index01;
create table index01(
                        col1 int not null,
                        col2 date not null,
                        col3 varchar(16) not null,
                        col4 int unsigned not null,
                        primary key (col1)
);
insert into index01 values(1, '1980-12-17','Abby', 21);
insert into index01 values(2, '1981-02-20','Bob', 22);
insert into index01 values(3, '1981-02-20','Bob', 22);
select count(*) from index01;

drop table if exists index02;
create table index02(col1 char, col2 int, col3 binary);
insert into index02 values('a', 33, 1);
insert into index02 values('c', 231, 0);
alter table index02 add key pk(col1) comment 'primary key';
select count(*) from index02;

drop database if exists db10;
create database db10;
use db10;
drop table if exists index03;
create table index03 (
                         emp_no      int             not null,
                         birth_date  date            not null,
                         first_name  varchar(14)     not null,
                         last_name   varchar(16)     not null,
                         gender      varchar(5)      not null,
                         hire_date   date            not null,
                         primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into index03 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                           (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20');

-- @session

drop snapshot if exists sp05;
create snapshot sp05 for account acc01;

-- @session:id=9&user=acc01:test_account&password=111
drop publication if exists pub05;
create publication pub05 database db09 account acc02 comment 'publish db09';
drop publication if exists pub06;
create publication pub06 database db10 account acc02 comment 'publish db10';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=10&user=acc02:test_account&password=111
drop database if exists sub05;
create database sub05 from acc01 publication pub05;
show databases;
use sub05;
show create table index01;
select * from index02;
-- @session

restore account acc01 from snapshot sp05;

-- @session:id=9&user=acc01:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db%';
-- @session

create account acc03 admin_name 'test_account' identified by '111';

restore account acc01 from snapshot sp05 to account acc03;
-- @bvt:issue#16497
-- @session:id=11&user=acc03:test_account&password=111
-- @ignore:5,6
show publications;
show databases like 'db%';
-- @session
-- @bvt:issue

drop snapshot sp05;
drop account acc01;
drop account acc02;
drop account acc03;
-- @ignore:1
show snapshots;
