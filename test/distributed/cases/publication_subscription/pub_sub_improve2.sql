-- pub-sub improvement test
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';



-- normal tenant creates the publication db, specifies the tenant to publish, verifies that the specified tenant is
-- subscribable, and displays sub_accounts in the show publications results as the specified tenant
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db01;
create database db01;
use db01;
create table table01 (col1 int, col2 enum ('a','b','c'));
insert into table01 values(1,'a');
insert into table01 values(2, 'b');
create table table02 (col1 int, col2 enum ('a','b','c'));
insert into table02 values(1,'a');
insert into table02 values(2, 'b');
drop database if exists db02;
create database db02;
use db02;
create table index01(col1 int,key key1(col1));
insert into index01 values (1);
insert into index01 values (2);

drop publication if exists pub01;
create publication pub01 database db01 account acc02;
create publication pub02 database db02 table index01 account acc03;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub01;
create database sub01 from acc01 publication pub01;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub03;
create database sub03 from acc01 publication pub02;
show databases;
use sub03;
select * from index01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,6
show publications;
alter publication pub01 account acc02 database db02;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub01;
show tables;
select * from index01;
select count(*) from index01;
show create table index01;
-- @ignore:5,7
show subscriptions;
drop database sub01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub01;
drop database db01;
-- @session




-- sys account publishes the db to all all. verify that show publications: the db is publishing name, the tables field is *,
-- the sub_account field is *, and the subscribed_accounts field is null. comments is a comment when the publication is
-- created. verify show subscriptions all: shows all authorized subscriptions
drop database if exists db04;
create database db04;
use db04;
drop table if exists index01;
create table index01(col1 char, col2 int, col3 binary);
insert into index01 values('a', 33, 1);
insert into index01 values('c', 231, 0);
alter table index01 add key pk(col1) comment 'primary key';
select count(*) from index01;

drop publication if exists pub04;
create publication pub04 database db04 account all comment 'pub to account all';
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists db05;
create database sub05 from sys publication pub04;
-- @ignore:5,7
show subscriptions all;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists db05;
create database sub05 from sys publication pub04;
-- @ignore:5,7
show subscriptions all;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists db05;
create database sub05 from sys publication pub04;
-- @ignore:5,7
show subscriptions all;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop database sub05;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub05;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop database sub05;
-- @session

drop publication pub04;
drop database db04;




-- sys account publishes part of the tables in the db to all tenants. verify that the show publications: the name of the
-- published db, the tables field is the list of published tables, the sub_account field is *. The subscribed_accounts
-- is null, and comments is the comment publication is created. show subscriptions all: shows all subscriptions with permissions
drop database if exists db05;
create database db05;
use db05;
drop table if exists t1;
create table t1 (a text);
insert into t1 values('abcdef'),('_bcdef'),('a_cdef'),('ab_def'),('abcd_f'),('abcde_');
drop table if exists t2;
create table t2 (a datetime(0) not null, primary key(a));
insert into t2 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
drop table if exists t3;
create table t3(col1 datetime);
insert into t3 values('2020-01-13 12:20:59.1234586153121');
insert into t3 values('2023-04-17 01:01:45');
insert into t3 values(NULL);
drop table if exists t4;
create table t4(a int unique key, b int, c int, unique key(b,c));
insert into t4 values(1,1,1);
insert into t4 values(2,3,3);
insert into t4 values(10,19,11);
drop publication if exists pub05;
create publication pub05 database db05 table t1,t3 account all comment 'publish t1、t3 to all account except sys';
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from sys publication pub05;
use sub06;
show tables;
select * from t1;
show create table t3;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from sys publication pub05;
use sub06;
show tables;
select * from t1;
show create table t3;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from sys publication pub05;
use sub06;
show tables;
select * from t1;
show create table t3;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop database sub06;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub06;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop database sub06;
-- @session

-- @ignore:5,6
show publications;

-- modify published to all tenants to specified tenants
alter publication pub05 account acc01,acc02 database db05;
-- @ignore:5,6
show publications;

-- @session:id=3&user=acc03:test_account&password=111
drop database sub06;
-- @ignore:5,7
show subscriptions all;
-- @session

drop publication pub05;
drop database db05;




-- normal tenant is the publisher, the publisher publishes all tables, tables in show subscriptions
-- are displayed as *, then delete some tables, and the tables in show subscriptions are verified as valid tables
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db06;
create database db06;
use db06;
drop table if exists t1;
create table t1 (a text);
insert into t1 values('abcdef'),('_bcdef'),('a_cdef'),('ab_def'),('abcd_f'),('abcde_');
drop table if exists t2;
create table t2 (a datetime(0) not null, primary key(a));
insert into t2 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
drop table if exists t3;
create table t3(col1 datetime);
insert into t3 values('2020-01-13 12:20:59.1234586153121');
insert into t3 values('2023-04-17 01:01:45');
insert into t3 values(NULL);
drop table if exists t4;
create table t4(a int unique key, b int, c int, unique key(b,c));
insert into t4 values(1,1,1);
insert into t4 values(2,3,3);
insert into t4 values(10,19,11);
drop publication if exists pub06;
create publication pub06 database db06 account acc02 comment 'publish all tables to account acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from acc01 publication pub06;
use sub06;
show tables;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
use db06;
drop table t1;
drop table t2;
drop table t3;
-- @ignore:5,6
show publications;           -- table字段需要实时更新
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub06;
show tables;
select * from t4;
show subscriptions;              -- table字段需要实时更新
-- @session


