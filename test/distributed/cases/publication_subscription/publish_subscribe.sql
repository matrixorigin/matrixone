-- @setup
drop database if exists test02;
drop database if exists db1;
drop database if exists test03;
drop database if exists procedure_test;
-- create tenant
create account test_tenant_1 admin_name 'test_account' identified by '111';
create account test_tenant_2 admin_name 'test_account' identified by '111';
create account test_tenant_3 admin_name 'test_account' identified by '111';
create account test_tenant_4 admin_name 'test_account' identified by '111';

-- test sys tenant database publication to test_tenant_1 tenant
show databases;
create database db1;
use db1;
create table t1(a int);
insert into t1 values (1),(2),(3);

-- @setup
-- publish to tenant test_tenant_1
create publication pubname1 database db1 account test_tenant_1 comment 'publish db1 database';

-- @session:id=2&user=test_tenant_1:test_account&password=111
create database sub_db1 from sys publication pubname1;
show databases;
use sub_db1;
show tables;
select * from t1;
-- @session

create table t2(b int);
insert into t2 values (4),(5),(6);

-- @session:id=2&user=test_tenant_1:test_account&password=111
show tables;
select * from t2;
insert into t2 values(10);
update t2 set b=10 where b=4;
delete from t2 where b=4;
-- @session

drop table t1;
-- @session:id=2&user=test_tenant_1:test_account&password=111
show tables;
-- @session

drop database db1;
-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases;
-- @session

-- @teardown
-- drop publication
drop publication pubname1;
show databases;
drop database db1;

-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases;
use sub_db1;
drop database sub_db1;
-- @session

-- test sys tenant publication to test_tenant_1,test_tenant_2,test_tenant3 tenant
create database db2;
use db2;
create table t3(a int);
create publication pubname2 database db2 account test_tenant_1,test_tenant_2,test_tenant_3 comment 'publish db2';

-- @session:id=2&user=test_tenant_1:test_account&password=111
create database sub_db1 from sys publication pubname2;
show databases;
use sub_db1;
show tables;
-- @session

-- @session:id=3&user=test_tenant_2:test_account&password=111
create database sub_db1 from sys publication pubname2;
show databases;
use sub_db1;
show tables;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
create database sub_db1 from sys publication pubname2;
show databases;
use sub_db1;
show tables;
-- @session

drop publication pubname2;
create publication pubname2 database db2 account test_tenant_1,test_tenant_2,test_tenant_3 comment 'publish db3';
-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases;
use sub_db1;
show tables;
-- @session

-- @session:id=3&user=test_tenant_2:test_account&password=111
show databases;
use sub_db1;
show tables;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
show databases;
use sub_db1;
show tables;
-- @session

drop publication pubname2;
create database db3;
create publication pubname2 database db3 account test_tenant_1,test_tenant_2,test_tenant_3 comment 'publish db3';
-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases;
use sub_db1;
show tables;
-- @session

-- @session:id=3&user=test_tenant_2:test_account&password=111
show databases;
use sub_db1;
show tables;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
show databases;
use sub_db1;
show tables;
-- @session

-- @teardown
drop publication pubname2;
drop database db2;
drop database db3;
-- @session:id=2&user=test_tenant_1:test_account&password=111
drop database sub_db1;
-- @session

-- @session:id=3&user=test_tenant_2:test_account&password=111
drop database sub_db1;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
drop database sub_db1;
-- @session


-- sys tenant publication to all test tenant
create database db4;
use db4;
create table t4(a int);
create publication pubname4 database db4 comment 'publication to all tenant';
create publication pubname4_1 database db4 comment 'publication to all tenant';

-- @session:id=2&user=test_tenant_1:test_account&password=111
create database sub_db4 from sys publication pubname4;
show databases;
use sub_db4;
show tables;
-- @session

-- test subscription db name exists
-- @session:id=2&user=test_tenant_1:test_account&password=111
create database test_db_1;
create database test_db_1 from sys publication pubname4;
-- @session

-- @session:id=3&user=test_tenant_2:test_account&password=111
create database sub_db4 from sys publication pubname4;
show databases;
use sub_db4;
show tables;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
create database sub_db4 from sys publication pubname4;
show databases;
use sub_db4;
show tables;
-- @session

-- @session:id=5&user=test_tenant_4:test_account&password=111
create database sub_db4 from sys publication pubname4;
create database sub_db4_1 from sys publication pubname4;
create database sub_db4_2 from sys publication pubname4;
show databases;
use sub_db4;
show tables;
-- @session

-- test publish first and create tenants later
create account test_tenant_5 admin_name 'test_account' identified by '111';
-- @session:id=6&user=test_tenant_5:test_account&password=111
create database sub_db4 from sys publication pubname4;
show databases;
use sub_db4;
show tables;
-- @session

alter publication pubname4 account test_tenant_1, test_tenant_2;
-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases;
use sub_db4;
show tables;
-- @session

-- @session:id=3&user=test_tenant_2:test_account&password=111
show databases;
use sub_db4;
show tables;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
show databases;
use sub_db4;
show tables;
-- @session

-- @session:id=5&user=test_tenant_4:test_account&password=111
show databases;
use sub_db4;
show tables;
-- @session

alter publication pubname4 account all;
-- @session:id=2&user=test_tenant_1:test_account&password=111
show databases;
use sub_db4;
show tables;
-- @session

-- @session:id=3&user=test_tenant_2:test_account&password=111
show databases;
use sub_db4;
show tables;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
show databases;
use sub_db4;
show tables;
-- @session

-- @session:id=5&user=test_tenant_4:test_account&password=111
show databases;
use sub_db4;
show tables;
-- @session

-- @teardown
drop publication pubname4;
drop publication pubname4_1;
drop database db4;
show databases;
-- @session:id=2&user=test_tenant_1:test_account&password=111
drop database sub_db4;
drop database test_db_1;
show databases;
-- @session

-- @session:id=3&user=test_tenant_2:test_account&password=111
drop database sub_db4;
show databases;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
drop database sub_db4;
show databases;
-- @session

-- @session:id=5&user=test_tenant_4:test_account&password=111
drop database sub_db4;
drop database sub_db4_1;
drop database sub_db4_2;
show databases;
-- @session

-- test common tenant publication scene
-- @session:id=2&user=test_tenant_1:test_account&password=111
create database db1;
create publication pubname5 database db1 account sys comment 'publication to sys tenant';
-- @session
create database sub_db5 from test_tenant_1 publication pubname5;
create publication test_pubname database sub_db5 comment 'publication to all tenant';

-- @session:id=2&user=test_tenant_1:test_account&password=111
drop publication pubname5;
-- @session
drop database sub_db5;

-- @session:id=2&user=test_tenant_1:test_account&password=111
create publication pubname5 database db1 comment 'publication to sys tenant';
-- @session
create database sub_db5 from test_tenant_1 publication pubname5;
use sub_db5;
create table t1 (a int);
create view test_view as select 1;

-- @session:id=3&user=test_tenant_2:test_account&password=111
create database sub_db5 from test_tenant_1 publication pubname5;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
create database sub_db5 from test_tenant_1 publication pubname5;
-- @session

-- @session:id=2&user=test_tenant_1:test_account&password=111
drop publication pubname5;
drop database db1;
show databases;
-- @session
drop database sub_db5;
show databases;

-- @session:id=3&user=test_tenant_2:test_account&password=111
drop database sub_db5;
show databases;
-- @session

-- @session:id=4&user=test_tenant_3:test_account&password=111
drop database sub_db5;
show databases;
-- @session

-- test subscription db query
-- @session:id=2&user=test_tenant_1:test_account&password=111
create database db7;
use db7;
create table t1(a int, b varchar(225));
insert into t1 values (1, 'a'),(2, 'b'),(3, 'c');
create table t2(col1 int, col2 text);
insert into t2 values (100, 'xx'),(200, 'yy'),(300, 'zz');
create publication pubname7 database db7 account sys comment 'publication to sys tenant';
-- @session
create database sub_db1 from test_tenant_1 publication pubname7;
use sub_db1;
select * from t1;
select * from t2;
select * from t1 left join t2 on t1.a=t2.col1;
select * from t1 right join t2 on t1.a=t2.col1;
select * from t1 union select * from t2;
create database db1;
use db1;
create table t3 (a int, b varchar(225));
insert into t3 values (10, 'aa');
select * from t3 left join sub_db1.t1 as t on t3.a=t.a;
select * from t3 right join sub_db1.t1 as t on t3.a=t.a;
select * from t3 union select * from sub_db1.t1;

-- @teardown
-- @session:id=2&user=test_tenant_1:test_account&password=111
drop publication pubname7;
drop database db7;
show databases;
-- @session
drop database sub_db1;
drop database db1;
show databases;

-- test show scenes
create database db6;
create publication pubname6 database db6 account test_tenant_1 comment 'publication to test_tenant_1';
show create publication pubname6;
show publications;
-- @session:id=2&user=test_tenant_1:test_account&password=111
create database sub_db6 from sys publication pubname6;
show subscriptions;
show create database sub_db6;
-- @session

-- @teardown
drop publication pubname6;
drop database db6;
-- @session:id=2&user=test_tenant_1:test_account&password=111
drop database sub_db6;
-- @session

-- test publication a view/function/sequence exists in the database
create database db7;
use db7;
create table t1(a int);
insert into t1 values (1),(2);
create view v_name as select * from t1;
create publication pubname7 database db7 account test_tenant_1 comment 'publication to test_tenant_1 tenant';
CREATE SEQUENCE seq_id INCREMENT BY 1 MAXVALUE 1000 START with 1;

-- @session:id=2&user=test_tenant_1:test_account&password=111
create database sub_db1 from sys publication pubname7;
use sub_db1;
show tables;
SELECT NEXTVAL('seq_id');
-- @session

-- @teardown
drop publication pubname7;
drop database db7;
-- @session:id=2&user=test_tenant_1:test_account&password=111
drop database sub_db1;
-- @session

-- test sys tenant publication other tenant database
-- @session:id=2&user=test_tenant_1:test_account&password=111
create database db100;
-- @session


create publication pubname8 database db100 commment 'publication failed';

-- @teardown
-- @session:id=2&user=test_tenant_1:test_account&password=111
drop database db100;
-- @session

-- @session:id=2&user=test_tenant_1:test_account&password=111
create database sub_db7 from sys publication pubname7;
use sub_db7;
show tables;
-- @session

-- @bvt:issue#9024
create database test_db;
create publication test_pubname database test_db account all comment 'publication to all tenant';
drop publication test_pubname;
drop database test_db;
-- @bvt:issue

create database sub_db4 from no_exists publication pubname4;

create publication test_pubname database mo_task account test_account comment 'publication to test_account tenant';
create publication test_pubname database information_schema account test_account comment 'publication to test_account tenant';
create publication test_pubname database mysql account test_account comment 'publication to test_account tenant';
create publication test_pubname database system_metrics account test_account comment 'publication to test_account tenant';
create publication test_pubname database system account test_account comment 'publication to test_account tenant';
create publication test_pubname database mo_catalog account test_account comment 'publication to test_account tenant';


create database db5;
create publication pubname5 database db5 comment 'publication to all tenant';
create database sub_db5 from sys publication pubname5;
drop publication pubname5;
drop database db5;

-- @teardown
show publications;
show subscriptions;
-- @session:id=2&user=test_tenant_1:test_account&password=111
show publications;
show subscriptions;
-- @session
-- @session:id=3&user=test_tenant_2:test_account&password=111
show publications;
show subscriptions;
-- @session
-- @session:id=4&user=test_tenant_3:test_account&password=111
show publications;
show subscriptions;
-- @session
-- @session:id=5&user=test_tenant_4:test_account&password=111
show publications;
show subscriptions;
-- @session
-- @session:id=6&user=test_tenant_5:test_account&password=111
show publications;
show subscriptions;
-- @session

-- drop account
drop account test_tenant_1;
drop account test_tenant_2;
drop account test_tenant_3;
drop account test_tenant_4;
drop account test_tenant_5;
