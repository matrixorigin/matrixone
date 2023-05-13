create publication pub1 database t;
create account acc0 admin_name 'root' identified by '111';
create account acc1 admin_name 'root' identified by '111';
create account acc2 admin_name 'root' identified by '111';
create database t;
create publication pub1 database t;
create publication pub2 database t account all;
create publication pub3 database t account acc0,acc1;
create publication pub4 database t account acc0,acc1,accx;
create publication pub5 database t account accx comment 'test';
show publications;
show create publication pub1;
show create publication pub2;
show create publication pub5;
alter publication pub1 account acc0,acc1;
show create publication pub1;
alter publication pub2 account drop acc1;
show create publication pub2;
alter publication pub3 account add accx;
show create publication pub3;
alter publication pub4 account drop acc1,acc2;
show create publication pub4;
alter publication pub5 account all comment '1212';
show create publication pub5;
drop publication pub1;
show publications;
create publication pub1 database t;
create publication pub1 database t;
alter publication pub1 account `all`;
show create publication pub1;
show create publication pubx;
drop publication pub1;
drop publication pub2;
drop publication pub3;
drop publication pub4;
drop publication pub5;
drop account acc0;
drop account acc1;
drop account acc2;
drop database t;

create account acc0 admin_name 'root' identified by '111';
create account acc1 admin_name 'root' identified by '111';
create account acc2 admin_name 'root' identified by '111';

-- 准备数据，发布端为sys租户
create database sys_db_1;
use sys_db_1;
create table sys_tbl_1(a int primary key );
insert into sys_tbl_1 values(1),(2),(3);
create view v1 as (select * from sys_tbl_1);
create publication sys_pub_1 database sys_db_1;
show publications;

-- acc0 订阅
-- @session:id=2&user=acc0:root&password=111
create database sub1 from sys publication sys_pub_1;
show databases;
show subscriptions;
use sub1;
show tables;
desc sys_tbl_1;
select * from sys_tbl_1 order by a;
select * from sub1.sys_tbl_1;
select * from v1;
show table_number from sub1;
show column_number from sys_tbl_1;
show table_values from sys_tbl_1;
-- @session


-- acc1 订阅
-- @session:id=3&user=acc1:root&password=111
create database sub1 from sys publication sys_pub_1;
show databases;
show subscriptions;
use sub1;
show tables;
desc sys_tbl_1;
select * from sys_tbl_1;
select * from sub1.sys_tbl_1;
-- @session

-- 限制acc0不能订阅
alter publication sys_pub_1 account acc1;

-- acc0 订阅
-- @session:id=2&user=acc0:root&password=111
show subscriptions;
select * from sub1.sys_tbl_1;
use sub1;
-- @session

-- acc1 订阅
-- @session:id=3&user=acc1:root&password=111
show subscriptions;
use sub1;
desc sys_tbl_1;
select * from sys_tbl_1;
-- @session

-- 恢复acc0 订阅权限
alter publication sys_pub_1 account add acc0;

-- acc0 订阅
-- @session:id=2&user=acc0:root&password=111
show subscriptions;
use sub1;
desc sys_tbl_1;
select * from sys_tbl_1;
-- @session

-- acc2 在没有权限时创建订阅
-- @session:id=4&user=acc2:root&password=111
create database sub1 from sys publication sys_pub_1;
-- @session

-- 发布端新增数据
use sys_db_1;
insert into sys_tbl_1 values(4);
create table sys_tbl_2(b text);

-- 查看acc0是否能感知新数据
-- @session:id=2&user=acc0:root&password=111
show subscriptions;
use sub1;
desc sys_tbl_1;
desc sys_tbl_2;
select * from sys_tbl_1;
-- @session


-- acc2发布数据
-- @session:id=4&user=acc2:root&password=111
create database acc2_db_1;
use acc2_db_1;
create table acc2_tbl_1(q text,c int primary key auto_increment);
insert into acc2_tbl_1(q) values ('acc2'),('acc1'),('acc0'),('sys');
create publication acc2_pub_1 database acc2_db_1;
-- @session

-- sys 创建订阅获取acc2的发布
create database sub2 from acc2 publication acc2_pub_1;
use sub2;
show subscriptions;
show tables;
desc acc2_tbl_1;
select * from acc2_tbl_1;

-- acc0 创建订阅获取acc2的发布
-- @session:id=2&user=acc0:root&password=111
create database sub2 from acc2 publication acc2_pub_1;
use sub2;
show subscriptions;
desc acc2_tbl_1;
select * from acc2_tbl_1;
-- @session

-- acc2限制只能sys租户使用发布,并新增数据
-- @session:id=4&user=acc2:root&password=111
alter publication acc2_pub_1 account `sys`;
use acc2_db_1;
create table acc2_tbl_2(c text);
insert into acc2_tbl_1(q) values ('mo');
-- @session

-- acc1 创建订阅获取acc2的数据
-- @session:id=3&user=acc1:root&password=111
create database sub2 from acc2 publication acc2_pub_1;
-- @session

-- acc0 查看数据
-- @session:id=2&user=acc0:root&password=111
select * from sub2.acc2_tbl_1;
use sub2;
-- @session

-- sys 查看数据
show tables;
desc acc2_tbl_2;
select * from sub2.acc2_tbl_2;


-- acc2 删除存在发布中的库
-- @session:id=4&user=acc2:root&password=111
drop database acc2_db_1;
drop publication acc2_pub_1;
drop database acc2_db_1;
-- @session

-- 清理资源
drop account acc0;
drop account acc1;
drop account acc2;
drop publication sys_pub_1;
drop database sys_db_1;
drop database sub2;

create database sub_db4 from no_exists publication pubname4;

create database db1;
create publication pubname4 database db1 comment 'publication to all tenant';
create database sub_db4 from sys publication pubname4;
drop publication pubname4;
drop database db1;