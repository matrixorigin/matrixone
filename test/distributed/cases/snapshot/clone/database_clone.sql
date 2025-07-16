drop database if exists db0;
create database db0;
use db0;

create table s1(a int);
insert into s1 select * from generate_series(1,5)g;

create database db0_copy_0 clone db0;
show tables from db0_copy_0;
select * from db0_copy_0.s1;

create database db0_copy_1 clone db0 to account sys;
show tables from db0_copy_1;
select * from db0_copy_1.s1;

drop database if exists db1;
create database db1;
use db1;

drop account if exists acc1;
drop account if exists acc2;

create account acc1 admin_name "root1" identified by "111";
create account acc2 admin_name "root2" identified by "111";

create table t1(a int, b int);
create table t2(a int, b int, primary key (a));
create table t3(a int, b int, primary key (a), index(a));

insert into t1 select *,* from generate_series(1,5)g;
insert into t2 select *,* from generate_series(1,5)g;
insert into t3 select *,* from generate_series(1,5)g;

-- across account clone need a snapshot.
create database db1_copy clone db1 to account acc1;
create snapshot sp_temp for database db1;
create database db1_copy clone db1 {snapshot = "sp_temp"} to account acc1;
drop snapshot sp_temp;

-- @session:id=2&user=acc1:root1&password=111
show tables from db1_copy;
select * from db1_copy.t1;
-- @session

drop snapshot if exists sp0;
create snapshot sp0 for account acc1;

create database db1_copy_copy clone db1_copy {snapshot = "sp0"} to account acc2;
-- @session:id=3&user=acc2:root2&password=111
show tables from db1_copy_copy;
select * from db1_copy_copy.t1;
-- @session

drop database if exists db2;
create database db2;
use db2;

create table r1 (a int);
insert into r1 values(1),(2),(3),(4);

create publication sys_pub database db2 account acc2;


-- @session:id=4&user=acc1:root1&password=111
drop database if exists db3;
create database db3;
use db3;

create table r2 (a int);
insert into r2 values(1),(2),(3),(4);

create publication acc1_pub database db3 account acc2;
-- @session

-- @session:id=5&user=acc2:root2&password=111
create database sub_sys from sys publication sys_pub;
create database sub_acc1 from acc1 publication acc1_pub;
-- @ignore:3,4,5,6,7,8
show subscriptions;

create database db4 clone sub_sys;
select * from db4.r1 order by a asc;

create database db5 clone sub_acc1;
select * from db5.r2 order by a asc;
-- @session

drop snapshot if exists sp0;
drop snapshot if exists sp1;
drop account if exists acc1;
drop account if exists acc2;
drop database if exists db0;
drop database if exists db0_copy_0;
drop database if exists db0_copy_1;
drop database if exists db1;
drop publication if exists sys_pub;
drop database if exists db2;