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

create database db1_copy clone db1 to account acc1;

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

drop snapshot if exists sp0;
drop snapshot if exists sp1;
drop account if exists acc1;
drop account if exists acc2;
drop database if exists db1;