-- case 1: cross account clone db/tbl with within db reference.
drop database if exists sys_db1;
create database sys_db1;

create table sys_db1.t1(a int primary key);
insert into sys_db1.t1 values(1),(2);

create table sys_db1.t2(a int primary key, b int, foreign key (b) references sys_db1.t1(a));
insert into sys_db1.t2 values(1,1),(2,2);

drop account if exists acc1;
create account acc1 admin_name "root1" identified by "111";

create publication pub1 database sys_db1 account acc1;

-- @session:id=2&user=acc1:root1&password=111
create database sub1 from sys publication pub1;
show tables from sub1;

create database test1 clone sub1;
select * from test1.t1;
select * from test1.t2;

create table test1.t3 clone sub1.t1;
create table test1.t4 clone sub1.t2;
drop database if exists test1;
drop database if exists sub1;
-- @session
drop publication pub1;
drop account acc1;
drop database sys_db1;

-- case 2: cross account clone db with cross database foreign key.
drop database if exists sys_fk_base;
drop database if exists sys_fk_cross;

create database sys_fk_base;
create table sys_fk_base.p1(id int primary key);
insert into sys_fk_base.p1 values(1),(2);

create database sys_fk_cross;
create table sys_fk_cross.c1(id int primary key, pid int, foreign key (pid) references sys_fk_base.p1(id));
insert into sys_fk_cross.c1 values(1,1),(2,2);

drop account if exists acc_fk1;
create account acc_fk1 admin_name "root_fk1" identified by "111";

create publication pub_fk_cross database sys_fk_cross account acc_fk1;

-- @session:id=3&user=acc_fk1:root_fk1&password=111
create database sys_fk_cross from sys publication pub_fk_cross;

create database clone_fk_cross clone sys_fk_cross;
drop database if exists clone_fk_cross;
-- @session

drop publication pub_fk_cross;
drop account acc_fk1;
drop database sys_fk_cross;
drop database sys_fk_base;

-- case 3: cross account clone db with self reference table.
drop database if exists sys_self_ref;
create database sys_self_ref;

create table sys_self_ref.org(id int primary key, parent_id int, foreign key (parent_id) references sys_self_ref.org(id));
insert into sys_self_ref.org values(1,null),(2,1),(3,2);

drop account if exists acc_self;
create account acc_self admin_name "root_self" identified by "111";

create publication pub_self_ref database sys_self_ref account acc_self;

-- @session:id=4&user=acc_self:root_self&password=111
create database sub_self_ref from sys publication pub_self_ref;

create database clone_self_ref clone sub_self_ref;
select * from clone_self_ref.org;
insert into clone_self_ref.org values(4,3);
drop database if exists clone_self_ref;
drop database if exists sub_self_ref;
-- @session

drop publication pub_self_ref;
drop account acc_self;
drop database sys_self_ref;

-- case 4: cross account clone db with multi-level foreign key chain.
drop database if exists sys_fk_chain;
create database sys_fk_chain;

create table sys_fk_chain.t4(id int primary key);
create table sys_fk_chain.t3(id int primary key, t4_id int, foreign key (t4_id) references sys_fk_chain.t4(id));
create table sys_fk_chain.t2(id int primary key, t3_id int, foreign key (t3_id) references sys_fk_chain.t3(id));
create table sys_fk_chain.t1(id int primary key, t2_id int, foreign key (t2_id) references sys_fk_chain.t2(id));

insert into sys_fk_chain.t4 values(1),(2);
insert into sys_fk_chain.t3 values(1,1),(2,2);
insert into sys_fk_chain.t2 values(1,1),(2,2);
insert into sys_fk_chain.t1 values(1,1),(2,2);

drop account if exists acc_chain;
create account acc_chain admin_name "root_chain" identified by "111";

create publication pub_fk_chain database sys_fk_chain account acc_chain;

-- @session:id=5&user=acc_chain:root_chain&password=111
create database sub_fk_chain from sys publication pub_fk_chain;
create database clone_fk_chain clone sub_fk_chain;

select * from clone_fk_chain.t1;
select * from clone_fk_chain.t2;
select * from clone_fk_chain.t3;
select * from clone_fk_chain.t4;

insert into clone_fk_chain.t4 values(3);
insert into clone_fk_chain.t3 values(3,3);
insert into clone_fk_chain.t2 values(3,3);
insert into clone_fk_chain.t1 values(3,3);

select count(*) from clone_fk_chain.t1;
drop database if exists clone_fk_chain;
drop database if exists sub_fk_chain;
-- @session

drop publication pub_fk_chain;
drop account acc_chain;
drop database sys_fk_chain;
