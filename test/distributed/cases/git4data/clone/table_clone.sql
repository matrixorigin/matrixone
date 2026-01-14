drop database if exists db1;
create database db1;
use db1;

create table t1(a int auto_increment, b int, primary key(a));
insert into t1(b) values(1),(2);
insert into t1(a,b) values(20,20),(21,21);
insert into t1(b) values(20000),(20001);
insert into t1(a,b) values(40000, 40000);

create table t2 clone t1;

select enable_fault_injection();
-- @ignore:0
select add_fault_point('fj/cn/subscribe_table_fail',':::','echo',40,'db1.t2');
select * from t2 order by a asc;
select disable_fault_injection();

select * from t2 order by a asc;

insert into t2(b) values(3),(4);
insert into t2(a,b) values (3, 5),(4, 6);
select * from t2 order by a asc;

create table t3 clone t2;
select * from t3 order by a asc;

insert into t3(b) values(7),(8);
insert into t3(a,b) values(5,9),(6,10);
select * from t3 order by a asc;

delete from t3 where a mod 2 = 0 or a > 100;
select * from t3 order by a asc;

insert into t3(b) values(100),(101);
insert into t3(a,b) values(2,2),(4,4),(6,6);
select * from t3 order by a asc;

create table t4 (a int, b int) cluster by(a,b);
insert into t4 select *,* from generate_series(1, 5)g;

create table t5 clone t4 to account sys;
select * from t5;

drop snapshot if exists sp0;
create snapshot sp0 for table db1 t4;
create table t6 clone t4 to account sys;
select * from t6;

drop account if exists acc1;
create account acc1 admin_name "root1" identified by "111";

create table t4_copy clone t4 {snapshot = "sp0"} to account acc1;
create table db1_copy.t4_copy clone t4 {snapshot = "sp0"} to account acc1;
-- @session:id=2&user=acc1:root1&password=111
create database db1_copy;
-- @session

create table db1_copy.t4_copy clone t4 {snapshot = "sp0"} to account acc1;
-- @session:id=3&user=acc1:root1&password=111
select * from db1_copy.t4_copy;
-- @session

drop snapshot if exists sp1;
create snapshot sp1 for account acc1;

drop account if exists acc2;
create account acc2 admin_name "root2" identified by "111";

create table db1_copy_copy.t4_copy_copy clone t4_copy {snapshot = "sp1"} to account acc2;

-- @session:id=4&user=acc2:root2&password=111
create database db1_copy_copy;
-- @session

create table db1_copy_copy.t4_copy_copy clone db1_copy.t4_copy to account acc2;
create table db1_copy_copy.t4_copy_copy clone db1_copy.t4_copy {snapshot = "sp1"} to account acc2;

-- @session:id=5&user=acc2:root2&password=111
select * from db1_copy_copy.t4_copy_copy;

drop snapshot if exists sp3;
create snapshot sp3 for table db1_copy_copy t4_copy_copy;

create table db1.t7 clone db1_copy_copy.t4_copy_copy {snapshot = "sp3"} to account acc1;
-- @session

create table db1.t8 (id INT auto_increment primary key, content text, fulltext(content));
insert into db1.t8(content) values ("this is a test for clone fulltext table 1");
insert into db1.t8(content) values ("this is a test for clone fulltext table 2");
insert into db1.t8(content) values ("this is a test for clone fulltext table 3");

create table db1.t8_copy clone db1.t8;
select * from db1.t8_copy order by id asc;


create table db1.t9(a int primary key, b vecf32(3));
insert into db1.t9 values(1, "[1,2,3]");
insert into db1.t9 values(2, "[1,2,4]");
insert into db1.t9 values(3, "[1,2.4,4]");
create index idx using IVFFLAT on db1.t9(b);

create table db1.t9_copy clone db1.t9;
select * from db1.t9_copy order by a asc;


drop snapshot if exists sp0;
drop snapshot if exists sp1;
drop account if exists acc1;
drop account if exists acc2;
drop database if exists db1;