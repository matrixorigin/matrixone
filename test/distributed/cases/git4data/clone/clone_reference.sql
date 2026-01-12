--- 1. within account, within db clone table, within db refer
drop database if exists db1;
create database db1;
create table db1.t1 (a int primary key);
create table db1.t2 (a int, b int, constraint `c1` foreign key (b) references db1.t1 (a));
create table db1.t3 clone db1.t2;
show create table db1.t3;

--- 2. within account, between db clone table, within db refer
drop database if exists db2;
create database db2;
create table db2.t1 clone db1.t2;
show create table db2.t1;

--- 3. within account, clone db, within db refer
drop database if exists db3;
create database db3 clone db1;
show create table db3.t2;
show create table db3.t3;

--- 4. within account, within db clone table, between db refer
create table db3.t4 clone db3.t2;
show create table db3.t4;

--- 5. within account, between db clone table, between db refer
create table db3.t5 clone db2.t1;
show create table db3.t5;

--- 6. between account, clone db, within db refer
create snapshot sp0 for account sys;
create account acc1 admin_name "root1" identified by "111";
create database db4 clone db1 {snapshot = "sp0"} to account acc1;
-- @session:id=1&user=acc1:root1&password=111
show create table db4.t2;
show create table db4.t3;
-- @session

--- 7. between account, clone table, within db refer
create table db4.t4 clone db1.t2 {snapshot = "sp0"} to account acc1;

--- 8. between account, clone table, between db refer
create table db4.t5 clone db2.t1 {snapshot = "sp0"} to account acc1;

--- 9. between account, clone db, between db refer
create database db5 clone db2 {snapshot = "sp0"} to account acc1;

drop account acc1;
drop snapshot sp0;
drop database db3;
drop database db2;
drop database db1;

--- 10. test issue #22297 (1): cannot drop the database
drop database if exists db6;
drop database if exists db7;

create database db6;
create table db6.t1(a int primary key );
create table db6.t2(a int, b int, constraint `c1` foreign key (b) references db6.t1 (a));

create database db7 clone db6;
show create table db7.t2;
drop database db6;
drop database db7;

--- 10. test issue #22297 (2): clone table to account failed
drop database if exists db8;
create database db8;

create account acc2 admin_name "root2" identified by "111";

create table db8.vector_index_01(a int primary key, b vecf32(128),c int,key c_k(c));
insert into db8.vector_index_01 values(9774 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]",3),(9775,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]",5),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]",3);
insert into db8.vector_index_01 values(9777, " [16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]",4),(9778,"[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]",4);

create index idx01 using ivfflat on db8.vector_index_01(b) lists=5 op_type "vector_l2_ops";

drop snapshot if exists sp1;
create snapshot sp1 for account;

-- @session:id=2&user=acc2:root2&password=111
drop database if exists test_pub01;
create database test_pub01;
-- @session

create table test_pub01.pub_table01 clone db8.vector_index_01 {snapshot = 'sp1'} to account acc2;

drop database db8;
drop account acc2;
drop snapshot sp1;

-- case: chain reference
drop database if exists db9;
create database db9;

create table db9.t4(id int primary key);
create table db9.t3(id int primary key, t4_id int, foreign key (t4_id) references db9.t4(id));
create table db9.t2(id int primary key, t3_id int, foreign key (t3_id) references db9.t3(id));
create table db9.t1(id int primary key, t2_id int, foreign key (t2_id) references db9.t2(id));

insert into db9.t4 values(1),(2);
insert into db9.t3 values(1,1),(2,2);
insert into db9.t2 values(1,1),(2,2);
insert into db9.t1 values(1,1),(2,2);

create database db10 clone db9;

select * from db10.t1;
select * from db10.t2;
select * from db10.t3;
select * from db10.t4;

insert into db10.t4 values(3);
insert into db10.t3 values(3,3);
insert into db10.t2 values(3,3);
insert into db10.t1 values(3,3);

select count(*) from db10.t1;

drop database db9;
drop database db10;