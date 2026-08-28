drop database if exists db0;
create database db0;
use db0;

create table s1(a int);
insert into s1 select * from generate_series(1,5)g;

-- issue#27091: database clone must restore database-scoped stored procedures,
-- including arguments and the SQL mode used to parse their bodies on CALL.
create procedure db0.p_answer() 'begin select 42 as answer; end';
create procedure db0.p_double(in input_value int, out output_value int) 'begin set output_value = input_value * 2; end';
create procedure db0.p_source_qualified() 'begin select count(*) as answer from db0.s1; end';
create procedure db0.p_labeled_repeat() 'begin declare n int default 0; repeat_label: repeat set n = n + 1; select count(*) as answer from db0.s1; until n >= 1 end repeat repeat_label; end';
create procedure db0.p_labeled_loop() 'begin loop_label: loop select count(*) as answer from db0.s1; if true then leave loop_label; end if; iterate loop_label; end loop loop_label; end';
set sql_mode = 'PIPES_AS_CONCAT';
create procedure db0.p_sql_mode() 'begin select ''a'' || ''b'' as answer; end';
set sql_mode = default;

-- Functions are catalog metadata too. They must be restored before a view that
-- binds them, and source-qualified routine queries must bind the clone.
create function db0.f_clone_answer() returns int language sql as 'select count(*) from db0.s1';
create view db0.v_clone_answer as select f_clone_answer() as answer;

create database db0_copy_0 clone db0;
show tables from db0_copy_0;
select * from db0_copy_0.s1;
select name from mo_catalog.mo_stored_procedure
where db = 'db0_copy_0'
order by name;
select name from mo_catalog.mo_user_defined_function
where db = 'db0_copy_0'
order by name;
call db0_copy_0.p_answer();
call db0_copy_0.p_double(21, @db0_copy_double);
select @db0_copy_double;
call db0_copy_0.p_sql_mode();

create database db0_copy_1 clone db0 to account sys;
show tables from db0_copy_1;
select * from db0_copy_1.s1;
call db0_copy_1.p_answer();

drop database db0;
use db0_copy_0;
call p_source_qualified();
call p_labeled_repeat();
call p_labeled_loop();
select f_clone_answer();
select * from v_clone_answer;

-- A routine may execute ALTER through the normal-SQL path. Nested foreign-key
-- references must bind the clone after its source database has been removed.
drop database if exists db_alter_clone_source;
drop database if exists db_alter_clone_copy;
create database db_alter_clone_source;
create table db_alter_clone_source.parent (id int primary key);
create table db_alter_clone_source.child (parent_id int);
create procedure db_alter_clone_source.p_add_fk() 'begin alter table db_alter_clone_source.child add constraint fk_child_parent foreign key (parent_id) references db_alter_clone_source.parent(id); end';
create database db_alter_clone_copy clone db_alter_clone_source;
drop database db_alter_clone_source;
call db_alter_clone_copy.p_add_fk();
drop database db_alter_clone_copy;

-- View dependency sorting runs before the restore loop. Its UDF lookup must
-- therefore use the source snapshot, even after the live source is gone.
drop database if exists db_snapshot_udf;
drop database if exists db_snapshot_udf_copy;
drop snapshot if exists sp_snapshot_udf;
create database db_snapshot_udf;
use db_snapshot_udf;
create table db_snapshot_udf.t(a int);
insert into db_snapshot_udf.t values (7);
create function db_snapshot_udf.f_snapshot_answer() returns int language sql as 'select count(*) from db_snapshot_udf.t';
create view db_snapshot_udf.v_snapshot_answer as select f_snapshot_answer() as answer;
create snapshot sp_snapshot_udf for database db_snapshot_udf;
drop database db_snapshot_udf;
create database db_snapshot_udf_copy clone db_snapshot_udf {snapshot = "sp_snapshot_udf"};
use db_snapshot_udf_copy;
select f_snapshot_answer();
select * from v_snapshot_answer;
drop snapshot sp_snapshot_udf;
drop database db_snapshot_udf_copy;

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
create procedure db1.p_cross_account() 'begin select 42 as answer; end';
create function db1.f_cross_account() returns int language sql as '42';

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
call db1_copy.p_cross_account();
use db1_copy;
select f_cross_account();
-- @session

drop snapshot if exists sp0;
create snapshot sp0 for account acc1;

create database db1_copy_copy clone db1_copy {snapshot = "sp0"} to account acc2;
-- @session:id=3&user=acc2:root2&password=111
show tables from db1_copy_copy;
select * from db1_copy_copy.t1;
call db1_copy_copy.p_cross_account();
use db1_copy_copy;
select f_cross_account();
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
