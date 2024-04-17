set global enable_privilege_cache = off;
drop account if exists  acc1;
create account if not exists acc1 ADMIN_NAME 'admin' IDENTIFIED BY '123';
alter account acc1 restricted;

-- @session:id=1&user=acc1:admin&password=123
show databases;
create table r_test(c1 int);
insert into r_test values(3);
update r_test set c1=5;
truncate table r_test;
drop table r_test;
-- @session
drop account if exists acc1;

create account if not exists acc1 ADMIN_NAME 'admin' IDENTIFIED BY '123efg' comment 'account comment';
-- @session:id=5&user=acc1:admin&password=123efg
create database res_test;
use res_test;
create table r_test(c1 int,c2 varchar(20), unique index ui(c1));
insert into r_test values(3,'a'),(4,'b'),(7,'h');
update r_test set c1=2 where c2='a';
-- @session
alter account acc1 restricted;
-- @session:id=5&user=acc1:admin&password=123efg
create database rdb;
drop database rdb;

create table r1(c1 int,c2 varchar(20));
insert into r_test values(8,'c');
load data infile '$resources/load_data/integer_numbers_1.csv' into table r_test fields terminated by ',';
update r_test set c1=5 where c2='h';
delete from r_test where c1=4;
select * from r_test;
truncate table r_test;
create view r_view as select * from r_test;
drop view r_view;

create  table ti2(a INT primary key AUTO_INCREMENT, b INT, c INT);
create  table tm2(a INT primary key AUTO_INCREMENT, b INT, c INT);
insert into ti1 values (1,1,1), (2,2,2);
insert into ti2 values (1,1,1), (2,2,2);
alter table ti1 add constraint fi1 foreign key (b) references ti2(a);

show databases;
use res_test;
desc r_test;
show tables;
show create table r_test;
show columns from r_test;
show full columns from r_test;
show variables where value = 'MatrixOne';
show grants;
show grants for 'admin'@'localhost';
SHOW CREATE TABLE information_schema.columns;
show index from r_test;
show node list;
show locks;
show table_values from r_test;
show column_number from r_test;
show TRIGGERS;
show TRIGGERS like '*%';
show collation like 'utf8mb4_general_ci%';
show full tables;
show full tables from res_test;;

select version();
alter database test set mysql_compatibility_mode = '8.0.30-MatrixOne-v0.7.0';
select privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where privilege_name = 'values';
select user_name from mo_catalog.mo_user;

create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';

alter table r_test drop index ui;
create role role1;
grant all on table *.* to role1;
grant create table, drop table on database *.* to role1;
create user user1 identified by 'pass1';
grant role1 to user1;
drop user user1;
drop role role1;
drop database account_res;
-- @session

alter account acc1 suspend;
select account_name,status,comments from mo_catalog.mo_account where account_name='acc1';

alter account acc1 open;
select account_name,status,comments from mo_catalog.mo_account where account_name='acc1';
-- @session:id=6&user=acc1:admin&password=123efg
create database rdb;
use rdb;
create table r1(c1 int,c2 varchar(20));
insert into res_test.r_test values(8,'c');
update res_test.r_test set c1=5 where c2='h';
delete from res_test.r_test where c1=4;
delete from system.statement_info;
select * from res_test.r_test;
truncate table res_test.r_test;
create view r_view as select * from res_test.r_test;
drop view r_view;

show databases;
use res_test;
show tables;
show create table r_test;
show columns from r_test;
show full columns from r_test;
show variables where value = 'MatrixOne';
show grants for 'hnadmin'@'localhost';
SHOW CREATE TABLE information_schema.columns;
show index from r_test;
show node list;
show locks;
show table_values from r_test;
show column_number from r_test;
show TRIGGERS;
show TRIGGERS like '*%';
show collation like 'utf8mb4_general_ci%';
show full tables;
show full tables from account_res;

select privilege_name, obj_type, privilege_level from mo_catalog.mo_role_privs where privilege_name = 'values';
select user_name from mo_catalog.mo_user;

create role role1;
grant all on table *.* to role1;
grant create table, drop table on database *.* to role1;
create user user1 identified by 'pass1';
grant role1 to user1;
drop user user1;
drop role role1;
drop database rdb;
drop database res_test;
-- @session
drop account if exists acc1;
set global enable_privilege_cache = on;
