drop database if exists insert_auto_pk;
create database insert_auto_pk; use insert_auto_pk;

-- -- if you insert multiple rows using a single INSERT statement, LAST_INSERT_ID() returns the value generated for the last inserted row. 
-- -- https://docs.matrixorigin.cn/en/1.1.3/MatrixOne/Reference/SQL-Reference/Data-Manipulation-Language/information-functions/last-insert-id/


-- Basic mode of testing: In three configuration modes, two sessions concurrently insert same values using [auto cache | manually specified] methods.

-- -- "None" means that no dup check when there is no value for auto-pk col
select `variable_value` from mo_catalog.mo_mysql_compatibility_mode where dat_name ="insert_auto_pk" and `variable_name`="unique_check_on_autoincr";


-- --
-- -- single primary key with auto-increment
-- --

-- INT / UIT
-- all select result should be empty set
drop table if exists t0;
create table t0(a int auto_increment, b int, primary key(a));
insert into t0(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t0(a) values (3);

delete from t0 where a=2;
insert into t0(b) values (1), (2);

commit;
-- @session}

insert into t0(b) values (1), (2);

select a, count(*) from t0 group by a having count(*) > 1;

drop table if exists t1;
create table t1(a int unsigned auto_increment , b int, primary key(a));
insert into t1(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t1(a) values (3);


delete from t1 where a=2;
insert into t1(b) values (1), (2);

commit;
-- @session}

insert into t1(b) values (1), (2);
select a, count(*) from t1 group by a having count(*) > 1;

-- BIG INT / UINT
-- all select result should be empty set
drop table if exists t2;
create table t2(a bigint auto_increment, b int, primary key(a));
insert into t2(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t2(a) values (3);
delete from t2 where a=2;
insert into t2(b) values (1), (2);

commit;
-- @session}

insert into t2(b) values (1), (2);
select a, count(*) from t2 group by a having count(*) > 1;

drop table if exists t3;
create table t3(a bigint unsigned auto_increment , b int, primary key(a));
insert into t3(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t3(a) values (3);


delete from t3 where a=2;
insert into t3(b) values (1), (2);

commit;
-- @session}

insert into t3(b) values (1), (2);

select a, count(*) from t3 group by a having count(*) > 1;

--
-- compound primary key with one col is auto-incrment type
--
drop table if exists t6;
create table t6(a int auto_increment, b int, primary key(a, b));
insert into t6(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t6(a, b) values (3, 3);


delete from t6 where a=2;
insert into t6(b) values (1), (2);

commit;
-- @session}

insert into t6(b) values (1), (2);

select a, count(*) from t6 group by a, b having count(*) > 1;

drop table if exists t7;
create table t7(a int unsigned auto_increment , b int, primary key(a, b));
insert into t7(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t7(a, b) values (3, 3);


delete from t7 where a=2;
insert into t7(b) values (1), (2);

commit;
-- @session}

insert into t7(b) values (1), (2);



select a, count(*) from t7 group by a, b having count(*) > 1;

-- BIG INT / UINT
-- all select result should be empty set
drop table if exists t8;
create table t8(a bigint auto_increment, b int, primary key(a, b));
insert into t8(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t8(a, b) values (3, 3);


delete from t8 where a=2;
insert into t8(b) values (1), (2);

commit;
-- @session}

insert into t8(b) values (1), (2);



select a, count(*) from t8 group by a, b having count(*) > 1;

drop table if exists t9;
create table t9(a bigint unsigned auto_increment , b int, primary key(a, b));
insert into t9(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t9(a, b) values (3, 3);


delete from t9 where a=2;
insert into t9(b) values (1), (2);

commit;
-- @session}

insert into t9(b) values (1), (2);

select a, count(*) from t9 group by a, b having count(*) > 1;

-- "Check" means that no dup check when there is no value for auto-pk col
alter database insert_auto_pk set unique_check_on_autoincr = "Check";
select `variable_value` from mo_catalog.mo_mysql_compatibility_mode where dat_name ="insert_auto_pk" and `variable_name`="unique_check_on_autoincr";


-- INT / UIT
-- all select result should be empty set
drop table if exists t0;
create table t0(a int auto_increment, b int, primary key(a));
insert into t0(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t0(a) values (3);


delete from t0 where a=2;
insert into t0(b) values (1), (2);

commit;
-- @session}

insert into t0(b) values (1), (2);



select a, count(*) from t0 group by a having count(*) > 1;

drop table if exists t1;
create table t1(a int unsigned auto_increment , b int, primary key(a));
insert into t1(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t1(a) values (3);


delete from t1 where a=2;
insert into t1(b) values (1), (2);

commit;
-- @session}

insert into t1(b) values (1), (2);

select a, count(*) from t1 group by a having count(*) > 1;

-- BIG INT / UINT
-- all select result should be empty set
drop table if exists t2;
create table t2(a bigint auto_increment, b int, primary key(a));
insert into t2(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t2(a) values (3);

delete from t2 where a=2;
insert into t2(b) values (1), (2);

commit;
-- @session}

insert into t2(b) values (1), (2);

select a, count(*) from t2 group by a having count(*) > 1;

drop table if exists t3;
create table t3(a bigint unsigned auto_increment , b int, primary key(a));
insert into t3(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t3(a) values (3);


delete from t3 where a=2;
insert into t3(b) values (1), (2);

commit;
-- @session}

insert into t3(b) values (1), (2);



select a, count(*) from t3 group by a having count(*) > 1;



--
-- compound primary key with one col is auto-incrment type
--
drop table if exists t6;
create table t6(a int auto_increment, b int, primary key(a, b));
insert into t6(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t6(a, b) values (3, 3);


delete from t6 where a=2;
insert into t6(b) values (1), (2);

commit;
-- @session}

insert into t6(b) values (1), (2);

select a, count(*) from t6 group by a, b having count(*) > 1;

drop table if exists t7;
create table t7(a int unsigned auto_increment , b int, primary key(a, b));
insert into t7(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t7(a, b) values (3, 3);

delete from t7 where a=2;
insert into t7(b) values (1), (2);

commit;
-- @session}

insert into t7(b) values (1), (2);
select a, count(*) from t7 group by a, b having count(*) > 1;

-- BIG INT / UINT
-- all select result should be empty set
drop table if exists t8;
create table t8(a bigint auto_increment, b int, primary key(a, b));
insert into t8(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t8(a, b) values (3, 3);

delete from t8 where a=2;
insert into t8(b) values (1), (2);

commit;
-- @session}

insert into t8(b) values (1), (2);
select a, count(*) from t8 group by a, b having count(*) > 1;

drop table if exists t9;
create table t9(a bigint unsigned auto_increment , b int, primary key(a, b));
insert into t9(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t9(a, b) values (3, 3);

delete from t9 where a=2;
insert into t9(b) values (1), (2);

commit;
-- @session}

insert into t9(b) values (1), (2);
select a, count(*) from t9 group by a, b having count(*) > 1;
-- "Error" means that no dup check when there is no value for auto-pk col
alter database insert_auto_pk set unique_check_on_autoincr = "Error";
select `variable_value` from mo_catalog.mo_mysql_compatibility_mode where dat_name ="insert_auto_pk" and `variable_name`="unique_check_on_autoincr";

-- INT / UIT
-- all select result should be empty set
drop table if exists t0;
create table t0(a int auto_increment, b int, primary key(a));
insert into t0(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t0(a) values (3);

delete from t0 where a=2;
insert into t0(b) values (1), (2);

commit;
-- @session}

insert into t0(b) values (1), (2);
select a, count(*) from t0 group by a having count(*) > 1;

drop table if exists t1;
create table t1(a int unsigned auto_increment , b int, primary key(a));
insert into t1(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t1(a) values (3);


delete from t1 where a=2;
insert into t1(b) values (1), (2);

commit;
-- @session}

insert into t1(b) values (1), (2);
select a, count(*) from t1 group by a having count(*) > 1;

-- BIG INT / UINT
-- all select result should be empty set
drop table if exists t2;
create table t2(a bigint auto_increment, b int, primary key(a));
insert into t2(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t2(a) values (3);

delete from t2 where a=2;
insert into t2(b) values (1), (2);

commit;
-- @session}

insert into t2(b) values (1), (2);

select a, count(*) from t2 group by a having count(*) > 1;

drop table if exists t3;
create table t3(a bigint unsigned auto_increment , b int, primary key(a));
insert into t3(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t3(a) values (3);

delete from t3 where a=2;
insert into t3(b) values (1), (2);

commit;
-- @session}

insert into t3(b) values (1), (2);

select a, count(*) from t3 group by a having count(*) > 1;

--
-- compound primary key with one col is auto-incrment type
--
drop table if exists t6;
create table t6(a int auto_increment, b int, primary key(a, b));
insert into t6(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t6(a, b) values (3, 3);


delete from t6 where a=2;
insert into t6(b) values (1), (2);

commit;
-- @session}

insert into t6(b) values (1), (2);
select a, count(*) from t6 group by a, b having count(*) > 1;

drop table if exists t7;
create table t7(a int unsigned auto_increment , b int, primary key(a, b));
insert into t7(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t7(a, b) values (3, 3);
delete from t7 where a=2;
insert into t7(b) values (1), (2);

commit;
-- @session}

insert into t7(b) values (1), (2);

select a, count(*) from t7 group by a, b having count(*) > 1;

-- BIG INT / UINT
-- all select result should be empty set
drop table if exists t8;
create table t8(a bigint auto_increment, b int, primary key(a, b));
insert into t8(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t8(a, b) values (3, 3);
delete from t8 where a=2;
insert into t8(b) values (1), (2);

commit;
-- @session}

insert into t8(b) values (1), (2);

select a, count(*) from t8 group by a, b having count(*) > 1;

drop table if exists t9;
create table t9(a bigint unsigned auto_increment , b int, primary key(a, b));
insert into t9(b) values (1), (2);
-- @session:id=1{
begin;
use insert_auto_pk;
insert into t9(a, b) values (3, 3);
delete from t9 where a=2;
insert into t9(b) values (1), (2);

commit;
-- @session}

insert into t9(b) values (1), (2);

select a, count(*) from t9 group by a, b having count(*) > 1;

-- drop database insert_auto_pk;