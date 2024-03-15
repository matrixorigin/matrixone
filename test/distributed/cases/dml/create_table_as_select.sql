-- cross-db cross-tenant
drop database if exists db2;
create database db2;
use db2;
drop role if exists role_r1;
drop role if exists role_r2;
drop user if exists role_u1;
drop user if exists role_u2;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
create role role_r2;
create user role_u2 identified by '111' default role role_r2;
drop table if exists t1;
create table t1(col1 int);
insert into t1 values(1);
insert into t1 values(2);
grant create database, drop database on account * to role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant create table, drop table on database *.* to role_r1;
grant show tables on database * to role_r1;
grant select on table * to role_r1;
grant insert on table * to role_r1;
-- @session:id=1&user=sys:role_u1:role_r1&password=111
use db2;
drop table if exists t2;
create table t2 as select * from t1;
-- @session
-- @session:id=2&user=sys:role_u2:role_r2&password=111
use db2;
drop table if exists t3;
create table t3 as select * from t2;
select * from t3;
-- @session
grant create database, drop database on account * to role_r2;
grant show databases on account * to role_r2;
grant connect on account * to role_r2;
grant create table, drop table on database *.* to role_r2;
grant show tables on database * to role_r2;
grant select on table * to role_r2;
grant insert on table * to role_r2;
-- @session:id=3&user=sys:role_u2:role_r2&password=111
use db2;
drop table if exists t3;
create table t3 as select * from t2;
select * from t3;
-- @session
drop table t1;
drop table t2;
drop table t3;
drop role role_r1;
drop role role_r2;
drop user role_u1;
drop user role_u2;
drop database db2;
