set global enable_privilege_cache = off;
drop role if exists r1;
create role r1;
drop user if exists u1;
create user u1 identified by '111' default role r1;

-- @session:id=2&user=sys:u1:r1&password=111
show tables;
use mo_catalog;
create database t;
-- @session

grant show databases on account * to r1;
grant show tables on database * to r1;

-- @session:id=2&user=sys:u1:r1&password=111
-- r1 without the privilege CONNECT
use mo_catalog;
show tables;
create database t;
-- @session

grant connect on account * to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use mo_catalog;
show tables;
create database t;
-- @session

grant create database on account * to r1;

-- @session:id=2&user=sys:u1:r1&password=111
create database t;
use t;
create table A(a int);
drop table A;
-- @session

grant create table on database * to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use t;
create table A(a int);
insert into A values (1),(1);
-- @session

grant insert on table t.* to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use t;
insert into A values (1),(1);
select a from A;
-- @session

grant select on table t.* to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use t;
select a from A;
update A set a = 2 where a = 1;
update A set a = 2;
-- @session

grant update on table t.* to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use t;
update A set a = 2 where a = 1;
update A set a = 2;
delete from A where a = 2;
delete from A;
-- @session

grant delete on table t.* to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use t;
delete from A where a = 2;
delete from A;
select a from A;
drop table A;
-- @session

grant drop table on database t to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use t;
drop table A;
create database s;
use s;
create table B(b int);
insert into B values (1),(1);
-- @session

grant select,insert,update,delete on table s.* to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use s;
insert into B values (1),(1);
select b from B;
update B set b = 2 where b=1;
update B set b = 2;
delete from B where b = 1;
delete from B;
drop table B;
-- @session

grant drop table on database s to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use s;
drop table B;
drop database t;
drop database s;
-- @session

-- multi tables in multi database

-- @session:id=2&user=sys:u1:r1&password=111
create database v;
use v;
-- @session

grant create table,drop table on database v to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use v;
create table A(a int);
create table B(b int);
create table C(c int);
create table D(d int);
create table E(e int);
create table F(f int);
create table G(g int);
create table H(h int);
-- @session

grant select on table v.A to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use v;
select * from A,B;
select * from A,B where A.a = B.b;
-- @session

grant select on table v.B to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use v;
select * from A,B;
select * from A,B where A.a = B.b;
update C,D set c = d+1 where c = d;
-- @session

grant update on table v.C to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use v;
update C,D set c = d+1 where c = d;
-- @session

grant update on table v.D to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use v;
update C,D set c = d+1 where c = d;
-- @session

-- @session:id=2&user=sys:u1:r1&password=111
use v;
delete E,F from E,F where E.e = F.f;
-- @session

grant update on table v.E to r1;
grant delete on table v.F to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use v;
delete E,F from E,F where E.e = F.f;
-- @session

grant delete on table v.E to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use v;
delete E,F from E,F where E.e = F.f;
-- @session

-- @session:id=2&user=sys:u1:r1&password=111
use v;
insert into G select A.a from A,B where A.a = B.b;
-- @session

grant insert on table v.G to r1;

-- @session:id=2&user=sys:u1:r1&password=111
use v;
insert into G select A.a from A,B where A.a = B.b;
-- @session

-- @session:id=2&user=sys:u1:r1&password=111
drop database if exists t;
drop database if exists s;
drop database if exists v;
-- @session

grant drop database on account * to r1;

-- @session:id=2&user=sys:u1:r1&password=111
drop database if exists t;
drop database if exists s;
drop database if exists v;
-- @session

drop role if exists r1;
drop user if exists u1;
set global enable_privilege_cache = on;