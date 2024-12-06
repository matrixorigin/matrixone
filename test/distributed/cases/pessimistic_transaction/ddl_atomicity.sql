create database db1;
use db1;
begin;
create table t2(a int);
-- @session:id=1{
-- @wait:0:commit
drop database db1;
-- @session}
commit;

create database db1;
use db1;
create table t2(a int);
begin;
alter table t2 add b int;
-- @session:id=1{
-- @wait:0:commit
drop database db1;
-- @session}
commit;

create database db1;
use db1;
create table t2(a int);
begin;
create index t2_idx on t2(a);
-- @session:id=1{
-- @wait:0:commit
drop database db1;
-- @session}
commit;

create database db1;
use db1;
create table t2(a int, key t2_idx(a));
begin;
drop index t2_idx on t2;
-- @session:id=1{
-- @wait:0:commit
drop database db1;
-- @session}
commit;