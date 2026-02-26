select enable_fault_injection();
select add_fault_point('fj/cn/recv/err', ':::', 'echo', 5, 'mo_tables');
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
begin;
drop database db1;
-- @session:id=1{
use db1;
-- @wait:0:commit
create table t2(a int);
-- @session}
commit;

create database db1;
use db1;
create table t2(a int);
begin;
drop table t2;
-- @session:id=1{
-- @wait:0:commit
drop database db1;
-- @session}
commit;

create database db1;
use db1;
create table t2(a int);
begin;
drop database db1;
-- @session:id=1{
use db1;
-- @wait:0:commit
drop table t2;
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
drop database db1;
-- @session:id=1{
use db1;
-- @wait:0:commit
alter table t2 add b int;
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
create table t2(a int);
begin;
drop database db1;
-- @session:id=1{
use db1;
-- @wait:0:commit
create index t2_idx on t2(a);
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

create database db1;
use db1;
create table t2(a int, key t2_idx(a));
begin;
drop database db1;
-- @session:id=1{
use db1;
-- @wait:0:commit
drop index t2_idx on t2;
-- @session}
commit;

create database db1;
use db1;
create table t2(a int);
begin;
insert into t2 values(1);
-- @session:id=1{
-- @wait:0:commit
drop database db1;
-- @session}
commit;

create database db1;
use db1;
create table t1(a int primary key);
create table t2(a int primary key, b int);
begin;
insert into t1 values(1);
-- @session:id=1{
use db1;
alter table t2 add constraint fk_t2_t1 foreign key(b) references t1(a);
-- @session}
commit;
begin;
insert into t1 values(2);
-- @session:id=1{
use db1;
alter table t2 drop foreign key fk_t2_t1;
-- @session}
commit;

select disable_fault_injection();
