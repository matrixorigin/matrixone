-- @suite

-- @case
-- @desc:test REPLACE and child FK validation contend on non-PK unique keys
-- @label:bvt
drop database if exists replace_fk_unique_lock;
create database replace_fk_unique_lock;
use replace_fk_unique_lock;

create table parent_restrict(id int primary key, u int unique);
create table child_restrict(id int primary key, parent_u int,
  foreign key(parent_u) references parent_restrict(u) on delete restrict);
insert into parent_restrict values (1, 10);
begin;
replace into parent_restrict values (1, 20);
-- @session:id=1{
use replace_fk_unique_lock;
set session lock_wait_timeout = 1;
begin;
-- @pattern
insert into child_restrict values (1, 10);
rollback;
-- @session}
commit;
select * from parent_restrict;
select * from child_restrict;

create table parent_cascade(id int primary key, u int unique);
create table child_cascade(id int primary key, parent_u int,
  foreign key(parent_u) references parent_cascade(u) on delete cascade);
insert into parent_cascade values (1, 10);
insert into child_cascade values (1, 10);
begin;
replace into parent_cascade values (1, 20);
-- @session:id=1{
use replace_fk_unique_lock;
set session lock_wait_timeout = 1;
begin;
-- @pattern
insert into child_cascade values (2, 10);
rollback;
-- @session}
commit;
select * from parent_cascade;
select * from child_cascade;

create table parent_decimal(id int primary key, u decimal(5,2) unique);
create table child_decimal(id int primary key, parent_u decimal(5,3),
  foreign key(parent_u) references parent_decimal(u) on delete restrict);
insert into parent_decimal values (1, 1.23);
begin;
insert into child_decimal values (1, 1.230);
-- @session:id=1{
use replace_fk_unique_lock;
set session lock_wait_timeout = 1;
begin;
-- @pattern
replace into parent_decimal values (1, 2.00);
rollback;
-- @session}
rollback;
select * from parent_decimal;
select * from child_decimal;

create table parent_generated(
  id int primary key,
  g int generated always as (id + 1),
  u int unique
);
create table child_generated(id int primary key, parent_id int,
  foreign key(parent_id) references parent_generated(id) on delete cascade);
insert into parent_generated(id, u) values (1, 10), (2, 20);
insert into child_generated values (1, 1), (2, 2);
replace into parent_generated values (3, 10);
replace into parent_generated(id, g, u) values (4, default, 20);
select id, g, u from parent_generated order by id;
select * from child_generated;

drop database replace_fk_unique_lock;
