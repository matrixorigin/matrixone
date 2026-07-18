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

drop database replace_fk_unique_lock;
