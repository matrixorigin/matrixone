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

create table parent_generated_unique(
  id int primary key,
  a int,
  g int generated always as (a + 1),
  unique key uk_g(g)
);
create table child_generated_unique(id int primary key, parent_id int,
  foreign key(parent_id) references parent_generated_unique(id) on delete cascade);
insert into parent_generated_unique(id, a) values (1, 10);
insert into child_generated_unique values (1, 1);
replace into parent_generated_unique(id, a) values (2, 10);
select id, a, g from parent_generated_unique;
select * from child_generated_unique;

create table parent_auto_zero(id int auto_increment primary key, v varchar(20));
create table child_auto_zero(id int primary key, parent_id int,
  foreign key(parent_id) references parent_auto_zero(id) on delete cascade);
set session sql_mode = 'NO_AUTO_VALUE_ON_ZERO';
insert into parent_auto_zero values (0, 'old');
insert into child_auto_zero values (1, 0);
set session sql_mode = '';
replace into parent_auto_zero values (0, 'allocated');
select * from parent_auto_zero order by id;
select * from child_auto_zero;
replace into parent_auto_zero values (0x0, 'allocated-hex');
replace into parent_auto_zero values (b'0', 'allocated-bit');
select * from parent_auto_zero order by id;
select * from child_auto_zero;
set session sql_mode = 'NO_AUTO_VALUE_ON_ZERO';
replace into parent_auto_zero values (0, 'explicit-zero');
select * from parent_auto_zero order by id;
select * from child_auto_zero;
set session sql_mode = '';

create table parent_cached(id int primary key, v int);
create table child_cached(id int primary key, parent_id int,
  foreign key(parent_id) references parent_cached(id) on delete cascade);
insert into parent_cached values (1, 10);
insert into child_cached values (1, 1);
prepare replace_cached from 'replace into parent_cached values (1, 20)';
set foreign_key_checks = 0;
execute replace_cached;
select * from child_cached;
set foreign_key_checks = 1;
execute replace_cached;
select * from child_cached;
deallocate prepare replace_cached;

insert into child_cached values (2, 1);
replace into parent_cached values (1, 30);
select * from child_cached;
insert into child_cached values (3, 1);
set foreign_key_checks = 0;
replace into parent_cached values (1, 30);
select * from child_cached;
set foreign_key_checks = 1;
replace into parent_cached values (1, 30);
select * from child_cached;

create table parent_lock_order(id int primary key, a int unique, b int unique);
create table child_lock_order(id int primary key, parent_b int, parent_a int,
  foreign key(parent_b) references parent_lock_order(b),
  foreign key(parent_a) references parent_lock_order(a));
insert into parent_lock_order values (1, 10, 20);
begin;
replace into parent_lock_order values (1, 11, 21);
-- @session:id=1{
use replace_fk_unique_lock;
set session lock_wait_timeout = 1;
begin;
-- @pattern
insert into child_lock_order values (1, 20, 10);
rollback;
-- @session}
commit;
select * from parent_lock_order;
select * from child_lock_order;

create table parent_dynamic(id int primary key, u int unique);
create table child_dynamic(id int primary key, parent_id int,
  foreign key(parent_id) references parent_dynamic(id) on delete cascade);
create table replace_source(id int, u int);
insert into parent_dynamic values (1, 10), (2, 20);
insert into child_dynamic values (1, 1), (2, 2);
prepare replace_dynamic from 'replace into parent_dynamic values (?, ?)';
set @replace_id = 1;
set @replace_u = 11;
execute replace_dynamic using @replace_id, @replace_u;
select * from parent_dynamic order by id;
select * from child_dynamic order by id;
deallocate prepare replace_dynamic;
insert into replace_source values (2, 22);
replace into parent_dynamic select id, u from replace_source;
select * from parent_dynamic order by id;
select * from child_dynamic order by id;

drop database replace_fk_unique_lock;
