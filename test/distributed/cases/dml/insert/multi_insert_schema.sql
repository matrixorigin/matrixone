-- Multi-table INSERT: destination tables with different schemas
drop database if exists mi_schema;
create database mi_schema;
use mi_schema;

create table src (id int primary key, name varchar(30), amount decimal(12,3), ts datetime, flag bool, note text);
insert into src values
  (1, 'alpha',   10.500, '2024-01-01 10:00:00', true,  'first row'),
  (2, 'beta',    20.250, '2024-02-02 11:30:00', false, 'second row'),
  (3, 'gamma',  -30.125, '2024-03-03 12:45:00', true,  NULL),
  (4, 'delta',    0.000, '2024-04-04 00:00:00', NULL,  'fourth row'),
  (5, 'epsilon', 99999.999, '2024-05-05 23:59:59', false, 'fifth row');

-- ================= wider / narrower / re-ordered targets, implicit casts to the target types
create table narrow (id int primary key, name varchar(30));
create table wide (id bigint primary key, name varchar(100), amount double, ts date, flag tinyint, note varchar(500), extra int default 42, created timestamp default current_timestamp);
create table reordered (note text, amount decimal(20,6), id smallint primary key);
create table typed (id int primary key, name char(10), amount int, ts_text varchar(30), amount_text varchar(30), when_day int);

insert all
  into narrow (id, name) values (id, name)
  into wide (id, name, amount, ts, flag, note) values (id, name, amount, ts, flag, note)
  into reordered (id, note, amount) values (id, note, amount)
  into typed (id, name, amount, ts_text, amount_text, when_day) values (id, name, amount, ts, amount, day(ts))
select id, name, amount, ts, flag, note from src;
select * from narrow order by id;
select id, name, amount, ts, flag, note, extra, created is not null as has_created from wide order by id;
select id, amount, note from reordered order by id;
select * from typed order by id;

-- ================= constraints of every kind per target
create table with_uk (id int primary key, name varchar(30), unique key uk_name(name), key ik_id(id));
create table with_cpk (id int, name varchar(30), amount decimal(12,3), primary key (id, name));
create table no_pk (id int, name varchar(30));
create table with_auto (seq bigint auto_increment primary key, id int, name varchar(30));
create table with_nn (id int primary key, name varchar(30) not null, amount decimal(12,3) not null default 0);
create table with_check (id int primary key, amount decimal(12,3), check (amount >= 0));
create table with_gen (id int primary key, amount decimal(12,3), doubled decimal(12,3) as (amount * 2) stored);
create table with_cluster (id int, name varchar(30), amount decimal(12,3)) cluster by (name, id);

insert all
  into with_uk (id, name) values (id, name)
  into with_cpk (id, name, amount) values (id, name, amount)
  into no_pk (id, name) values (id, name)
  into no_pk (id, name) values (id + 100, upper(name))
  into with_auto (id, name) values (id, name)
  into with_nn (id, name) values (id, name)
  into with_gen (id, amount) values (id, amount)
  into with_cluster (id, name, amount) values (id, name, amount)
select id, name, amount from src;
select * from with_uk order by id;
select * from with_cpk order by id;
select * from no_pk order by id;
select * from with_auto order by seq;
select * from with_nn order by id;
select * from with_gen order by id;
select * from with_cluster order by id;

-- CHECK constraint enforced on the target it belongs to (gamma has a negative amount): whole statement fails
insert all
  into with_check (id, amount) values (id, amount)
  into no_pk (id, name) values (id + 1000, name)
select id, name, amount from src;
select count(*) from with_check;
select count(*) from no_pk where id > 1000;
-- the non-negative rows alone succeed
insert all
  into with_check (id, amount) values (id, amount)
select id, amount from src where amount >= 0;
select * from with_check order by id;

-- NOT NULL without default enforced per target
insert all into with_nn (id, name) values (id + 10, note) select id, note from src;
select count(*) from with_nn;
-- a generated column cannot be a target column
insert all into with_gen (id, amount, doubled) values (id + 10, amount, amount) select id, amount from src;

-- ================= target types that need conversion: json, enum, varbinary, uuid-ish strings, dates from strings
create table conv (id int primary key, j json, e enum('alpha','beta','gamma','other'), b varbinary(20), d date, t time, f float);
insert first
  when name in ('alpha', 'beta', 'gamma') then into conv (id, j, e, b, d, t, f) values (id, json_object('name', name, 'amount', amount), name, name, ts, ts, amount)
  else                                          into conv (id, j, e, b, d, t, f) values (id, json_array(id, name), 'other', 'x', ts, ts, amount)
select id, name, amount, ts from src;
select id, j, e, hex(b), d, t, f from conv order by id;
-- an enum value outside the definition is rejected
insert all into conv (id, e) values (id + 10, name) select id, name from src where name = 'delta';
select count(*) from conv;

-- ================= string width: too-long values are rejected like in a single insert
create table tight (id int primary key, name varchar(3));
insert all into tight (id, name) values (id, name) select id, name from src where id = 1;
select count(*) from tight;
insert all into tight (id, name) values (id, left(name, 3)) select id, name from src;
select * from tight order by id;

-- ================= targets in another database, temporary target, and a target that is also the source
create database mi_schema_other;
create table mi_schema_other.copy_t (id int primary key, name varchar(30));
create temporary table tmp_t (id int primary key, name varchar(30));
insert all
  into mi_schema_other.copy_t (id, name) values (id, name)
  into tmp_t (id, name) values (id, reverse(name))
  into src (id, name, amount, ts, flag, note) values (id + 100, name, amount, ts, flag, note)
select id, name, amount, ts, flag, note from src;
select * from mi_schema_other.copy_t order by id;
select * from tmp_t order by id;
select count(*) from src;
select id, name from src where id > 100 order by id;
drop database mi_schema_other;

-- ================= column names are case-insensitive; qualified column lists are validated
create table cased (ID int primary key, Name varchar(30));
insert all into cased (id, NAME) values (id, name) into cased (Id, name) values (id + 10, upper(name)) select id, name from src where id <= 2;
select * from cased order by ID;
insert all into cased (cased.id, cased.name) values (id + 20, name) select id, name from src where id = 1;
insert all into cased (other.id, cased.name) values (id + 30, name) select id, name from src where id = 1;
select count(*) from cased;

-- ================= same table with different column lists is widened to the union of the lists
create table union_t (id int primary key, a int default 1, b int default 2, c varchar(10) default 'c');
insert all
  into union_t (id, a) values (id, 10)
  into union_t (id, b) values (id + 10, 20)
  into union_t (id, c) values (id + 20, 'x')
  into union_t (id, a, b, c) values (id + 30, 1, 2, 'y')
select id from src;
select * from union_t order by id;

-- ================= errors: value count vs column list, unknown columns, missing table, view target
insert all into narrow (id, name) values (id) select id, name from src;
insert all into narrow select id, name, amount from src;
insert all into narrow (id, nosuch) values (id, name) select id, name from src;
insert all into nosuch_table (id) values (id) select id from src;
create view v_src as select id, name from src;
insert all into v_src (id, name) values (id, name) select id, name from src;
select count(*) from narrow;

drop database mi_schema;
