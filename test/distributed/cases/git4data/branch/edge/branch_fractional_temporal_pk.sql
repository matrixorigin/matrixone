-- DATA BRANCH must preserve fractional temporal primary-key identity during
-- LCA probing, DIFF, MERGE, and selected-key PICK.
drop database if exists br_fractional_temporal_29772;
create database br_fractional_temporal_29772;
use br_fractional_temporal_29772;

-- The whole-second key and two fractional keys coexist in every affected type.
create table dt3_base(k datetime(3) primary key, v varchar(16));
insert into dt3_base values
  ('2024-01-01 00:00:00.000', 'whole'),
  ('2024-01-01 00:00:00.001', 'old'),
  ('2024-01-01 00:00:00.002', 'delete-me');
data branch create table dt3_branch from dt3_base;
update dt3_branch set v = 'changed' where k = '2024-01-01 00:00:00.001';
delete from dt3_branch where k = '2024-01-01 00:00:00.002';
insert into dt3_branch values ('2024-01-01 00:00:00.003', 'inserted');
-- @sortkey:2
data branch diff dt3_branch against dt3_base;
data branch merge dt3_branch into dt3_base;
select k, v from dt3_base order by k;

create table dt6_base(k datetime(6) primary key, v varchar(16));
insert into dt6_base values
  ('2024-01-01 00:00:00.000000', 'whole'),
  ('2024-01-01 00:00:00.000001', 'old'),
  ('2024-01-01 00:00:00.000002', 'delete-me');
data branch create table dt6_branch from dt6_base;
update dt6_branch set v = 'changed' where k = '2024-01-01 00:00:00.000001';
delete from dt6_branch where k = '2024-01-01 00:00:00.000002';
insert into dt6_branch values ('2024-01-01 00:00:00.000003', 'inserted');
-- @sortkey:2
data branch diff dt6_branch against dt6_base;
data branch merge dt6_branch into dt6_base;
select k, v from dt6_base order by k;

create table ts3_base(k timestamp(3) primary key, v varchar(16));
insert into ts3_base values
  ('2024-01-01 00:00:00.000', 'whole'),
  ('2024-01-01 00:00:00.001', 'old'),
  ('2024-01-01 00:00:00.002', 'delete-me');
data branch create table ts3_branch from ts3_base;
update ts3_branch set v = 'changed' where k = '2024-01-01 00:00:00.001';
delete from ts3_branch where k = '2024-01-01 00:00:00.002';
insert into ts3_branch values ('2024-01-01 00:00:00.003', 'inserted');
-- @sortkey:2
data branch diff ts3_branch against ts3_base;
data branch merge ts3_branch into ts3_base;
select k, v from ts3_base order by k;

create table ts6_base(k timestamp(6) primary key, v varchar(16));
insert into ts6_base values
  ('2024-01-01 00:00:00.000000', 'whole'),
  ('2024-01-01 00:00:00.000001', 'old'),
  ('2024-01-01 00:00:00.000002', 'delete-me');
data branch create table ts6_branch from ts6_base;
update ts6_branch set v = 'changed' where k = '2024-01-01 00:00:00.000001';
delete from ts6_branch where k = '2024-01-01 00:00:00.000002';
insert into ts6_branch values ('2024-01-01 00:00:00.000003', 'inserted');
-- @sortkey:2
data branch diff ts6_branch against ts6_base;
data branch merge ts6_branch into ts6_base;
select k, v from ts6_base order by k;

create table time3_base(k time(3) primary key, v varchar(16));
insert into time3_base values
  ('00:00:00.000', 'whole'),
  ('00:00:00.001', 'old'),
  ('00:00:00.002', 'delete-me');
data branch create table time3_branch from time3_base;
update time3_branch set v = 'changed' where k = '00:00:00.001';
delete from time3_branch where k = '00:00:00.002';
insert into time3_branch values ('00:00:00.003', 'inserted');
-- @sortkey:2
data branch diff time3_branch against time3_base;
data branch merge time3_branch into time3_base;
select k, v from time3_base order by k;

create table time6_base(k time(6) primary key, v varchar(16));
insert into time6_base values
  ('00:00:00.000000', 'whole'),
  ('00:00:00.000001', 'old'),
  ('00:00:00.000002', 'delete-me');
data branch create table time6_branch from time6_base;
update time6_branch set v = 'changed' where k = '00:00:00.000001';
delete from time6_branch where k = '00:00:00.000002';
insert into time6_branch values ('00:00:00.000003', 'inserted');
-- @sortkey:2
data branch diff time6_branch against time6_base;
data branch merge time6_branch into time6_base;
select k, v from time6_base order by k;

-- Scale-zero and zero-fraction controls retain their existing behavior.
create table datetime0_base(k datetime(0) primary key, v varchar(16));
insert into datetime0_base values
  ('2024-01-01 00:00:00', 'whole'),
  ('2024-01-01 00:00:01', 'old'),
  ('2024-01-01 00:00:02', 'delete-me');
data branch create table datetime0_branch from datetime0_base;
update datetime0_branch set v = 'changed' where k = '2024-01-01 00:00:01';
delete from datetime0_branch where k = '2024-01-01 00:00:02';
insert into datetime0_branch values ('2024-01-01 00:00:03', 'inserted');
-- @sortkey:2
data branch diff datetime0_branch against datetime0_base;
data branch merge datetime0_branch into datetime0_base;
select k, v from datetime0_base order by k;

create table datetime6_zero_base(k datetime(6) primary key, v varchar(16));
insert into datetime6_zero_base values
  ('2024-01-01 00:00:00.000000', 'whole'),
  ('2024-01-01 00:00:01.000000', 'old'),
  ('2024-01-01 00:00:02.000000', 'delete-me');
data branch create table datetime6_zero_branch from datetime6_zero_base;
update datetime6_zero_branch set v = 'changed' where k = '2024-01-01 00:00:01.000000';
delete from datetime6_zero_branch where k = '2024-01-01 00:00:02.000000';
insert into datetime6_zero_branch values ('2024-01-01 00:00:03.000000', 'inserted');
-- @sortkey:2
data branch diff datetime6_zero_branch against datetime6_zero_base;
data branch merge datetime6_zero_branch into datetime6_zero_base;
select k, v from datetime6_zero_base order by k;

-- Temporal payload formatting is independent of primary-key matching.
create table payload_base(id int primary key, payload datetime(6));
insert into payload_base values
  (1, '2024-01-01 00:00:00.000001'),
  (2, '2024-01-01 00:00:00.000003');
data branch create table payload_branch from payload_base;
update payload_branch set payload = '2024-01-01 00:00:00.000002' where id = 1;
delete from payload_branch where id = 2;
insert into payload_branch values (3, '2024-01-01 00:00:00.000004');
-- @sortkey:2
data branch diff payload_branch against payload_base;
data branch merge payload_branch into payload_base;
select id, payload from payload_base order by id;

-- Fractional DATETIME in either composite-key position, plus selected PICK.
create table composite_first_base(k datetime(6), id int, v varchar(16), primary key (k, id));
insert into composite_first_base values
  ('2024-01-01 00:00:00.000000', 0, 'whole'),
  ('2024-01-01 00:00:00.000001', 1, 'old'),
  ('2024-01-01 00:00:00.000002', 2, 'delete-me');
data branch create table composite_first_branch from composite_first_base;
update composite_first_branch set v = 'changed' where k = '2024-01-01 00:00:00.000001' and id = 1;
delete from composite_first_branch where k = '2024-01-01 00:00:00.000002' and id = 2;
insert into composite_first_branch values ('2024-01-01 00:00:00.000003', 3, 'inserted');
-- @sortkey:2,3
data branch diff composite_first_branch against composite_first_base;
data branch merge composite_first_branch into composite_first_base;
select k, id, v from composite_first_base order by k, id;

create table composite_second_base(id int, k datetime(6), v varchar(16), primary key (id, k));
insert into composite_second_base values
  (0, '2024-01-01 00:00:00.000000', 'whole'),
  (1, '2024-01-01 00:00:00.000001', 'old'),
  (2, '2024-01-01 00:00:00.000002', 'delete-me');
data branch create table composite_second_branch from composite_second_base;
update composite_second_branch set v = 'changed' where id = 1 and k = '2024-01-01 00:00:00.000001';
delete from composite_second_branch where id = 2 and k = '2024-01-01 00:00:00.000002';
insert into composite_second_branch values (3, '2024-01-01 00:00:00.000003', 'inserted');
-- @sortkey:2,3
data branch diff composite_second_branch against composite_second_base;
data branch merge composite_second_branch into composite_second_base;
select id, k, v from composite_second_base order by id, k;

create table pick_base(id int, k datetime(6), v varchar(16), primary key (id, k));
insert into pick_base values
  (0, '2024-01-01 00:00:00.000000', 'whole'),
  (1, '2024-01-01 00:00:00.000001', 'old'),
  (2, '2024-01-01 00:00:00.000002', 'delete-me');
data branch create table pick_source from pick_base;
update pick_source set v = 'picked' where id = 1 and k = '2024-01-01 00:00:00.000001';
data branch pick pick_source into pick_base keys((1, '2024-01-01 00:00:00.000001')) when conflict accept;
select id, k, v from pick_base order by id, k;

-- Session timezone and the rounding boundary must not change fractional identity.
set time_zone = '+00:00';
create table boundary_datetime_base(k datetime(6) primary key, v varchar(24));
insert into boundary_datetime_base values
  ('2024-01-01 00:00:00.999999', 'before-boundary'),
  ('2024-01-01 00:00:01.000000', 'next-second');
data branch create table boundary_datetime_branch from boundary_datetime_base;
update boundary_datetime_branch set v = 'changed' where k = '2024-01-01 00:00:00.999999';
delete from boundary_datetime_branch where k = '2024-01-01 00:00:01.000000';
insert into boundary_datetime_branch values ('2024-01-01 00:00:01.000001', 'inserted');
-- @sortkey:2
data branch diff boundary_datetime_branch against boundary_datetime_base;
data branch merge boundary_datetime_branch into boundary_datetime_base;
select k, v from boundary_datetime_base order by k;
select k, v from boundary_datetime_branch order by k;

set time_zone = '+08:00';
create table boundary_timestamp_base(k timestamp(6) primary key, v varchar(24));
insert into boundary_timestamp_base values
  ('2024-01-01 00:00:00.999999', 'before-boundary'),
  ('2024-01-01 00:00:01.000000', 'next-second');
data branch create table boundary_timestamp_branch from boundary_timestamp_base;
update boundary_timestamp_branch set v = 'changed' where k = '2024-01-01 00:00:00.999999';
delete from boundary_timestamp_branch where k = '2024-01-01 00:00:01.000000';
insert into boundary_timestamp_branch values ('2024-01-01 00:00:01.000001', 'inserted');
-- @sortkey:2
data branch diff boundary_timestamp_branch against boundary_timestamp_base;
data branch merge boundary_timestamp_branch into boundary_timestamp_base;
select k, v from boundary_timestamp_base order by k;
select k, v from boundary_timestamp_branch order by k;

set time_zone = '+00:00';
create table boundary_time_base(k time(6) primary key, v varchar(24));
insert into boundary_time_base values
  ('00:00:00.999999', 'before-boundary'),
  ('00:00:01.000000', 'next-second');
data branch create table boundary_time_branch from boundary_time_base;
update boundary_time_branch set v = 'changed' where k = '00:00:00.999999';
delete from boundary_time_branch where k = '00:00:01.000000';
insert into boundary_time_branch values ('00:00:01.000001', 'inserted');
-- @sortkey:2
data branch diff boundary_time_branch against boundary_time_base;
data branch merge boundary_time_branch into boundary_time_base;
select k, v from boundary_time_base order by k;
select k, v from boundary_time_branch order by k;

-- Negative fractional TIME values use the same exact-key path.
create table negative_time_base(k time(6) primary key, v varchar(24));
insert into negative_time_base values
  ('-01:00:00.000000', 'negative-whole'),
  ('-01:00:00.000001', 'negative-old'),
  ('-01:00:00.000002', 'negative-delete');
data branch create table negative_time_branch from negative_time_base;
update negative_time_branch set v = 'negative-changed' where k = '-01:00:00.000001';
delete from negative_time_branch where k = '-01:00:00.000002';
insert into negative_time_branch values ('-01:00:00.000003', 'negative-inserted');
-- @sortkey:2
data branch diff negative_time_branch against negative_time_base;
data branch merge negative_time_branch into negative_time_base;
select k, v from negative_time_base order by k;
select k, v from negative_time_branch order by k;

-- A fractional-key conflict fails without applying the branch's other change.
create table temporal_conflict_base(k datetime(6) primary key, v varchar(32));
insert into temporal_conflict_base values
  ('2024-01-01 00:00:00.000001', 'old-conflict'),
  ('2024-01-01 00:00:00.000002', 'old-safe');
data branch create table temporal_conflict_branch from temporal_conflict_base;
update temporal_conflict_branch set v = 'branch-conflict' where k = '2024-01-01 00:00:00.000001';
update temporal_conflict_branch set v = 'branch-safe' where k = '2024-01-01 00:00:00.000002';
update temporal_conflict_base set v = 'base-conflict' where k = '2024-01-01 00:00:00.000001';
-- @sortkey:2
data branch diff temporal_conflict_branch against temporal_conflict_base;
data branch merge temporal_conflict_branch into temporal_conflict_base;
select k, v from temporal_conflict_base order by k;
select k, v from temporal_conflict_branch order by k;

-- Composite temporal conflicts must report the complete key and remain atomic.
create table temporal_composite_conflict_base(id int, k datetime(6), v varchar(32), primary key (id, k));
insert into temporal_composite_conflict_base values
  (1, '2024-01-01 00:00:00.000001', 'old-conflict'),
  (2, '2024-01-01 00:00:00.000002', 'old-safe');
data branch create table temporal_composite_conflict_branch from temporal_composite_conflict_base;
update temporal_composite_conflict_branch set v = 'branch-conflict' where id = 1 and k = '2024-01-01 00:00:00.000001';
update temporal_composite_conflict_branch set v = 'branch-safe' where id = 2 and k = '2024-01-01 00:00:00.000002';
update temporal_composite_conflict_base set v = 'base-conflict' where id = 1 and k = '2024-01-01 00:00:00.000001';
data branch merge temporal_composite_conflict_branch into temporal_composite_conflict_base when conflict fail;
select id, k, v from temporal_composite_conflict_base order by id;
select id, k, v from temporal_composite_conflict_branch order by id;

-- A no-primary-key table uses every user column as its fake primary key. Vector
-- values in that key must be formatted without a type assertion panic when
-- both sides delete the same ancestor row and conflict handling is requested.
create table vector_fake_pk_conflict_f32(id int, embedding vecf32(2));
insert into vector_fake_pk_conflict_f32 values (1, '[1,2]'), (2, '[1,2]');
data branch create table vector_fake_pk_conflict_f32_branch from vector_fake_pk_conflict_f32;
delete from vector_fake_pk_conflict_f32_branch where id = 1;
delete from vector_fake_pk_conflict_f32 where id = 1;
data branch merge vector_fake_pk_conflict_f32_branch into vector_fake_pk_conflict_f32 when conflict fail;
select id, embedding from vector_fake_pk_conflict_f32 order by id;
select id, embedding from vector_fake_pk_conflict_f32_branch order by id;

create table vector_fake_pk_conflict_f64(id int, embedding vecf64(2));
insert into vector_fake_pk_conflict_f64 values (1, '[3,4]'), (2, '[3,4]');
data branch create table vector_fake_pk_conflict_f64_branch from vector_fake_pk_conflict_f64;
delete from vector_fake_pk_conflict_f64_branch where id = 1;
delete from vector_fake_pk_conflict_f64 where id = 1;
data branch merge vector_fake_pk_conflict_f64_branch into vector_fake_pk_conflict_f64 when conflict fail;
select id, embedding from vector_fake_pk_conflict_f64 order by id;
select id, embedding from vector_fake_pk_conflict_f64_branch order by id;

-- Stored JSON in a no-primary-key table must be decoded before conflict-key formatting.
create table json_fake_pk_conflict(id int, payload json);
insert into json_fake_pk_conflict values
  (1, '{"name":"same","n":1}'),
  (2, '{"name":"same","n":2}');
data branch create table json_fake_pk_conflict_branch from json_fake_pk_conflict;
delete from json_fake_pk_conflict_branch where id = 1;
delete from json_fake_pk_conflict where id = 1;
data branch merge json_fake_pk_conflict_branch into json_fake_pk_conflict when conflict fail;
select id, payload from json_fake_pk_conflict order by id;
select id, payload from json_fake_pk_conflict_branch order by id;

drop database br_fractional_temporal_29772;
