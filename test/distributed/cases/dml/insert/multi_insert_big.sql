-- Multi-table INSERT: a source large enough that every target's write pipeline writes S3 objects.
-- The write mode is chosen per target at compile time from the planner's row estimate
-- (Outcnt x SingleLineSizeEstimate > DistributedThreshold => write S3 objects directly, flushing every
-- 64MB of buffered rows); 300k rows x ~520 bytes ~ 150MB per wide target.
drop database if exists mi_big;
create database mi_big;
use mi_big;

create table src (id int primary key, cat int, val bigint, pad varchar(600));
insert into src select result, result % 7, result * 11, repeat('p', 500) from generate_series(1, 300000) g;
select count(*), sum(val), sum(length(pad)) from src;

create table t_plain (id int primary key, cat int, val bigint, pad varchar(600));
create table t_idx (id int primary key, cat int, val bigint, pad varchar(600), unique key uk_val(val), key ik_cat(cat));
create table t_cpk (cat int, id int, val bigint, pad varchar(600), primary key (cat, id));
create table t_narrow (id int primary key, cat int);

-- ================= unconditional: 4 targets, three wide ones (~150MB each) and one narrow (id, cat)
insert all
  into t_plain
  into t_idx (id, cat, val, pad) values (id, cat, val, pad)
  into t_cpk (cat, id, val, pad) values (cat, id, val, pad)
  into t_narrow (id, cat) values (id, cat)
select id, cat, val, pad from src;
select count(*), sum(val), min(id), max(id), sum(length(pad)) from t_plain;
select count(*), sum(val), min(id), max(id), sum(length(pad)) from t_idx;
select count(*), sum(val), min(id), max(id), sum(length(pad)) from t_cpk;
select count(*), sum(cat) from t_narrow;
-- every target was written as S3 objects by the insert itself (300k estimated rows each), holding all of its rows
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_big.t_plain', 'id') g;
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_big.t_idx', 'id') g;
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_big.t_cpk', 'id') g;
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_big.t_narrow', 'id') g;
-- indexes built by the same statement answer point and range lookups
select id, cat from t_idx where val = 11 * 123456;
select count(*) from t_idx where cat = 3;
select count(*) from t_cpk where cat = 3 and id between 1000 and 2000;
select id from t_plain where id in (1, 150000, 300000) order by id;

-- ================= INSERT FIRST over the big source with overlapping conditions
create table f_zero (id int primary key, cat int);
create table f_low (id int primary key, cat int, val bigint, pad varchar(600));
create table f_rest (cat int, id int, val bigint, pad varchar(600), primary key (cat, id));
insert first
  when cat = 0 then into f_zero (id, cat) values (id, cat)
  when cat < 3 then into f_low (id, cat, val, pad) values (id, cat, val, pad)
  else              into f_rest (cat, id, val, pad) values (cat, id, val, pad)
select id, cat, val, pad from src;
select count(*), min(cat), max(cat) from f_zero;
select count(*), min(cat), max(cat) from f_low;
select count(*), min(cat), max(cat) from f_rest;
select (select count(*) from f_zero) + (select count(*) from f_low) + (select count(*) from f_rest) = (select count(*) from src) as covers_source_once;
select (select count(*) from f_zero) = (select count(*) from src where cat = 0) as zero_ok;
select (select count(*) from f_low) = (select count(*) from src where cat in (1, 2)) as low_ok;
select (select count(*) from f_rest) = (select count(*) from src where cat >= 3) as rest_ok;
select count(*) > 0 as has_objects from metadata_scan('mi_big.f_low', 'id') g;
select count(*) > 0 as has_objects from metadata_scan('mi_big.f_rest', 'id') g;

-- INSERT ALL over the same conditions: overlapping rows land in every matching target
create table a_zero (id int primary key, cat int);
create table a_low (id int primary key, cat int, val bigint, pad varchar(600));
create table a_rest (cat int, id int, val bigint, pad varchar(600), primary key (cat, id));
insert all
  when cat = 0 then into a_zero (id, cat) values (id, cat)
  when cat < 3 then into a_low (id, cat, val, pad) values (id, cat, val, pad)
  else              into a_rest (cat, id, val, pad) values (cat, id, val, pad)
select id, cat, val, pad from src;
select count(*) from a_zero;
select count(*) from a_low;
select count(*) from a_rest;
select count(*) from a_zero z join a_low l on z.id = l.id;

-- ================= the same big table written by several clauses (merged into one pipeline)
create table merged (id int primary key, cat int, val bigint, pad varchar(600));
insert all
  into merged (id, cat, val, pad) values (id, cat, val, pad)
  into merged (id, cat, val, pad) values (id + 1000000, cat + 10, val, pad)
select id, cat, val, pad from src;
select count(*), sum(cat) from merged;
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_big.merged', 'id') g;
-- overlapping keys across clauses: rejected, nothing added
insert all
  into merged (id, cat, val, pad) values (id + 2000000, cat, val, pad)
  into merged (id, cat, val, pad) values (id + 2000000, cat, val, pad)
select id, cat, val, pad from src;
select count(*) from merged;

-- ================= a failure detected late rolls back objects already written by other targets
create table late_fail (id int primary key, cat int, val bigint, pad varchar(600));
insert into late_fail values (299999, 0, 0, 'existing');
create table other_big (id int primary key, cat int, val bigint, pad varchar(600));
insert all
  into other_big (id, cat, val, pad) values (id, cat, val, pad)
  into late_fail (id, cat, val, pad) values (id, cat, val, pad)
select id, cat, val, pad from src;
select count(*) from late_fail;
select count(*) from other_big;
select count(*) > 0 as has_objects from metadata_scan('mi_big.other_big', 'id') g;

-- ================= flush the tables and read everything back
select mo_ctl('dn', 'flush', 'mi_big.t_plain');
select mo_ctl('dn', 'flush', 'mi_big.t_idx');
select mo_ctl('dn', 'flush', 'mi_big.t_narrow');
select mo_ctl('dn', 'flush', 'mi_big.merged');
select count(*), sum(val) from t_plain;
select count(*), sum(val) from t_idx;
select count(*), sum(cat) from t_narrow;
select count(*), sum(cat) from merged;
select id, cat from t_idx where val = 11 * 299999;

-- ================= big insert inside an explicit transaction, rolled back
begin;
insert all into t_narrow (id, cat) values (id + 1000000, cat) into t_plain (id, cat, val, pad) values (id + 1000000, cat, val, pad) select id, cat, val, pad from src;
select count(*) from t_plain;
rollback;
select count(*) from t_plain;
select count(*) from t_narrow;

drop database mi_big;
