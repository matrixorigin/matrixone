-- Multi-table INSERT: extreme imbalance between targets, targets that receive nothing, ELSE branches that never fire
drop database if exists mi_skew;
create database mi_skew;
use mi_skew;

-- 300k rows, ~520 bytes each. The write mode of each target is chosen at compile time from the planner's
-- row estimate (Outcnt x SingleLineSizeEstimate > DistributedThreshold => S3 objects, else in-memory), so a
-- target expected to take (almost) everything writes S3 objects, and a target that receives nothing never
-- produces objects in either mode. Object checks are only made on those two deterministic kinds of target.
create table src (id int primary key, cat int, val bigint, pad varchar(600));
insert into src select result, result % 7, result * 11, repeat('p', 500) from generate_series(1, 300000) g;
select count(*) from src;

create table t_all  (id int primary key, cat int, val bigint, pad varchar(600), unique key uk_val(val), key ik_cat(cat));
create table t_few  (id int primary key, cat int, val bigint, pad varchar(600), unique key uk_val(val));
create table t_one  (id int primary key, cat int, val bigint, pad varchar(600));
create table t_none (id int primary key, cat int, val bigint, pad varchar(600), unique key uk_val(val), key ik_cat(cat));
create table t_else (id int primary key, cat int, val bigint, pad varchar(600));

-- ================= INSERT FIRST: the first WHEN catches everything except 3 rows and one row; ELSE never fires
insert first
  when id in (1, 2, 3)   then into t_few  (id, cat, val, pad) values (id, cat, val, pad)
  when id = 300000       then into t_one  (id, cat, val, pad) values (id, cat, val, pad)
  when id > 0            then into t_all  (id, cat, val, pad) values (id, cat, val, pad)
  when id < 0            then into t_none (id, cat, val, pad) values (id, cat, val, pad)
  else                        into t_else (id, cat, val, pad) values (id, cat, val, pad)
select id, cat, val, pad from src;
select count(*), min(id), max(id), sum(val) from t_all;
select count(*), min(id), max(id) from t_few;
select count(*), min(id), max(id) from t_one;
select count(*) from t_none;
select count(*) from t_else;
select (select count(*) from t_all) + (select count(*) from t_few) + (select count(*) from t_one) = 300000 as covers_source_once;
-- the dominant target flushed objects holding every one of its rows; the empty targets have none
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_skew.t_all', 'id') g;
select count(*) > 0 as has_objects from metadata_scan('mi_skew.t_none', 'id') g;
select count(*) > 0 as has_objects from metadata_scan('mi_skew.t_else', 'id') g;
-- indexes of the dominant target are complete, and the empty target's indexes answer nothing
select id from t_all where val = 11 * 299997;
select count(*) from t_all where cat = 6;
select count(*) from t_none where cat = 6;
select count(*) from t_none where val = 11;

-- ================= INSERT ALL: every row matches the dominant WHEN; the others match nothing; no ELSE
delete from t_all; delete from t_few; delete from t_one; delete from t_none; delete from t_else;
insert all
  when cat >= 0       then into t_all  (id, cat, val, pad) values (id, cat, val, pad)
  when cat > 100      then into t_none (id, cat, val, pad) values (id, cat, val, pad)
  when pad is null    then into t_few  (id, cat, val, pad) values (id, cat, val, pad)
select id, cat, val, pad from src;
select count(*), sum(val) from t_all;
select count(*) from t_none;
select count(*) from t_few;
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_skew.t_all', 'id') g;
select count(*) > 0 as has_objects from metadata_scan('mi_skew.t_none', 'id') g;

-- ================= ELSE receives everything: no WHEN ever matches
delete from t_all; delete from t_few; delete from t_one; delete from t_none; delete from t_else;
insert first
  when cat < 0  then into t_none (id, cat, val, pad) values (id, cat, val, pad)
  when id = 0   then into t_one  (id, cat, val, pad) values (id, cat, val, pad)
  else               into t_else (id, cat, val, pad) values (id, cat, val, pad)
select id, cat, val, pad from src;
select count(*), sum(val) from t_else;
select count(*) from t_none;
select count(*) from t_one;
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_skew.t_else', 'id') g;
select count(*) > 0 as has_objects from metadata_scan('mi_skew.t_none', 'id') g;

-- ================= nothing matches and there is no ELSE: the statement succeeds and writes nothing
delete from t_all; delete from t_few; delete from t_one; delete from t_none; delete from t_else;
insert all
  when cat < 0 then into t_all  (id, cat, val, pad) values (id, cat, val, pad)
  when id = 0  then into t_none (id, cat, val, pad) values (id, cat, val, pad)
select id, cat, val, pad from src;
select count(*) from t_all;
select count(*) from t_none;
insert first
  when cat < 0 then into t_all (id, cat, val, pad) values (id, cat, val, pad)
select id, cat, val, pad from src;
select count(*) from t_all;

-- ================= unconditional INSERT ALL where the source is empty: every target stays empty
insert all
  into t_all  (id, cat, val, pad) values (id, cat, val, pad)
  into t_none (id, cat, val, pad) values (id, cat, val, pad)
  into t_else (id, cat, val, pad) values (id, cat, val, pad)
select id, cat, val, pad from src where id > 1000000;
select count(*) from t_all;
select count(*) from t_none;
select count(*) from t_else;

-- ================= skew in the other direction: one row to the dominant table, everything else to a narrow table
create table t_narrow (id int primary key, cat int);
insert first
  when id = 150000 then into t_all    (id, cat, val, pad) values (id, cat, val, pad)
  else                  into t_narrow (id, cat) values (id, cat)
select id, cat, val, pad from src;
select count(*), min(id) from t_all;
select count(*), sum(cat) from t_narrow;
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_skew.t_narrow', 'id') g;

-- ================= the same table receives the dominant share from one clause and nothing from another
delete from t_all; delete from t_narrow;
insert all
  when cat >= 0 then into t_all (id, cat, val, pad) values (id, cat, val, pad)
  when cat < 0  then into t_all (id, cat, val, pad) values (id + 1000000, cat, val, pad)
select id, cat, val, pad from src;
select count(*), max(id) from t_all;
select count(*) > 0 as has_objects, sum(rows_cnt) from metadata_scan('mi_skew.t_all', 'id') g;

-- ================= empty targets are still constraint-checked when they finally get rows
insert into t_none values (7, 0, 77, 'seven');
insert first
  when id = 7 then into t_none (id, cat, val, pad) values (id, cat, val, pad)
  else             into t_narrow (id, cat) values (id + 1000000, cat)
select id, cat, val, pad from src where id <= 10;
select count(*) from t_none;
select count(*) from t_narrow where id > 1000000;

-- ================= small-scale versions of the same shapes (no S3 flush anywhere)
create table s_src (id int primary key, k int);
insert into s_src values (1, 1), (2, 1), (3, 1), (4, 1), (5, 2);
create table s_a (id int primary key, k int);
create table s_b (id int primary key, k int);
create table s_c (id int primary key, k int);
insert first
  when k = 1 then into s_a (id, k) values (id, k)
  when k = 2 then into s_b (id, k) values (id, k)
  else            into s_c (id, k) values (id, k)
select id, k from s_src;
select * from s_a order by id;
select * from s_b order by id;
select count(*) from s_c;
delete from s_a; delete from s_b;
insert all
  when k = 3 then into s_a (id, k) values (id, k)
  when k = 4 then into s_b (id, k) values (id, k)
  else            into s_c (id, k) values (id, k)
select id, k from s_src;
select count(*) from s_a;
select count(*) from s_b;
select * from s_c order by id;
delete from s_c;
insert first
  when k = 1 then into s_a (id, k) values (id, k)
select id, k from s_src;
select count(*) from s_a;
select count(*) from s_b;
select count(*) from s_c;

drop database mi_skew;
