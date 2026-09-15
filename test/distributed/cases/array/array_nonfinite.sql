-- Regression for #28688: a vector literal with a NaN or ±Inf component must be rejected at
-- the cast boundary on every write path (INSERT / UPDATE / LOAD DATA) instead of being stored
-- and then panicking the IVFFLAT index build (non-finite poisons kmeans centroid means and the
-- nearest-centroid argmin, which returned -1 and crashed in Vector.UnionOne). Covers every
-- float vector type; integer narrow vectors (vecint8/vecuint8) cannot be non-finite.
drop database if exists array_nonfinite;
create database array_nonfinite;
use array_nonfinite;

-- ============================================================
-- 1. INSERT: non-finite literals rejected for every float type
-- ============================================================
create table t32(id bigint primary key, v vecf32(3));
insert into t32 values (1,'[1,2,3]'),(2,'[0,0,0]'),(4,'[2,1,0]');
insert into t32 values (3,'[NaN,0,0]');
insert into t32 values (5,'[Inf,0,0]');
insert into t32 values (6,'[-Inf,0,0]');
insert into t32 values (7,'[0,nan,0]');
insert into t32 values (8,'[0,0,+Inf]');

create table t64(id bigint primary key, v vecf64(3));
insert into t64 values (1,'[1,2,3]');
insert into t64 values (2,'[NaN,0,0]');
insert into t64 values (3,'[-Inf,0,0]');

create table tbf16(id bigint primary key, v vecbf16(3));
insert into tbf16 values (1,'[1,2,3]');
insert into tbf16 values (2,'[NaN,0,0]');
insert into tbf16 values (3,'[Inf,0,0]');

create table tf16(id bigint primary key, v vecf16(3));
insert into tf16 values (1,'[1,2,3]');
insert into tf16 values (2,'[NaN,0,0]');
insert into tf16 values (3,'[-Inf,0,0]');

-- integer narrow vectors are unaffected: a finite literal still parses
create table ti8(id bigint primary key, v vecint8(3));
insert into ti8 values (1,'[1,2,3]');

-- only the finite rows survived each insert
select id from t32 order by id;
select id from t64 order by id;
select id from tbf16 order by id;
select id from tf16 order by id;
select id from ti8 order by id;

-- ============================================================
-- 2. UPDATE: setting a stored vector to a non-finite value is rejected
-- ============================================================
update t32 set v='[Inf,0,0]' where id=1;
update t64 set v='[NaN,0,0]' where id=1;
-- a finite UPDATE still works
update t32 set v='[3,3,3]' where id=1;
select id, v from t32 where id=1;

-- ============================================================
-- 3. LOAD DATA: a CSV row with a non-finite vector fails the load
-- ============================================================
-- control: an all-finite CSV loads normally
create table lt_ok(id int, v vecf32(3));
load data infile '$resources/load_data/vec_f32_dim_ok.csv' into table lt_ok fields terminated by ',' ignore 1 lines;
select count(*) as cnt from lt_ok;

-- non-finite CSV: the load fails (all-or-nothing), 0 rows inserted
create table lt_bad_f32(id int, v vecf32(3));
load data infile '$resources/load_data/vec_nonfinite.csv' into table lt_bad_f32 fields terminated by ',' ignore 1 lines;
select count(*) as cnt from lt_bad_f32;

-- same fixture into vecf64: also rejected
create table lt_bad_f64(id int, v vecf64(3));
load data infile '$resources/load_data/vec_nonfinite.csv' into table lt_bad_f64 fields terminated by ',' ignore 1 lines;
select count(*) as cnt from lt_bad_f64;

-- ============================================================
-- 4. the issue's scenario: with non-finite rows kept out, IVFFLAT builds
-- ============================================================
create index ix using ivfflat on t32(v) lists=2 op_type 'vector_l2_ops';
select id from t32 order by id;

drop database array_nonfinite;
