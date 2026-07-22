-- Test: LOAD DATA INFILE into narrow vector base columns
-- (vecbf16 / vecf16 / vecint8 / vecuint8).
--
-- Before the fix, the external/CSV import switches in external.go only handled
-- T_array_float32/float64, so loading a narrow vector column failed with
-- "the value type N is not support now". INSERT already worked; only the bulk
-- LOAD path was missing the narrow cases.
--
-- int8/uint8 string parse is strict (integer, in range); fractional or
-- out-of-range values are rejected — mirroring INSERT.

drop database if exists load_narrow_vec;
create database load_narrow_vec;
use load_narrow_vec;

-- ============================================================
-- 1. Happy path: load all four narrow types from one CSV.
--    Values are exactly representable in bf16/f16 (and integer
--    for int8/uint8), so the round-trip is loss-free.
-- ============================================================
create table nvec(id int, a vecbf16(3), b vecf16(3), c vecint8(3), d vecuint8(3));
load data infile '$resources/load_data/narrow_vec_array.csv' into table nvec fields terminated by ',' ignore 1 lines;
select * from nvec order by id;

-- distance functions work on a loaded narrow column
-- round to 4 digits: l2_distance (sqrt, float64) low-order bits vary across SIMD kernels
select id, round(l2_distance(c, '[0,0,0]'), 4) as dist from nvec order by id;

-- ORDER BY the vector column itself. pkg/sort/sort.go dispatches on the column
-- type and had arms for vecf32/vecf64 only, so a narrow vector sort key fell
-- through. ASC and DESC are SEPARATE branches there (arrayElementLess vs
-- arrayElementGreater), so both directions are exercised per type.
select id from nvec order by a, id;
select id from nvec order by a desc, id;
select id from nvec order by b, id;
select id from nvec order by b desc, id;
-- int8 must order by SIGN: row 2 holds [10,-10,5], row 1 holds [-128,0,127],
-- so row 1 sorts first. A raw-byte comparison would put -128 (0x80) last.
select id from nvec order by c, id;
select id from nvec order by c desc, id;
-- uint8 must order UNSIGNED: row 1 holds [0,128,255], row 2 holds [1,2,3],
-- so row 1 sorts first only if 0 < 1 is compared unsigned.
select id from nvec order by d, id;
select id from nvec order by d desc, id;

-- ============================================================
-- 2. Strict int8 parse: out-of-range value (200) is rejected.
-- ============================================================
create table nvec_oor(id int, c vecint8(3));
load data infile '$resources/load_data/narrow_vec_int8_oor.csv' into table nvec_oor fields terminated by ',' ignore 1 lines;
select count(*) as cnt from nvec_oor;

-- ============================================================
-- 3. Strict int8 parse: fractional value (0.5) is rejected.
-- ============================================================
create table nvec_frac(id int, c vecint8(3));
load data infile '$resources/load_data/narrow_vec_int8_frac.csv' into table nvec_frac fields terminated by ',' ignore 1 lines;
select count(*) as cnt from nvec_frac;

-- ============================================================
-- 4. Dimension mismatch is rejected (vecuint8(3) given 2 elems).
-- ============================================================
create table nvec_dim(id int, d vecuint8(3));
load data infile '$resources/load_data/narrow_vec_dim_bad.csv' into table nvec_dim fields terminated by ',' ignore 1 lines;
select count(*) as cnt from nvec_dim;

-- cleanup
drop database load_narrow_vec;
