-- Test: LOAD DATA should enforce vecf32/vecf64 dimension constraint
-- Related to: https://github.com/matrixorigin/matrixone/issues/23872
--
-- Bug in external.go getColData(): StringToArrayToBytes skips dimension validation.
-- When loading vector data via LOAD DATA INFILE, a CSV row with mismatched
-- dimension is silently accepted instead of being rejected.

drop database if exists test_load_vec_dim;
create database test_load_vec_dim;
use test_load_vec_dim;

-- ============================================================
-- 1. vecf32(3): LOAD DATA with correct dimensions — should succeed
-- ============================================================
drop table if exists t_vecf32;
create table t_vecf32 (id int, v vecf32(3));

load data infile '$resources/load_data/vec_f32_dim_ok.csv' into table t_vecf32 fields terminated by ',' ignore 1 lines;
select id, vector_dims(v) as dim from t_vecf32 order by id;

-- ============================================================
-- 2. vecf32(3): LOAD DATA with dimension mismatch — should fail
--    CSV row2 has 5-dim vector, but column is vecf32(3)
-- ============================================================
drop table if exists t_vecf32_bad;
create table t_vecf32_bad (id int, v vecf32(3));

load data infile '$resources/load_data/vec_f32_dim_bad.csv' into table t_vecf32_bad fields terminated by ',' ignore 1 lines;

-- If bug exists: load succeeds, 2 rows inserted (one with wrong dimension)
-- If fixed: load fails with dimension mismatch error, 0 rows
select count(*) as cnt from t_vecf32_bad;

-- ============================================================
-- 3. vecf64(4): LOAD DATA with dimension mismatch — should fail
--    CSV row2 has 2-dim vector, but column is vecf64(4)
-- ============================================================
drop table if exists t_vecf64_bad;
create table t_vecf64_bad (id int, v vecf64(4));

load data infile '$resources/load_data/vec_f64_dim_bad.csv' into table t_vecf64_bad fields terminated by ',' ignore 1 lines;

-- If bug exists: load succeeds, 2 rows inserted
-- If fixed: load fails with dimension mismatch error, 0 rows
select count(*) as cnt from t_vecf64_bad;

-- cleanup
drop database test_load_vec_dim;
