-- Parquet External Table Tests
-- This file tests external table functionality with Parquet format

-- cleanup
drop database if exists ext_parquet_db;
create database ext_parquet_db;
use ext_parquet_db;

-- ============================================================================
-- 1. Basic Parquet External Table
-- ============================================================================

-- 1.1 Basic types (int, bigint, double, varchar, bool, date, time)
drop table if exists ext_supported_types;
create external table ext_supported_types (
    int32_col INT,
    int64_col BIGINT,
    uint32_col INT UNSIGNED,
    float32_col FLOAT,
    float64_col DOUBLE,
    string_col VARCHAR(255),
    bool_col BOOL,
    date_col DATE,
    time_col TIME
) infile{"filepath"='$resources/parquet/supported_types.parquet', "format"='parquet'};
select int32_col, int64_col, uint32_col, float32_col, float64_col, string_col, bool_col, date_col, time_col from ext_supported_types;
select count(*) from ext_supported_types;

-- 1.2 Iris dataset
drop table if exists ext_iris;
create external table ext_iris (
    `sepal.length` double,
    `sepal.width` double,
    `petal.length` double,
    `petal.width` double,
    `variety` varchar(20)
) infile{"filepath"='$resources/parquet/Iris.parquet', "format"='parquet'};
select count(*) from ext_iris;
select variety, count(*) as cnt from ext_iris group by variety order by variety;
select * from ext_iris where `sepal.length` > 7.0 order by `sepal.length` limit 5;

-- ============================================================================
-- 2. Compression Formats
-- ============================================================================

-- 2.1 No compression
drop table if exists ext_none;
create external table ext_none (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
) infile{"filepath"='$resources/parquet/test_none.parquet', "format"='parquet'};
select 'NONE' as codec, count(*), sum(value) from ext_none;

-- 2.2 Snappy compression
drop table if exists ext_snappy;
create external table ext_snappy (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
) infile{"filepath"='$resources/parquet/test_snappy.parquet', "format"='parquet'};
select 'SNAPPY' as codec, count(*), sum(value) from ext_snappy;

-- 2.3 Gzip compression
drop table if exists ext_gzip;
create external table ext_gzip (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
) infile{"filepath"='$resources/parquet/test_gzip.parquet', "format"='parquet'};
select 'GZIP' as codec, count(*), sum(value) from ext_gzip;

-- 2.4 LZ4 compression
drop table if exists ext_lz4;
create external table ext_lz4 (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
) infile{"filepath"='$resources/parquet/test_lz4.parquet', "format"='parquet'};
select 'LZ4' as codec, count(*), sum(value) from ext_lz4;

-- 2.5 Zstd compression
drop table if exists ext_zstd;
create external table ext_zstd (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
) infile{"filepath"='$resources/parquet/test_zstd.parquet', "format"='parquet'};
select 'ZSTD' as codec, count(*), sum(value) from ext_zstd;

-- 2.6 Brotli compression
drop table if exists ext_brotli;
create external table ext_brotli (
    id BIGINT,
    name VARCHAR(100),
    value DOUBLE,
    status BOOL
) infile{"filepath"='$resources/parquet/test_brotli.parquet', "format"='parquet'};
select 'BROTLI' as codec, count(*), sum(value) from ext_brotli;

-- ============================================================================
-- 3. Parquet Version Compatibility
-- ============================================================================

-- 3.1 Parquet v1 (legacy)
drop table if exists ext_v1;
create external table ext_v1 (
    id bigint,
    user_name varchar(50),
    login_time timestamp
) infile{"filepath"='$resources/parquet/test_v1_legacy.parquet', "format"='parquet'};
select * from ext_v1;

-- 3.2 Parquet v2 (modern)
drop table if exists ext_v2;
create external table ext_v2 (
    id bigint,
    user_name varchar(50),
    login_time timestamp
) infile{"filepath"='$resources/parquet/test_v2_modern.parquet', "format"='parquet'};
select * from ext_v2;

-- ============================================================================
-- 4. NULL Handling
-- ============================================================================

drop table if exists ext_nullable;
create external table ext_nullable (
    id BIGINT,
    col_nullable_with_null DOUBLE,
    col_nullable_no_null VARCHAR(10),
    col_not_nullable BIGINT,
    col_all_null DOUBLE
) infile{"filepath"='$resources/parquet/nullable_test.parquet', "format"='parquet'};
select count(*) as total_rows,
       sum(case when col_nullable_with_null is null then 1 else 0 end) as null_count_col1,
       sum(case when col_all_null is null then 1 else 0 end) as null_count_all_null
from ext_nullable;
select * from ext_nullable order by id;

-- ============================================================================
-- 5. Decimal Types
-- ============================================================================

drop table if exists ext_decimal;
create external table ext_decimal (
    dec_p9_s2 DECIMAL(9, 2),
    dec_p18_s9 DECIMAL(18, 9),
    dec_p38_s0 DECIMAL(38, 0),
    dec_p38_s38 DECIMAL(38, 38)
) infile{"filepath"='$resources/parquet/decimal_boundary_test.parquet', "format"='parquet'};
select * from ext_decimal;
select dec_p38_s0 from ext_decimal where dec_p38_s0 > 0;

-- ============================================================================
-- 6. Binary Types
-- ============================================================================

drop table if exists ext_binary;
create external table ext_binary (
    bin_fixed BINARY(10),
    bin_var VARBINARY(255),
    bin_blob BLOB,
    bin_empty VARBINARY(10)
) infile{"filepath"='$resources/parquet/binary_mapping_test.parquet', "format"='parquet'};
select hex(bin_fixed) as fixed_hex, hex(bin_var) as var_hex from ext_binary;
select length(bin_fixed) from ext_binary;

-- ============================================================================
-- 7. Empty File
-- ============================================================================

drop table if exists ext_empty;
create external table ext_empty (
    id bigint,
    name varchar(100)
) infile{"filepath"='$resources/parquet/empty_test.parquet', "format"='parquet'};
select * from ext_empty;

-- ============================================================================
-- 8. Query Features on External Table
-- ============================================================================

-- 8.1 WHERE filter
select * from ext_iris where variety = 'Setosa' limit 3;

-- 8.2 GROUP BY / HAVING
select variety, avg(`sepal.length`) as avg_sepal_len 
from ext_iris 
group by variety 
having avg(`sepal.length`) > 5.5 
order by avg_sepal_len;

-- 8.3 ORDER BY / LIMIT
select * from ext_iris order by `sepal.length` desc limit 5;

-- 8.4 Aggregate functions
select 
    count(*) as cnt,
    sum(`sepal.length`) as sum_val,
    avg(`sepal.length`) as avg_val,
    min(`sepal.length`) as min_val,
    max(`sepal.length`) as max_val
from ext_iris;

-- 8.5 JOIN external table with internal table
drop table if exists variety_info;
create table variety_info (
    variety varchar(20),
    description varchar(100)
);
insert into variety_info values 
    ('Setosa', 'Small flowers'),
    ('Versicolor', 'Medium flowers'),
    ('Virginica', 'Large flowers');
select e.variety, v.description, count(*) as cnt
from ext_iris e
join variety_info v on e.variety = v.variety
group by e.variety, v.description
order by e.variety;

-- 8.6 Subquery
select * from ext_iris 
where `sepal.length` > (select avg(`sepal.length`) from ext_iris)
order by `sepal.length`
limit 5;

-- ============================================================================
-- 9. Column Statistics (with_stats vs no_stats)
-- ============================================================================

drop table if exists ext_with_stats;
create external table ext_with_stats (
    id BIGINT,
    score DOUBLE
) infile{"filepath"='$resources/parquet/with_stats.parquet', "format"='parquet'};
select 'with_stats' as source, count(*), min(id), max(score) from ext_with_stats;

drop table if exists ext_no_stats;
create external table ext_no_stats (
    id BIGINT,
    score DOUBLE
) infile{"filepath"='$resources/parquet/no_stats.parquet', "format"='parquet'};
select 'no_stats' as source, count(*), min(id), max(score) from ext_no_stats;

-- ============================================================================
-- 10. Wide Table (100 columns)
-- ============================================================================

drop table if exists ext_wide;
create external table ext_wide (
    col_0 BIGINT, col_1 DOUBLE, col_2 BIGINT, col_3 DOUBLE, col_4 BIGINT,
    col_5 DOUBLE, col_6 BIGINT, col_7 DOUBLE, col_8 BIGINT, col_9 DOUBLE,
    col_10 BIGINT, col_11 DOUBLE, col_12 BIGINT, col_13 DOUBLE, col_14 BIGINT,
    col_15 DOUBLE, col_16 BIGINT, col_17 DOUBLE, col_18 BIGINT, col_19 DOUBLE,
    col_20 BIGINT, col_21 DOUBLE, col_22 BIGINT, col_23 DOUBLE, col_24 BIGINT,
    col_25 DOUBLE, col_26 BIGINT, col_27 DOUBLE, col_28 BIGINT, col_29 DOUBLE,
    col_30 BIGINT, col_31 DOUBLE, col_32 BIGINT, col_33 DOUBLE, col_34 BIGINT,
    col_35 DOUBLE, col_36 BIGINT, col_37 DOUBLE, col_38 BIGINT, col_39 DOUBLE,
    col_40 BIGINT, col_41 DOUBLE, col_42 BIGINT, col_43 DOUBLE, col_44 BIGINT,
    col_45 DOUBLE, col_46 BIGINT, col_47 DOUBLE, col_48 BIGINT, col_49 DOUBLE,
    col_50 BIGINT, col_51 DOUBLE, col_52 BIGINT, col_53 DOUBLE, col_54 BIGINT,
    col_55 DOUBLE, col_56 BIGINT, col_57 DOUBLE, col_58 BIGINT, col_59 DOUBLE,
    col_60 BIGINT, col_61 DOUBLE, col_62 BIGINT, col_63 DOUBLE, col_64 BIGINT,
    col_65 DOUBLE, col_66 BIGINT, col_67 DOUBLE, col_68 BIGINT, col_69 DOUBLE,
    col_70 BIGINT, col_71 DOUBLE, col_72 BIGINT, col_73 DOUBLE, col_74 BIGINT,
    col_75 DOUBLE, col_76 BIGINT, col_77 DOUBLE, col_78 BIGINT, col_79 DOUBLE,
    col_80 BIGINT, col_81 DOUBLE, col_82 BIGINT, col_83 DOUBLE, col_84 BIGINT,
    col_85 DOUBLE, col_86 BIGINT, col_87 DOUBLE, col_88 BIGINT, col_89 DOUBLE,
    col_90 BIGINT, col_91 DOUBLE, col_92 BIGINT, col_93 DOUBLE, col_94 BIGINT,
    col_95 DOUBLE, col_96 BIGINT, col_97 DOUBLE, col_98 BIGINT, col_99 DOUBLE
) infile{"filepath"='$resources/parquet/wide_table_100.parquet', "format"='parquet'};
select count(*) from ext_wide;
select col_0, col_50, col_99 from ext_wide limit 3;

-- ============================================================================
-- 11. Wildcard Pattern (Multiple Files with Same Schema)
-- ============================================================================

-- Compression test files (test_none, test_snappy, test_gzip, test_lz4, test_zstd, test_brotli)
-- all have the same schema (id, name, value, status), but test_v1_legacy and test_v2_modern
-- have different schema (id, user_name, login_time), so we need a more specific pattern

-- Test wildcard with files that have identical schema
drop table if exists ext_wildcard_v;
create external table ext_wildcard_v (
    id bigint,
    user_name varchar(50),
    login_time timestamp
) infile{"filepath"='$resources/parquet/test_v*.parquet', "format"='parquet'};
select count(*) as total_rows from ext_wildcard_v;
select * from ext_wildcard_v order by id;

-- ============================================================================
-- 12. Error Cases
-- ============================================================================

-- 12.1 External table does not support INSERT
drop table if exists ext_readonly;
create external table ext_readonly (
    id BIGINT,
    name VARCHAR(100)
) infile{"filepath"='$resources/parquet/test_none.parquet', "format"='parquet'};
insert into ext_readonly values (100, 'test');

-- 12.2 External table does not support UPDATE
update ext_readonly set name = 'updated' where id = 1;

-- 12.3 External table does not support DELETE
delete from ext_readonly where id = 1;

-- ============================================================================
-- Cleanup
-- ============================================================================

drop table if exists ext_supported_types;
drop table if exists ext_iris;
drop table if exists ext_none;
drop table if exists ext_snappy;
drop table if exists ext_gzip;
drop table if exists ext_lz4;
drop table if exists ext_zstd;
drop table if exists ext_brotli;
drop table if exists ext_v1;
drop table if exists ext_v2;
drop table if exists ext_nullable;
drop table if exists ext_decimal;
drop table if exists ext_binary;
drop table if exists ext_empty;
drop table if exists variety_info;
drop table if exists ext_with_stats;
drop table if exists ext_no_stats;
drop table if exists ext_wide;
drop table if exists ext_wildcard_v;
drop table if exists ext_readonly;
drop database ext_parquet_db;
