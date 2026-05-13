-- Hive-style Partitioned External Table BVT Tests
--
-- Coverage overview:
--   1. DDL validation (success + negative)
--   2. Single-level partition queries (full / EQ / IN / NOT IN / range / IS NULL)
--   3. Multi-level partition queries (full / single / double / cross-level)
--   4. String partition (EQ / IN / LIKE / ORDER BY partition col)
--   5. NULL partition (__HIVE_DEFAULT_PARTITION__)
--   6. Zero-padded integer partition
--   7. __mo_filepath virtual column (hive + non-hive)
--   8. EXPLAIN (CHECK + ANALYZE)
--   9. Complex predicates (OR / BETWEEN / CAST / arithmetic)
--  10. Aggregations & subqueries
--  11. JOIN hive × hive, hive × internal
--  12. UNION ALL / DISTINCT / HAVING
--  13. Edge cases (type failure, URL-encoded, physical column overlap, stage rejection)

drop database if exists hive_part_db;
create database hive_part_db;
use hive_part_db;

-- ============================================================================
-- 1. DDL Validation
-- ============================================================================

-- 1.1 Basic creation (single level)
drop table if exists hive_single;
create external table hive_single (
    id int,
    amount double,
    year int
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year'};

-- 1.2 DDL error: missing partition_columns
drop table if exists hive_err1;
create external table hive_err1 (
    id int, year int
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true'};

-- 1.3 DDL error: format not parquet
drop table if exists hive_err2;
create external table hive_err2 (
    id int, year int
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='csv', 'hive_partitioning'='true', 'hive_partition_columns'='year'};

-- 1.4 DDL error: column not found
drop table if exists hive_err3;
create external table hive_err3 (
    id int, amount double
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='nonexistent'};

-- 1.5 DDL error: duplicate hive key
drop table if exists hive_err4;
create external table hive_err4 (
    id int, year int
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partitioning'='false', 'hive_partition_columns'='year'};

-- 1.6 DDL error: hive_partitioning value not boolean
drop table if exists hive_err5;
create external table hive_err5 (
    id int, year int
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='yes', 'hive_partition_columns'='year'};

-- 1.7 Partition column name matched case-insensitively (succeeds, no error)
-- Both the declared column `YEAR` and the partition reference `YeaR` lowercase
-- to `year`, so findColInTableDefCaseInsensitive finds the column. Same flow
-- as test 10.7 (hive_mixed_case), kept here for DDL-validation coverage.
drop table if exists hive_err6;
create external table hive_err6 (
    id int, YEAR int
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='YeaR'};

-- 1.8 DDL error: duplicate partition column names
drop table if exists hive_err7;
create external table hive_err7 (
    id int, year int, amount double
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year,year'};

-- 1.9 DDL error: VECTOR partition column rejected
drop table if exists hive_err8;
create external table hive_err8 (
    id int,
    emb vecf32(3)
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='emb'};

-- 1.10 SHOW CREATE TABLE
show create table hive_single;

-- 1.11 LOAD DATA into hive table should be rejected (external table generic rejection)
load data infile '$resources/hive_partition/single_level/year=2024/data.parquet' into table hive_single;

-- 1.12 hive_partitioning='false' treated as disabled (existing external table path)
drop table if exists hive_disabled;
create external table hive_disabled (
    id int,
    amount double
) infile{'filepath'='$resources/hive_partition/non_hive/simple.parquet', 'format'='parquet', 'hive_partitioning'='false'};
select count(*) from hive_disabled;

-- ============================================================================
-- 2. Single Level Partition Queries
-- ============================================================================

-- 2.1 Full scan (all 5 partitions x 5 rows = 25 rows)
select count(*) as cnt from hive_single;

-- 2.2 EQ pruning
select year, count(*) as cnt from hive_single where year = 2024 group by year;

-- 2.3 IN pruning
select year, count(*) as cnt from hive_single where year in (2020, 2024) group by year order by year;

-- 2.4 IN pruning with single value
select year, count(*) as cnt from hive_single where year in (2022) group by year;

-- 2.5 NOT IN (rowFilter fallback)
select year, count(*) as cnt from hive_single where year not in (2020, 2021, 2022) group by year order by year;

-- 2.6 Non-prunable GT (rowFilter fallback, must not lose data)
select count(*) as cnt from hive_single where year > 2022;

-- 2.7 BETWEEN (rowFilter fallback)
select year, count(*) as cnt from hive_single where year between 2021 and 2023 group by year order by year;

-- 2.8 OR condition (rowFilter fallback; not prunable in P0)
select year, count(*) as cnt from hive_single where year = 2020 or year = 2024 group by year order by year;

-- 2.9 Partition column only in SELECT
select distinct year from hive_single order by year;

-- 2.10 Partition column only in WHERE
select sum(amount) as total from hive_single where year = 2020;

-- 2.11 Partition col in HAVING (threshold < 105 to include all partitions)
select year, sum(amount) as total from hive_single group by year having sum(amount) >= 100 order by year;

-- 2.12 COUNT DISTINCT on partition column
select count(distinct year) as distinct_years from hive_single;

-- 2.13 Partition column in arithmetic expression (rowFilter evaluates)
select count(*) from hive_single where year + 1 = 2025;

-- 2.14 CAST on partition column (rowFilter only, not pruned)
select count(*) from hive_single where cast(year as varchar) = '2024';

-- 2.15 ORDER BY partition column with LIMIT
select id, year from hive_single order by year asc, id desc limit 5;

-- 2.16 Partition column IS NOT NULL (trivially true for non-null data)
select count(*) as cnt from hive_single where year is not null;

-- 2.17 Partition column used both as predicate and projection
select year, id from hive_single where year = 2023 order by id;

-- 2.18 Subquery with partition pruning
select year, cnt from (
    select year, count(*) as cnt from hive_single where year in (2020, 2021) group by year
) t order by year;

-- ============================================================================
-- 3. Multi Level Partition Queries
-- ============================================================================

drop table if exists hive_multi;
create external table hive_multi (
    id int,
    amount double,
    year int,
    month varchar(2)
) infile{'filepath'='$resources/hive_partition/multi_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year,month'};

-- 3.1 Full scan (2 years x 3 months x 3 rows = 18)
select count(*) as cnt from hive_multi;

-- 3.2 Single level pruning (outer)
select year, count(*) as cnt from hive_multi where year = 2024 group by year;

-- 3.3 Single level pruning (inner only)
select month, count(*) as cnt from hive_multi where month = '01' group by month order by month;

-- 3.4 Double level pruning
select year, month, count(*) as cnt from hive_multi where year = 2024 and month = '01' group by year, month;

-- 3.5 Outer IN + inner IN
select year, month, count(*) as cnt from hive_multi
where year in (2024, 2025) and month in ('01', '02')
group by year, month order by year, month;

-- 3.6 GROUP BY partition columns
select year, month, sum(amount) as total from hive_multi group by year, month order by year, month;

-- 3.7 ORDER BY mixed partition + physical
select id, year, month, amount from hive_multi where year = 2025 order by month asc, id asc limit 6;

-- 3.8 HAVING with partition columns
select year, count(*) as cnt from hive_multi group by year having count(*) >= 9 order by year;

-- 3.9 Two-column distinct
select distinct year, month from hive_multi order by year, month;

-- 3.10 Self join on partition columns
select a.year, a.month, count(*) as cnt
from hive_multi a join hive_multi b
on a.year = b.year and a.month = b.month and a.id = b.id
where a.year = 2024
group by a.year, a.month order by a.year, a.month;

-- 3.11 Group-by with subquery to count month per year
select year, mc from (
    select year, count(distinct month) as mc from hive_multi group by year
) t order by year;

-- ============================================================================
-- 4. String Partition
-- ============================================================================

drop table if exists hive_string;
create external table hive_string (
    id int,
    amount double,
    country varchar(10)
) infile{'filepath'='$resources/hive_partition/string_part/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='country'};

-- 4.1 String EQ (exact-byte match → prunable)
select country, count(*) as cnt from hive_string where country = 'US' group by country;

-- 4.2 String IN
select country, count(*) as cnt from hive_string where country in ('US', 'CN') group by country order by country;

-- 4.3 String LIKE (rowFilter only, not prunable)
select country, count(*) as cnt from hive_string where country like 'U%' group by country;

-- 4.4 All countries
select country, count(*) as cnt from hive_string group by country order by country;

-- 4.5 String partition != condition (rowFilter fallback)
select country, count(*) as cnt from hive_string where country != 'JP' group by country order by country;

-- 4.6 String partition in ORDER BY (partition value in output)
select id, country from hive_string order by country, id limit 6;

-- 4.7 Partition col length function
select country, length(country) as ln from hive_string group by country order by country;

-- ============================================================================
-- 5. NULL Partition (__HIVE_DEFAULT_PARTITION__)
-- ============================================================================

drop table if exists hive_null;
create external table hive_null (
    id int,
    amount double,
    year int
) infile{'filepath'='$resources/hive_partition/null_part/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year'};

-- 5.1 NULL partition is visible
select id, year from hive_null order by id;

-- 5.2 IS NULL filter
select count(*) as cnt from hive_null where year is null;

-- 5.3 IS NOT NULL filter
select count(*) as cnt from hive_null where year is not null;

-- 5.4 Aggregation handling NULL groups
select year, count(*) as cnt from hive_null group by year order by year;

-- 5.5 Coalesce partition column
select coalesce(year, -1) as y, count(*) as cnt from hive_null group by y order by y;

-- ============================================================================
-- 6. Zero-padded Integer Partition
-- ============================================================================

drop table if exists hive_zeropad;
create external table hive_zeropad (
    id int,
    amount double,
    month int
) infile{'filepath'='$resources/hive_partition/zero_pad/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='month'};

-- 6.1 Integer comparison with zero-padded directory (month=01 matches WHERE month = 1)
select month, count(*) as cnt from hive_zeropad where month = 1 group by month;

-- 6.2 Integer IN with mixed zero-padded targets
select month, count(*) as cnt from hive_zeropad where month in (1, 12) group by month order by month;

-- 6.3 All months
select month, count(*) as cnt from hive_zeropad group by month order by month;

-- 6.4 Non-matching value prunes all partitions
select count(*) as cnt from hive_zeropad where month = 99;

-- ============================================================================
-- 7. __mo_filepath Virtual Column
-- ============================================================================

-- 7.1 __mo_filepath on hive table (verify path contains partition directory)
select count(*) as cnt from hive_single where year = 2024 and __mo_filepath like '%year=2024%';

-- 7.2 __mo_filepath returns distinct paths per partition (projection, exercises
-- parquet prepare() filepathColIndex branch + fillVirtualColumns, not FilterFileList)
select count(distinct __mo_filepath) as paths from hive_single;

-- 7.3 __mo_filepath projection + partition column aggregation (distinct value per partition)
select year, count(distinct __mo_filepath) as files from hive_single group by year order by year;

-- 7.4 __mo_filepath as ONLY projected column (rowCountOnly path — no physical col read)
-- count(length(...)>0) confirms SetConstBytes fill produced non-empty bytes for every row.
select count(*) as rows_with_path from hive_single where length(__mo_filepath) > 0;

-- 7.5 Non-hive parquet external table — projection-level assertion for pre-existing bug fix
-- The row count where __mo_filepath is non-empty must equal the file row count (3).
drop table if exists parquet_non_hive;
create external table parquet_non_hive (
    id int,
    amount double
) infile{'filepath'='$resources/hive_partition/non_hive/simple.parquet', 'format'='parquet'};
select count(*) as cnt from parquet_non_hive where __mo_filepath like '%simple.parquet';
select count(distinct __mo_filepath) as paths from parquet_non_hive;
select count(*) as rows_with_path from parquet_non_hive where length(__mo_filepath) > 0;

-- 7.6 Combined partition col + __mo_filepath filter (both conditions prune/filter)
select count(*) as cnt from hive_single where year = 2024 and __mo_filepath like '%year=2024%';

-- 7.7 Contradictory partition + filepath (empty result, but evaluates correctly)
select count(*) as cnt from hive_single where year = 2024 and __mo_filepath like '%year=2020%';

-- ============================================================================
-- 8. EXPLAIN Verification
-- ============================================================================

-- 8.1 EXPLAIN shows External Scan with Filter Cond
explain (check '["External Scan", "Filter Cond"]') select * from hive_single where year = 2024;

-- 8.2 EXPLAIN ANALYZE has runtime stats
-- @regex("inputRows=",true)
explain (analyze true, check '["External Scan", "inputRows=", "outputRows="]') select * from hive_single where year = 2024;

-- 8.3 EXPLAIN multi-level partition scan shows both filter conditions retained (double-filter safety)
explain (check '["External Scan", "Filter Cond"]') select * from hive_multi where year = 2024 and month = '01';

-- 8.4 EXPLAIN with IN list
explain (check '["External Scan", "Filter Cond"]') select * from hive_single where year in (2020, 2024);

-- ============================================================================
-- 9. Complex Query Patterns
-- ============================================================================

-- 9.1 JOIN hive x hive on partition column
select hs.year, count(*) as cnt
from hive_single hs join hive_multi hm on hs.year = hm.year
where hs.year = 2024
group by hs.year;

-- 9.2 JOIN hive x internal (dimension) table
drop table if exists year_dim;
create table year_dim (y int, label varchar(20));
insert into year_dim values (2020, 'y2020'), (2024, 'y2024'), (2025, 'y2025');

select d.label, count(*) as cnt
from hive_single h join year_dim d on h.year = d.y
where h.year in (2020, 2024)
group by d.label order by d.label;

-- 9.3 LEFT JOIN preserves rows without match
select d.y, count(h.id) as cnt
from year_dim d left join hive_single h on h.year = d.y
group by d.y order by d.y;

-- 9.4 UNION ALL merges partitions from two tables
select 'single' as src, count(*) as cnt from hive_single where year = 2024
union all
select 'multi' as src, count(*) as cnt from hive_multi where year = 2024;

-- 9.5 Scalar subquery with partition predicate
select id, year from hive_single
where year = (select max(year) from year_dim where label = 'y2024')
order by id limit 5;

-- 9.6 IN subquery with hive partition column
select year, count(*) as cnt from hive_single
where year in (select y from year_dim where label like 'y2024%')
group by year;

-- 9.7 EXISTS subquery
select count(*) as cnt from hive_single h
where exists (select 1 from year_dim d where d.y = h.year);

-- 9.8 CTE over hive external table
with yearly as (
    select year, sum(amount) as total from hive_single group by year
)
select year, total from yearly where total > 100 order by year;

-- 9.9 Aggregation with conditional sum (above vs below median amount)
select year,
       round(sum(case when amount > 21 then amount else 0 end), 1) as above,
       round(sum(case when amount <= 21 then amount else 0 end), 1) as below
from hive_single
where year in (2020, 2024)
group by year order by year;

-- 9.10 Window function over partition column
-- Note: ROW_NUMBER() OVER (PARTITION BY year ORDER BY id) — demonstrates partition column usable in window spec
select year, id, row_number() over (partition by year order by id) as rn
from hive_single where year in (2020, 2021) order by year, id;

-- 9.11 COUNT with subquery filter
select count(*) as cnt from (
    select id, year from hive_single where year = 2023
    union all
    select id, year from hive_single where year = 2024
) t;

-- ============================================================================
-- 10. Edge Cases
-- ============================================================================

-- 10.1 Type conversion failure: year declared as INT but directory has 'abc'
-- Error contains col + value + relative path (stable across machines).
drop table if exists hive_invalid_type;
create external table hive_invalid_type (
    id int,
    amount double,
    year int
) infile{'filepath'='$resources/hive_partition/invalid_type/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year'};
select * from hive_invalid_type;

-- 10.2 URL-encoded directory name containing '%' should report error (P0 known limitation)
drop table if exists hive_url_encoded;
create external table hive_url_encoded (
    id int,
    amount double,
    country varchar(20)
) infile{'filepath'='$resources/hive_partition/url_encoded/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='country'};
select * from hive_url_encoded;

-- 10.3 Stage hive external table should be rejected at DDL
drop table if exists hive_stage_err;
create external table hive_stage_err (
    id int, year int
) infile{'filepath'='stage://mystage/data/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year'};

-- 10.4 __HIVE_DEFAULT_PARTITION__ with NOT NULL column
-- Error contains col + "NOT NULL" + relative path (stable across machines).
drop table if exists hive_not_null_default;
create external table hive_not_null_default (
    id int,
    amount double,
    year int not null
) infile{'filepath'='$resources/hive_partition/not_null_default/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year'};
select * from hive_not_null_default;

-- 10.5 Physical column overlap: parquet file has physical 'year=9999', path has year=2024
-- Partition value from path (2024) must override the physical column (9999)
drop table if exists hive_col_overlap;
create external table hive_col_overlap (
    id int,
    amount double,
    year int
) infile{'filepath'='$resources/hive_partition/col_overlap/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year'};
select distinct year from hive_col_overlap;
select id, year from hive_col_overlap order by id;

-- 10.6 Non-hive parquet smoke: regular physical column query (not just __mo_filepath)
select id, amount from parquet_non_hive order by id;

-- 10.7 Case-insensitive column name in DDL
drop table if exists hive_mixed_case;
create external table hive_mixed_case (
    id int,
    amount double,
    Year int
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='Year'};
select count(*) as cnt from hive_mixed_case where Year = 2024;
select count(*) as cnt from hive_mixed_case where year = 2024;

-- 10.8 Partition column referenced with table qualifier
select count(*) as cnt from hive_single hs where hs.year = 2024;

-- 10.9 DROP then re-CREATE (catalog round-trip)
drop table if exists hive_single;
create external table hive_single (
    id int,
    amount double,
    year int
) infile{'filepath'='$resources/hive_partition/single_level/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='year'};
select count(*) from hive_single;

-- ============================================================================
-- 11. Cleanup
-- ============================================================================
drop database if exists hive_part_db;
