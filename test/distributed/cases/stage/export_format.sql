-- Test export FORMAT and SPLITSIZE syntax for SELECT INTO OUTFILE
-- This test covers the new features: FORMAT 'csv|jsonline|parquet' and SPLITSIZE

-- ============================================================================
-- PART 1: Setup - Create test tables with various data types
-- ============================================================================

-- Table 1: Basic types
drop table if exists export_format_test;
CREATE TABLE export_format_test (
    id INT NOT NULL,
    name VARCHAR(50),
    age INT,
    salary DECIMAL(10,2),
    is_active BOOL,
    created_at DATETIME,
    PRIMARY KEY (id)
);

insert into export_format_test values
(1, 'Alice', 30, 50000.00, true, '2023-01-15 10:30:00'),
(2, 'Bob', 25, 45000.50, true, '2023-02-20 14:45:00'),
(3, 'Charlie', 35, 60000.75, false, '2023-03-10 09:15:00'),
(4, 'Diana', 28, 55000.25, true, '2023-04-05 16:00:00'),
(5, 'Eve', 32, 52000.00, false, '2023-05-12 11:30:00');

-- Table 2: Extended types for comprehensive testing
drop table if exists export_types_test;
CREATE TABLE export_types_test (
    id INT NOT NULL,
    tiny_val TINYINT,
    small_val SMALLINT,
    big_val BIGINT,
    float_val FLOAT,
    double_val DOUBLE,
    date_val DATE,
    time_val TIME,
    timestamp_val TIMESTAMP,
    text_val TEXT,
    json_val JSON,
    uuid_val UUID,
    PRIMARY KEY (id)
);

insert into export_types_test values
(1, 127, 32767, 9223372036854775807, 3.14, 3.141592653589793, '2023-01-15', '10:30:00', '2023-01-15 10:30:00', 'Hello World', '{"key": "value"}', '550e8400-e29b-41d4-a716-446655440000'),
(2, -128, -32768, -9223372036854775808, -3.14, -3.141592653589793, '2023-12-31', '23:59:59', '2023-12-31 23:59:59', 'Test Text', '{"array": [1,2,3]}', '6ba7b810-9dad-11d1-80b4-00c04fd430c8');

-- Table 3: NULL values testing
drop table if exists export_null_test;
CREATE TABLE export_null_test (
    id INT NOT NULL,
    nullable_int INT,
    nullable_str VARCHAR(50),
    nullable_decimal DECIMAL(10,2),
    PRIMARY KEY (id)
);

insert into export_null_test values
(1, 100, 'has value', 99.99),
(2, NULL, NULL, NULL),
(3, 200, 'another value', NULL),
(4, NULL, 'only string', 50.00);

-- Table 4: Special characters testing
drop table if exists export_special_chars;
CREATE TABLE export_special_chars (
    id INT NOT NULL,
    content VARCHAR(200),
    PRIMARY KEY (id)
);

insert into export_special_chars values
(1, 'normal text'),
(2, 'text with "quotes"'),
(3, 'text with ''single quotes'''),
(4, 'text,with,commas'),
(5, 'text	with	tabs'),
(6, 'mixed "quotes", commas');

-- Table 5: Empty table for edge case
drop table if exists export_empty_test;
CREATE TABLE export_empty_test (
    id INT NOT NULL,
    name VARCHAR(50),
    PRIMARY KEY (id)
);

-- Create local stage for testing
drop stage if exists export_test_stage;
create stage export_test_stage URL= 'file:///$resources/into_outfile/stage';

-- ============================================================================
-- PART 2: Normal/Valid Cases - FORMAT keyword
-- ============================================================================

-- Test 2.1: Default format (CSV without FORMAT keyword)
select * from export_format_test into outfile 'stage://export_test_stage/test_default.csv';

-- Test 2.2: Explicit FORMAT 'csv' (lowercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_lower.csv' format 'csv';

-- Test 2.3: FORMAT 'CSV' (uppercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_upper.csv' format 'CSV';

-- Test 2.4: FORMAT 'Csv' (mixed case)
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_mixed.csv' format 'Csv';

-- Test 2.5: FORMAT 'jsonline' (lowercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_jsonline_lower.jsonl' format 'jsonline';

-- Test 2.6: FORMAT 'JSONLINE' (uppercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_jsonline_upper.jsonl' format 'JSONLINE';

-- Test 2.7: FORMAT 'JsonLine' (mixed case)
select * from export_format_test into outfile 'stage://export_test_stage/test_jsonline_mixed.jsonl' format 'JsonLine';

-- Test 2.8: FORMAT 'parquet' (lowercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_parquet_lower.parquet' format 'parquet';

-- Test 2.9: FORMAT 'PARQUET' (uppercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_parquet_upper.parquet' format 'PARQUET';

-- Test 2.10: FORMAT 'Parquet' (mixed case)
select * from export_format_test into outfile 'stage://export_test_stage/test_parquet_mixed.parquet' format 'Parquet';

-- Test 2.11: Infer format from .jsonl suffix (no FORMAT keyword)
select * from export_format_test into outfile 'stage://export_test_stage/test_infer_jsonl.jsonl';

-- Test 2.12: Infer format from .jsonline suffix (no FORMAT keyword)
select * from export_format_test into outfile 'stage://export_test_stage/test_infer_jsonline.jsonline';

-- Test 2.13: Infer format from .ndjson suffix (no FORMAT keyword)
select * from export_format_test into outfile 'stage://export_test_stage/test_infer_ndjson.ndjson';

-- Test 2.14: Infer format from .parquet suffix (no FORMAT keyword)
select * from export_format_test into outfile 'stage://export_test_stage/test_infer_parquet.parquet';

-- ============================================================================
-- PART 3: Normal/Valid Cases - SPLITSIZE keyword
-- ============================================================================

-- Test 3.1: SPLITSIZE with bytes (no unit)
select * from export_format_test into outfile 'stage://export_test_stage/test_split_bytes_%d.csv' format 'csv' splitsize '1048576';

-- Test 3.2: SPLITSIZE with K unit (lowercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_split_k_lower_%d.csv' format 'csv' splitsize '100k';

-- Test 3.3: SPLITSIZE with K unit (uppercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_split_k_upper_%d.csv' format 'csv' splitsize '100K';

-- Test 3.4: SPLITSIZE with M unit (lowercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_split_m_lower_%d.csv' format 'csv' splitsize '1m';

-- Test 3.5: SPLITSIZE with M unit (uppercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_split_m_upper_%d.csv' format 'csv' splitsize '1M';

-- Test 3.6: SPLITSIZE with G unit (lowercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_split_g_lower_%d.csv' format 'csv' splitsize '1g';

-- Test 3.7: SPLITSIZE with G unit (uppercase)
select * from export_format_test into outfile 'stage://export_test_stage/test_split_g_upper_%d.csv' format 'csv' splitsize '1G';

-- Test 3.8: SPLITSIZE with %d format specifier
select * from export_format_test into outfile 'stage://export_test_stage/test_split_d_%d.csv' format 'csv' splitsize '1M';

-- Test 3.9: SPLITSIZE with %05d format specifier (zero-padded)
select * from export_format_test into outfile 'stage://export_test_stage/test_split_05d_%05d.csv' format 'csv' splitsize '1M';

-- Test 3.10: SPLITSIZE with jsonline format
select * from export_format_test into outfile 'stage://export_test_stage/test_split_jsonline_%d.jsonl' format 'jsonline' splitsize '1M';

-- ============================================================================
-- PART 4: Normal/Valid Cases - CSV Options
-- ============================================================================

-- Test 4.1: CSV with custom field terminator
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_tab.csv' format 'csv' fields terminated by '\t';

-- Test 4.2: CSV with custom enclosed by
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_enclosed.csv' format 'csv' fields enclosed by '"';

-- Test 4.3: CSV with custom line terminator
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_lines.csv' format 'csv' lines terminated by '\r\n';

-- Test 4.4: CSV with header false
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_no_header.csv' format 'csv' header 'false';

-- Test 4.5: CSV with header true
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_with_header.csv' format 'csv' header 'true';

-- Test 4.6: CSV with all options combined
select * from export_format_test into outfile 'stage://export_test_stage/test_csv_all_opts.csv' format 'csv' fields terminated by '|' enclosed by '\'' lines terminated by '\n' header 'true';

-- ============================================================================
-- PART 5: Normal/Valid Cases - Data Types Export
-- ============================================================================

-- Test 5.1: Export extended types to CSV
select * from export_types_test into outfile 'stage://export_test_stage/test_types_csv.csv' format 'csv';

-- Test 5.2: Export extended types to JSONLINE
select * from export_types_test into outfile 'stage://export_test_stage/test_types_jsonline.jsonl' format 'jsonline';

-- Test 5.3: Export extended types to Parquet
select * from export_types_test into outfile 'stage://export_test_stage/test_types_parquet.parquet' format 'parquet';

-- Test 5.4: Export NULL values to CSV
select * from export_null_test into outfile 'stage://export_test_stage/test_null_csv.csv' format 'csv';

-- Test 5.5: Export NULL values to JSONLINE
select * from export_null_test into outfile 'stage://export_test_stage/test_null_jsonline.jsonl' format 'jsonline';

-- Test 5.6: Export NULL values to Parquet
select * from export_null_test into outfile 'stage://export_test_stage/test_null_parquet.parquet' format 'parquet';

-- Test 5.7: Export special characters to CSV
select * from export_special_chars into outfile 'stage://export_test_stage/test_special_csv.csv' format 'csv' fields enclosed by '"';

-- Test 5.8: Export special characters to JSONLINE
select * from export_special_chars into outfile 'stage://export_test_stage/test_special_jsonline.jsonl' format 'jsonline';

-- Test 5.9: Export empty table to CSV
select * from export_empty_test into outfile 'stage://export_test_stage/test_empty_csv.csv' format 'csv';

-- Test 5.10: Export empty table to JSONLINE
select * from export_empty_test into outfile 'stage://export_test_stage/test_empty_jsonline.jsonl' format 'jsonline';

-- Test 5.11: Export empty table to Parquet
select * from export_empty_test into outfile 'stage://export_test_stage/test_empty_parquet.parquet' format 'parquet';

-- ============================================================================
-- PART 6: Normal/Valid Cases - SELECT variations
-- ============================================================================

-- Test 6.1: Export with WHERE clause
select * from export_format_test where age > 30 into outfile 'stage://export_test_stage/test_where.csv' format 'csv';

-- Test 6.2: Export with ORDER BY
select * from export_format_test order by salary desc into outfile 'stage://export_test_stage/test_orderby.csv' format 'csv';

-- Test 6.3: Export with LIMIT
select * from export_format_test limit 3 into outfile 'stage://export_test_stage/test_limit.csv' format 'csv';

-- Test 6.4: Export specific columns
select id, name, salary from export_format_test into outfile 'stage://export_test_stage/test_columns.csv' format 'csv';

-- Test 6.5: Export with column alias
select id as employee_id, name as employee_name from export_format_test into outfile 'stage://export_test_stage/test_alias.csv' format 'csv';

-- Test 6.6: Export with expression
select id, name, salary * 12 as annual_salary from export_format_test into outfile 'stage://export_test_stage/test_expr.csv' format 'csv';

-- ============================================================================
-- PART 7: Verify exported data can be loaded back
-- ============================================================================

-- Test 7.1: Verify CSV export
drop table if exists export_verify;
CREATE TABLE export_verify (
    id INT NOT NULL,
    name VARCHAR(50),
    age INT,
    salary DECIMAL(10,2),
    is_active BOOL,
    created_at DATETIME,
    PRIMARY KEY (id)
);
load data infile 'stage://export_test_stage/test_default.csv' into table export_verify fields terminated by ',' ignore 1 lines;
select * from export_verify order by id;

-- Test 7.2: Verify CSV with custom delimiter
truncate table export_verify;
load data infile 'stage://export_test_stage/test_csv_tab.csv' into table export_verify fields terminated by '\t' ignore 1 lines;
select * from export_verify order by id;

-- Test 7.3: Verify CSV without header
truncate table export_verify;
load data infile 'stage://export_test_stage/test_csv_no_header.csv' into table export_verify fields terminated by ',';
select * from export_verify order by id;

-- ============================================================================
-- PART 8: Abnormal/Invalid Cases - FORMAT errors
-- ============================================================================

-- Test 8.1: Invalid format 'xml'
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_xml.txt' format 'xml';
-- @bvt:issue

-- Test 8.2: Invalid format 'json' (should be 'jsonline')
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_json.txt' format 'json';
-- @bvt:issue

-- Test 8.3: Invalid format 'txt'
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_txt.txt' format 'txt';
-- @bvt:issue

-- Test 8.4: Invalid format empty string
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_empty.txt' format '';
-- @bvt:issue

-- Test 8.5: Invalid format with number
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_num.txt' format '123';
-- @bvt:issue

-- Test 8.6: FORMAT 'jsonline' does not match file suffix '.csv'
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_mismatch1.csv' format 'jsonline';
-- @bvt:issue

-- Test 8.7: FORMAT 'parquet' does not match file suffix '.csv'
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_mismatch2.csv' format 'parquet';
-- @bvt:issue

-- Test 8.8: FORMAT 'csv' does not match file suffix '.jsonl'
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_mismatch3.jsonl' format 'csv';
-- @bvt:issue

-- Test 8.9: FORMAT 'csv' does not match file suffix '.parquet'
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_mismatch4.parquet' format 'csv';
-- @bvt:issue

-- Test 8.10: FORMAT 'jsonline' does not match file suffix '.parquet'
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_mismatch5.parquet' format 'jsonline';
-- @bvt:issue

-- ============================================================================
-- PART 9: Abnormal/Invalid Cases - SPLITSIZE errors
-- ============================================================================

-- Test 9.1: Invalid splitsize unit 'X'
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_unit_%d.csv' format 'csv' splitsize '100X';
-- @bvt:issue

-- Test 9.2: Invalid splitsize negative value
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_neg_%d.csv' format 'csv' splitsize '-100M';
-- @bvt:issue

-- Test 9.3: Invalid splitsize empty string
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_empty_%d.csv' format 'csv' splitsize '';
-- @bvt:issue

-- Test 9.4: Invalid splitsize non-numeric
-- @bvt:issue#23270
select * from export_format_test into outfile 'stage://export_test_stage/test_invalid_nan_%d.csv' format 'csv' splitsize 'abc';
-- @bvt:issue

-- ============================================================================
-- PART 10: Edge Cases
-- ============================================================================

-- Test 10.1: Very long filename
select * from export_format_test into outfile 'stage://export_test_stage/test_very_long_filename_that_is_quite_long_indeed_for_testing_purposes.csv' format 'csv';

-- Test 10.2: Export single row
select * from export_format_test where id = 1 into outfile 'stage://export_test_stage/test_single_row.csv' format 'csv';

-- Test 10.3: Export single column
select name from export_format_test into outfile 'stage://export_test_stage/test_single_col.csv' format 'csv';

-- Test 10.4: Export with all NULL row
select nullable_int, nullable_str, nullable_decimal from export_null_test where id = 2 into outfile 'stage://export_test_stage/test_all_null_row.csv' format 'csv';

-- ============================================================================
-- PART 11: Cleanup
-- ============================================================================
drop table if exists export_format_test;
drop table if exists export_types_test;
drop table if exists export_null_test;
drop table if exists export_special_chars;
drop table if exists export_empty_test;
drop table if exists export_verify;
drop stage if exists export_test_stage;
