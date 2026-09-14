drop database if exists parquet_dictionary;
create database parquet_dictionary;
use parquet_dictionary;

create table dictionary_types (
    int_col INT,
    float_col FLOAT,
    bool_col BOOL,
    nullable_bool_col BOOL
);
load data infile {'filepath'='$resources/parquet/parquet_dictionary_types.parquet', 'format'='parquet'} into table dictionary_types;
-- Mixed scalar dictionary pages, including nullable BOOLEAN.
select int_col, float_col, bool_col, nullable_bool_col
from dictionary_types order by int_col, nullable_bool_col;
select count(*), min(int_col), max(int_col), min(float_col), max(float_col)
from dictionary_types;
drop table dictionary_types;

-- Required BOOLEAN accepts a required dictionary page.
create table dictionary_bool_required (bool_col BOOL NOT NULL);
load data infile {'filepath'='$resources/parquet/parquet_dictionary_bool_required.parquet', 'format'='parquet'} into table dictionary_bool_required;
select bool_col, count(*) from dictionary_bool_required group by bool_col order by bool_col;
drop table dictionary_bool_required;

-- A NULL from a nullable dictionary page must be rejected atomically.
create table dictionary_bool_not_null_guard (nullable_bool_col BOOL NOT NULL);
load data infile {'filepath'='$resources/parquet/parquet_dictionary_bool_nullable.parquet', 'format'='parquet'} into table dictionary_bool_not_null_guard;
select count(*) from dictionary_bool_not_null_guard;
drop table dictionary_bool_not_null_guard;

-- Nullable BOOLEAN preserves NULL, false, and true values.
create table dictionary_bool_nullable (nullable_bool_col BOOL);
load data infile {'filepath'='$resources/parquet/parquet_dictionary_bool_nullable.parquet', 'format'='parquet'} into table dictionary_bool_nullable;
select nullable_bool_col, count(*) from dictionary_bool_nullable group by nullable_bool_col order by nullable_bool_col;
drop table dictionary_bool_nullable;

drop database parquet_dictionary;
