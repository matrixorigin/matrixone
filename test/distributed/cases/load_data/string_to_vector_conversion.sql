-- Test for STRING to VECF32/VECF64 conversion in Parquet loading.
-- Issue #24914: STRING(optional) should load into nullable vector columns.

drop database if exists test_string_to_vector;
create database test_string_to_vector;
use test_string_to_vector;

create table test_string_to_vector_optional(
    id int not null,
    c_vecf32 vecf32(3),
    c_vecf64 vecf64(3)
);

load data infile {'filepath'='$resources/load_data/string_to_vector_optional.parq', 'format'='parquet'}
into table test_string_to_vector_optional parallel 'true';

select id, c_vecf32, c_vecf64 from test_string_to_vector_optional order by id;

drop database test_string_to_vector;
