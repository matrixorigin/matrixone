-- pre
drop database if exists vecdb2;
create database vecdb2;
use vecdb2;
drop table if exists vec_table;
create table vec_table(a int, b vecf32(3), c vecf64(3));

-- read from an already exported file
load data infile '$resources/load_data/array_out.csv' into table vec_table fields terminated by ',' ignore 1 lines;
select * from vec_table;

-- write a new export file
-- NOTE: write to `$resources/into_outfile` only. Else it will fail.
select * from vec_table into outfile '$resources/into_outfile/array_out.csv';
delete from vec_table;

-- read from the newly exported file
load data infile '$resources/into_outfile/array_out.csv' into table vec_table fields terminated by ',' ignore 1 lines;
select * from vec_table;

-- Native JSON arrays retain their element boundaries in both JSONLine record shapes.
truncate table vec_table;
load data infile {'filepath'='$resources/load_data/vector_array_object.jl','format'='jsonline','jsondata'='object'} into table vec_table;
select a, b, c, vector_dims(b), vector_dims(c) from vec_table order by a;
truncate table vec_table;
load data infile {'filepath'='$resources/load_data/vector_array_array.jl','format'='jsonline','jsondata'='array'} into table vec_table parallel 'true';
select a, b, c, vector_dims(b), vector_dims(c) from vec_table order by a;

-- Direct external scans use the same conversion, including projected vector columns.
create external table json_object_vectors(a int, b vecf32(3), c vecf64(3)) infile {'filepath'='$resources/load_data/vector_array_object.jl','format'='jsonline','jsondata'='object'};
select c from json_object_vectors where a = 1;
create external table json_array_vectors(a int, b vecf32(3), c vecf64(3)) infile {'filepath'='$resources/load_data/vector_array_array.jl','format'='jsonline','jsondata'='array'};
select a, b, c from json_array_vectors order by a;

-- post
drop database vecdb2;
