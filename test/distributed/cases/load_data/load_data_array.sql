-- pre
drop database if exists vecdb2;
create database vecdb2;
use vecdb2;
drop table if exists vec_table;
create table vec_table(a int, b vecf32(3), c vecf64(3));

-- read from an already exported file
load data infile '$resources/load_data/array_out.csv' into table vec_table ignore 1 lines;
select * from vec_table;

-- write a new export file
-- NOTE: write to `$resources/into_outfile` only. Else it will fail.
select * from vec_table into outfile '$resources/into_outfile/array_out.csv';
delete from vec_table;

-- read from the newly exported file
load data infile '$resources/into_outfile/array_out.csv' into table vec_table ignore 1 lines;
select * from vec_table;

-- post
drop database vecdb2;