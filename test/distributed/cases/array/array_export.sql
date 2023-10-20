-- pre
drop database if exists vecdb2;
create database vecdb2;
use vecdb2;
drop table if exists vec_table;
create table vec_table(a int, b vecf32(3), c vecf64(3));

-- read old file
load data infile '$resources/array/array_out.csv' into table vec_table ignore 1 lines;
select * from vec_table;

-- write new file
-- NOTE: This does not work, if array_out.csv file already present in the resource folder.
select * from vec_table into outfile '$resources/array/array_out.csv';
delete from vec_table;

-- read new file
load data infile '$resources/array/array_out.csv' into table vec_table ignore 1 lines;
select * from vec_table;

-- post
drop database vecdb2;